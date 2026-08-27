/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.micro.integrator.observability.metric.handler;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.AbstractExtendedSynapseHandler;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.api.API;
import org.apache.synapse.api.version.URLBasedVersionStrategy;
import org.apache.synapse.config.SynapsePropertiesLoader;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.rest.RESTConstants;
import org.apache.synapse.rest.RESTUtils;
import org.apache.synapse.transport.nhttp.NhttpConstants;
import org.wso2.micro.integrator.core.internal.MicroIntegratorBaseConstants;
import org.wso2.micro.integrator.core.services.CarbonServerConfigurationService;
import org.wso2.micro.integrator.observability.util.MetricConstants;
import org.wso2.micro.integrator.observability.util.MetricUtils;

/**
 * Class for extracting metric information by wrapping the implementation and
 * there by allows the implementation to plug into the handler in runtime.
 */
public class MetricHandler extends AbstractExtendedSynapseHandler {

    private static Log log = LogFactory.getLog(MetricHandler.class);
    private static final String DELIMITER = "/";
    private static final String EMPTY = "";

    private MetricReporter metricReporterInstance;
    private int serviceInvokePort;

    private static final String SERVER_PORT_OFFSET = System.getProperty(MetricConstants.PORT_OFFSET);
    private static final String HOST = System.getProperty(MicroIntegratorBaseConstants.LOCAL_IP_ADDRESS);
    private static final String PORT = System.getProperty(MetricConstants.HTTP_PORT);
    private static final String JAVA_VERSION = System.getProperty(MetricConstants.JAVA_VERSION);
    private static final String JAVA_HOME = System.getProperty(MetricConstants.JAVA_HOME);
    private int internalHttpApiPort = getInternalHttpApiPort();


    @Override
    public boolean handleServerInit() {
        metricReporterInstance = MetricUtils.getMetricReporter();
        CarbonServerConfigurationService serverConfig = CarbonServerConfigurationService.getInstance();
        String miVersion = serverConfig.getServerVersion();
        String updateLevel = System.getProperty(MetricConstants.UPDATE_LEVEL);
        metricReporterInstance.initMetrics();
        metricReporterInstance.serverUp(HOST, PORT, JAVA_HOME, JAVA_VERSION);
        metricReporterInstance.serverVersion(miVersion, updateLevel);
        return true;
    }

    @Override
    public boolean handleRequestInFlow(MessageContext synCtx) {
        synCtx.setProperty(RESTConstants.IS_PROMETHEUS_ENGAGED, null);
        org.apache.axis2.context.MessageContext axis2MessageContext = ((Axis2MessageContext) synCtx)
                .getAxis2MessageContext();

        if (null != synCtx.getProperty(SynapseConstants.PROXY_SERVICE)) {
            String proxyName = axis2MessageContext.getAxisService().getName();

            incrementProxyCount(proxyName);
            startTimers(synCtx, proxyName, SynapseConstants.PROXY_SERVICE_TYPE, null);
        } else if (null != synCtx.getProperty(SynapseConstants.IS_INBOUND)) {
            String inboundEndpointName = synCtx.getProperty(SynapseConstants.INBOUND_ENDPOINT_NAME).toString();

            incrementInboundEndPointCount(inboundEndpointName);
            startTimers(synCtx, inboundEndpointName, MetricConstants.INBOUND_ENDPOINT,
                    null);
        } else {
            serviceInvokePort = getServiceInvokePort(synCtx);

            if ((serviceInvokePort != internalHttpApiPort) && (null !=
                    axis2MessageContext.getProperty(MetricConstants.SERVICE_PREFIX))) {
                String url = axis2MessageContext.getProperty(MetricConstants.TRANSPORT_IN_URL).
                        toString();
                String apiName = getApiName(url, synCtx);
                if (apiName != null) {
                    String context = "";
                    if (synCtx.getConfiguration() != null) {
                        API api = synCtx.getConfiguration().getAPI(apiName);
                        if (api != null) {
                            context = api.getContext();
                        }
                    }
                    String apiInvocationUrl = axis2MessageContext.getProperty(MetricConstants.SERVICE_PREFIX).
                            toString() + context.replaceFirst(DELIMITER, EMPTY);
                    incrementAPICount(apiName, apiInvocationUrl);
                    startTimers(synCtx, apiName, SynapseConstants.FAIL_SAFE_MODE_API, apiInvocationUrl);
                }
            }
        }
        return true;
    }

    @Override
    public boolean handleRequestOutFlow(MessageContext synCtx) {
        return true;
    }

    @Override
    public boolean handleResponseInFlow(MessageContext synCtx) {
        return true;
    }

    @Override
    public boolean handleResponseOutFlow(MessageContext synCtx) {
        if (null != synCtx.getProperty(SynapseConstants.PROXY_SERVICE)) {
            stopTimers(synCtx.getProperty(MetricConstants.PROXY_LATENCY_TIMER), synCtx);
        } else {
            serviceInvokePort = getServiceInvokePort(synCtx);
            if (null != synCtx.getProperty(SynapseConstants.IS_INBOUND) &&
                    (serviceInvokePort != internalHttpApiPort)) {
                stopTimers(synCtx.
                        getProperty(MetricConstants.INBOUND_ENDPOINT_LATENCY_TIMER), synCtx);
            }
            if (serviceInvokePort != internalHttpApiPort) {
                stopTimers(synCtx.getProperty(MetricConstants.API_LATENCY_TIMER), synCtx);
                synCtx.setProperty(RESTConstants.IS_PROMETHEUS_ENGAGED, true);
            }
        }
        return true;
    }

    @Override
    public boolean handleError(MessageContext synCtx) {
        org.apache.axis2.context.MessageContext axis2MessageContext = ((Axis2MessageContext) synCtx)
                .getAxis2MessageContext();

        if (null == synCtx.getProperty(SynapseConstants.IS_ERROR_COUNT_ALREADY_PROCESSED)) {
            if (null != synCtx.getProperty("proxy.name")) {
                String name = synCtx.getProperty(SynapseConstants.PROXY_SERVICE).toString();
                incrementProxyErrorCount(name);
                stopTimers(synCtx.getProperty(MetricConstants.PROXY_LATENCY_TIMER), synCtx);
            } else if (null != axis2MessageContext.getProperty("TransportInURL") &&
                    !axis2MessageContext.getProperty("TransportInURL").toString().contains("services")) {
                if (null != synCtx.getProperty(SynapseConstants.IS_INBOUND) &&
                        synCtx.getProperty(SynapseConstants.IS_INBOUND).toString().equals("true")) {
                    String inboundEndpointName = synCtx.getProperty(SynapseConstants.INBOUND_ENDPOINT_NAME).
                            toString();
                    incrementInboundEndpointErrorCount(inboundEndpointName);
                    stopTimers(synCtx.getProperty
                            (MetricConstants.INBOUND_ENDPOINT_LATENCY_TIMER), synCtx);
                } else {
                    serviceInvokePort = getServiceInvokePort(synCtx);

                    if (null != synCtx.getProperty(RESTConstants.SYNAPSE_REST_API) &&
                            (serviceInvokePort != internalHttpApiPort)) {
                        String context = axis2MessageContext.getProperty(MetricConstants.TRANSPORT_IN_URL).
                                toString();
                        String apiInvocationUrl = axis2MessageContext.getProperty(MetricConstants.SERVICE_PREFIX).
                                toString() + context.replaceFirst(DELIMITER, EMPTY);
                        String apiName = getApiName(context, synCtx);
                        incrementApiErrorCount(apiName, apiInvocationUrl);
                        stopTimers(synCtx.getProperty(MetricConstants.API_LATENCY_TIMER), synCtx);
                    }
                }
            }
        }
        synCtx.setProperty(SynapseConstants.IS_ERROR_COUNT_ALREADY_PROCESSED, true);
        return true;
    }

    @Override
    public boolean handleServerShutDown() {
        this.metricReporterInstance.serverDown(HOST, PORT, JAVA_HOME, JAVA_VERSION);
        return true;
    }

    @Override
    public boolean handleArtifactDeployment(String artifactName, String artifactType, String startTime) {
        this.metricReporterInstance.serviceUp(artifactName, artifactType);
        return true;
    }

    @Override
    public boolean handleArtifactUnDeployment(String artifactName, String artifactType, String startTime) {
        this.metricReporterInstance.serviceDown(artifactName, artifactType);
        return true;
    }

    /**
     * Start the timers to observe request latency.
     *
     * @param synCtx           The Synapse Message Context
     * @param serviceName      The proxy/api/inbound endpoint name
     * @param serviceType      The service type (proxy/api/inbound endpoint)
     * @param apiInvocationUrl The api invocation url
     */
    private void startTimers(MessageContext synCtx, String serviceName, String serviceType, String apiInvocationUrl) {
        switch (serviceType) {
            case SynapseConstants.PROXY_SERVICE_TYPE:
                synCtx.setProperty(MetricConstants.PROXY_LATENCY_TIMER,
                        metricReporterInstance.getTimer(MetricConstants.PROXY_LATENCY_SECONDS,
                                new String[]{serviceName, serviceType}));
                break;
            case MetricConstants.INBOUND_ENDPOINT:
                synCtx.setProperty(MetricConstants.INBOUND_ENDPOINT_LATENCY_TIMER,
                        metricReporterInstance.getTimer(MetricConstants.INBOUND_ENDPOINT_LATENCY_SECONDS,
                                new String[]{serviceName, serviceType}));
                break;
            case SynapseConstants.FAIL_SAFE_MODE_API:
                synCtx.setProperty(MetricConstants.API_LATENCY_TIMER,
                        metricReporterInstance.getTimer(MetricConstants.API_LATENCY_SECONDS,
                                new String[]{serviceName, serviceType, apiInvocationUrl}));
                break;
            default:
                log.error("No proper service type found");
                break;
        }
    }

    /**
     * Stop the timer when the response is leaving the Synapse Engine.
     *
     * @param timer The Timer object used to observe latency
     * @ param synCtx The Synapse Message Context
     */
    private void stopTimers(Object timer, MessageContext synCtx) {
        if (null == synCtx.getProperty(SynapseConstants.IS_ERROR_COUNT_ALREADY_PROCESSED) && (null != timer)) {
             metricReporterInstance.observeTime(timer);
        }
    }

    /**
     * Increment the request count received by a proxy service.
     *
     * @param proxyName The invoked proxy name
     */
    private void incrementProxyCount(String proxyName) {
        metricReporterInstance.incrementCount(MetricConstants.PROXY_REQUEST_COUNT_TOTAL, new String[] {proxyName,
                SynapseConstants.PROXY_SERVICE_TYPE});
    }

    /**
     * Increment the request count received by an api.
     *
     * @param apiName          The invoked api name
     * @param apiInvocationUrl api Invocation URL
     */
    private void incrementAPICount(String apiName, String apiInvocationUrl) {
        metricReporterInstance.incrementCount(MetricConstants.API_REQUEST_COUNT_TOTAL, new String[]{apiName,
                SynapseConstants.FAIL_SAFE_MODE_API, apiInvocationUrl});
    }

    /**
     * Increment the request count received by an inbound endpoint.
     *
     * @param inboundEndpointName The invoked inbound endpoint name
     */
    private void incrementInboundEndPointCount(String inboundEndpointName) {
        metricReporterInstance.incrementCount(MetricConstants.INBOUND_ENDPOINT_REQUEST_COUNT_TOTAL,
                new String[]{inboundEndpointName, MetricConstants.INBOUND_ENDPOINT});
    }

    /**
     * Increment the error request count received by a proxy service.
     *
     * @param name The metric name
     */
    private void incrementProxyErrorCount(String name) {
        metricReporterInstance.incrementCount(MetricConstants.PROXY_REQUEST_COUNT_ERROR_TOTAL, new String[]{name,
                SynapseConstants.PROXY_SERVICE_TYPE});
    }

    /**
     * Increment the error request count received by an api.
     *
     * @param name             The metric name
     * @param apiInvocationUrl api Invocation URL
     */
    private void incrementApiErrorCount(String name, String apiInvocationUrl) {
        metricReporterInstance.incrementCount(MetricConstants.API_REQUEST_COUNT_ERROR_TOTAL,  new String[]{name,
                SynapseConstants.FAIL_SAFE_MODE_API, apiInvocationUrl});
    }

    /**
     * Increment the error request count received by an inbound endpoint.
     *
     * @param name The metric name
     */
    private void incrementInboundEndpointErrorCount(String name) {
        metricReporterInstance.incrementCount(MetricConstants.INBOUND_ENDPOINT_REQUEST_COUNT_ERROR_TOTAL,
                new String[]{name, MetricConstants.INBOUND_ENDPOINT});
    }

    /**
     * Get the api name.
     *
     * The APIs can be defined with three version types as None, Context and URL
     * None - DefaultVersionStrategy i.e. no versioning (eg:- https://api.example.com/resources)
     * Context - ContextBasedVersionStrategy : {version} is replaced by given version[v1] (eg:- https://api.example.com/{version}/resources)
     * URL - URLBasedVersionStrategy : given version[v1] get append to end of context (eg:- https://api.example.com/v1/resources)
     *
     * [ADDITIONAL INFO] When resolving the API NAME priority will be is as follows if the Api context and method are same.
     * This priority-based resolution behavior remains consistent regardless of whether metrics are enabled or disabled in the system.
     *  1. ContextBasedVersionStrategy
     *  2. URLBasedVersionStrategy
     *  3. DefaultVersionStrategy
     *
     * @param contextPath The api context path
     * @param synCtx The Synapse Message Context
     * @return String The api name
     */
    private String getApiName(String contextPath, MessageContext synCtx) {
        Collection<API> apiList = synCtx.getEnvironment().getSynapseConfiguration().getAPIs();
        Collection<API> withVersionsApiList = new ArrayList<>();
        Collection<API> defaultApiList = new ArrayList<>();
        updateApiLists(apiList, withVersionsApiList, defaultApiList);

        // Check APIs defined with ContextBasedVersionStrategy or URLBasedVersionStrategy
        if (!withVersionsApiList.isEmpty()) {
            String apiName = getResolvedApiName(contextPath, synCtx, withVersionsApiList, false);
            if (apiName != null) {
                return apiName;
            }
        }
        // Check APIs defined with DefaultVersionStrategy
        return getResolvedApiName(contextPath, synCtx, defaultApiList, true);
    }

    private static String getResolvedApiName(String contextPath, MessageContext synCtx,
                                                       Collection<API> apiList, boolean defaultVersionStrategy) {
        String apiName = null;
        for (API api : apiList) {
            String apiContextPath = api.getContext();
            if (!defaultVersionStrategy) {
                if (api.getVersionStrategy() instanceof URLBasedVersionStrategy) {
                    apiContextPath = apiContextPath + "/" + api.getVersionStrategy().getVersion();
                }
            }

            // Cross-check invoking API context with current API context
            if (RESTUtils.matchApiPath(contextPath, apiContextPath)) {
                apiName = api.getName();
                synCtx.setProperty(RESTConstants.PROCESSED_API, api);
                break;
            }
        }
        return apiName;
    }

    private void updateApiLists(Collection<API> apiList, Collection<API> withVersionsApiList,
                                           Collection<API> defaultApiList) {
        for (API api : apiList) {
            if (StringUtils.isNotBlank(api.getVersionStrategy().getVersion())) {
                withVersionsApiList.add(api);
            } else {
                defaultApiList.add(api);
            }
        }
    }

    /**
     * Return the port the service was invoked.
     *
     * @param synCtx Synapse message context
     * @return port the port used to invoke the service
     */
    private int getServiceInvokePort(MessageContext synCtx) {
        int invokePort = 0;
        if (null != ((Axis2MessageContext) synCtx).getAxis2MessageContext().
                getProperty(NhttpConstants.SERVICE_PREFIX)) {
            String servicePrefix = ((Axis2MessageContext) synCtx).getAxis2MessageContext().
                    getProperty(NhttpConstants.SERVICE_PREFIX).toString();
            invokePort = getServiceInvokePort(servicePrefix);
        }
        return invokePort;
    }

    /**
     * Extract the port from a service prefix.
     *
     * @param servicePrefix the service prefix
     * @return the port, or {@code -1} when it is absent or the prefix is invalid
     */
    static int getServiceInvokePort(String servicePrefix) {
        try {
            return new URI(servicePrefix).getPort();
        } catch (URISyntaxException e) {
            return -1;
        }
    }

    /**
     * Return the internal http api port.
     *
     * @return port internal http api port
     */
    private int getInternalHttpApiPort() {
        internalHttpApiPort = Integer.parseInt(SynapsePropertiesLoader.
                getPropertyValue(MetricConstants.INTERNAL_HTTP_API_PORT, String.valueOf(9191)));

        return internalHttpApiPort + Integer.parseInt(SERVER_PORT_OFFSET);
    }
}

/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.micro.integrator.management.apis;

import com.google.gson.JsonObject;
import org.apache.axiom.om.OMElement;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.ServerConfigurationInformation;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.config.xml.ProxyServiceSerializer;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.core.axis2.ProxyService;
import org.json.JSONObject;
import org.wso2.carbon.inbound.endpoint.internal.http.api.APIResource;
import org.wso2.micro.core.util.AuditLogger;
import org.wso2.micro.integrator.management.apis.security.handler.SecurityUtils;
import org.wso2.micro.integrator.security.user.api.UserStoreException;
import org.wso2.micro.service.mgt.ServiceAdmin;
import org.wso2.micro.service.mgt.ServiceMetaData;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.wso2.micro.integrator.management.apis.Constants.ACTIVE_STATUS;
import static org.wso2.micro.integrator.management.apis.Constants.INACTIVE_STATUS;
import static org.wso2.micro.integrator.management.apis.Constants.NAME;
import static org.wso2.micro.integrator.management.apis.Constants.SEARCH_KEY;
import static org.wso2.micro.integrator.management.apis.Constants.STATUS;
import static org.wso2.micro.integrator.management.apis.Constants.SYNAPSE_CONFIGURATION;
import static org.wso2.micro.integrator.management.apis.Constants.USERNAME_PROPERTY;

public class ProxyServiceResource extends APIResource {

    private static Log LOG = LogFactory.getLog(ProxyServiceResource.class);

    private static final String PROXY_NAME = "proxyName";
    private static final String PROXY_SERVICE_NAME = "proxyServiceName";

    private static ServiceAdmin serviceAdmin = null;

    private static final String WSDL11 = "wsdl1_1";
    private static final String WSDL20 = "wsdl2_0";

    public ProxyServiceResource(String urlTemplate) {

        super(urlTemplate);
    }

    @Override
    public Set<String> getMethods() {

        Set<String> methods = new HashSet<>();
        methods.add(Constants.HTTP_GET);
        methods.add(Constants.HTTP_POST);
        return methods;
    }

    @Override
    public boolean invoke(MessageContext messageContext) {

        buildMessage(messageContext);
        org.apache.axis2.context.MessageContext axis2MessageContext =
                ((Axis2MessageContext) messageContext).getAxis2MessageContext();

        if (serviceAdmin == null) {
            serviceAdmin = Utils.getServiceAdmin(messageContext);
        }

        String proxyServiceName = Utils.getQueryParameter(messageContext, PROXY_SERVICE_NAME);
        String searchKey = Utils.getQueryParameter(messageContext, SEARCH_KEY);

        if (messageContext.isDoingGET()) {
            if (Objects.nonNull(proxyServiceName)) {
                populateProxyServiceData(messageContext, proxyServiceName);
            } else if (Objects.nonNull(searchKey) && !searchKey.trim().isEmpty()) {
                populateSearchResults(messageContext, searchKey.toLowerCase());
            } else {
                populateProxyServiceList(messageContext);
            }
            axis2MessageContext.removeProperty(Constants.NO_ENTITY_BODY);
        } else {
            String userName = (String) messageContext.getProperty(USERNAME_PROPERTY);
            try {
                if (SecurityUtils.canUserEdit(messageContext, userName)) {
                    if (!JsonUtil.hasAJsonPayload(axis2MessageContext)) {
                        Utils.setJsonPayLoad(axis2MessageContext, Utils.createJsonErrorObject("JSON payload is missing"));
                        return true;
                    }
                    JsonObject payload = Utils.getJsonPayload(axis2MessageContext);
                    String performedBy = Constants.ANONYMOUS_USER;
                    if (messageContext.getProperty(Constants.USERNAME_PROPERTY) !=  null) {
                        performedBy = messageContext.getProperty(Constants.USERNAME_PROPERTY).toString();
                    }
                    JSONObject info = new JSONObject();
                    String name = payload.get(NAME).getAsString();
                    info.put(PROXY_NAME, name);
                    if (payload.has(NAME) && payload.has(STATUS)) {
                        changeProxyState(performedBy, info, messageContext, axis2MessageContext, payload);
                    } else {
                        handleTracing(performedBy, info, payload, messageContext, axis2MessageContext);
                    }
                } else {
                    Utils.sendForbiddenFaultResponse(axis2MessageContext);
                }
            } catch (IOException e) {
                LOG.error("Error when parsing JSON payload", e);
                Utils.setJsonPayLoad(axis2MessageContext, Utils.createJsonErrorObject("Error when parsing JSON payload"));
            } catch (UserStoreException e) {
                LOG.error("Error occurred while retrieving the user data", e);
                Utils.setJsonPayLoad(axis2MessageContext,Utils.createJsonErrorObject("Error occurred while retrieving the user data"));
            }
        }
        return true;
    }

    private List<ProxyService> getSearchResults(MessageContext messageContext, String searchKey) {
        SynapseConfiguration configuration = messageContext.getConfiguration();
        return configuration.getProxyServices().stream()
                .filter(artifact -> artifact.getName().toLowerCase().contains(searchKey))
                .collect(Collectors.toList());
    }

    private void populateSearchResults(MessageContext messageContext, String searchKey) {
        List<ProxyService> searchResultList = getSearchResults(messageContext, searchKey);
        setResponseBody(searchResultList, messageContext);
    }

    private void setResponseBody(Collection<ProxyService> proxyServices, MessageContext messageContext) {
        org.apache.axis2.context.MessageContext axis2MessageContext =
                ((Axis2MessageContext) messageContext).getAxis2MessageContext();
        JSONObject jsonBody = Utils.createJSONList(proxyServices.size());
        for (ProxyService proxyService : proxyServices) {
            JSONObject proxyObject = new JSONObject();
            try {
                ServiceMetaData data = serviceAdmin.getServiceData(proxyService.getName());
                proxyObject.put(Constants.NAME, proxyService.getName());
                String[] wsdlUrls = data.getWsdlURLs();
                proxyObject.put(WSDL11, wsdlUrls[0]);
                proxyObject.put(WSDL20, wsdlUrls[1]);
            } catch (Exception e) {
                LOG.error("Error occurred while processing service data", e);
            }
            jsonBody.getJSONArray(Constants.LIST).put(proxyObject);
        }
        Utils.setJsonPayLoad(axis2MessageContext, jsonBody);
    }

    private void handleTracing(String performedBy, JSONObject info, JsonObject payload, MessageContext msgCtx,
                               org.apache.axis2.context.MessageContext axisMsgCtx) {
        JSONObject response;
        if (payload.has(NAME)) {
            String proxyName = payload.get(NAME).getAsString();
            SynapseConfiguration configuration = msgCtx.getConfiguration();
            ProxyService proxyService = configuration.getProxyService(proxyName);
            if (proxyService != null) {
                response = Utils.handleResourceStateChange(performedBy, Constants.AUDIT_LOG_TYPE_PROXY_SERVICE_STATE_CHANGE,
                                               info, proxyService.getAspectConfiguration(),
                                               proxyName, axisMsgCtx, payload);
            } else {
                response = Utils.createJsonError("Specified proxy ('" + proxyName + "') not found", axisMsgCtx,
                                                 Constants.BAD_REQUEST);
            }
        } else {
            response = Utils.createJsonError("Unsupported operation", axisMsgCtx, Constants.BAD_REQUEST);
        }
        Utils.setJsonPayLoad(axisMsgCtx, response);
    }

    private void populateProxyServiceList(MessageContext messageContext) {

        SynapseConfiguration configuration = messageContext.getConfiguration();
        Collection<ProxyService> proxyServices = configuration.getProxyServices();
        setResponseBody(proxyServices, messageContext);
    }

    private void populateProxyServiceData(MessageContext messageContext, String proxyServiceName) {

        org.apache.axis2.context.MessageContext axis2MessageContext =
                ((Axis2MessageContext) messageContext).getAxis2MessageContext();

        JSONObject jsonBody = getProxyServiceByName(messageContext, proxyServiceName);

        if (Objects.nonNull(jsonBody)) {
            Utils.setJsonPayLoad(axis2MessageContext, jsonBody);
        } else {
            axis2MessageContext.setProperty(Constants.HTTP_STATUS_CODE, Constants.NOT_FOUND);
        }
    }

    private JSONObject getProxyServiceByName(MessageContext messageContext, String proxyServiceName) {

        SynapseConfiguration configuration = messageContext.getConfiguration();
        ProxyService proxyService = configuration.getProxyService(proxyServiceName);
        return convertProxyServiceToJsonObject(proxyService);
    }

    private JSONObject convertProxyServiceToJsonObject(ProxyService proxyService) {

        if (Objects.isNull(proxyService)) {
            return null;
        }

        JSONObject proxyObject = new JSONObject();
        proxyObject.put(Constants.NAME, proxyService.getName());

        try {

            ServiceMetaData data = serviceAdmin.getServiceData(proxyService.getName());

            String[] wsdlUrls = data.getWsdlURLs();

            proxyObject.put(WSDL11, wsdlUrls[0]);
            proxyObject.put(WSDL20, wsdlUrls[1]);
        } catch (Exception e) {
            LOG.error("Error occurred while processing service data", e);
        }

        String statisticState = proxyService.getAspectConfiguration().isStatisticsEnable() ? Constants.ENABLED : Constants.DISABLED;
        proxyObject.put(Constants.STATS, statisticState);

        String tracingState = proxyService.getAspectConfiguration().isTracingEnabled() ? Constants.ENABLED : Constants.DISABLED;
        proxyObject.put(Constants.TRACING, tracingState);

        OMElement proxyConfiguration =
                Utils.maskSecretsInConfiguration(ProxyServiceSerializer.serializeProxy(null, proxyService));
        proxyObject.put(SYNAPSE_CONFIGURATION, proxyConfiguration.toString());
        proxyObject.put("eprs", proxyService.getAxisService().getEPRs());
        proxyObject.put("isRunning", proxyService.isRunning());
        return proxyObject;
    }

    /**
     * Change the state of the proxy based on the payload.
     *
     * @param messageContext      Synapse message context
     * @param axis2MessageContext AXIS2 message context
     * @param payload             json payload
     */
    private void changeProxyState(String performedBy, JSONObject info, MessageContext messageContext,
                                  org.apache.axis2.context.MessageContext axis2MessageContext, JsonObject payload) {

        SynapseConfiguration synapseConfiguration = messageContext.getConfiguration();
        String name = payload.get(NAME).getAsString();
        String status = payload.get(STATUS).getAsString();
        ProxyService proxyService = synapseConfiguration.getProxyService(name);
        if (proxyService == null) {
            Utils.setJsonPayLoad(axis2MessageContext, Utils.createJsonError("Proxy service could not be found",
                    axis2MessageContext, Constants.NOT_FOUND));
            return;
        }
        List pinnedServers = proxyService.getPinnedServers();
        JSONObject jsonResponse = new JSONObject();
        if (ACTIVE_STATUS.equalsIgnoreCase(status)) {
            if (pinnedServers.isEmpty() ||
                    pinnedServers.contains(getServerConfigInformation(synapseConfiguration).getServerName())) {
                proxyService.start(synapseConfiguration);
                jsonResponse.put("Message", "Proxy service " + name + " started successfully");
                Utils.setJsonPayLoad(axis2MessageContext, jsonResponse);
                AuditLogger.logAuditMessage(performedBy, Constants.AUDIT_LOG_TYPE_PROXY_SERVICE,
                                            Constants.AUDIT_LOG_ACTION_ENABLE, info);
            }
        } else if (INACTIVE_STATUS.equalsIgnoreCase(status)) {
            if (pinnedServers.isEmpty() ||
                    pinnedServers.contains(getServerConfigInformation(synapseConfiguration).getSynapseXMLLocation())) {
                proxyService.stop(synapseConfiguration);
                jsonResponse.put("Message", "Proxy service " + name + " stopped successfully");
                Utils.setJsonPayLoad(axis2MessageContext, jsonResponse);

                AuditLogger.logAuditMessage(performedBy, Constants.AUDIT_LOG_TYPE_PROXY_SERVICE,
                                            Constants.AUDIT_LOG_ACTION_DISABLED, info);
            }
        } else {
            Utils.setJsonPayLoad(axis2MessageContext,
                    Utils.createJsonError("Provided state is not valid", axis2MessageContext, Constants.BAD_REQUEST));
        }
    }

    /**
     * Return ServerConfigurationInformation of a given SynapseConfiguration.
     *
     * @param synapseConfiguration synapse configuration of the proxy
     * @return ServerConfigurationInformation
     */
    private ServerConfigurationInformation getServerConfigInformation(SynapseConfiguration synapseConfiguration) {

        return (ServerConfigurationInformation) synapseConfiguration.getAxisConfiguration().
                getParameter(SynapseConstants.SYNAPSE_SERVER_CONFIG_INFO).getValue();
    }
}

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
import org.apache.axis2.description.Parameter;
import org.apache.axis2.description.TransportInDescription;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.api.API;
import org.apache.synapse.api.Resource;
import org.apache.synapse.api.dispatch.DispatcherHelper;
import org.apache.synapse.api.dispatch.URITemplateHelper;
import org.apache.synapse.api.dispatch.URLMappingHelper;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.config.xml.rest.APISerializer;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.json.JSONArray;
import org.json.JSONObject;
import org.wso2.carbon.inbound.endpoint.internal.http.api.APIResource;
import org.wso2.micro.core.util.NetworkUtils;
import org.wso2.micro.integrator.management.apis.security.handler.SecurityUtils;
import org.wso2.micro.integrator.security.user.api.UserStoreException;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.wso2.micro.integrator.management.apis.Constants.SEARCH_KEY;
import static org.wso2.micro.integrator.management.apis.Constants.USERNAME_PROPERTY;

public class ApiResource extends APIResource {

    private static Log LOG = LogFactory.getLog(ApiResource.class);

    private static final String API_NAME = "apiName";
    private static final String URL_VERSION_TYPE = "url";
    private List<String> serverContext = new ArrayList<>();  // base server urls

    public ApiResource(String urlTemplate) {
        super(urlTemplate);
    }

    public Set<String> getMethods() {

        Set<String> methods = new HashSet<>();
        methods.add(Constants.HTTP_GET);
        methods.add(Constants.HTTP_POST);
        return methods;
    }

    public boolean invoke(MessageContext messageContext) {

        buildMessage(messageContext);
        org.apache.axis2.context.MessageContext axisMsgCtx =
                ((Axis2MessageContext) messageContext).getAxis2MessageContext();

        String apiName = Utils.getQueryParameter(messageContext, API_NAME);
        String searchKey = Utils.getQueryParameter(messageContext, SEARCH_KEY);

        if (messageContext.isDoingGET()) {
            if (Objects.nonNull(apiName)) {
                populateApiData(messageContext, apiName);
            } else if (Objects.nonNull(searchKey) && !searchKey.trim().isEmpty()) {
                populateSearchResults(messageContext, searchKey.toLowerCase());
            } else {
                populateApiList(messageContext);
            }
        } else {
            String userName = (String) messageContext.getProperty(USERNAME_PROPERTY);
            try {
                if (SecurityUtils.canUserEdit(messageContext, userName)) {
                    handlePost(messageContext, axisMsgCtx);
                } else {
                    Utils.sendForbiddenFaultResponse(axisMsgCtx);
                }
            } catch (UserStoreException e) {
                LOG.error("Error occurred while retrieving the user data", e);
                Utils.setJsonPayLoad(axisMsgCtx, Utils.createJsonErrorObject("Error occurred while retrieving the user data"));
            }
        }
        return true;
    }

    private List<API> getSearchResults(MessageContext messageContext, String searchKey) {
        SynapseConfiguration configuration = messageContext.getConfiguration();
        return configuration.getAPIs().stream()
                .filter(artifact -> artifact.getAPIName().toLowerCase().contains(searchKey))
                .collect(Collectors.toList());
    }

    private void populateSearchResults(MessageContext messageContext, String searchKey) {
        List<API> searchResultList = getSearchResults(messageContext, searchKey);
        setResponseBody(searchResultList, messageContext);
    }

    private void setResponseBody(Collection<API> resultList, MessageContext messageContext) {

        org.apache.axis2.context.MessageContext axis2MessageContext =
                ((Axis2MessageContext) messageContext).getAxis2MessageContext();
        JSONObject jsonBody = Utils.createJSONList(resultList.size());
        for (API api: resultList) {
            JSONObject apiObject = new JSONObject();
            String apiUrl = getApiUrl(api, messageContext);
            JSONArray urlArray = new JSONArray(apiUrl.split(","));
            apiObject.put(Constants.NAME, api.getName());
            apiObject.put(Constants.URL, urlArray.get(0));
            apiObject.put(Constants.URL_LIST, urlArray);
            apiObject.put(Constants.TRACING,
                    api.getAspectConfiguration().isTracingEnabled() ? Constants.ENABLED : Constants.DISABLED);
            jsonBody.getJSONArray(Constants.LIST).put(apiObject);
        }
        Utils.setJsonPayLoad(axis2MessageContext, jsonBody);
    }
    private void handlePost(MessageContext msgCtx, org.apache.axis2.context.MessageContext axisMsgCtx) {

        JSONObject response;
        try {
            JsonObject payload = Utils.getJsonPayload(axisMsgCtx);
            if (payload.has(Constants.NAME)) {
                String apiName = payload.get(Constants.NAME).getAsString();
                SynapseConfiguration configuration = msgCtx.getConfiguration();
                API api = configuration.getAPI(apiName);
                String performedBy = Constants.ANONYMOUS_USER;
                if (msgCtx.getProperty(Constants.USERNAME_PROPERTY) !=  null) {
                    performedBy = msgCtx.getProperty(Constants.USERNAME_PROPERTY).toString();
                }
                JSONObject info = new JSONObject();
                info.put(API_NAME, apiName);
                if (api != null) {
                    response = Utils.handleResourceStateChange(performedBy, Constants.AUDIT_LOG_TYPE_API_STATE_CHANGE,
                                                   info, api.getAspectConfiguration(), apiName, axisMsgCtx, payload);
                } else {
                    response = Utils.createJsonError("Specified API ('" + apiName + "') not found", axisMsgCtx,
                            Constants.BAD_REQUEST);
                }
            } else {
                response = Utils.createJsonError("Unsupported operation", axisMsgCtx, Constants.BAD_REQUEST);
            }
            Utils.setJsonPayLoad(axisMsgCtx, response);
        } catch (IOException e) {
            LOG.error("Error when parsing JSON payload", e);
            Utils.setJsonPayLoad(axisMsgCtx, Utils.createJsonErrorObject("Error when parsing JSON payload"));
        }
    }

    private void populateApiList(MessageContext messageContext) {

        SynapseConfiguration configuration = messageContext.getConfiguration();

        Collection<API> apis = configuration.getAPIs();

        setResponseBody(apis, messageContext);

    }

    private void populateApiData(MessageContext messageContext, String apiName) {

        org.apache.axis2.context.MessageContext axis2MessageContext =
                ((Axis2MessageContext) messageContext).getAxis2MessageContext();

        JSONObject jsonBody = getApiByName(messageContext, apiName);

        if (Objects.nonNull(jsonBody)) {
            Utils.setJsonPayLoad(axis2MessageContext, jsonBody);
        } else {
            axis2MessageContext.setProperty(Constants.HTTP_STATUS_CODE, Constants.NOT_FOUND);
        }
    }

    private JSONObject getApiByName(MessageContext messageContext, String apiName) {

        SynapseConfiguration configuration = messageContext.getConfiguration();
        API api = configuration.getAPI(apiName);
        return convertApiToJsonObject(api, messageContext);
    }

    private JSONObject convertApiToJsonObject(API api, MessageContext messageContext) {

        if (Objects.isNull(api)) {
            return null;
        }

        JSONObject apiObject = new JSONObject();

        apiObject.put(Constants.NAME, api.getName());
        String apiUrl = getApiUrl(api, messageContext);
        JSONArray urlArray = new JSONArray(apiUrl.split(","));
        apiObject.put(Constants.URL, urlArray.get(0));
        apiObject.put(Constants.URL_LIST, urlArray);

        String version = api.getVersion().equals("") ? "N/A" : api.getVersion();

        apiObject.put(Constants.VERSION, version);

        String statisticState = api.getAspectConfiguration().isStatisticsEnable() ? Constants.ENABLED : Constants.DISABLED;
        apiObject.put(Constants.STATS, statisticState);

        String tracingState = api.getAspectConfiguration().isTracingEnabled() ? Constants.ENABLED : Constants.DISABLED;
        apiObject.put(Constants.TRACING, tracingState);

        JSONArray resourceListObject = new JSONArray();
        apiObject.put("resources", resourceListObject);

        apiObject.put("host", api.getHost());
        apiObject.put("context", api.getContext());
        apiObject.put("port", api.getPort());

        OMElement apiConfiguration = Utils.maskSecretsInConfiguration(APISerializer.serializeAPI(api));
        apiObject.put(Constants.SYNAPSE_CONFIGURATION, apiConfiguration.toString());
        Resource[] resources = api.getResources();

        for (Resource resource : resources) {

            JSONObject resourceObject = new JSONObject();

            String[] methods = resource.getMethods();

            resourceObject.put("methods", methods);

            DispatcherHelper dispatcherHelper = resource.getDispatcherHelper();
            if (dispatcherHelper instanceof URITemplateHelper) {
                resourceObject.put(Constants.URL, dispatcherHelper.getString());

            } else if (dispatcherHelper instanceof URLMappingHelper) {
                resourceObject.put(Constants.URL, dispatcherHelper.getString());
            } else {
                resourceObject.put(Constants.URL, "N/A");
            }
            resourceListObject.put(resourceObject);
        }
        return apiObject;
    }

    private String getApiUrl(API api, MessageContext msgCtx) {

        org.apache.axis2.context.MessageContext axisMsgCtx = ((Axis2MessageContext) msgCtx).getAxis2MessageContext();
        List<String> serverUrls = serverContext.isEmpty() ? getServerContext(axisMsgCtx.getConfigurationContext().getAxisConfiguration()) : serverContext;

        // Build version specific context
        String versionUrl;
        if (URL_VERSION_TYPE.equals(api.getVersionStrategy().getVersionType())) {
            versionUrl = "/" + api.getVersion();
        } else {
            versionUrl = "";
        }

        return serverUrls.isEmpty() ? api.getContext() : serverUrls.stream()
                .map(url -> url + api.getContext() + versionUrl)
                .collect(Collectors.joining(","));
    }

    private List<String> getServerContext(AxisConfiguration configuration) {

        // Capture protocol list
        List<String> protocols = Arrays.asList("http", "https");
        Map<String, String> protocolPortMap = protocols.stream()
                .map(configuration::getTransportIn)
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(
                        TransportInDescription::getName,
                        transportInDescription -> (String) transportInDescription.getParameter("port").getValue()
                ));

        if (protocolPortMap.isEmpty()) {
            return Collections.emptyList();
        }

        String host = Optional.ofNullable(configuration.getParameter("hostname"))
                .map(Parameter::getValue)
                .map(String.class::cast)
                .orElseGet(() -> {
                    try {
                        return NetworkUtils.getLocalHostname();
                    } catch (SocketException e) {
                        return "localhost";
                    }
                });

        // Build URLs
        serverContext = protocolPortMap.entrySet().stream()
                .map(entry -> {
                    String protocol = entry.getKey();
                    int port = Integer.parseInt(entry.getValue());
                    try {
                        return new URL(protocol, host, port, "").toExternalForm();
                    } catch (MalformedURLException e) {
                        return null;
                    }
                })
                .filter(Objects::nonNull) // Filter out any null URLs
                .collect(Collectors.toList());
        return serverContext;
    }
}

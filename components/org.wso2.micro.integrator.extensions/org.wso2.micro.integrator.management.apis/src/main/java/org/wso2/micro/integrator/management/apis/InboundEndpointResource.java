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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.config.xml.inbound.InboundEndpointSerializer;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.util.DynamicControlOperationResult;
import org.apache.synapse.inbound.InboundEndpoint;
import org.json.JSONArray;
import org.json.JSONObject;
import org.wso2.carbon.inbound.endpoint.internal.http.api.APIResource;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericEventBasedConsumer;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericEventBasedConsumerRegistry;
import org.wso2.micro.integrator.management.apis.security.handler.SecurityUtils;
import org.wso2.micro.integrator.security.user.api.UserStoreException;
import org.wso2.micro.core.util.AuditLogger;

import java.io.IOException;

import java.util.Set;
import java.util.List;
import java.util.HashSet;
import java.util.Map;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.wso2.micro.integrator.management.apis.Constants.ACTIVE_STATUS;
import static org.wso2.micro.integrator.management.apis.Constants.INACTIVE_STATUS;
import static org.wso2.micro.integrator.management.apis.Constants.NAME;
import static org.wso2.micro.integrator.management.apis.Constants.SEARCH_KEY;
import static org.wso2.micro.integrator.management.apis.Constants.STATUS;
import static org.wso2.micro.integrator.management.apis.Constants.SYNAPSE_CONFIGURATION;
import static org.wso2.micro.integrator.management.apis.Constants.USERNAME_PROPERTY;

public class InboundEndpointResource extends APIResource {

    private static Log LOG = LogFactory.getLog(InboundEndpointResource.class);

    private static final String INBOUND_ENDPOINT_NAME = "inboundEndpointName";

    public InboundEndpointResource(String urlTemplate){
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
        org.apache.axis2.context.MessageContext axisMsgCtx =
                ((Axis2MessageContext) messageContext).getAxis2MessageContext();
        String inboundName = Utils.getQueryParameter(messageContext, INBOUND_ENDPOINT_NAME);
        String searchKey = Utils.getQueryParameter(messageContext, SEARCH_KEY);

        if (messageContext.isDoingGET()) {
            if (Objects.nonNull(inboundName)) {
                populateInboundEndpointData(messageContext, inboundName);
            } else if (Objects.nonNull(searchKey) && !searchKey.trim().isEmpty()) {
                populateSearchResults(messageContext, searchKey.toLowerCase());
            } else {
                populateInboundEndpointList(messageContext);
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

    private List<InboundEndpoint> getSearchResults (MessageContext messageContext, String searchKey) {

        SynapseConfiguration configuration = messageContext.getConfiguration();
        return configuration.getInboundEndpoints().stream()
                .filter(artifact -> artifact.getName().toLowerCase().contains(searchKey))
                .collect(Collectors.toList());
    }

    private void populateSearchResults(MessageContext messageContext, String searchKey) {

        List<InboundEndpoint> resultsList = getSearchResults(messageContext, searchKey);
        setResponseBody(resultsList, messageContext);
    }

    private void handlePost(MessageContext msgCtx,
                            org.apache.axis2.context.MessageContext axisMsgCtx) {

        JSONObject response;
        try {
            JsonObject payload = Utils.getJsonPayload(axisMsgCtx);
            if (payload.has(Constants.NAME)) {
                String inboundName = payload.get(Constants.NAME).getAsString();
                SynapseConfiguration configuration = msgCtx.getConfiguration();
                InboundEndpoint inboundEndpoint = configuration.getInboundEndpoint(inboundName);
                if (inboundEndpoint != null) {
                    String performedBy = Constants.ANONYMOUS_USER;
                    if (msgCtx.getProperty(Constants.USERNAME_PROPERTY) !=  null) {
                        performedBy = msgCtx.getProperty(Constants.USERNAME_PROPERTY).toString();
                    }
                    JSONObject info = new JSONObject();
                    info.put(INBOUND_ENDPOINT_NAME, inboundName);
                    if (payload.has(STATUS)) {
                        response = handleStatusUpdate(inboundEndpoint, performedBy, info, msgCtx, payload);
                    } else {
                        response = Utils.handleResourceStateChange(performedBy, Constants.AUDIT_LOG_TYPE_INBOUND_ENDPOINT_STATE_CHANGE,
                                info,
                                inboundEndpoint.getAspectConfiguration(), inboundName, axisMsgCtx, payload);
                    }

                } else {
                    response = Utils.createJsonError("Specified inbound endpoint ('" + inboundName + "') not found",
                            axisMsgCtx, Constants.BAD_REQUEST);
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

    private void populateInboundEndpointList(MessageContext messageContext) {

        SynapseConfiguration configuration = messageContext.getConfiguration();
        Collection<InboundEndpoint> inboundEndpoints = configuration.getInboundEndpoints();
        setResponseBody(inboundEndpoints, messageContext);
    }

    private void setResponseBody(Collection<InboundEndpoint> inboundEndpointCollection, MessageContext messageContext) {

        org.apache.axis2.context.MessageContext axis2MessageContext =
                ((Axis2MessageContext) messageContext).getAxis2MessageContext();

        JSONObject jsonBody = Utils.createJSONList(inboundEndpointCollection.size());

        for (InboundEndpoint inboundEndpoint : inboundEndpointCollection) {
            JSONObject inboundObject = new JSONObject();

            inboundObject.put(Constants.NAME, inboundEndpoint.getName());
            inboundObject.put("protocol", inboundEndpoint.getProtocol());
            Map<String, Object> health = getConsumerHealth(inboundEndpoint);
            inboundObject.put(Constants.STATUS, getInboundEndpointState(inboundEndpoint, health));

            jsonBody.getJSONArray(Constants.LIST).put(inboundObject);
        }
        Utils.setJsonPayLoad(axis2MessageContext, jsonBody);
    }

    private void populateInboundEndpointData(MessageContext messageContext, String inboundEndpointName) {

        org.apache.axis2.context.MessageContext axis2MessageContext =
                ((Axis2MessageContext) messageContext).getAxis2MessageContext();

        JSONObject jsonBody = getInboundEndpointByName(messageContext, inboundEndpointName);

        if (Objects.nonNull(jsonBody)) {
            Utils.setJsonPayLoad(axis2MessageContext, jsonBody);
        } else {
            axis2MessageContext.setProperty(Constants.HTTP_STATUS_CODE, Constants.NOT_FOUND);
        }
    }

    private JSONObject getInboundEndpointByName(MessageContext messageContext, String inboundEndpointName) {

        SynapseConfiguration configuration = messageContext.getConfiguration();
        InboundEndpoint ep = configuration.getInboundEndpoint(inboundEndpointName);
        return convertInboundEndpointToJsonObject(ep);
    }

    private JSONObject convertInboundEndpointToJsonObject(InboundEndpoint inboundEndpoint) {

        if (Objects.isNull(inboundEndpoint)) {
            return null;
        }

        JSONObject inboundObject = new JSONObject();

        inboundObject.put(Constants.NAME, inboundEndpoint.getName());
        inboundObject.put("protocol", inboundEndpoint.getProtocol());
        inboundObject.put("sequence", inboundEndpoint.getInjectingSeq());
        inboundObject.put("error", inboundEndpoint.getOnErrorSeq());
        Map<String, Object> health = getConsumerHealth(inboundEndpoint);
        inboundObject.put(Constants.STATUS, getInboundEndpointState(inboundEndpoint, health));
        if (health != null) {
            inboundObject.put("health", new JSONObject(health));
        }

        String statisticState = inboundEndpoint.getAspectConfiguration().isStatisticsEnable() ? Constants.ENABLED : Constants.DISABLED;
        inboundObject.put(Constants.STATS, statisticState);

        String tracingState = inboundEndpoint.getAspectConfiguration().isTracingEnabled() ? Constants.ENABLED : Constants.DISABLED;
        inboundObject.put(Constants.TRACING, tracingState);
        inboundObject.put(SYNAPSE_CONFIGURATION, Utils.maskSecretsInConfiguration(
                InboundEndpointSerializer.serializeInboundEndpoint(inboundEndpoint)));

        JSONArray parameterListObject = new JSONArray();

        inboundObject.put("parameters", parameterListObject);

        Map<String, String> params = inboundEndpoint.getParametersMap();

        for (Map.Entry<String,String> param : params.entrySet()) {

            JSONObject paramObject = new JSONObject();

            paramObject.put(Constants.NAME, param.getKey());
            paramObject.put("value", Utils.maskSecretValue(param.getKey(), param.getValue()));

            parameterListObject.put(paramObject);
        }
        return inboundObject;
    }

    /**
     * Determines the current state of the specified inbound endpoint.
     *
     * @param inboundEndpoint The {@link InboundEndpoint} instance whose state is to be retrieved.
     * @return A {@link String} representing the state of the inbound endpoint:
     *         - {@code INACTIVE_STATUS} if the inbound endpoint is deactivated.
     *         - {@code ACTIVE_STATUS} otherwise.
     */
    private String getInboundEndpointState(InboundEndpoint inboundEndpoint, Map<String, Object> health) {
        // health is the one map the caller read for this endpoint, so status and health describe the same instant.
        if (health != null && health.get(Constants.STATUS) instanceof String) {
            // One of active, standby, connected, recovering, stopped or failed (design 7.12).
            return (String) health.get(Constants.STATUS);
        }
        if (inboundEndpoint.isDeactivated()) {
            return INACTIVE_STATUS;
        }
        return ACTIVE_STATUS;
    }

    private Map<String, Object> getConsumerHealth(InboundEndpoint inboundEndpoint) {
        GenericEventBasedConsumer consumer = GenericEventBasedConsumerRegistry.get(inboundEndpoint.getName());
        if (consumer == null) {
            return null;
        }
        try {
            return consumer.getHealth();
        } catch (Throwable t) {
            // A consumer compiled against an older base class, or a failing provider, never breaks the API.
            LOG.debug("Health not available for inbound endpoint " + inboundEndpoint.getName(), t);
            return null;
        }
    }

    /**
     * Handles the activation or deactivation of an inbound endpoint based on the provided status.
     *
     * @param performedBy    The user performing the operation, used for audit logging.
     * @param info           A JSON object containing additional audit information.
     * @param messageContext The current Synapse {@link MessageContext} for accessing the configuration.
     * @param payload        A {@link JsonObject} containing the inbound endpoint name and desired status.
     *
     * @return A {@link JSONObject} indicating the result of the operation. If successful, contains
     *         a confirmation message. If unsuccessful, contains an error message with appropriate
     *         HTTP error codes.
     */
    private JSONObject handleStatusUpdate(InboundEndpoint inboundEndpoint, String performedBy, JSONObject info,
                                          MessageContext messageContext, JsonObject payload) {
        String name = payload.get(NAME).getAsString();
        String status = payload.get(STATUS).getAsString();

        org.apache.axis2.context.MessageContext axis2MessageContext =
                ((Axis2MessageContext) messageContext).getAxis2MessageContext();
        JSONObject jsonResponse = new JSONObject();
        if (inboundEndpoint == null) {
            return Utils.createJsonError("Inbound Endpoint could not be found",
                    axis2MessageContext, Constants.NOT_FOUND);
        }

        if (INACTIVE_STATUS.equalsIgnoreCase(status)) {
            DynamicControlOperationResult result = inboundEndpoint.deactivate();
            if (result.isSuccess()) {
                jsonResponse.put(Constants.MESSAGE_JSON_ATTRIBUTE, name + " : is deactivated");
                AuditLogger.logAuditMessage(performedBy, Constants.AUDIT_LOG_TYPE_INBOUND_ENDPOINT,
                        Constants.AUDIT_LOG_ACTION_DISABLED, info);
            } else {
                jsonResponse = Utils.createJsonError(result.getMessage(), axis2MessageContext, Constants.INTERNAL_SERVER_ERROR);
            }
        } else if (ACTIVE_STATUS.equalsIgnoreCase(status)) {
            DynamicControlOperationResult result = inboundEndpoint.activate();
            if (result.isSuccess()) {
                jsonResponse.put(Constants.MESSAGE_JSON_ATTRIBUTE, name + " : is activated");
                AuditLogger.logAuditMessage(performedBy, Constants.AUDIT_LOG_TYPE_MESSAGE_PROCESSOR,
                        Constants.AUDIT_LOG_ACTION_ENABLE, info);
            } else {
                jsonResponse = Utils.createJsonError(result.getMessage(), axis2MessageContext, Constants.INTERNAL_SERVER_ERROR);
            }
        } else {
            jsonResponse = Utils.createJsonError("Provided state is not valid", axis2MessageContext, Constants.BAD_REQUEST);
        }

        return jsonResponse;
    }
}

/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import org.apache.synapse.config.xml.TemplateMediatorSerializer;
import org.apache.synapse.config.xml.endpoints.TemplateSerializer;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.endpoints.Template;
import org.apache.synapse.mediators.template.TemplateMediator;
import org.json.JSONArray;
import org.json.JSONObject;
import org.wso2.carbon.inbound.endpoint.internal.http.api.APIResource;
import org.wso2.micro.integrator.management.apis.security.handler.SecurityUtils;
import org.wso2.micro.integrator.security.user.api.UserStoreException;

import java.io.IOException;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.wso2.micro.integrator.management.apis.Constants.SEARCH_KEY;
import static org.wso2.micro.integrator.management.apis.Constants.USERNAME_PROPERTY;

/**
 * Represents template resources defined in the synapse configuration.
 **/
public class TemplateResource extends APIResource {

    private static final Log LOG = LogFactory.getLog(TemplateResource.class);

    /* Template type parameter */
    private static final String TEMPLATE_TYPE_PARAM = "type";
    /* Template name parameter */
    private static final String TEMPLATE_NAME_PARAM = "name";
    /* Possible values for TEMPLATE_TYPE_PARAM  */
    private static final String ENDPOINT_TEMPLATE_TYPE = "endpoint";
    private static final String SEQUENCE_TEMPLATE_TYPE = "sequence";
    /* Name of the template parameter list */
    private static final String PARAMETERS = "Parameters";
    private static final String SEQUENCE_NAME = "sequenceName";
    private static final String SEQUENCE_TYPE = "sequenceType";

    public TemplateResource(String urlTemplate) {
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
    public boolean invoke(MessageContext msgCtx) {

        buildMessage(msgCtx);
        org.apache.axis2.context.MessageContext axis2MsgCtx = ((Axis2MessageContext) msgCtx).getAxis2MessageContext();
        String templateTypeParam = Utils.getQueryParameter(msgCtx, TEMPLATE_TYPE_PARAM);
        String searchKey = Utils.getQueryParameter(msgCtx, SEARCH_KEY);
        
        if (msgCtx.isDoingGET()) {
            if (Objects.nonNull(templateTypeParam)) {
                String templateNameParam = Utils.getQueryParameter(msgCtx, TEMPLATE_NAME_PARAM);
                if (Objects.nonNull(templateNameParam)) {
                    populateTemplateData(msgCtx, templateNameParam, templateTypeParam);
                } else {
                    populateTemplateListByType(msgCtx, templateTypeParam);
                }
            } else if (Objects.nonNull(searchKey) && !searchKey.trim().isEmpty()) {
                populateSearchResults(msgCtx, searchKey.toLowerCase());
            } else {
                populateFullTemplateList(msgCtx);
            }
        } else {
            JSONObject response;
            String userName = (String) msgCtx.getProperty(USERNAME_PROPERTY);
            try {
                if (SecurityUtils.canUserEdit(msgCtx, userName)) {
                    JsonObject payload = Utils.getJsonPayload(axis2MsgCtx);
                    if (payload.has(TEMPLATE_TYPE_PARAM)) {
                        templateTypeParam = payload.get(TEMPLATE_TYPE_PARAM).getAsString();
                    }
                    if (payload.has(Constants.NAME) && SEQUENCE_TEMPLATE_TYPE.equals(templateTypeParam)) {
                        String seqTempName = payload.get(Constants.NAME).getAsString();
                        response = handleStateChange(seqTempName, msgCtx, axis2MsgCtx, payload);
                    } else {
                        response = Utils.createJsonError("Unsupported operation", axis2MsgCtx, Constants.BAD_REQUEST);
                    }
                    Utils.setJsonPayLoad(axis2MsgCtx, response);
                } else {
                    Utils.sendForbiddenFaultResponse(axis2MsgCtx);
                }
            } catch (IOException e) {
                LOG.error("Error when parsing JSON payload", e);
                Utils.setJsonPayLoad(axis2MsgCtx, Utils.createJsonErrorObject("Error when parsing JSON payload"));
            } catch (UserStoreException e) {
                LOG.error("Error occurred while retrieving the user data", e);
                Utils.setJsonPayLoad(axis2MsgCtx, Utils.createJsonErrorObject("Error occurred while retrieving the user data"));
            }
        }
        return true;
    }

    private JSONObject handleStateChange(String seqTempName, MessageContext msgCtx,
                                     org.apache.axis2.context.MessageContext axisMsgCtx, JsonObject payload) {

        JSONObject response;
        SynapseConfiguration configuration = msgCtx.getConfiguration();
        TemplateMediator sequenceTemplate = configuration.getSequenceTemplate(seqTempName);
        if (sequenceTemplate != null) {
            String performedBy = Constants.ANONYMOUS_USER;
            if (msgCtx.getProperty(Constants.USERNAME_PROPERTY) !=  null) {
                performedBy = msgCtx.getProperty(Constants.USERNAME_PROPERTY).toString();
            }
            JSONObject info = new JSONObject();
            info.put(SEQUENCE_NAME, seqTempName);
            info.put(SEQUENCE_TYPE, SEQUENCE_TEMPLATE_TYPE);
            response = Utils.handleResourceStateChange(performedBy, Constants.AUDIT_LOG_TYPE_SEQUENCE_TEMPLATE_STATE_CHANGE,
                                           info, sequenceTemplate.getAspectConfiguration(),
                                           seqTempName, axisMsgCtx, payload);
        } else {
            response = Utils.createJsonError("Specified sequence template ('" + seqTempName + "') not found",
                    axisMsgCtx, Constants.BAD_REQUEST);
        }
        return response;
    }

    private void populateSearchResults(MessageContext messageContext, String searchKey) {

        SynapseConfiguration configuration = messageContext.getConfiguration();
        List<Template> epSearchResultList = configuration.getEndpointTemplates().values().stream()
                .filter(artifact -> artifact.getName().toLowerCase().contains(searchKey))
                .collect(Collectors.toList());
        List<TemplateMediator> seqSearchResultList = configuration.getSequenceTemplates().values().stream()
                .filter(artifact -> artifact.getName().toLowerCase().contains(searchKey))
                .collect(Collectors.toList());
        setResponseBody(epSearchResultList, seqSearchResultList, messageContext);
    }

    private void setResponseBody(List<Template> epList, List<TemplateMediator> seqList, MessageContext messageContext) {
        org.apache.axis2.context.MessageContext axis2MessageContext =
                ((Axis2MessageContext) messageContext).getAxis2MessageContext();
        int listLength = epList.size() + seqList.size();
        JSONObject jsonBody = Utils.createJSONList(listLength);

        for (Template epTemplate: epList) {
            JSONObject templateObject = getEndpointTemplateAsJson(epTemplate);
            jsonBody.getJSONArray(Constants.LIST).put(templateObject);
        }

        for (TemplateMediator seqTemplate: seqList) {
            JSONObject templateObject = getSequenceTemplateAsJson(seqTemplate);
            jsonBody.getJSONArray(Constants.LIST).put(templateObject);
        }
        Utils.setJsonPayLoad(axis2MessageContext, jsonBody);
    }

    /**
     * Creates a response json with all templates available in the synapse configuration
     *
     * @param messageContext      Synapse msg ctx
     */
    private void populateFullTemplateList(MessageContext messageContext) {

        SynapseConfiguration synapseConfiguration = messageContext.getConfiguration();
        Map<String, Template> endpointTemplateMap = synapseConfiguration.getEndpointTemplates();
        Map<String, TemplateMediator> sequenceTemplateMap = synapseConfiguration.getSequenceTemplates();
        setResponseBody(endpointTemplateMap.values().stream().collect(Collectors.toList()), sequenceTemplateMap.values().stream().collect(Collectors.toList()), messageContext);
    }

    /**
     * Sets the list of templates of a given type to the Axis2MessageContext as a JSON object
     *
     * @param templateType   type of the template Ex: sequence, endpoint
     */
    private void populateTemplateListByType(MessageContext messageContext, String templateType) {
        org.apache.axis2.context.MessageContext axis2MessageContext =
                ((Axis2MessageContext) messageContext).getAxis2MessageContext();
        SynapseConfiguration synapseConfiguration = messageContext.getConfiguration();
        if (ENDPOINT_TEMPLATE_TYPE.equalsIgnoreCase(templateType)) {
            Map<String, Template> endpointTemplateMap = synapseConfiguration.getEndpointTemplates();
            JSONObject jsonBody = Utils.createJSONList(endpointTemplateMap.size());
            endpointTemplateMap.forEach((key, value) ->
                    addToJsonList(jsonBody.getJSONArray(Constants.LIST), value.getName()));
            Utils.setJsonPayLoad(axis2MessageContext, jsonBody);
        } else if (SEQUENCE_TEMPLATE_TYPE.equalsIgnoreCase(templateType)) {
            Map<String, TemplateMediator> sequenceTemplateMap = synapseConfiguration.getSequenceTemplates();
            JSONObject jsonBody = Utils.createJSONList(sequenceTemplateMap.size());
            sequenceTemplateMap.forEach((key, value) ->
                    addToJsonList(jsonBody.getJSONArray(Constants.LIST), value.getName()));
            Utils.setJsonPayLoad(axis2MessageContext, jsonBody);
        } else {
            LOG.warn("Provided template type does not exist");
            Utils.setJsonPayLoad(axis2MessageContext,
                    Utils.createJsonErrorObject("Received type " + templateType + " is not a valid template type"));
        }
    }

    /**
     * Sets the name of the template to the JSON Array
     *
     * @param jsonList body of the json object
     * @param name     template name
     */
    private void addToJsonList(JSONArray jsonList, String name) {
        //Create new JSONObject for the template resource
        JSONObject templateObject = new JSONObject();
        templateObject.put(Constants.NAME, name);
        //Add the template JSONObject into the JSON array on the parent object
        jsonList.put(templateObject);
    }

    /**
     * Set the information related to a specific template to the Axis2MessageContext as a JSON object
     *
     * @param templateName   name of the template
     * @param templateType   type of the template
     */
    private void populateTemplateData(MessageContext messageContext, String templateName, String templateType) {
        org.apache.axis2.context.MessageContext axis2MessageContext =
                ((Axis2MessageContext) messageContext).getAxis2MessageContext();
        SynapseConfiguration synapseConfiguration = messageContext.getConfiguration();
        JSONObject templateObject = null;
        if (ENDPOINT_TEMPLATE_TYPE.equalsIgnoreCase(templateType)) {
            Template endpointTemplate = synapseConfiguration.getEndpointTemplate(templateName);
            if (Objects.nonNull(endpointTemplate)) {
                templateObject = getEndpointTemplateAsJson(endpointTemplate);
            }
        } else if (SEQUENCE_TEMPLATE_TYPE.equalsIgnoreCase(templateType)) {
            TemplateMediator sequenceTemplate = synapseConfiguration.getSequenceTemplate(templateName);
            if (Objects.nonNull(sequenceTemplate)) {
                templateObject = getSequenceTemplateAsJson(sequenceTemplate);
            }
        }
        if (Objects.nonNull(templateObject)) {
            Utils.setJsonPayLoad(axis2MessageContext, templateObject);
        } else {
            axis2MessageContext.setProperty(Constants.HTTP_STATUS_CODE, Constants.NOT_FOUND);
        }
    }

    /**
     * Returns the JSON representation of a endpoint template
     *
     * @param endpointTemplate
     */
    private JSONObject getEndpointTemplateAsJson(Template endpointTemplate) {
        JSONObject endpointTemplateJSONObject = new JSONObject();
        endpointTemplateJSONObject.put(Constants.NAME, endpointTemplate.getName());
        endpointTemplateJSONObject.put(PARAMETERS, endpointTemplate.getParameters());
        endpointTemplateJSONObject.put(Constants.SYNAPSE_CONFIGURATION, Utils.maskSecretsInConfiguration(
                new TemplateSerializer().serializeEndpointTemplate(endpointTemplate, null)));
        endpointTemplateJSONObject.put(TEMPLATE_TYPE_PARAM, ENDPOINT_TEMPLATE_TYPE);

        return endpointTemplateJSONObject;
    }

    /**
     * Returns the JSON representation of a sequence template
     *
     * @param sequenceTemplate
     */
    private JSONObject getSequenceTemplateAsJson(TemplateMediator sequenceTemplate) {
        JSONObject sequenceTemplateJSONObject = new JSONObject();
        sequenceTemplateJSONObject.put(Constants.NAME, sequenceTemplate.getName());
        sequenceTemplateJSONObject.put(PARAMETERS,
                Utils.maskSecretTemplateParameters(sequenceTemplate.getParameters()));
        sequenceTemplateJSONObject.put(Constants.SYNAPSE_CONFIGURATION, Utils.maskSecretsInConfiguration(
                new TemplateMediatorSerializer().serializeMediator(null, sequenceTemplate)));
        sequenceTemplateJSONObject.put(TEMPLATE_TYPE_PARAM, SEQUENCE_TEMPLATE_TYPE);
        return sequenceTemplateJSONObject;
    }
}

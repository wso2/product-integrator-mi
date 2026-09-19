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
import org.apache.axiom.om.OMAttribute;
import org.apache.axiom.om.OMElement;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.Startup;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.util.DynamicControlOperationResult;
import org.apache.synapse.startup.quartz.StartUpController;
import org.apache.synapse.task.TaskDescription;
import org.apache.synapse.task.TaskDescriptionSerializer;
import org.json.JSONObject;
import org.wso2.carbon.inbound.endpoint.internal.http.api.APIResource;
import org.wso2.micro.core.util.AuditLogger;
import org.wso2.micro.integrator.management.apis.security.handler.SecurityUtils;
import org.wso2.micro.integrator.security.user.api.UserStoreException;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.xml.namespace.QName;

import static org.wso2.micro.integrator.management.apis.Constants.ACTIVE_STATUS;
import static org.wso2.micro.integrator.management.apis.Constants.INACTIVE_STATUS;
import static org.wso2.micro.integrator.management.apis.Constants.NAME;
import static org.wso2.micro.integrator.management.apis.Constants.SEARCH_KEY;
import static org.wso2.micro.integrator.management.apis.Constants.STATUS;
import static org.wso2.micro.integrator.management.apis.Constants.TRIGGER_STATUS;
import static org.wso2.micro.integrator.management.apis.Constants.USERNAME_PROPERTY;

public class TaskResource extends APIResource {

    private static final String TASK_NAME = "taskName";
    private static Log log = LogFactory.getLog(TaskResource.class);

    public TaskResource(String urlTemplate){
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

        String param = Utils.getQueryParameter(messageContext, TASK_NAME);
        String searchKey = Utils.getQueryParameter(messageContext, SEARCH_KEY);

        if (messageContext.isDoingGET()) {
            if (Objects.nonNull(param)) {
                populateTaskData(messageContext, param);
            } else if (Objects.nonNull(searchKey) && !searchKey.trim().isEmpty()) {
                populateSearchResults(messageContext, searchKey.toLowerCase());
            } else {
                populateTasksList(messageContext);
            }
        } else {
            String userName = (String) messageContext.getProperty(USERNAME_PROPERTY);
            org.apache.axis2.context.MessageContext axisMsgCtx =
                    ((Axis2MessageContext) messageContext).getAxis2MessageContext();
            try {
                if (SecurityUtils.canUserEdit(messageContext, userName)) {
                    handlePost(messageContext, axisMsgCtx);
                } else {
                    Utils.sendForbiddenFaultResponse(axisMsgCtx);
                }
            } catch (UserStoreException e) {
                log.error("Error occurred while retrieving the user data", e);
                Utils.setJsonPayLoad(axisMsgCtx, Utils.createJsonErrorObject("Error occurred while retrieving the user data"));
            }
        }

        axis2MessageContext.removeProperty(Constants.NO_ENTITY_BODY);
        return true;
    }

    private void handlePost(MessageContext msgCtx, org.apache.axis2.context.MessageContext axisMsgCtx) {

        JSONObject response = null;
        try {
            JsonObject payload = Utils.getJsonPayload(axisMsgCtx);
            if (payload.has(NAME)) {
                String taskName = payload.get(NAME).getAsString();
                SynapseConfiguration configuration = msgCtx.getConfiguration();
                Startup task = configuration.getStartup(taskName);
                if (task != null) {
                    String performedBy = Constants.ANONYMOUS_USER;
                    if (msgCtx.getProperty(USERNAME_PROPERTY) != null) {
                        performedBy = msgCtx.getProperty(USERNAME_PROPERTY).toString();
                    }
                    JSONObject info = new JSONObject();
                    info.put(TASK_NAME, taskName);
                    if (payload.has(STATUS)) {
                        response = handleStatusUpdate(task, performedBy, info, msgCtx, payload);
                    } else {
                        response = Utils.createJsonError("Unsupported operation", axisMsgCtx, Constants.BAD_REQUEST);
                    }
                } else {
                    response = Utils.createJsonError("Specified task ('" + taskName + "') not found",
                            axisMsgCtx, Constants.BAD_REQUEST);
                }
            } else {
                response = Utils.createJsonError("Unsupported operation", axisMsgCtx, Constants.BAD_REQUEST);
            }
            Utils.setJsonPayLoad(axisMsgCtx, response);
        } catch (IOException e) {
            log.error("Error when parsing JSON payload", e);
            Utils.setJsonPayLoad(axisMsgCtx, Utils.createJsonErrorObject("Error when parsing JSON payload"));
        }
    }

    private static List<Startup> getSearchResults(MessageContext messageContext, String searchKey) {
        SynapseConfiguration configuration = messageContext.getConfiguration();
        return configuration.getStartups().stream()
                .filter(artifact -> artifact.getName().toLowerCase().contains(searchKey))
                .collect(Collectors.toList());
    }
    private void populateSearchResults(MessageContext messageContext, String searchKey) {

        List<Startup> searchResultList = getSearchResults(messageContext, searchKey);
        setResponseBody(searchResultList, messageContext);
    }

    private void setResponseBody(Collection<Startup> tasks, MessageContext messageContext) {

        org.apache.axis2.context.MessageContext axis2MessageContext =
                ((Axis2MessageContext) messageContext).getAxis2MessageContext();

        JSONObject jsonBody = Utils.createJSONList(tasks.size());
        for (Startup task : tasks) {
            JSONObject taskObject = new JSONObject();
            taskObject.put(NAME, task.getName());
            jsonBody.getJSONArray(Constants.LIST).put(taskObject);
        }
        Utils.setJsonPayLoad(axis2MessageContext, jsonBody);
    }

    private void populateTasksList(MessageContext messageContext) {

        SynapseConfiguration configuration = messageContext.getConfiguration();
        Collection<Startup> tasks = configuration.getStartups();
        setResponseBody(tasks, messageContext);
    }

    private void populateTaskData(MessageContext messageContext, String taskName) {

        org.apache.axis2.context.MessageContext axis2MessageContext =
                ((Axis2MessageContext) messageContext).getAxis2MessageContext();

        SynapseConfiguration configuration = messageContext.getConfiguration();
        Startup task = configuration.getStartup(taskName);

        if (Objects.nonNull(task)) {
            SynapseEnvironment synapseEnvironment =
                    getSynapseEnvironment(axis2MessageContext.getConfigurationContext().getAxisConfiguration());
            TaskDescription description =
                    synapseEnvironment.getTaskManager().getTaskDescriptionRepository().getTaskDescription(task.getName());
            JSONObject jsonBody = getTaskAsJson(description);
            Utils.setJsonPayLoad(axis2MessageContext, jsonBody);
        } else {
            Utils.setJsonPayLoad(axis2MessageContext, Utils.createJsonError("Specified task " + taskName + " not found",
                    axis2MessageContext, Constants.NOT_FOUND));
        }
    }

    private JSONObject convertTaskToJsonObject(TaskDescription task) {

        if (Objects.isNull(task)) {
            return null;
        }

        JSONObject taskObject = new JSONObject();

        taskObject.put(NAME, task.getName());

        String triggerType = "cron";

        if (Objects.isNull(task.getCronExpression())) {
            triggerType = "simple";
        }

        taskObject.put("triggerType", triggerType);

        taskObject.put("triggerCount", String.valueOf(task.getCount()));
        taskObject.put("triggerInterval", String.valueOf(task.getInterval()));
        taskObject.put("triggerCron", task.getCronExpression());

        return taskObject;
    }

    /**
     * Returns a String map of properties of the task.
     *
     * @param xmlProperties xml property set
     * @return Map
     */
    private Map<String, String> getProperties(Set<OMElement> xmlProperties) {
        Map<String, String> properties = new HashMap<>();
        Iterator<OMElement> propertiesItr = xmlProperties.iterator();

        while (propertiesItr.hasNext()) {
            OMElement propertyElem = propertiesItr.next();
            String propertyName = propertyElem.getAttributeValue(new QName("name"));
            OMAttribute valueAttr = propertyElem.getAttribute(new QName("value"));

            String value;
            if (valueAttr != null) {
                value = valueAttr.getAttributeValue();
            } else {
                value = propertyElem.getFirstElement().toString();
            }
            properties.put(propertyName, value);
        }
        return properties;
    }

    /**
     * Returns the Synapse environment from the axis configuration.
     *
     * @param axisCfg Axis configuration
     * @return SynapseEnvironment
     */
    private SynapseEnvironment getSynapseEnvironment(AxisConfiguration axisCfg) {

        return (SynapseEnvironment) axisCfg.getParameter(SynapseConstants.SYNAPSE_ENV).getValue();
    }

    /**
     * Returns the json representation of a given scheduled task.
     *
     * @param task Scheduled task
     * @return json representation of atsk
     */
    private JSONObject getTaskAsJson(TaskDescription task) {

        JSONObject taskObject = new JSONObject();

        taskObject.put(NAME, task.getName());
        taskObject.put("taskGroup", task.getTaskGroup());
        taskObject.put("implementation", task.getTaskImplClassName());
        String triggerType = "simple";

        if (task.getCronExpression() != null) {
            triggerType = "cron";
            taskObject.put("cronExpression", task.getCronExpression());
        } else {
            taskObject.put("triggerCount", String.valueOf(task.getCount()));
            taskObject.put("triggerInterval", String.valueOf(task.getInterval()));
        }
        taskObject.put("triggerType", triggerType);
        taskObject.put("properties", Utils.maskSecretParameters(getProperties(task.getXmlProperties())));
        taskObject.put(Constants.SYNAPSE_CONFIGURATION, Utils.maskSecretsInConfiguration(
                TaskDescriptionSerializer.serializeTaskDescription(null, task)));

        return taskObject;
    }

    /**
     * Handles the activation or deactivation of a schedule task based on the provided status.
     *
     * @param task           The task the operation is performed, used for audit logging.
     * @param performedBy    The user performing the operation, used for audit logging.
     * @param info           A JSON object containing additional audit information.
     * @param messageContext The current Synapse {@link MessageContext} for accessing the configuration.
     * @param payload        A {@link JsonObject} containing the task name and desired status.
     *
     * @return A {@link JSONObject} indicating the result of the operation. If successful, contains
     *         a confirmation message. If unsuccessful, contains an error message with appropriate
     *         HTTP error codes.
     */
    private JSONObject handleStatusUpdate(Startup task, String performedBy, JSONObject info,
                                          MessageContext messageContext, JsonObject payload) {
        String name = payload.get(NAME).getAsString();
        String status = payload.get(STATUS).getAsString();

        org.apache.axis2.context.MessageContext axis2MessageContext =
                ((Axis2MessageContext) messageContext).getAxis2MessageContext();
        JSONObject jsonResponse = new JSONObject();
        StartUpController controllerTask = null;
        if (task instanceof StartUpController) {
            controllerTask = (StartUpController)task;
        } else {
            return Utils.createJsonError("Task could not be found",
                    axis2MessageContext, Constants.NOT_FOUND);
        }

        if (INACTIVE_STATUS.equalsIgnoreCase(status)) {
            DynamicControlOperationResult result = controllerTask.deactivate();
            if (result.isSuccess()) {
                jsonResponse.put(Constants.MESSAGE_JSON_ATTRIBUTE, result.getMessage());
                AuditLogger.logAuditMessage(performedBy, Constants.AUDIT_LOG_TYPE_TASK,
                        Constants.AUDIT_LOG_ACTION_DISABLED, info);
            } else {
                jsonResponse = Utils.createJsonError(result.getMessage(), axis2MessageContext, Constants.INTERNAL_SERVER_ERROR);
            }
        } else if (ACTIVE_STATUS.equalsIgnoreCase(status)) {
            DynamicControlOperationResult result = controllerTask.activate();
            if (result.isSuccess()) {
                jsonResponse.put(Constants.MESSAGE_JSON_ATTRIBUTE, result.getMessage());
                AuditLogger.logAuditMessage(performedBy, Constants.AUDIT_LOG_TYPE_TASK,
                        Constants.AUDIT_LOG_ACTION_ENABLE, info);
            } else {
                jsonResponse = Utils.createJsonError(result.getMessage(), axis2MessageContext, Constants.INTERNAL_SERVER_ERROR);
            }
        } else if (TRIGGER_STATUS.equalsIgnoreCase(status)) {
            DynamicControlOperationResult result = controllerTask.trigger();
            if (result.isSuccess()) {
                jsonResponse.put(Constants.MESSAGE_JSON_ATTRIBUTE, result.getMessage());
                AuditLogger.logAuditMessage(performedBy, Constants.AUDIT_LOG_TYPE_TASK,
                        Constants.AUDIT_LOG_ACTION_TRIGGERED, info);
            } else {
                jsonResponse = Utils.createJsonError(result.getMessage(), axis2MessageContext, Constants.INTERNAL_SERVER_ERROR);
            }
        } else {
            jsonResponse = Utils.createJsonError("Provided state is not valid", axis2MessageContext, Constants.BAD_REQUEST);
        }

        return jsonResponse;
    }
}

/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.json.JSONObject;
import org.wso2.carbon.inbound.endpoint.internal.http.api.APIResource;
import org.wso2.micro.integrator.ntask.common.TaskException;
import org.wso2.micro.integrator.ntask.coordination.TaskCoordinationException;
import org.wso2.micro.integrator.ntask.coordination.task.store.TaskStore;
import org.wso2.micro.integrator.ntask.coordination.task.store.connector.RDMBSConnector;
import org.wso2.micro.integrator.ntask.core.TaskInfo;
import org.wso2.micro.integrator.ntask.core.impl.ScheduleFingerprintEncoder;
import org.wso2.micro.integrator.ntask.core.impl.standalone.ScheduledTaskManager;
import org.wso2.micro.integrator.ntask.core.internal.DataHolder;
import org.wso2.micro.integrator.ntask.core.internal.TaskHandlingConfigUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * POST /management/task-reconfigure?task=&lt;name&gt;&amp;expectedIncarnation=&lt;n&gt;&amp;expectedFp=&lt;hash&gt;
 * — the generation CHANGE (the offline-CApp-swap / parked-mismatch case). The operator reads the
 * expected tuple from /management/task-status first; the handling node computes the target fingerprint
 * from ITS deployed artifact, and an optional targetFp parameter, when supplied, must equal that
 * computed value or the request is refused.
 */
public class TaskReconfigureResource extends APIResource {

    private static final Log LOG = LogFactory.getLog(TaskReconfigureResource.class);
    private static final Pattern FINGERPRINT_PATTERN = Pattern.compile("^v1:sha256:[0-9a-f]{64}$");

    public TaskReconfigureResource(String urlTemplate) {
        super(urlTemplate);
    }

    @Override
    public Set<String> getMethods() {
        Set<String> methods = new HashSet<>();
        methods.add(Constants.HTTP_POST);
        return methods;
    }

    @Override
    public boolean invoke(MessageContext messageContext) {

        buildMessage(messageContext);
        org.apache.axis2.context.MessageContext axis2MessageContext =
                ((Axis2MessageContext) messageContext).getAxis2MessageContext();
        TaskStore taskStore = DataHolder.getInstance().getTaskStore();
        if (!TaskHandlingConfigUtils.isCoordinationHardeningEnabled() || taskStore == null) {
            respondError(axis2MessageContext, "503", "coordination-hardening-disabled",
                    "the coordination hardening master envelope is off or coordination is unavailable on this "
                            + "node", null);
            return true;
        }
        String task = Utils.getQueryParameter(messageContext, "task");
        String expectedIncarnationRaw = Utils.getQueryParameter(messageContext, "expectedIncarnation");
        String expectedScheduleFingerprint = Utils.getQueryParameter(messageContext, "expectedFp");
        String requestedTargetFingerprint = Utils.getQueryParameter(messageContext, "targetFp");
        if (task == null || task.isEmpty() || expectedIncarnationRaw == null || expectedScheduleFingerprint == null) {
            respondError(axis2MessageContext, Constants.BAD_REQUEST, "bad-request",
                    "task, expectedIncarnation and expectedFp are required", null);
            return true;
        }
        int expectedIncarnation;
        try {
            expectedIncarnation = Integer.parseInt(expectedIncarnationRaw.trim());
        } catch (NumberFormatException invalidIncarnation) {
            respondError(axis2MessageContext, Constants.BAD_REQUEST, "bad-request",
                    "expectedIncarnation must be an integer >= 1", null);
            return true;
        }
        if (expectedIncarnation < 1 || !FINGERPRINT_PATTERN.matcher(expectedScheduleFingerprint).matches()
                || (requestedTargetFingerprint != null && !FINGERPRINT_PATTERN.matcher(requestedTargetFingerprint).matches())) {
            respondError(axis2MessageContext, Constants.BAD_REQUEST, "bad-request",
                    "expectedIncarnation must be >= 1 and fingerprints must be full v1:sha256:<hex> values",
                    null);
            return true;
        }
        ScheduledTaskManager taskManager = DataHolder.getInstance().getTaskManager();
        if (taskManager == null) {
            respondError(axis2MessageContext, "503", "coordination-hardening-disabled",
                    "the coordinated task manager is not initialized on this node", null);
            return true;
        }
        TaskInfo taskInfo = null;
        try {
            taskInfo = taskManager.getTask(task);
        } catch (TaskException taskLookupFailure) {
            // Absence from this node is reported as a state conflict below.
            if (LOG.isDebugEnabled()) {
                LOG.debug("Task [" + task + "] is not present in this node repository.", taskLookupFailure);
            }
        }
        if (taskInfo == null || taskInfo.getTriggerInfo() == null) {
            JSONObject observed = new JSONObject();
            observed.put("localArtifactPresent", false);
            respondError(axis2MessageContext, "409", "state-conflict",
                    "this node does not carry the task's deployed definition; the target fingerprint is "
                            + "computed from the handling node's deployed artifact", observed);
            return true;
        }
        String computedTargetFingerprint = ScheduleFingerprintEncoder.encode(taskInfo.getTriggerInfo());
        String targetFamily = ScheduledTaskManager.deriveTriggerFamily(taskInfo.getTriggerInfo());
        if (requestedTargetFingerprint != null && !requestedTargetFingerprint.equals(computedTargetFingerprint)) {
            JSONObject observed = new JSONObject();
            observed.put("computedTargetFp", computedTargetFingerprint);
            respondError(axis2MessageContext, "409", "state-conflict",
                    "the supplied targetFp does not equal the fingerprint computed from this node's deployed "
                            + "artifact — the node does not carry the intended definition", observed);
            return true;
        }
        RDMBSConnector.ReconfigureResult reconfigureResult;
        try {
            reconfigureResult = taskStore.reconfigureTask(task, expectedIncarnation, expectedScheduleFingerprint,
                    computedTargetFingerprint, targetFamily);
        } catch (TaskCoordinationException reconfigureFailure) {
            LOG.error("The reconfigure of task [" + task + "] failed.", reconfigureFailure);
            respondError(axis2MessageContext, "503", "coordination-hardening-disabled",
                    "coordination is unavailable: " + reconfigureFailure.getMessage(), null);
            return true;
        }
        JSONObject responseBody = new JSONObject();
        switch (reconfigureResult.getOutcome()) {
        case RECONFIGURED:
            responseBody.put("task", task);
            responseBody.put("status", "RECONFIGURED");
            responseBody.put("oldFp", reconfigureResult.getOldFp());
            responseBody.put("newFp", reconfigureResult.getNewFp());
            responseBody.put("incarnation", reconfigureResult.getIncarnation());
            Utils.setJsonPayLoad(axis2MessageContext, responseBody);
            break;
        case ALREADY_APPLIED:
            responseBody.put("task", task);
            responseBody.put("status", "ALREADY_APPLIED");
            responseBody.put("incarnation", reconfigureResult.getIncarnation());
            Utils.setJsonPayLoad(axis2MessageContext, responseBody);
            break;
        case REOPENED:
            responseBody.put("task", task);
            responseBody.put("status", "REOPENED");
            responseBody.put("incarnation", reconfigureResult.getIncarnation());
            Utils.setJsonPayLoad(axis2MessageContext, responseBody);
            break;
        case NO_CHANGE:
            respondError(axis2MessageContext, "409", "no-change", reconfigureResult.getDetail(),
                    toObserved(reconfigureResult.getObserved()));
            break;
        case TRIGGER_FAMILY_IMMUTABLE:
            respondError(axis2MessageContext, "409", "trigger-family-immutable", reconfigureResult.getDetail(),
                    toObserved(reconfigureResult.getObserved()));
            break;
        case RETIRE_IN_PROGRESS:
            respondError(axis2MessageContext, "409", "retire-in-progress", reconfigureResult.getDetail(),
                    toObserved(reconfigureResult.getObserved()));
            break;
        case GUARD_ORPHAN:
            respondError(axis2MessageContext, "409", "guard-orphan", reconfigureResult.getDetail(),
                    toObserved(reconfigureResult.getObserved()));
            break;
        case UNKNOWN_TASK:
            respondError(axis2MessageContext, Constants.NOT_FOUND, "unknown-task", reconfigureResult.getDetail(), null);
            break;
        default:
            respondError(axis2MessageContext, "409", "state-conflict", reconfigureResult.getDetail(),
                    toObserved(reconfigureResult.getObserved()));
        }
        axis2MessageContext.removeProperty(Constants.NO_ENTITY_BODY);
        return true;
    }

    static JSONObject toObserved(Map<String, Object> observed) {

        if (observed == null) {
            return null;
        }
        JSONObject json = new JSONObject();
        for (Map.Entry<String, Object> entry : observed.entrySet()) {
            json.put(entry.getKey(), entry.getValue() == null ? JSONObject.NULL : entry.getValue());
        }
        return json;
    }

    static void respondError(org.apache.axis2.context.MessageContext axis2MessageContext, String statusCode,
                             String error, String detail, JSONObject observed) {

        JSONObject responseBody = new JSONObject();
        responseBody.put("error", error);
        responseBody.put("detail", detail == null ? JSONObject.NULL : detail);
        responseBody.put("observed", observed == null ? new JSONObject() : observed);
        axis2MessageContext.setProperty(Constants.HTTP_STATUS_CODE, statusCode);
        Utils.setJsonPayLoad(axis2MessageContext, responseBody);
        axis2MessageContext.removeProperty(Constants.NO_ENTITY_BODY);
    }
}

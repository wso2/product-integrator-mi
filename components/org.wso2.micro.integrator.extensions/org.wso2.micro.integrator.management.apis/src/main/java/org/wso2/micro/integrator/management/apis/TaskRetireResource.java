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
import org.json.JSONArray;
import org.json.JSONObject;
import org.wso2.carbon.inbound.endpoint.internal.http.api.APIResource;
import org.wso2.micro.integrator.coordination.ClusterCoordinator;
import org.wso2.micro.integrator.coordination.exception.ClusterCoordinationException;
import org.wso2.micro.integrator.ntask.coordination.TaskCoordinationException;
import org.wso2.micro.integrator.ntask.coordination.task.store.TaskStore;
import org.wso2.micro.integrator.ntask.coordination.task.store.connector.RDMBSConnector;
import org.wso2.micro.integrator.ntask.core.BootLeaseController;
import org.wso2.micro.integrator.ntask.core.internal.BootLeaseControllerImpl;
import org.wso2.micro.integrator.ntask.core.internal.DataHolder;
import org.wso2.micro.integrator.ntask.core.internal.TaskHandlingConfigUtils;
import org.wso2.micro.integrator.ntask.core.internal.TasksDSComponent;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

import static org.wso2.micro.integrator.management.apis.TaskReconfigureResource.respondError;
import static org.wso2.micro.integrator.management.apis.TaskReconfigureResource.toObserved;

/**
 * POST /management/task-retire?task=&lt;name&gt;&amp;expectedIncarnation=&lt;n&gt;&amp;operationId=&lt;uuid&gt;
 * — the operator-initiated delete wave: the sanctioned form of "remove the orphan row". The
 * caller-stable operationId BECOMES the wave's durable guard; completion and retry are reads of
 * state, keyed by the operation identity.
 */
public class TaskRetireResource extends APIResource {

    private static final Log LOG = LogFactory.getLog(TaskRetireResource.class);
    private static final Pattern UUID_PATTERN =
            Pattern.compile("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$");
    private static final int OPERATION_ID_MAX_UTF8_BYTES = 36;

    public TaskRetireResource(String urlTemplate) {
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
        ClusterCoordinator clusterCoordinator = DataHolder.getInstance().getClusterCoordinator();
        BootLeaseController bootLeaseController = TasksDSComponent.getBootLeaseController();
        if (!TaskHandlingConfigUtils.isCoordinationHardeningEnabled() || taskStore == null
                || clusterCoordinator == null || !(bootLeaseController instanceof BootLeaseControllerImpl)) {
            respondError(axis2MessageContext, "503", "coordination-hardening-disabled",
                    "the coordination hardening master envelope is off or coordination is unavailable on this "
                            + "node", null);
            return true;
        }
        String task = Utils.getQueryParameter(messageContext, "task");
        String expectedIncarnationRaw = Utils.getQueryParameter(messageContext, "expectedIncarnation");
        String operationIdRaw = Utils.getQueryParameter(messageContext, "operationId");
        if (task == null || task.isEmpty() || expectedIncarnationRaw == null || operationIdRaw == null) {
            respondError(axis2MessageContext, Constants.BAD_REQUEST, "bad-request",
                    "task, expectedIncarnation and operationId are required", null);
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
        // canonicalize operationId UUIDs to lowercase before compare/store; boundary validation only
        String operationId = operationIdRaw.trim().toLowerCase(Locale.ROOT);
        if (expectedIncarnation < 1
                || operationId.getBytes(StandardCharsets.UTF_8).length > OPERATION_ID_MAX_UTF8_BYTES
                || !UUID_PATTERN.matcher(operationId).matches()) {
            respondError(axis2MessageContext, Constants.BAD_REQUEST, "bad-request",
                    "expectedIncarnation must be >= 1 and operationId must be a canonical lowercase UUID of "
                            + "at most " + OPERATION_ID_MAX_UTF8_BYTES + " bytes", null);
            return true;
        }
        String groupId = ((BootLeaseControllerImpl) bootLeaseController).getGroupId();
        List<String> liveNodeIds;
        try {
            liveNodeIds = clusterCoordinator.getAllNodeIdsOrThrow();
        } catch (ClusterCoordinationException membershipReadFailure) {
            LOG.error("The live-member read for the retire of task [" + task + "] failed.", membershipReadFailure);
            respondError(axis2MessageContext, "503", "coordination-hardening-disabled",
                    "the live-member read failed; a retire wave must never be created against an unknown "
                            + "membership: " + membershipReadFailure.getMessage(), null);
            return true;
        }
        String localNodeId = DataHolder.getInstance().getLocalNodeId();
        List<String> unadvertisedNodeIds = new ArrayList<>();
        try {
            for (String memberNodeId : liveNodeIds) {
                if (memberNodeId.equals(localNodeId)) {
                    continue;
                }
                if (taskStore.getNodeAdvertisement(groupId, memberNodeId) == null) {
                    unadvertisedNodeIds.add(memberNodeId);
                }
            }
        } catch (TaskCoordinationException advertisementReadFailure) {
            LOG.error("The advertisement read for the retire of task [" + task + "] failed.", advertisementReadFailure);
            respondError(axis2MessageContext, "503", "coordination-hardening-disabled",
                    "coordination is unavailable: " + advertisementReadFailure.getMessage(), null);
            return true;
        }
        if (!unadvertisedNodeIds.isEmpty()) {
            // a wave that structurally cannot complete must not be creatable: an unpatched member can
            // never answer a retire wave — retire becomes available when every live member advertises
            JSONObject observed = new JSONObject();
            observed.put("nodes", new JSONArray(unadvertisedNodeIds));
            respondError(axis2MessageContext, "409", "unadvertised-members",
                    "live member(s) " + unadvertisedNodeIds + " advertise no boot lease and can never answer a "
                            + "retire wave; retry after the roll completes", observed);
            return true;
        }
        long currentTimeMillis = System.currentTimeMillis();
        long deadlineAt = currentTimeMillis + clusterCoordinator.getHeartbeatMaxRetryInterval();
        RDMBSConnector.RetireResult retireResult;
        try {
            retireResult = taskStore.retireTask(task, expectedIncarnation, operationId, liveNodeIds, deadlineAt,
                    currentTimeMillis);
        } catch (TaskCoordinationException retireFailure) {
            LOG.error("The retire of task [" + task + "] failed.", retireFailure);
            respondError(axis2MessageContext, "503", "coordination-hardening-disabled",
                    "coordination is unavailable: " + retireFailure.getMessage(), null);
            return true;
        }
        JSONObject responseBody = new JSONObject();
        switch (retireResult.getOutcome()) {
        case RETIRING_CREATED:
        case RETIRING_JOINED:
            responseBody.put("task", task);
            responseBody.put("operationId", operationId);
            responseBody.put("status", "RETIRING");
            axis2MessageContext.setProperty(Constants.HTTP_STATUS_CODE, "202");
            Utils.setJsonPayLoad(axis2MessageContext, responseBody);
            break;
        case RETIRED:
            responseBody.put("task", task);
            responseBody.put("operationId", operationId);
            responseBody.put("status", "RETIRED");
            Utils.setJsonPayLoad(axis2MessageContext, responseBody);
            break;
        case RETIRE_REFUSED_REPORTED:
            responseBody.put("task", task);
            responseBody.put("operationId", operationId);
            responseBody.put("status", "RETIRE_REFUSED");
            String dissentingNodeId = bestEffortDissenter(taskStore, task, operationId);
            responseBody.put("dissenter", dissentingNodeId == null ? JSONObject.NULL : dissentingNodeId);
            Utils.setJsonPayLoad(axis2MessageContext, responseBody);
            break;
        case UNKNOWN_TASK:
            respondError(axis2MessageContext, Constants.NOT_FOUND, "unknown-task", retireResult.getDetail(), null);
            break;
        case GUARD_ORPHAN:
            respondError(axis2MessageContext, "409", "state-conflict",
                    retireResult.getDetail() == null ? "guard-orphan observed — background recovery owns the repair"
                            : retireResult.getDetail(), toObserved(retireResult.getObserved()));
            break;
        default:
            respondError(axis2MessageContext, "409", "state-conflict", retireResult.getDetail(),
                    toObserved(retireResult.getObserved()));
        }
        axis2MessageContext.removeProperty(Constants.NO_ENTITY_BODY);
        return true;
    }

    /**
     * The dissenter, named best-effort from live-missing evidence — no durable dissenter field exists:
     * the dissenting node is in EXPECTED and never acked. Named only when unambiguous.
     */
    private String bestEffortDissenter(TaskStore taskStore, String task, String operationId) {

        try {
            List<String> candidates = new ArrayList<>(taskStore.getBarrierExpectedNodes(task, operationId));
            candidates.removeAll(taskStore.getBarrierAckNodes(task, operationId));
            return candidates.size() == 1 ? candidates.get(0) : null;
        } catch (Throwable readFailure) {
            return null;
        }
    }
}

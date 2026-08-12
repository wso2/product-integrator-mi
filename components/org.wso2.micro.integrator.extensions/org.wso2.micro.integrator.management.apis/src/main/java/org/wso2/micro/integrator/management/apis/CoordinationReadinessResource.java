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

import org.apache.synapse.MessageContext;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.json.JSONArray;
import org.json.JSONObject;
import org.wso2.carbon.inbound.endpoint.internal.http.api.APIResource;
import org.wso2.config.mapper.ConfigParser;
import org.wso2.micro.integrator.ntask.core.BootLeaseController;
import org.wso2.micro.integrator.ntask.core.CoordinationReadinessRegistry;
import org.wso2.micro.integrator.ntask.core.impl.EpisodeLog;
import org.wso2.micro.integrator.ntask.core.internal.BootLeaseControllerImpl;
import org.wso2.micro.integrator.ntask.core.internal.DataHolder;
import org.wso2.micro.integrator.ntask.core.internal.TasksDSComponent;

import java.util.HashSet;
import java.util.Set;

/**
 * GET /management/coordination-readiness — the node-local readiness owner's view. The endpoint exists
 * regardless of the master envelope: on a coordination_hardening=false node it reports the single
 * condition hardening-disabled, so the ALL-GREEN activation runbook automatically refuses activation
 * while any master-OFF node is in the cluster. The registry is node-local and honest about it.
 *
 * <pre>
 * GET /management/coordination-readiness                -> {"node", "green", "conditions":[...]}
 * GET /management/coordination-readiness?view=counters  -> the episode limiter counters
 * GET /management/coordination-readiness?view=liveness  -> the Liveness Probe result — explicitly
 *     includes the terminal lease state, with an intentional grace period (one advertised heartbeat
 *     window) before "live" turns false
 * </pre>
 */
public class CoordinationReadinessResource extends APIResource {

    public CoordinationReadinessResource(String urlTemplate) {
        super(urlTemplate);
    }

    @Override
    public Set<String> getMethods() {
        Set<String> methods = new HashSet<>();
        methods.add(Constants.HTTP_GET);
        return methods;
    }

    @Override
    public boolean invoke(MessageContext messageContext) {

        buildMessage(messageContext);
        org.apache.axis2.context.MessageContext axis2MessageContext =
                ((Axis2MessageContext) messageContext).getAxis2MessageContext();
        String requestedView = Utils.getQueryParameter(messageContext, "view");
        JSONObject responseBody;
        if ("counters".equalsIgnoreCase(requestedView)) {
            responseBody = counters();
        } else if ("liveness".equalsIgnoreCase(requestedView)) {
            responseBody = liveness();
        } else {
            responseBody = readiness();
        }
        Utils.setJsonPayLoad(axis2MessageContext, responseBody);
        axis2MessageContext.removeProperty(Constants.NO_ENTITY_BODY);
        return true;
    }

    private JSONObject readiness() {

        CoordinationReadinessRegistry registry = TasksDSComponent.getReadinessRegistry();
        JSONArray conditions = new JSONArray();
        for (CoordinationReadinessRegistry.Condition condition : registry.snapshot()) {
            JSONObject entry = new JSONObject();
            entry.put("name", condition.getName());
            entry.put("clazz", condition.getClazz().name());
            entry.put("detail", condition.getDetail());
            entry.put("sinceEpochMillis", condition.getSinceEpochMillis());
            conditions.put(entry);
        }
        JSONObject responseBody = new JSONObject();
        putNode(responseBody);
        responseBody.put("green", registry.green());
        responseBody.put("conditions", conditions);
        return responseBody;
    }

    private JSONObject counters() {

        JSONArray counters = new JSONArray();
        for (EpisodeLog.CounterView counter : EpisodeLog.snapshotCounters()) {
            JSONObject entry = new JSONObject();
            entry.put("taskName", counter.getTaskName());
            entry.put("reason", counter.getReason());
            entry.put("count", counter.getCount());
            entry.put("firstAtEpochMillis", counter.getFirstAt());
            entry.put("open", counter.isOpen());
            counters.put(entry);
        }
        JSONObject responseBody = new JSONObject();
        putNode(responseBody);
        responseBody.put("counters", counters);
        return responseBody;
    }

    private JSONObject liveness() {

        JSONObject responseBody = new JSONObject();
        putNode(responseBody);
        BootLeaseController bootLeaseController = TasksDSComponent.getBootLeaseController();
        if (!(bootLeaseController instanceof BootLeaseControllerImpl)) {
            // no lease machinery on this node (STOCK/REFUSING): the probe has nothing to restart for
            responseBody.put("live", true);
            responseBody.put("leaseState", JSONObject.NULL);
            return responseBody;
        }
        BootLeaseControllerImpl lease = (BootLeaseControllerImpl) bootLeaseController;
        long gracePeriodMillis = lease.advertisedWindowMillis();
        boolean terminalLeaseState = lease.isTerminal();
        long terminalSinceEpochMillis = lease.terminalSinceEpochMillis();
        boolean graceExpired = terminalLeaseState && terminalSinceEpochMillis > 0
                && System.currentTimeMillis() - terminalSinceEpochMillis > gracePeriodMillis;
        // the "scheduler cycle recently completed" input (used by both readiness and pause detection): the Ownership
        // Sweep's completion stamp of the coordinated scheduler. A stopped scheduler under a
        // non-PROVEN lease is a lease episode with its own exits and stays exempt; a stopped
        // scheduler under a PROVEN lease has no lease exit to restart it, so past the grace it is
        // dead, not lapsing — not live.
        DataHolder dataHolder = DataHolder.getInstance();
        boolean schedulerRunning = dataHolder.getTaskScheduler() != null;
        boolean leaseProven = "PROVEN".equals(lease.leaseStateName());
        long sweepStampNanos = dataHolder.getLastSweepCompletionNanos();
        long cycleAgeMillis = sweepStampNanos == 0 ? -1
                : (System.nanoTime() - sweepStampNanos) / 1_000_000;
        boolean cycleGraceExpired = (schedulerRunning || leaseProven) && sweepStampNanos != 0
                && cycleAgeMillis > gracePeriodMillis;
        responseBody.put("live", !(graceExpired || cycleGraceExpired));
        responseBody.put("leaseState", lease.leaseStateName());
        responseBody.put("renewalAgeMillis", lease.renewalAgeMillis());
        responseBody.put("advertisedWindowMillis", lease.advertisedWindowMillis());
        JSONObject terminalView = new JSONObject();
        terminalView.put("terminal", terminalLeaseState);
        terminalView.put("sinceEpochMillis", terminalSinceEpochMillis > 0 ? terminalSinceEpochMillis : JSONObject.NULL);
        terminalView.put("gracePeriodMillis", gracePeriodMillis);
        terminalView.put("graceExpired", graceExpired);
        responseBody.put("terminalLeaseState", terminalView);
        JSONObject cycleView = new JSONObject();
        cycleView.put("schedulerRunning", schedulerRunning);
        cycleView.put("lastCompletedAgeMillis", cycleAgeMillis < 0 ? JSONObject.NULL : cycleAgeMillis);
        cycleView.put("gracePeriodMillis", gracePeriodMillis);
        cycleView.put("graceExpired", cycleGraceExpired);
        responseBody.put("schedulerCycle", cycleView);
        return responseBody;
    }

    private void putNode(JSONObject responseBody) {

        String nodeId = DataHolder.getInstance().getLocalNodeId();
        if (nodeId == null) {
            nodeId = resolveConfiguredNodeId();
        }
        responseBody.put("node", nodeId == null ? JSONObject.NULL : nodeId);
    }

    /**
     * A node with no cluster coordinator (REFUSING_PROFILE_INVALID, or stock without a coordination
     * datasource) still has its configured identity: the same sysprop -> env -> cluster_config.node_id
     * chain the coordinator itself reads, minus its random-id fallback — an id invented per request
     * would name nothing.
     */
    private String resolveConfiguredNodeId() {

        String nodeId = System.getProperty("nodeId");
        if (nodeId == null || nodeId.isEmpty()) {
            nodeId = System.getenv("nodeId");
        }
        if (nodeId == null || nodeId.isEmpty()) {
            Object configuredNodeIdValue = ConfigParser.getParsedConfigs().get("cluster_config.node_id");
            nodeId = configuredNodeIdValue == null ? null : configuredNodeIdValue.toString();
        }
        return nodeId == null || nodeId.isEmpty() ? null : nodeId;
    }
}

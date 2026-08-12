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

package org.wso2.micro.integrator.ntask.coordination.task;

import org.wso2.micro.integrator.ntask.coordination.TaskCoordinationException;
import org.wso2.micro.integrator.ntask.coordination.task.store.TaskStore;
import org.wso2.micro.integrator.ntask.coordination.task.store.connector.RDMBSConnector;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Barrier Liveness classification — dead is not dissenting. The deadline CLASSIFIES the missing
 * nodes; it never authorizes a delete. Exact definitions are part of the safety contract, because a misclassification
 * cannot duplicate a body entry (Claim Gate) but CAN cause destructive state loss — this is a
 * state-integrity decision, not telemetry:
 * <ul>
 * <li>DEAD — the node's heartbeat age exceeds its effective window: effectiveWindow(P) =
 * max(myConfiguredWindow, P.advertisedWindow) (a missing advertisement row falls back to the local
 * window — status quo). Heartbeat age alone never authorizes anything except this consent; there is
 * no separate "timeout" branch.</li>
 * <li>BOOT_REPLACED — the node's NODE_ADVERTISEMENT.BOOT_STARTED_AT is later than the wave's
 * creation instant (synchronized clocks are a precondition — both are node-written timestamps under
 * the ±500 ms NTP premise). Its successor's artifacts are governed by registration, not by this
 * wave.</li>
 * <li>LIVE (same boot, or ANY liveness/boot read failure — a node in its join-grace window is judged
 * by these same rules) — holdout: no delete, no unassign, no write. The wave stays OPEN.</li>
 * </ul>
 * now = System.currentTimeMillis() computed in Java and passed as a parameter — the recorded
 * never-DB-time decision.
 */
public final class BarrierLivenessClassifier {

    public enum NodeClass { DEAD, BOOT_REPLACED, LIVE }

    private final TaskStore taskStore;
    private final String groupId;
    private final long localWindowMillis;

    public BarrierLivenessClassifier(TaskStore taskStore, String groupId, long localWindowMillis) {

        this.taskStore = taskStore;
        this.groupId = groupId;
        this.localWindowMillis = localWindowMillis;
    }

    /**
     * The pure classification decision for one missing node. The existing barrier table
     * carries no CREATED_AT column, so the BOOT_REPLACED comparison uses the wave's durable creation
     * upper bound min(UPDATED_AT, DEADLINE_AT) — both are always at or after the creation instant, so
     * this can withhold a consent briefly but never grant a wrong one.
     *
     * @param lastHeartbeatMillis  the peer's LAST_HEARTBEAT, or null when the membership layer holds
     *                             no row for it (already evicted after its window — heartbeat age has
     *                             exceeded every window)
     * @param advertisement        the peer's advertisement row, or null when unadvertised
     *                             (unpatched/master-off peer — local-window fallback)
     * @param waveCreatedUpperBound durable upper bound of the wave's creation instant
     * @param localWindowMillis    this judge's configured window
     * @param now                  app-clock epoch millis, computed in Java by the caller
     * @return the classification; consent = DEAD or BOOT_REPLACED, holdout = LIVE
     */
    public static NodeClass classifyNode(Long lastHeartbeatMillis, RDMBSConnector.NodeAdvertisement advertisement,
                                         long waveCreatedUpperBound, long localWindowMillis, long now) {

        if (advertisement != null && advertisement.getBootStartedAt() > waveCreatedUpperBound) {
            return NodeClass.BOOT_REPLACED;
        }
        long effectiveWindow = advertisement == null ? localWindowMillis
                : Math.max(localWindowMillis, advertisement.getHeartbeatWindow());
        if (lastHeartbeatMillis == null || now - lastHeartbeatMillis > effectiveWindow) {
            return NodeClass.DEAD;
        }
        return NodeClass.LIVE;
    }

    /**
     * Advisory classification of every EXPECTED node without an ack, on the store's own connections
     * (the authoritative reads the finalize decision stands on run inside the finalize transaction;
     * this pre-check informs logging and the skip-wait hint only). Any read failure classifies that
     * node LIVE — fail toward holdout.
     *
     * @return missing node id to classification; empty when the wave is gone or fully acked
     */
    public Map<String, NodeClass> classifyMissing(String taskName, String guardUuid, long now)
            throws TaskCoordinationException {

        RDMBSConnector.DeleteBarrierView wave = taskStore.getDeleteBarrier(taskName, guardUuid);
        if (wave == null) {
            return Collections.emptyMap();
        }
        long waveCreatedUpperBound = Math.min(wave.getUpdatedAt(), wave.getDeadlineAt());
        List<String> expected = taskStore.getBarrierExpectedNodes(taskName, guardUuid);
        Set<String> acked = new HashSet<>(taskStore.getBarrierAckNodes(taskName, guardUuid));
        Map<String, NodeClass> classification = new LinkedHashMap<>();
        for (String node : expected) {
            if (acked.contains(node)) {
                continue;
            }
            try {
                RDMBSConnector.NodeAdvertisement advertisement = taskStore.getNodeAdvertisement(groupId, node);
                Long lastHeartbeat = taskStore.getPeerLastHeartbeat(groupId, node);
                classification.put(node,
                        classifyNode(lastHeartbeat, advertisement, waveCreatedUpperBound, localWindowMillis, now));
            } catch (Throwable readFailure) {
                classification.put(node, NodeClass.LIVE);
            }
        }
        return classification;
    }

}

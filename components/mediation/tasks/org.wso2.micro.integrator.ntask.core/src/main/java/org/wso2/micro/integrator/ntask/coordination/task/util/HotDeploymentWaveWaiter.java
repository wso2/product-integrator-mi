/*
 * Copyright (c) 2026, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.micro.integrator.ntask.coordination.task.util;

import org.apache.commons.logging.Log;
import org.wso2.micro.integrator.coordination.ClusterCoordinator;
import org.wso2.micro.integrator.ntask.coordination.TaskCoordinationException;
import org.wso2.micro.integrator.ntask.coordination.task.store.TaskStore;

/**
 * Shared wait helper to pause operations until the current hot deployment wave settles.
 */
public final class HotDeploymentWaveWaiter {

    private static final long HOT_DEPLOYMENT_SETTLE_BUFFER_MILLIS = 5000L;
    private static final long WAIT_POLL_INTERVAL_MILLIS = 200L;

    private HotDeploymentWaveWaiter() {
    }

    /**
     * Waits until hot deployment activity becomes settled before proceeding with cleanup.
     * A one time delay is insufficient because guard timestamps may continue to move while waiting.
     *
     * @param taskStore       task store
     * @param clusterCoordinator cluster coordinator
     * @param log             logger
     * @param operationName   operation name for logs
     * @throws TaskCoordinationException when DB operations fail
     */
    public static void waitForHotDeploymentWaveToSettle(TaskStore taskStore, ClusterCoordinator clusterCoordinator
            , Log log, String operationName) throws TaskCoordinationException {
        long heartbeatMaxRetryInterval = clusterCoordinator.getHeartbeatMaxRetryInterval();
        long quietWindowMillis = heartbeatMaxRetryInterval + HOT_DEPLOYMENT_SETTLE_BUFFER_MILLIS;
        long initialNow = System.currentTimeMillis();
        long initialLatestGuardUpdatedAt = taskStore.getLatestDeleteGuardUpdatedAt();
        if (initialLatestGuardUpdatedAt <= 0
                || (initialNow - initialLatestGuardUpdatedAt) >= quietWindowMillis) {
            log.info("No recent hot deployment wave detected for " + operationName + ". Proceeding without wait.");
            return;
        }

        long startedAt = System.currentTimeMillis();
        long maxWaitMillis = quietWindowMillis + 3 * heartbeatMaxRetryInterval;
        Long previousLatestGuardUpdatedAt = null;

        log.info("Waiting to start " + operationName + " until hot deployment wave is settled. "
                + "Minimum wait [" + quietWindowMillis + "] ms, max wait [" + maxWaitMillis + "] ms.");
        while (true) {
            long now = System.currentTimeMillis();
            long elapsed = now - startedAt;
            long latestGuardUpdatedAt = taskStore.getLatestDeleteGuardUpdatedAt();
            long sinceLatestGuardUpdate = now - latestGuardUpdatedAt;

            if (previousLatestGuardUpdatedAt == null || previousLatestGuardUpdatedAt != latestGuardUpdatedAt) {
                log.info("Latest hot deployment wave signal timestamp changed while waiting for " + operationName
                        + ". Previous [" + previousLatestGuardUpdatedAt + "], current ["
                        + latestGuardUpdatedAt + "], elapsed [" + elapsed + "] ms.");
                previousLatestGuardUpdatedAt = latestGuardUpdatedAt;
            }

            boolean minimumWaitSatisfied = elapsed >= quietWindowMillis;
            boolean guardQuietSatisfied = latestGuardUpdatedAt <= 0 || sinceLatestGuardUpdate >= quietWindowMillis;

            if (minimumWaitSatisfied && guardQuietSatisfied) {
                log.info("Hot deployment wave settled for " + operationName + ". Waited [" + elapsed
                        + "] ms. Latest guard updated at [" + latestGuardUpdatedAt + "].");
                return;
            }
            if (elapsed >= maxWaitMillis) {
                log.warn("Max wait time [" + maxWaitMillis + "] ms exceeded for " + operationName
                        + ". Proceeding after [" + elapsed + "] ms despite unsettled hot deployment wave.");
                return;
            }
            if (!sleepWithInterruptHandling(operationName, log)) {
                return;
            }
        }
    }

    private static boolean sleepWithInterruptHandling(String operationName, Log log) {
        try {
            Thread.sleep(WAIT_POLL_INTERVAL_MILLIS);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while waiting for hot deployment wave to settle for " + operationName + ".");
            return false;
        }
    }
}

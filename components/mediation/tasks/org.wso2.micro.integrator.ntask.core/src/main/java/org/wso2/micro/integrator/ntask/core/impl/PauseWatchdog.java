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

package org.wso2.micro.integrator.ntask.core.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.micro.integrator.coordination.ClusterCoordinator;
import org.wso2.micro.integrator.ntask.core.BootLeaseController;
import org.wso2.micro.integrator.ntask.core.internal.BootLeaseControllerImpl;
import org.wso2.micro.integrator.ntask.core.internal.DataHolder;
import org.wso2.micro.integrator.ntask.core.internal.TaskHandlingConfigUtils;
import org.wso2.micro.integrator.ntask.core.internal.TasksDSComponent;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Pause Watchdog — monotonic freeze detection. Uses a monotonic one-second ticker to detect JVM pauses
 * before refreshing its timestamp. A pause longer than
 * the warning margin signals the boot-lease controller, whose LAPSED transition owns the dispatch veto, scheduler
 * shutdown, and recovery. The watchdog also observes whether ownership reconciliation has stopped completing; that
 * signal follows the same lease-controlled recovery path.
 * The watchdog clears its own suspended flag when the machine reports PROVEN.
 * {@link #isStale()} is telemetry only — never read by dispatch.
 */
public class PauseWatchdog {

    private static final Log LOG = LogFactory.getLog(PauseWatchdog.class);
    private static final String STALL_EPISODE_KEY = "coordinated-task-scheduler";
    private static final String STALL_EPISODE_REASON = "CYCLE_STALLED";

    private final AtomicLong lastTickNanos = new AtomicLong(System.nanoTime());
    private volatile boolean suspended = false;
    private volatile boolean stopped = false;
    private final ClusterCoordinator clusterCoordinator;
    private final EpisodeLog episodeLog;
    private final Thread ticker;
    // ticker-thread-only: tracks an open stall episode so its clearing INFO fires once
    private boolean stallEpisodeOpen = false;

    public PauseWatchdog(ClusterCoordinator clusterCoordinator) {

        this.clusterCoordinator = clusterCoordinator;
        this.episodeLog = new EpisodeLog(clusterCoordinator.getThisNodeId());
        this.ticker = new Thread(new Runnable() {
            @Override
            public void run() {
                tickLoop();
            }
        }, "task-pause-watchdog");
        this.ticker.setDaemon(true);
    }

    public void start() {

        ticker.start();
    }

    public void stop() {

        stopped = true;
        ticker.interrupt();
    }

    /**
     * Telemetry + probe/latch use ONLY — never read by dispatch: the watchdog's own suspend flag
     * plus its computed monotonic tick age.
     */
    public boolean isStale() {

        return suspended
                || (System.nanoTime() - lastTickNanos.get()) / 1_000_000 > thresholdMs();
    }

    private void tickLoop() {

        while (!stopped) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            try {
                tick();
            } catch (Throwable t) {
                LOG.warn("Pause Watchdog tick failed; the ticker continues.", t);
            }
        }
    }

    private void tick() {

        long now = System.nanoTime();
        long gapMs = (now - lastTickNanos.get()) / 1_000_000;
        if (gapMs > thresholdMs()) {          // check BEFORE refreshing the tick
            suspended = true;                 // latch FIRST — vetoes see it either way
            onLapse(gapMs);
        }
        lastTickNanos.set(now);
        if (suspended && machineReportsProven()) {
            suspended = false;
        }
        if (TaskHandlingConfigUtils.isCoordinationHardeningEnabled()) {
            checkReconcileStall();
        }
    }

    /**
     * Mirrors heartbeatWarningMargin (coord/RDBMSCoordinationStrategy).
     */
    private long thresholdMs() {

        return (long) (0.75 * clusterCoordinator.getHeartbeatMaxRetryInterval());
    }

    private void onLapse(long gapMs) {

        LOG.error("Pause Watchdog: JVM freeze detected — the monotonic tick is " + gapMs
                + " ms old (threshold " + thresholdMs() + " ms). Signaling the boot lease controller's "
                + "lapse path; dispatch resumes only via its RECOVERING->PROVEN exit.");
        signalControllerLapse("the Pause Watchdog detected a JVM freeze: monotonic tick gap " + gapMs + " ms");
    }

    /**
     * The Reconcile-Stall Latch: DB-free, observes the Ownership Sweep's completion stamp of the
     * RUNNING coordinated scheduler (the stamp re-initializes at every scheduler start; with the
     * scheduler shut down there is no cycle to observe). Recovers automatically when a cycle
     * completes; a permanently hung cycle is ended by the Liveness Probe restarting the pod.
     */
    private void checkReconcileStall() {

        DataHolder dataHolder = DataHolder.getInstance();
        long stamp = dataHolder.getLastSweepCompletionNanos();
        if (stamp == 0 || dataHolder.getTaskScheduler() == null) {
            return;
        }
        long stallMs = (System.nanoTime() - stamp) / 1_000_000;
        if (stallMs > thresholdMs()) {
            stallEpisodeOpen = true;
            episodeLog.episodeWarn(STALL_EPISODE_KEY, STALL_EPISODE_REASON,
                    "coordinated scheduler cycle stalled " + (stallMs / 1000) + "s");
            signalControllerLapse("the Reconcile-Stall Latch: coordinated scheduler cycle stalled "
                    + (stallMs / 1000) + "s");
        } else if (stallEpisodeOpen) {
            stallEpisodeOpen = false;
            episodeLog.cleared(STALL_EPISODE_KEY);
        }
    }

    // the controller slot is read AT CALL TIME — it is per component activation
    private void signalControllerLapse(String reason) {

        BootLeaseController controller = TasksDSComponent.getBootLeaseController();
        if (controller instanceof BootLeaseControllerImpl) {
            ((BootLeaseControllerImpl) controller).signalLapse(reason);
        }
    }

    private boolean machineReportsProven() {

        BootLeaseController controller = TasksDSComponent.getBootLeaseController();
        return controller instanceof BootLeaseControllerImpl
                && "PROVEN".equals(((BootLeaseControllerImpl) controller).leaseStateName());
    }

}

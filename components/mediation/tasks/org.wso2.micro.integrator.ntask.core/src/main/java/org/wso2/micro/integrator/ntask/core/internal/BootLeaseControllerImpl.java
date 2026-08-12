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

package org.wso2.micro.integrator.ntask.core.internal;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.micro.integrator.coordination.ClusterCoordinator;
import org.wso2.micro.integrator.coordination.HeartbeatHook;
import org.wso2.micro.integrator.coordination.exception.ClusterCoordinationException;
import org.wso2.micro.integrator.ntask.coordination.TaskCoordinationException;
import org.wso2.micro.integrator.ntask.coordination.task.CoordinatedTask;
import org.wso2.micro.integrator.ntask.coordination.task.resolver.TaskLocationResolver;
import org.wso2.micro.integrator.ntask.coordination.task.store.TaskStore;
import org.wso2.micro.integrator.ntask.coordination.task.store.connector.RDMBSConnector;
import org.wso2.micro.integrator.ntask.core.BootLeaseController;
import org.wso2.micro.integrator.ntask.core.BootPassHandle;
import org.wso2.micro.integrator.ntask.core.CoordinationReadinessRegistry;
import org.wso2.micro.integrator.ntask.core.impl.standalone.ScheduledTaskManager;
import org.wso2.micro.integrator.ntask.core.service.TaskService;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.SQLTransactionRollbackException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.ELIGIBILITY_ELIGIBLE;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.ELIGIBILITY_LAPSED;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.ELIGIBILITY_RECOVERING;

/**
 * The one boot lease controller implementation — owned by TasksDSComponent's coordination wiring and
 * constructed per component activation (a fresh controller is a fresh lease generation scope). Also
 * implements the coordination-owned HeartbeatHook: afterHeartbeat delegates to renewOnHeartbeat() and
 * isCoordinatorParticipationAllowed() returns !isDispatchBlocked() — the single dispatch predicate, no
 * second state computation.
 */
public class BootLeaseControllerImpl implements BootLeaseController, HeartbeatHook {

    private static final Log LOG = LogFactory.getLog(BootLeaseControllerImpl.class);

    // The skew contract: all lease/liveness math uses local epoch millis under the NTP ops
    // requirement, stated as a number — maximum pairwise skew 500 ms.
    private static final long SKEW_ALLOWANCE_MILLIS = 500L;
    // The narrowest NODE_ID column in the coordination schema (the barrier expected/ack tables) —
    // the node-ID width preflight bound.
    private static final int NODE_ID_MAX_UTF8_BYTES = 220;

    private static final String NON_PROVEN_LEASE_STATE_CONDITION = "non-proven-lease-state";
    private static final String BOOT_PASS_INCOMPLETE_CONDITION = "boot-pass-incomplete";
    private static final String TIMING_VALIDATION_CONDITION = "timing-validation";
    private static final String CONFIG_FENCE_CONVERGENCE_CONDITION = "config-fence-correctness-convergence";
    private static final String SEEDING_PARITY_CONDITION = "seeding-parity";
    private static final String STANDBY_HOLDS_COORDINATORSHIP_CONDITION = "standby-holds-coordinatorship";

    private enum LeaseState { UNPROVEN, PROVEN, LAPSED, RECOVERING, TERMINAL }

    private static final class Generation {

        private final String bootId;
        private final long bootStartedAt;
        private final Set<String> typesPassed = ConcurrentHashMap.newKeySet();
        private final Map<String, String> recordedFailures = new ConcurrentHashMap<>();
        private volatile boolean completed;

        private Generation(String bootId, long bootStartedAt) {
            this.bootId = bootId;
            this.bootStartedAt = bootStartedAt;
        }
    }

    private final TaskStore taskStore;
    private final TaskService taskService;
    private final ClusterCoordinator clusterCoordinator;
    private final TaskLocationResolver resolver;
    private final String groupId;
    private final String nodeId;
    private final String configFingerprint;

    private final long advertisedWindowMillis;
    private final long takeoverMarginMillis;
    private final long takeoverMarginNanos;
    private final long renewalIntervalMillis;

    private final Object stateLock = new Object();
    private volatile LeaseState state = LeaseState.UNPROVEN;
    private volatile Generation generation;
    private int lapseEpoch;
    private long nextRecoveryDelayMillis;

    // every boot id this process has successfully written into its advertisement row, adopted or not.
    // An acquisition that writes and is then refused (freshness margin, adoption-guard race) orphans
    // its row; recovery must recognize that token as its own candidate, never another boot's.
    private final Set<String> mintedBootIds = ConcurrentHashMap.newKeySet();

    private volatile boolean everRenewed;
    private volatile long lastSuccessfulRenewalNanos;

    // the terminal lease state's entry instant (app clock) — the Liveness Probe result explicitly
    // includes the terminal lease state with an intentional grace period measured from here
    private volatile long terminalSinceEpochMillis;

    private final Object mintLock = new Object();
    private long lastPublishedUpdatedAt;

    private volatile boolean configConvergenceReached;

    private final ScheduledExecutorService controllerExecutor;
    private volatile boolean stopped;

    public BootLeaseControllerImpl(TaskStore taskStore, TaskService taskService,
                                   CoordinationHardeningConfig config, ClusterCoordinator clusterCoordinator,
                                   TaskLocationResolver resolver, String groupId, String nodeId,
                                   String configFingerprint) throws TaskCoordinationException {

        this.taskStore = taskStore;
        this.taskService = taskService;
        this.clusterCoordinator = clusterCoordinator;
        this.resolver = resolver;
        this.groupId = groupId;
        this.nodeId = nodeId;
        this.configFingerprint = configFingerprint;

        // Timing-domain validation: startup, overflow-safe, before lease acquisition. Invalid values
        // refuse readiness rather than produce a zero/negative margin or an overflowed expiry.
        long interval = config.getHeartbeatIntervalMillis();
        int retries = config.getHeartbeatMaxRetries();
        try {
            if (interval <= 0 || retries < 1) {
                throw refuseTiming("positive heartbeat/renewal intervals are required (heartbeat interval ["
                        + interval + "] ms, max retries [" + retries + "])");
            }
            long window = Math.multiplyExact(interval, (long) retries);
            if (window <= SKEW_ALLOWANCE_MILLIS) {
                throw refuseTiming("advertised heartbeat window [" + window + "] ms must exceed the skew allowance ["
                        + SKEW_ALLOWANCE_MILLIS + "] ms");
            }
            long margin = Math.subtractExact(window, SKEW_ALLOWANCE_MILLIS);
            if (interval > margin / 2) {
                throw refuseTiming("renewal interval [" + interval + "] ms must not exceed half the takeover margin ["
                        + margin + "] ms");
            }
            // the expiry threshold must be computable without overflow
            Math.addExact(window, SKEW_ALLOWANCE_MILLIS);
            this.advertisedWindowMillis = window;
            this.takeoverMarginMillis = margin;
            this.takeoverMarginNanos = Math.multiplyExact(margin, 1_000_000L);
            this.renewalIntervalMillis = interval;
        } catch (ArithmeticException ex) {
            throw refuseTiming("lease timing thresholds overflow with heartbeat interval [" + interval
                    + "] ms and max retries [" + retries + "]");
        }
        this.nextRecoveryDelayMillis = renewalIntervalMillis;
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("BootLeaseController-%d").build();
        this.controllerExecutor = Executors.newSingleThreadScheduledExecutor(namedThreadFactory);
        TasksDSComponent.getReadinessRegistry().raise(NON_PROVEN_LEASE_STATE_CONDITION,
                CoordinationReadinessRegistry.ConditionClass.CONTINUOUS,
                "lease state UNPROVEN — every coordinated fire is vetoed until the boot pass completes");
    }

    private TaskCoordinationException refuseTiming(String message) {

        TasksDSComponent.getReadinessRegistry().raise(TIMING_VALIDATION_CONDITION,
                CoordinationReadinessRegistry.ConditionClass.CONTINUOUS, message);
        return new TaskCoordinationException("Lease timing-domain validation refused readiness: " + message);
    }

    @Override
    public void acquire() throws TaskCoordinationException {

        if (stopped) {
            throw new TaskCoordinationException("The boot lease controller is stopped.");
        }
        preflightNodeIdWidth();
        Generation freshGeneration = acquisitionLoop();
        synchronized (stateLock) {
            generation = freshGeneration;
        }
        LOG.info("Boot lease acquired for node [" + nodeId + "] in group [" + groupId + "]: boot id ["
                + freshGeneration.bootId + "], row PENDING.");
    }

    /**
     * The node-ID width preflight: the narrowest NODE_ID column the coordination schema stores this id
     * in is VARCHAR(220) (the barrier expected/ack tables) — refuse before the lease, never truncate.
     */
    private void preflightNodeIdWidth() throws TaskCoordinationException {

        if (nodeId == null || nodeId.isEmpty()) {
            throw new TaskCoordinationException("The node id is empty; refusing boot lease acquisition.");
        }
        int bytes = nodeId.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > NODE_ID_MAX_UTF8_BYTES) {
            String message = "Node id [" + nodeId + "] is [" + bytes + "] UTF-8 bytes — longer than the narrowest "
                    + "NODE_ID column (VARCHAR(" + NODE_ID_MAX_UTF8_BYTES + ")). Refusing boot lease acquisition.";
            LOG.error(message);
            throw new TaskCoordinationException(message);
        }
    }

    /**
     * Retries internally when another process wins an insert or takeover race. A live process with the same node
     * identifier, an invalid row, or a database failure refuses acquisition and leaves coordinated scheduling stopped.
     * Recovery uses this same loop when the advertisement row is absent, producing a new boot generation.
     */
    private Generation acquisitionLoop() throws TaskCoordinationException {

        while (true) {
            if (stopped) {
                throw new TaskCoordinationException("The boot lease controller is stopped.");
            }
            RDMBSConnector.NodeAdvertisement row = taskStore.getNodeAdvertisement(groupId, nodeId);
            long now = System.currentTimeMillis();
            if (row == null) {
                String bootId = UUID.randomUUID().toString();
                long operationStartNanos = System.nanoTime();
                boolean inserted = taskStore.insertNodeAdvertisement(groupId, nodeId, advertisedWindowMillis, bootId,
                        now, configFingerprint, now);
                if (!inserted) {
                    // unique-violation race — roll back to the read
                    continue;
                }
                mintedBootIds.add(bootId);
                ensureAcquisitionFresh(operationStartNanos);
                return installGeneration(bootId, now, now, operationStartNanos);
            }
            if (row.getBootId() == null || row.getHeartbeatWindow() <= 0) {
                String message = "NODE_ADVERTISEMENT row for node [" + nodeId + "] in group [" + groupId
                        + "] carries an invalid stored window/boot id — failing closed, never an unsafe takeover.";
                LOG.error(message);
                throw new TaskCoordinationException(message);
            }
            long threshold;
            try {
                // expiry from the row's STORED window — never from the local window
                threshold = Math.subtractExact(now, Math.addExact(row.getHeartbeatWindow(), SKEW_ALLOWANCE_MILLIS));
            } catch (ArithmeticException ex) {
                String message = "NODE_ADVERTISEMENT row for node [" + nodeId + "] in group [" + groupId
                        + "] carries an out-of-range stored window [" + row.getHeartbeatWindow()
                        + "] — failing closed, never an unsafe takeover.";
                LOG.error(message);
                throw new TaskCoordinationException(message);
            }
            boolean expired = row.getUpdatedAt() < threshold;
            if (!expired) {
                String message = "A live process holds this node's name: node id [" + nodeId + "], row boot id ["
                        + row.getBootId() + "]. Not running the startup cleanup and not starting coordinated "
                        + "scheduling; acquisition is refused until the holder's window expires.";
                LOG.error(message);
                throw new TaskCoordinationException(message);
            }
            String bootId = UUID.randomUUID().toString();
            long operationStartNanos = System.nanoTime();
            int rows = taskStore.takeoverNodeAdvertisement(bootId, advertisedWindowMillis, configFingerprint, now,
                    now, groupId, nodeId, row.getBootId(), row.getHeartbeatWindow(), row.getUpdatedAt());
            if (rows == 1) {
                mintedBootIds.add(bootId);
                ensureAcquisitionFresh(operationStartNanos);
                return installGeneration(bootId, now, now, operationStartNanos);
            }
            // zero rows — a race; go back to the read
        }
    }

    /**
     * Applies the renewal freshness rule to acquisition and reacquisition. A response that consumes the entire
     * takeover margin is rejected even if its database write succeeded.
     */
    private void ensureAcquisitionFresh(long operationStartNanos) throws TaskCoordinationException {

        if (System.nanoTime() - operationStartNanos > takeoverMarginNanos) {
            throw new TaskCoordinationException("Boot lease acquisition response arrived after the takeover margin ["
                    + takeoverMarginMillis + "] ms — never treated as fresh; acquisition refused.");
        }
    }

    private Generation installGeneration(String bootId, long bootStartedAt, long writtenUpdatedAt, long operationStartNanos) {

        Generation freshGeneration = new Generation(bootId, bootStartedAt);
        adoptPublishedUpdatedAt(writtenUpdatedAt);
        lastSuccessfulRenewalNanos = operationStartNanos;
        everRenewed = true;
        return freshGeneration;
    }

    /**
     * The guarded startup handback — replaces the retired raw startup cleanup
     * (taskStore.deleteTasks(thisNode)) with the destination-conditioned unassignment transition,
     * preserving task rows and claim history. Runs AFTER lease acquisition and BEFORE the coordinated
     * scheduler starts, over every task currently destined to this node, one transaction per task in
     * deterministic name order. A guarded-cleanup failure fails closed — never catch-log-continue:
     * this throws and the caller retries on the acquisition backoff loop (idempotent per task; a
     * partial handback is completed on the next retry).
     */
    public void runGuardedStartupHandback() throws TaskCoordinationException {

        Generation leaseGeneration = generation;
        if (leaseGeneration == null) {
            throw new TaskCoordinationException("The startup handback requires an acquired boot lease.");
        }
        List<String> tasks = taskStore.retrieveTaskNamesDestinedToNode(nodeId);
        for (String task : tasks) {
            long nextUpdatedAt = mintUpdatedAt();
            RDMBSConnector.HandbackResult result = taskStore.handBackDestinedTask(task, nodeId, groupId, nodeId,
                    leaseGeneration.bootId, nextUpdatedAt);
            if (result.getVerdict() == RDMBSConnector.HandbackResult.Verdict.IDENTITY_LOST) {
                onIdentityLost(leaseGeneration, "the startup handback's advertisement touch found the identity no longer owned");
                throw new TaskCoordinationException("Boot lease identity was lost during the startup handback of "
                        + "task [" + task + "].");
            }
            adoptPublishedUpdatedAt(result.getAdoptedUpdatedAt());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Startup handback for task [" + task + "]: " + result.getVerdict());
            }
        }
    }

    @Override
    public void renewOnHeartbeat() {

        Generation leaseGeneration = generation;
        if (leaseGeneration == null || stopped) {
            return;
        }
        long operationStartNanos = System.nanoTime();
        long nextUpdatedAt = mintUpdatedAt();
        RDMBSConnector.AdvertisementWriteResult result;
        try {
            result = taskStore.touchNodeAdvertisement(groupId, nodeId, leaseGeneration.bootId, nextUpdatedAt);
        } catch (TaskCoordinationException ex) {
            if (isLockWaitClass(ex)) {
                // a lock-wait-class error on my own row is NO transition — retry next beat
                LOG.warn("Lease renewal hit a lock-wait-class error; retrying next beat.", ex);
                return;
            }
            onRenewalTransientFailure(leaseGeneration, "lease renewal failed: " + ex.getMessage());
            return;
        }
        if (result.getVerdict() == RDMBSConnector.AdvertisementWriteResult.Verdict.IDENTITY_LOST) {
            onIdentityLost(leaseGeneration, "the advertisement row no longer carries this boot id — the identity is no longer "
                    + "this process's");
            return;
        }
        adoptPublishedUpdatedAt(result.getAdoptedUpdatedAt());
        if (System.nanoTime() - operationStartNanos > takeoverMarginNanos) {
            // a response arriving with the margin already exhausted is never treated as fresh
            onRenewalTransientFailure(leaseGeneration, "lease renewal response arrived after the takeover margin ["
                    + takeoverMarginMillis + "] ms");
            return;
        }
        synchronized (stateLock) {
            if (generation != leaseGeneration) {
                return;     // a stale renewal cannot touch a freshly reacquired generation
            }
            lastSuccessfulRenewalNanos = operationStartNanos;
            everRenewed = true;
            // in PROVEN inside the margin the renewal maintains PROVEN; in LAPSED it changes NOTHING —
            // an ordinary renewal never owns the LAPSED->PROVEN transition
        }
    }

    /**
     * Connection-class error / timeout / unsafe monotonic age: latches LAPSED from PROVEN — atomically,
     * before any later success can publish; a new lapse event during RECOVERING returns it to LAPSED.
     * No transition in any other state.
     */
    private void onRenewalTransientFailure(Generation leaseGeneration, String reason) {

        boolean newEpisode = false;
        boolean bounced = false;
        synchronized (stateLock) {
            if (generation != leaseGeneration || stopped) {
                return;
            }
            if (state == LeaseState.PROVEN) {
                state = LeaseState.LAPSED;
                lapseEpoch++;
                nextRecoveryDelayMillis = renewalIntervalMillis;
                newEpisode = true;
            } else if (state == LeaseState.RECOVERING) {
                state = LeaseState.LAPSED;
                lapseEpoch++;
                bounced = true;
            }
        }
        if (newEpisode) {
            runLapseEntryAction(leaseGeneration, reason);
        } else if (bounced) {
            LOG.warn("A new lapse event fired during recovery: " + reason);
            scheduleRecovery(nextRecoveryBackoff());
        }
    }

    /**
     * Identity loss (another BOOT_ID, row gone, smaller value on read-back): from PROVEN this latches
     * LAPSED with the lapse-entry action; from RECOVERING it returns to LAPSED (the next recovery pass
     * reads the row and classifies terminal/absent); mid-initialization (UNPROVEN) it aborts the
     * sequence and re-enters the acquisition loop on the controller thread.
     */
    private void onIdentityLost(Generation leaseGeneration, String reason) {

        boolean newEpisode = false;
        boolean bounced = false;
        boolean abortMidInit = false;
        synchronized (stateLock) {
            if (generation != leaseGeneration || stopped) {
                return;
            }
            if (state == LeaseState.PROVEN) {
                state = LeaseState.LAPSED;
                lapseEpoch++;
                nextRecoveryDelayMillis = renewalIntervalMillis;
                newEpisode = true;
            } else if (state == LeaseState.RECOVERING) {
                state = LeaseState.LAPSED;
                lapseEpoch++;
                bounced = true;
            } else if (state == LeaseState.UNPROVEN) {
                abortMidInit = true;
            }
        }
        if (newEpisode || bounced || abortMidInit) {
            // in LAPSED/TERMINAL the identity loss is already latched and alarmed — never a log storm
            LOG.error("Boot lease identity event for node [" + nodeId + "], boot id [" + leaseGeneration.bootId + "]: " + reason);
        }
        if (newEpisode) {
            runLapseEntryAction(leaseGeneration, reason);
        } else if (bounced) {
            scheduleRecovery(nextRecoveryBackoff());
        } else if (abortMidInit) {
            scheduleReacquisition(leaseGeneration);
        }
    }

    /**
     * The external lapse signal (the Pause Watchdog and its Reconcile-Stall Latch, the reconciliation-stall rule):
     * enters the lapse path exactly as a renewal failure would — PROVEN latches LAPSED with the
     * lapse-entry action, RECOVERING returns to LAPSED, no transition in any other state. The
     * signaler owns no resume; only the RECOVERING-&gt;PROVEN exit restarts dispatch.
     */
    public void signalLapse(String reason) {

        Generation leaseGeneration = generation;
        if (leaseGeneration == null) {
            return;
        }
        onRenewalTransientFailure(leaseGeneration, reason);
    }

    /**
     * Entering LAPSED runs one idempotent action: the local veto is set first (the latched state),
     * then the advisory ELIGIBILITY publication (failure tolerated — the local veto is the fence),
     * then scheduler standby + pauseAllLocallyRunningTasks() + one alarm (never one action per Quartz
     * worker).
     */
    private void runLapseEntryAction(Generation leaseGeneration, String reason) {

        try {
            taskStore.publishEligibility(groupId, nodeId, leaseGeneration.bootId, mintUpdatedAt(), ELIGIBILITY_LAPSED);
        } catch (Throwable failure) {
            LOG.warn("Advisory ELIGIBILITY='LAPSED' publication failed; the local veto is the fence.", failure);
        }
        ScheduledExecutorService taskScheduler = DataHolder.getInstance().getTaskScheduler();
        if (taskScheduler != null) {
            LOG.info("Shutting down the coordinated task scheduler: the boot lease lapsed.");
            taskScheduler.shutdownNow();
            DataHolder.getInstance().setTaskScheduler(null);
        }
        ScheduledTaskManager taskManager = DataHolder.getInstance().getTaskManager();
        if (taskManager != null) {
            taskManager.pauseAllLocallyRunningTasks();
        }
        LOG.error("Boot lease LAPSED for node [" + nodeId + "], boot id [" + leaseGeneration.bootId + "]: " + reason
                + ". Every coordinated fire is vetoed until recovery completes.");
        TasksDSComponent.getReadinessRegistry().raise(NON_PROVEN_LEASE_STATE_CONDITION,
                CoordinationReadinessRegistry.ConditionClass.CONTINUOUS,
                "lease state LAPSED — " + reason);
        scheduleRecovery(nextRecoveryBackoff());
    }

    @Override
    public boolean isDispatchBlocked() {

        if (state != LeaseState.PROVEN) {
            return true;
        }
        // the lapse half is COMPUTED, never a stored latch: latch OR monotonic age beyond the margin
        if (!everRenewed) {
            return true;
        }
        return System.nanoTime() - lastSuccessfulRenewalNanos > takeoverMarginNanos;
    }

    @Override
    public long lastSuccessfulRenewalNanos() {

        return lastSuccessfulRenewalNanos;
    }

    @Override
    public String bootId() {

        Generation leaseGeneration = generation;
        return leaseGeneration == null ? null : leaseGeneration.bootId;
    }

    @Override
    public long bootStartedAt() {

        Generation leaseGeneration = generation;
        return leaseGeneration == null ? 0L : leaseGeneration.bootStartedAt;
    }

    @Override
    public BootPassHandle beginTypeBootPass(String taskType) {

        Generation leaseGeneration = generation;
        if (leaseGeneration == null || stopped || leaseGeneration.completed) {
            return null;
        }
        if (!leaseGeneration.typesPassed.add(taskType)) {
            return null;    // not the type's first pass in this generation -> STEADY_STATE
        }
        return new BootPassHandleImpl(leaseGeneration);
    }

    @Override
    public BootPassHandle getOpenTypeBootPass(String taskType) {

        Generation leaseGeneration = generation;
        if (leaseGeneration == null || stopped || leaseGeneration.completed || !leaseGeneration.typesPassed.contains(taskType)) {
            return null;
        }
        return new BootPassHandleImpl(leaseGeneration);
    }

    @Override
    public void completeBootPass(String bootId) {

        Generation leaseGeneration = generation;
        if (leaseGeneration == null || !leaseGeneration.bootId.equals(bootId)) {
            LOG.info("Stale boot-pass completion for boot id [" + bootId + "] is a no-op — the lease was lost and "
                    + "re-acquired mid-enumeration; the newer generation's own rerun completes itself.");
            return;
        }
        if (!leaseGeneration.recordedFailures.isEmpty()) {
            String entries = String.join(", ", leaseGeneration.recordedFailures.keySet());
            TasksDSComponent.getReadinessRegistry().raise(BOOT_PASS_INCOMPLETE_CONDITION,
                    CoordinationReadinessRegistry.ConditionClass.GATE,
                    "the boot pass recorded registration failures: [" + entries + "]");
            LOG.error("Boot pass for boot id [" + bootId + "] recorded registration failures [" + entries
                    + "]; the node stays PENDING/UNPROVEN. The only exit is lease re-acquisition's fresh "
                    + "generation.");
            return;
        }
        runCompletionBranch(leaseGeneration);
    }

    /**
     * The completion branch: the ELIGIBLE publication (BOOT_ID-conditioned) and lease PROVEN —
     * coordinated scheduling starts last. One of the two Config Fence-checked completion transitions
     * (the other is the recovery exit): every boot, including an in-process re-acquisition's rerun,
     * runs the comparison here before the node can reach ELIGIBLE.
     */
    private void runCompletionBranch(Generation leaseGeneration) {

        synchronized (stateLock) {
            if (generation != leaseGeneration || leaseGeneration.completed || stopped
                    || (state != LeaseState.UNPROVEN && state != LeaseState.RECOVERING)) {
                return;
            }
        }
        try {
            if (!runConfigFenceComparison()) {
                scheduleCompletionRetry(leaseGeneration);
                return;
            }
        } catch (TaskCoordinationException ex) {
            LOG.error("The Config Fence comparison failed for boot id [" + leaseGeneration.bootId
                    + "]; retrying on the lease backoff loop.", ex);
            scheduleCompletionRetry(leaseGeneration);
            return;
        }
        // Seeding parity — the activation-readiness anti-join machine-verifies completeness before
        // the ELIGIBLE publication. Violations are alarmed as the GATE condition (the ALL-GREEN
        // activation runbook refuses over it); a query failure refuses the completion step.
        try {
            Map<String, String> violations = taskStore.getActivationReadinessViolations();
            if (violations.isEmpty()) {
                TasksDSComponent.getReadinessRegistry().clear(SEEDING_PARITY_CONDITION);
            } else {
                StringBuilder detail = new StringBuilder("the seeding-parity anti-join names ");
                boolean first = true;
                for (Map.Entry<String, String> violation : violations.entrySet()) {
                    if (!first) {
                        detail.append("; ");
                    }
                    detail.append("task [").append(violation.getKey()).append("]: ").append(violation.getValue());
                    first = false;
                }
                TasksDSComponent.getReadinessRegistry().raise(SEEDING_PARITY_CONDITION,
                        CoordinationReadinessRegistry.ConditionClass.GATE, detail.toString());
                LOG.error("Seeding parity is broken at boot-pass completion for boot id [" + leaseGeneration.bootId + "]: "
                        + detail + ". The ELIGIBLE/PROVEN publication is refused; retrying on the lease "
                        + "backoff loop.");
                // Activation parity gate: red parity refuses publication — the generation stays uncompleted
                // (its pass stays open, so late registrations still seed) and the retry re-runs this
                // branch.
                scheduleCompletionRetry(leaseGeneration);
                return;
            }
        } catch (TaskCoordinationException ex) {
            LOG.error("The seeding-parity anti-join query failed for boot id [" + leaseGeneration.bootId
                    + "]; retrying on the lease backoff loop.", ex);
            scheduleCompletionRetry(leaseGeneration);
            return;
        }
        long operationStartNanos = System.nanoTime();
        long nextUpdatedAt = mintUpdatedAt();
        RDMBSConnector.AdvertisementWriteResult result;
        try {
            result = taskStore.publishEligibility(groupId, nodeId, leaseGeneration.bootId, nextUpdatedAt, ELIGIBILITY_ELIGIBLE);
        } catch (TaskCoordinationException ex) {
            LOG.error("The ELIGIBLE publication failed for boot id [" + leaseGeneration.bootId
                    + "]; retrying on the lease backoff loop.", ex);
            scheduleCompletionRetry(leaseGeneration);
            return;
        }
        if (result.getVerdict() == RDMBSConnector.AdvertisementWriteResult.Verdict.IDENTITY_LOST) {
            onIdentityLost(leaseGeneration, "the ELIGIBLE publication found the identity no longer owned");
            return;
        }
        adoptPublishedUpdatedAt(result.getAdoptedUpdatedAt());
        boolean proven = false;
        synchronized (stateLock) {
            if (generation == leaseGeneration && !stopped
                    && (state == LeaseState.UNPROVEN || state == LeaseState.RECOVERING)) {
                state = LeaseState.PROVEN;
                leaseGeneration.completed = true;
                lastSuccessfulRenewalNanos = operationStartNanos;
                everRenewed = true;
                nextRecoveryDelayMillis = renewalIntervalMillis;
                proven = true;
            }
        }
        if (!proven) {
            return;
        }
        TasksDSComponent.getReadinessRegistry().clear(BOOT_PASS_INCOMPLETE_CONDITION);
        TasksDSComponent.getReadinessRegistry().clear(NON_PROVEN_LEASE_STATE_CONDITION);
        LOG.info("Boot lease PROVEN for node [" + nodeId + "], boot id [" + leaseGeneration.bootId
                + "]: ELIGIBLE published; coordinated scheduling starts.");
        handBackOwnRunningRowsOnLeaseExit("completion");
        startCoordinatedScheduler(" upon boot lease completion");
    }

    private void scheduleCompletionRetry(final Generation leaseGeneration) {

        if (stopped) {
            return;
        }
        controllerExecutor.schedule(new Runnable() {
            @Override
            public void run() {
                runCompletionBranch(leaseGeneration);
            }
        }, renewalIntervalMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void wakeRecovery() {

        if (stopped) {
            return;
        }
        if (state == LeaseState.LAPSED) {
            // external signals may WAKE the recovery loop early, never execute it
            controllerExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    runRecovery();
                }
            });
        }
    }

    /**
     * Membership-rejoin restart. A coordination-DB stall trips the membership layer —
     * TaskEventListener.becameUnresponsive kills the coordinated scheduler and pauses every locally
     * running job — but lapses no lease: the Pause Watchdog observes JVM freezes via the monotonic
     * tick, and a DB stall freezes nothing. With the lease still PROVEN, neither lease exit
     * (completion :startCoordinatedScheduler / recovery) ever fires, so the rejoin event is the
     * stall's only recovery edge. Only in PROVEN (a lapse in flight owns its own restart through
     * runRecovery), hand this node's surviving RUNNING rows back to the scheduler (RUNNING -> NONE,
     * destination retained because those rows have no local Quartz edge left) and restart the
     * scheduler. Safe by construction: the start is a no-op while a scheduler exists, and every fire
     * still passes the dispatch veto and the claim CAS.
     */
    public void restartAfterMembershipRejoin() {

        if (stopped || state != LeaseState.PROVEN) {
            return;
        }
        controllerExecutor.execute(new Runnable() {
            @Override
            public void run() {
                if (stopped || state != LeaseState.PROVEN) {
                    return;
                }
                try {
                    int handedBack = taskStore.handBackNodeRunningTasks(nodeId);
                    if (handedBack > 0) {
                        LOG.info("Membership rejoin handed [" + handedBack + "] RUNNING row(s) of node ["
                                + nodeId + "] back to the scheduler.");
                    }
                } catch (Throwable failure) {
                    LOG.warn("The membership-rejoin hand-back of RUNNING rows failed; the leader's store "
                            + "cleanup remains the fallback for stale assignments.", failure);
                }
                startCoordinatedScheduler(" upon rejoining the cluster with the boot lease still PROVEN");
            }
        });
    }

    private void scheduleRecovery(long delayMillis) {

        if (stopped) {
            return;
        }
        controllerExecutor.schedule(new Runnable() {
            @Override
            public void run() {
                runRecovery();
            }
        }, delayMillis, TimeUnit.MILLISECONDS);
    }

    private long nextRecoveryBackoff() {

        synchronized (stateLock) {
            long delay = nextRecoveryDelayMillis;
            nextRecoveryDelayMillis = Math.min(advertisedWindowMillis, Math.max(renewalIntervalMillis, delay * 2));
            return delay;
        }
    }

    /**
     * The single-flight recovery pass (LAPSED -> RECOVERING -> PROVEN), generation-checked so a stale
     * recovery completion can never overwrite a newer lapse. The lease matrix: same token -> renew ->
     * ownership proof -> Config Fence check -> PROVEN; absent -> re-acquire a fresh generation and
     * rerun the boot pass; another boot's token -> terminal standby; indeterminate -> remain LAPSED
     * and veto.
     */
    private void runRecovery() {

        Generation leaseGeneration;
        int lapseEpochAtStart;
        synchronized (stateLock) {
            if (stopped || state != LeaseState.LAPSED || generation == null) {
                return;
            }
            leaseGeneration = generation;
            lapseEpochAtStart = lapseEpoch;
            state = LeaseState.RECOVERING;
        }
        TasksDSComponent.getReadinessRegistry().raise(NON_PROVEN_LEASE_STATE_CONDITION,
                CoordinationReadinessRegistry.ConditionClass.CONTINUOUS, "lease state RECOVERING");
        try {
            taskStore.publishEligibility(groupId, nodeId, leaseGeneration.bootId, mintUpdatedAt(), ELIGIBILITY_RECOVERING);
        } catch (Throwable failure) {
            LOG.warn("Advisory ELIGIBILITY='RECOVERING' publication failed.", failure);
        }
        long recoveryStartNanos = System.nanoTime();
        try {
            RDMBSConnector.NodeAdvertisement row = taskStore.getNodeAdvertisement(groupId, nodeId);
            if (row == null) {
                // an evictor's removeNode deleted the row: re-acquire a fresh lease generation, then
                // full reconcile + republish before PROVEN — completing like every completion
                Generation freshGeneration = acquisitionLoop();
                synchronized (stateLock) {
                    if (stopped || state != LeaseState.RECOVERING || lapseEpoch != lapseEpochAtStart) {
                        return;
                    }
                    generation = freshGeneration;
                }
                LOG.info("Boot lease re-acquired with a fresh generation [" + freshGeneration.bootId
                        + "] after the advertisement row was found absent.");
                rerunBootPass(freshGeneration);
                return;
            }
            if (!leaseGeneration.bootId.equals(row.getBootId())) {
                if (mintedBootIds.contains(row.getBootId())) {
                    // this process's own orphaned acquisition candidate, not another boot's token:
                    // release it (BOOT_ID-conditioned, can never touch a foreign row) and retry
                    int released = taskStore.releaseNodeAdvertisement(groupId, nodeId, row.getBootId());
                    backToLapsed(leaseGeneration, lapseEpochAtStart, "the advertisement row carried this "
                            + "process's own orphaned acquisition candidate [" + row.getBootId()
                            + "] (released " + released + " row(s)); re-acquiring on the next recovery pass");
                    return;
                }
                enterTerminal(leaseGeneration, row.getBootId());
                return;
            }
            // same token: renew
            long operationStartNanos = System.nanoTime();
            RDMBSConnector.AdvertisementWriteResult renewalResult = taskStore.touchNodeAdvertisement(groupId, nodeId,
                    leaseGeneration.bootId, mintUpdatedAt());
            if (renewalResult.getVerdict() == RDMBSConnector.AdvertisementWriteResult.Verdict.IDENTITY_LOST) {
                backToLapsed(leaseGeneration, lapseEpochAtStart, "the recovery renewal found the identity no longer owned");
                return;
            }
            adoptPublishedUpdatedAt(renewalResult.getAdoptedUpdatedAt());
            if (System.nanoTime() - operationStartNanos > takeoverMarginNanos) {
                backToLapsed(leaseGeneration, lapseEpochAtStart, "the recovery renewal arrived after the takeover margin");
                return;
            }
            synchronized (stateLock) {
                if (generation == leaseGeneration) {
                    lastSuccessfulRenewalNanos = operationStartNanos;
                    everRenewed = true;
                }
            }
            // the ownership proof: read-only + local stops — resume only what is provably still yours
            runOwnershipProof();
            // recovery step 3 — the Config Fence recheck
            if (!runConfigFenceComparison()) {
                backToLapsed(leaseGeneration, lapseEpochAtStart, "the Config Fence recheck found a fingerprint mismatch against a "
                        + "live member");
                return;
            }
            if (System.nanoTime() - recoveryStartNanos > takeoverMarginNanos) {
                backToLapsed(leaseGeneration, lapseEpochAtStart, "the recovery pass outlived the freshness margin");
                return;
            }
            RDMBSConnector.AdvertisementWriteResult eligibilityPublication = taskStore.publishEligibility(groupId, nodeId, leaseGeneration.bootId,
                    mintUpdatedAt(), ELIGIBILITY_ELIGIBLE);
            if (eligibilityPublication.getVerdict() == RDMBSConnector.AdvertisementWriteResult.Verdict.IDENTITY_LOST) {
                backToLapsed(leaseGeneration, lapseEpochAtStart, "the recovery ELIGIBLE publication found the identity no longer owned");
                return;
            }
            adoptPublishedUpdatedAt(eligibilityPublication.getAdoptedUpdatedAt());
            boolean proven = false;
            synchronized (stateLock) {
                if (generation == leaseGeneration && state == LeaseState.RECOVERING && lapseEpoch == lapseEpochAtStart && !stopped) {
                    state = LeaseState.PROVEN;
                    nextRecoveryDelayMillis = renewalIntervalMillis;
                    proven = true;
                }
            }
            if (proven) {
                TasksDSComponent.getReadinessRegistry().clear(NON_PROVEN_LEASE_STATE_CONDITION);
                LOG.info("Boot lease recovered to PROVEN for node [" + nodeId + "], boot id [" + leaseGeneration.bootId + "].");
                handBackOwnRunningRowsOnLeaseExit("recovery");
                startCoordinatedScheduler(" upon boot lease recovery");
            }
        } catch (Throwable failure) {
            backToLapsed(leaseGeneration, lapseEpochAtStart, "a recovery step failed: " + failure.getMessage());
        }
    }

    private void backToLapsed(Generation leaseGeneration, int lapseEpochAtStart, String reason) {

        boolean rescheduled = false;
        synchronized (stateLock) {
            if (generation == leaseGeneration && state == LeaseState.RECOVERING && lapseEpoch == lapseEpochAtStart && !stopped) {
                state = LeaseState.LAPSED;
                rescheduled = true;
            }
        }
        if (rescheduled) {
            LOG.warn("Boot lease recovery pass failed for boot id [" + leaseGeneration.bootId + "]: " + reason
                    + ". Remaining LAPSED; the recovery loop retries on backoff.");
            TasksDSComponent.getReadinessRegistry().raise(NON_PROVEN_LEASE_STATE_CONDITION,
                    CoordinationReadinessRegistry.ConditionClass.CONTINUOUS, "lease state LAPSED — " + reason);
            scheduleRecovery(nextRecoveryBackoff());
        }
    }

    private void enterTerminal(Generation leaseGeneration, String otherBootId) {

        synchronized (stateLock) {
            if (generation != leaseGeneration || stopped) {
                return;
            }
            state = LeaseState.TERMINAL;
            terminalSinceEpochMillis = System.currentTimeMillis();
        }
        LOG.error("Boot lease TERMINAL for node [" + nodeId + "]: the advertisement row carries another boot's "
                + "token [" + otherBootId + "] (this generation was [" + leaseGeneration.bootId + "]) — this process lost its "
                + "identity. Terminal standby exits only via process restart / the Liveness Probe; dispatch is "
                + "never authorized.");
        TasksDSComponent.getReadinessRegistry().raise(NON_PROVEN_LEASE_STATE_CONDITION,
                CoordinationReadinessRegistry.ConditionClass.CONTINUOUS,
                "lease state TERMINAL — the advertisement row carries another boot's token [" + otherBootId + "]");
    }

    /**
     * Recovery step 2 — the ownership proof (read-only + local stops): read DESTINED_NODE_ID for every
     * locally scheduled task; all still mine -> nothing was redistributed (the symmetric-DB-blip case);
     * some moved -> stop each moved task locally (the same resumable stop the Ownership Sweep's
     * periodic pass wraps), then proceed with what remains.
     */
    private void runOwnershipProof() throws Exception {

        ScheduledTaskManager taskManager = DataHolder.getInstance().getTaskManager();
        if (taskManager == null) {
            return;
        }
        Set<String> running = new HashSet<>(taskManager.getLocallyRunningCoordinatedTasks());
        if (running.isEmpty()) {
            return;
        }
        Map<String, String> destinedByTask = new HashMap<>();
        for (CoordinatedTask task : taskStore.getAllTaskNames()) {
            destinedByTask.put(task.getTaskName(), task.getDestinedNodeId());
        }
        for (String task : running) {
            if (!nodeId.equals(destinedByTask.get(task))) {
                LOG.warn("Task [" + task + "] is running locally but no longer destined here — stopping the local "
                        + "instance (boot lease recovery ownership proof).");
                taskManager.stopExecutionTemporarily(task);
            }
        }
    }

    /**
     * The Config Fence comparison — HARDCODED refuse, no policy knob, no warn mode: whole-string
     * fingerprint equality against every LIVE member with a non-empty advertised fingerprint (a member
     * with no advertisement row or an empty fingerprint is unpatched or master-off and is skipped). A
     * mismatch raises the continuous correctness-convergence condition naming the differing keys and
     * returns false — the caller refuses (activation stays PENDING on the lease retry loop; recovery
     * stays LAPSED). No mismatch clears the condition; all live members advertising the same
     * fingerprint is visible-member configuration convergence — one INFO when first reached. Detection
     * only: no runtime adaptation to mixed flags.
     */
    public boolean runConfigFenceComparison() throws TaskCoordinationException {

        List<String> mismatches = new ArrayList<>();
        Set<String> differingKeys = new TreeSet<>();
        List<String> liveMemberIds;
        try {
            liveMemberIds = clusterCoordinator.getAllNodeIdsOrThrow();
        } catch (ClusterCoordinationException ex) {
            throw new TaskCoordinationException(
                    "The Config Fence comparison could not enumerate the live members; the step refuses.", ex);
        }
        for (String memberId : liveMemberIds) {
            if (nodeId.equals(memberId)) {
                continue;
            }
            RDMBSConnector.NodeAdvertisement row = taskStore.getNodeAdvertisement(groupId, memberId);
            if (row == null || row.getConfigFingerprint() == null || row.getConfigFingerprint().isEmpty()) {
                continue;
            }
            if (!configFingerprint.equals(row.getConfigFingerprint())) {
                differingKeys.addAll(ConfigFingerprint.differingKeys(configFingerprint, row.getConfigFingerprint()));
                mismatches.add("live member [" + memberId + "] advertises [" + row.getConfigFingerprint() + "]");
            }
        }
        if (!mismatches.isEmpty()) {
            String detail = "config fingerprint mismatch — differing keys " + differingKeys + ": this node ["
                    + nodeId + "] advertises [" + configFingerprint + "]; " + String.join("; ", mismatches);
            TasksDSComponent.getReadinessRegistry().raise(CONFIG_FENCE_CONVERGENCE_CONDITION,
                    CoordinationReadinessRegistry.ConditionClass.CONTINUOUS, detail);
            LOG.error("Config Fence refuses coordination: " + detail);
            return false;
        }
        TasksDSComponent.getReadinessRegistry().clear(CONFIG_FENCE_CONVERGENCE_CONDITION);
        if (!configConvergenceReached) {
            configConvergenceReached = true;
            LOG.info("Visible-member configuration convergence: every live advertising member carries this "
                    + "config fingerprint [" + configFingerprint + "].");
        }
        return true;
    }

    /**
     * The re-acquisition rerun — the same bracket completedServerStartup() runs, executed by the
     * controller itself through the TaskService reference it received at construction: capture the
     * fresh generation's boot id, rerun the enumeration as a fresh BOOT_PASS (per-type state is fresh
     * by construction), and complete in a finally. Before the first server initialization has been
     * dispatched, the initial startup braid enumerates and completes the fresh generation itself.
     */
    private void rerunBootPass(Generation freshGeneration) {

        if (taskService == null || !taskService.isServerInit()) {
            return;
        }
        String freshBootId = freshGeneration.bootId;
        try {
            taskService.serverInitialized();
        } finally {
            completeBootPass(freshBootId);
        }
    }

    /**
     * Mid-initialization identity loss (UNPROVEN): abort the sequence and re-enter the acquisition
     * loop on the controller thread — it refuses, loudly, while a live twin holds the name; every
     * wake-side write is BOOT_ID-conditioned and fails closed.
     */
    private void scheduleReacquisition(final Generation stale) {

        if (stopped) {
            return;
        }
        controllerExecutor.schedule(new Runnable() {
            @Override
            public void run() {
                runReacquisition(stale);
            }
        }, renewalIntervalMillis, TimeUnit.MILLISECONDS);
    }

    private void runReacquisition(Generation stale) {

        synchronized (stateLock) {
            if (stopped || generation != stale || state != LeaseState.UNPROVEN) {
                return;
            }
        }
        try {
            Generation freshGeneration = acquisitionLoop();
            synchronized (stateLock) {
                if (stopped || generation != stale || state != LeaseState.UNPROVEN) {
                    return;
                }
                generation = freshGeneration;
            }
            LOG.info("Boot lease re-acquired with a fresh generation [" + freshGeneration.bootId
                    + "] after a mid-initialization identity loss.");
            rerunBootPass(freshGeneration);
        } catch (TaskCoordinationException ex) {
            LOG.error("Boot lease re-acquisition refused/failed; retrying on backoff. " + ex.getMessage());
            scheduleReacquisition(stale);
        }
    }

    /**
     * Lease-exit hand-back. The lapse pause drained every local Quartz edge while this node's
     * RUNNING rows kept it as destined, and the scheduler pass only schedules NONE/ACTIVATED rows —
     * so without a hand-back those rows are never re-scheduled. A simultaneous all-nodes freeze
     * produces no membership-rejoin edge (no survivor ever evicts anyone), leaving the lease exit
     * as the only recovery edge; it must therefore hand back exactly like the rejoin path.
     * Destination is retained, and every fire still passes the dispatch veto and the claim CAS.
     */
    private void handBackOwnRunningRowsOnLeaseExit(String exitName) {

        try {
            int handedBack = taskStore.handBackNodeRunningTasks(nodeId);
            if (handedBack > 0) {
                LOG.info("Boot lease " + exitName + " handed [" + handedBack + "] RUNNING row(s) of node ["
                        + nodeId + "] back to the scheduler.");
            }
        } catch (Throwable failure) {
            LOG.warn("The boot lease " + exitName + " hand-back of RUNNING rows failed; locally paused tasks "
                    + "resume only on the next recovery edge or the leader's store cleanup.", failure);
        }
    }

    private void startCoordinatedScheduler(String message) {

        if (DataHolder.getInstance().getTaskScheduler() != null) {
            return;
        }
        ScheduledTaskManager taskManager = DataHolder.getInstance().getTaskManager();
        if (taskManager == null) {
            LOG.warn("The coordinated task scheduler cannot start yet: the task manager is not initialized.");
            return;
        }
        CoordinatedTaskScheduleManager scheduleManager = new CoordinatedTaskScheduleManager(taskManager, taskStore,
                clusterCoordinator, resolver);
        scheduleManager.startTaskScheduler(message);
    }

    /**
     * Stops the controller for component deactivation: the lease release as required during shutdown,
     * BOOT_ID-conditioned — a stale process can never delete its successor's row.
     */
    public void stop() {

        stopped = true;
        controllerExecutor.shutdownNow();
        Generation leaseGeneration = generation;
        if (leaseGeneration != null) {
            try {
                taskStore.releaseNodeAdvertisement(groupId, nodeId, leaseGeneration.bootId);
            } catch (Throwable failure) {
                LOG.warn("Boot lease release failed on shutdown for boot id [" + leaseGeneration.bootId
                        + "]; the row expires on its window.", failure);
            }
        }
    }

    // ---- HeartbeatHook: the coordination-owned callback surface ----

    @Override
    public void afterHeartbeat(long epochMillis) {

        renewOnHeartbeat();
        observeStandbyCoordinatorship();
    }

    /**
     * The standby-holds-coordinatorship observation (NTASK-side — coordination cannot import this
     * registry): a terminal-standby or otherwise non-PROVEN node observed holding the coordinator row
     * must resign; red until it does. The resignation itself is the coordination component's per-cycle
     * election gate reading isCoordinatorParticipationAllowed().
     */
    private void observeStandbyCoordinatorship() {

        try {
            if (state != LeaseState.PROVEN && clusterCoordinator.isLeader()) {
                TasksDSComponent.getReadinessRegistry().raise(STANDBY_HOLDS_COORDINATORSHIP_CONDITION,
                        CoordinationReadinessRegistry.ConditionClass.CONTINUOUS,
                        "node [" + nodeId + "] holds the coordinator row in lease state [" + state
                                + "] — it must resign; red until it does");
            } else {
                TasksDSComponent.getReadinessRegistry().clear(STANDBY_HOLDS_COORDINATORSHIP_CONDITION);
            }
        } catch (Throwable observeFailure) {
            // observation only: never breaks the heartbeat
            if (LOG.isDebugEnabled()) {
                LOG.debug("The standby-coordinatorship observation did not complete this beat.", observeFailure);
            }
        }
    }

    /**
     * The lease state, for the readiness/liveness views.
     */
    public String leaseStateName() {

        return state.name();
    }

    /**
     * True while the lease state is terminal standby.
     */
    public boolean isTerminal() {

        return state == LeaseState.TERMINAL;
    }

    /**
     * The terminal state's entry instant (app clock), 0 when never entered.
     */
    public long terminalSinceEpochMillis() {

        return terminalSinceEpochMillis;
    }

    /**
     * Monotonic age of the last successful renewal in millis, -1 before the first one.
     */
    public long renewalAgeMillis() {

        if (!everRenewed) {
            return -1L;
        }
        return (System.nanoTime() - lastSuccessfulRenewalNanos) / 1_000_000L;
    }

    /**
     * The advertised heartbeat window (interval x max retries) in millis.
     */
    public long advertisedWindowMillis() {

        return advertisedWindowMillis;
    }

    @Override
    public boolean isCoordinatorParticipationAllowed() {

        // the single dispatch predicate, no second state computation: a node that may not dispatch may
        // not lead
        return !isDispatchBlocked();
    }

    /**
     * The lease group id, for fenced transactions that must head with this node's own advertisement
     * touch (barrier acks, wave creation, the registration doorway).
     */
    public String getGroupId() {

        return groupId;
    }

    /**
     * The lease node id — the advertisement row key's second half.
     */
    public String getNodeId() {

        return nodeId;
    }

    /**
     * Mints a strictly-forward advertisement touch value for a caller-owned fenced transaction. The
     * mint stays under the controller's atomicity — no caller computes its own timestamp.
     */
    public long mintAdvertisementTouch() {

        return mintUpdatedAt();
    }

    /**
     * Adopts a committed touch value observed by a caller-owned fenced transaction, so later mints
     * stay strictly forward of every published value.
     */
    public void adoptPublishedTouch(long observed) {

        adoptPublishedUpdatedAt(observed);
    }

    /**
     * Produces every advertisement timestamp under one lock so callers cannot publish conflicting values. The next
     * value is strictly forward: max(wallClockNow, lastPublishedUpdatedAt + 1).
     */
    private long mintUpdatedAt() {

        synchronized (mintLock) {
            lastPublishedUpdatedAt = Math.max(System.currentTimeMillis(), lastPublishedUpdatedAt + 1);
            return lastPublishedUpdatedAt;
        }
    }

    private void adoptPublishedUpdatedAt(long observed) {

        synchronized (mintLock) {
            if (observed > lastPublishedUpdatedAt) {
                lastPublishedUpdatedAt = observed;
            }
        }
    }

    private static boolean isLockWaitClass(Throwable failure) {

        Throwable current = failure;
        while (current != null) {
            if (current instanceof SQLTransactionRollbackException) {
                return true;
            }
            if (current instanceof SQLException) {
                String sqlState = ((SQLException) current).getSQLState();
                if (sqlState != null && (sqlState.startsWith("40") || sqlState.startsWith("41"))) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    /**
     * The generation-bound pass handle: records into ITS generation's failure set; a stale pass can
     * never mutate a newer generation's state.
     */
    private final class BootPassHandleImpl implements BootPassHandle {

        private final Generation boundGeneration;

        private BootPassHandleImpl(Generation boundGeneration) {
            this.boundGeneration = boundGeneration;
        }

        @Override
        public String bootId() {
            return boundGeneration.bootId;
        }

        @Override
        public void recordFailure(String name, Throwable cause) {

            if (generation != boundGeneration) {
                LOG.warn("Stale boot-pass failure record for [" + name + "] under boot id ["
                        + boundGeneration.bootId + "] is a no-op — the lease generation has moved on.");
                return;
            }
            boundGeneration.recordedFailures.put(name,
                    cause == null ? "" : String.valueOf(cause.getMessage()));
        }
    }
}

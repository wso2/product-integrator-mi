/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.micro.integrator.ntask.core.impl.standalone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.commons.util.MiscellaneousUtil;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.inbound.InboundEndpoint;
import org.apache.synapse.message.processor.MessageProcessor;
import org.apache.synapse.registry.Registry;
import org.apache.synapse.task.TaskDescription;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.SchedulerException;
import org.wso2.micro.integrator.coordination.ClusterCoordinator;
import org.wso2.micro.integrator.core.util.MicroIntegratorBaseUtils;
import org.wso2.micro.integrator.ntask.common.TaskConstants;
import org.wso2.micro.integrator.ntask.common.TaskException;
import org.wso2.micro.integrator.ntask.coordination.TaskCoordinationException;
import org.wso2.micro.integrator.ntask.coordination.task.BarrierLivenessClassifier;
import org.wso2.micro.integrator.ntask.coordination.task.CoordinatedTask;
import org.wso2.micro.integrator.ntask.coordination.task.store.TaskStore;
import org.wso2.micro.integrator.ntask.coordination.task.store.connector.RDMBSConnector;
import org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper;
import org.wso2.micro.integrator.ntask.core.BootLeaseController;
import org.wso2.micro.integrator.ntask.core.BootPassHandle;
import org.wso2.micro.integrator.ntask.core.CoordinationReadinessRegistry;
import org.wso2.micro.integrator.ntask.core.RegistrationPhase;
import org.wso2.micro.integrator.ntask.core.RegistrationResult;
import org.wso2.micro.integrator.ntask.core.TaskInfo;
import org.wso2.micro.integrator.ntask.core.TaskRepository;
import org.wso2.micro.integrator.ntask.core.TaskUtils;
import org.wso2.micro.integrator.ntask.core.impl.AbstractQuartzTaskManager;
import org.wso2.micro.integrator.ntask.core.impl.CoordinatedTaskTriggerListener;
import org.wso2.micro.integrator.ntask.core.impl.ScheduleFingerprintEncoder;
import org.wso2.micro.integrator.ntask.core.internal.BootLeaseControllerImpl;
import org.wso2.micro.integrator.ntask.core.internal.CoordinationBootstrapState;
import org.wso2.micro.integrator.ntask.core.internal.DataHolder;
import org.wso2.micro.integrator.ntask.core.internal.TaskHandlingConfigUtils;
import org.wso2.micro.integrator.ntask.core.internal.TasksDSComponent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is responsible for handling / scheduling all tasks in Micro Integrator.
 */
public class ScheduledTaskManager extends AbstractQuartzTaskManager {

    private static Log log = LogFactory.getLog(ScheduledTaskManager.class);

    private static final String MISFIRE_VALIDATION_CONDITION = "misfire-validation";
    private static final String PARKED_TASK_CONDITION = "parked-task";
    private static final String ADMIN_DEACTIVATE_REASON = "ADMIN_DEACTIVATE";

    /**
     * Declared misfire policies that never fire a missed occurrence — misfire skips forward to the
     * next future fire.
     */
    private static final Set<TaskConstants.TaskMisfirePolicy> SKIP_FORWARD_COMPATIBLE_MISFIRE_POLICIES =
            EnumSet.of(TaskConstants.TaskMisfirePolicy.DEFAULT, TaskConstants.TaskMisfirePolicy.DO_NOTHING,
                    TaskConstants.TaskMisfirePolicy.NEXT_WITH_EXISTING_COUNT,
                    TaskConstants.TaskMisfirePolicy.NEXT_WITH_REMAINING_COUNT);
    /**
     * The list which holds the list of coordinated tasks deployed in this node. CopyOnWriteArrayList:
     * a drop-in for every existing call site, preserving shipped List semantics exactly (duplicates
     * included), while contains() reads an immutable snapshot — no torn reads for the scheduler-thread
     * and readiness consumers. Mutations are deploy/undeploy-rare.
     */
    private List<String> deployedCoordinatedTasks = new CopyOnWriteArrayList<>();

    /**
     * The structural parking set (local scheduling gate): no parked task reaches Quartz. Transitions are
     * idempotent and mutated ONLY by doorway results and undeploy; check sites only read and skip.
     * Empty at boot and rebuilt naturally, because deployment re-runs registration.
     */
    private final Set<String> parkedTasks = ConcurrentHashMap.newKeySet();

    /**
     * The list of tasks for which the addition failed. CopyOnWriteArrayList: the add (a DB-failure
     * catch), the getter's snapshot copy, the remove, and the undeploy purge's removeIf run on
     * different threads with no common lock — each is atomic on it.
     */
    private List<TaskEntry> additionFailedTasks = new CopyOnWriteArrayList<>();

    /**
     * The skip-wait hint: once one deletion classifies a live holdout, every further deletion in the
     * same sweep skips the waiting. Advisory and JVM-local, and appears in no safety proof — losing it
     * (restart, leader change) affects only drain latency, never delete authorization; its clearing
     * rule is deliberately loose: next successfully finalized wave, or restart.
     */
    private static volatile boolean skipWaitHint;

    // The observation writer and local task status API read this list outside the scheduler thread, while
    // task start/stop paths update it. CopyOnWriteArrayList gives those readers a stable snapshot without
    // adding locks around the scheduler's normal path.
    private final List<String> locallyRunningCoordinatedTasks = new CopyOnWriteArrayList<>();

    // Episode limiter for the refusing guard: one WARN per task name per JVM.
    private final Set<String> refusalWarnedTasks = ConcurrentHashMap.newKeySet();

    private SynapseEnvironment synapseEnvironment = null;
    private TaskStore taskStore;
    private String localNodeId;
    private ClusterCoordinator clusterCoordinator;
    private final boolean taskDeleteBarrierEnabled;
    private final boolean coordinationHardened;

    private static final String REG_PROCESSOR_BASE_PATH = "/repository/components/org.apache.synapse.message.processor/";
    private static final String MP_STATE = "MESSAGE_PROCESSOR_STATE";
    private Registry registry = null;
    private static final Map<String, String> recentlyUpdatedStates = new ConcurrentHashMap<>();

    ScheduledTaskManager(TaskRepository taskRepository, TaskStore taskStore) throws TaskException {

        super(taskRepository, taskStore);
        this.taskStore = taskStore;
        this.localNodeId = DataHolder.getInstance().getLocalNodeId();
        this.clusterCoordinator = DataHolder.getInstance().getClusterCoordinator();
        this.taskDeleteBarrierEnabled = TaskHandlingConfigUtils.isTaskDeleteBarrierEnabled();
        this.coordinationHardened = TaskHandlingConfigUtils.isCoordinationHardeningEnabled();
        log.info("Clustered task delete barrier flow is " + (taskDeleteBarrierEnabled ? "enabled" : "disabled")
                + ". Configure [" + TaskHandlingConfigUtils.TASK_DELETE_BARRIER_ENABLED_CONFIG + "] to control it.");
    }

    @Override
    public void initStartupTasks(BootPassHandle handle) throws TaskException {
        this.scheduleAllTasks(handle);
    }

    private boolean isMyTaskTypeRegistered() {
        return TasksDSComponent.getTaskService().getRegisteredTaskTypes().contains(this.getTaskType());
    }

    /**
     * Handles the task with given name. Schedule if its not coordinated else update the task data base.
     *
     * @param taskName The name of the task
     * @param handle   the generation-bound boot-pass handle, or null for STEADY_STATE
     * @throws TaskException - Exception
     */
    public void handleTask(String taskName, BootPassHandle handle) throws TaskException {
        handleTask(taskName, false, handle);
    }

    public void handleTask(String taskName, boolean scheduledInPausedMode, BootPassHandle handle)
            throws TaskException {

        if (isCoordinatedTask(taskName)) {
            // The single refusing-state guard: a REFUSING_PROFILE_INVALID node never tracks,
            // registers, or schedules a coordinated task; local tasks fall through untouched.
            if (TaskHandlingConfigUtils.getBootstrapState() == CoordinationBootstrapState.REFUSING_PROFILE_INVALID) {
                if (refusalWarnedTasks.add(taskName)) {
                    log.warn("Coordinated task [" + taskName + "] is neither tracked, registered nor scheduled: "
                            + "this node is in REFUSING_PROFILE_INVALID (invalid coordination hardening profile). "
                            + "Local tasks are unaffected. Fix the deployment.toml and restart.");
                }
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("Adding task [" + taskName + "] to the data base since this is a coordinated task.");
            }
            deployedCoordinatedTasks.add(taskName);
            CoordinatedTask.States state;
            if (scheduledInPausedMode) {
                state = CoordinatedTask.States.PAUSED;
            } else {
                state = CoordinatedTask.States.NONE;
            }
            // A null boot-pass handle is resolved against the controller's open pass for this task type —
            // an open pass in the current generation means BOOT_PASS, none means STEADY_STATE.
            // STOCK/REFUSING hold no controller, so a null handle stays null (today's path).
            BootPassHandle effectiveHandle = handle;
            try {
                if (coordinationHardened) {
                    if (effectiveHandle == null) {
                        BootLeaseController controller = TasksDSComponent.getBootLeaseController();
                        if (controller != null) {
                            effectiveHandle = controller.getOpenTypeBootPass(this.getTaskType());
                        }
                    }
                    // the doorway call site maps the handle into the doorway's RegistrationPhase
                    registerThroughDoorway(taskName, state,
                            effectiveHandle != null ? RegistrationPhase.BOOT_PASS
                                    : RegistrationPhase.STEADY_STATE);
                } else {
                    taskStore.addTaskIfNotExist(taskName, state);
                }
            } catch (TaskCoordinationException ex) {
                additionFailedTasks.add(new TaskEntry(taskName, state));
                if (effectiveHandle != null) {
                    effectiveHandle.recordFailure(taskName, ex);
                }
                throw new TaskException("Error adding task : " + taskName, TaskException.Code.DATABASE_ERROR, ex);
            }
            return;
        }
        if (scheduledInPausedMode) {
            scheduleTaskInPausedMode(taskName);
        } else {
            scheduleTask(taskName);
        }
    }

    public List<TaskEntry> getAdditionFailedTasks() {
        return new ArrayList<>(additionFailedTasks);
    }

    public void removeTaskFromAdditionFailedTaskList(TaskEntry taskName) {
        additionFailedTasks.remove(taskName);
    }

    /**
     * The ONLY way any code asks "does this node hold the artifact?". No concurrent collection makes
     * check-then-act atomic: a caller can read "absent" while a deployment thread is mid-handleTask —
     * that race is accepted and covered, because the arriving artifact's registration serializes on
     * the task lock against any wave. Do NOT "fix" this with a lock around the check.
     */
    public boolean hasLocalArtifact(String taskName) {
        return deployedCoordinatedTasks.contains(taskName)
                || parkedTasks.contains(taskName);
    }

    /**
     * A check site only reads the set and skips; only doorway results mutate it.
     */
    public boolean isParked(String taskName) {
        return parkedTasks.contains(taskName);
    }

    /**
     * The single registration doorway call: computes the canonical fingerprint and trigger family
     * from the local definition BEFORE the call (ScheduleFingerprintEncoder is the only producer) and
     * applies the documented parked-set transitions to the result.
     */
    private RegistrationResult registerThroughDoorway(String taskName, CoordinatedTask.States initialState,
                                                      RegistrationPhase phase) throws TaskCoordinationException {

        TaskInfo taskInfo;
        try {
            taskInfo = this.getTaskRepository().getTask(taskName);
        } catch (TaskException e) {
            throw new TaskCoordinationException("Unable to read the local definition of task [" + taskName
                    + "] for registration.", e);
        }
        if (taskInfo == null || taskInfo.getTriggerInfo() == null) {
            throw new TaskCoordinationException("No local definition exists for task [" + taskName
                    + "]; the registration doorway needs the artifact in hand.");
        }
        validateMisfirePolicy(taskName, taskInfo.getTriggerInfo());
        String scheduleFingerprint = ScheduleFingerprintEncoder.encode(taskInfo.getTriggerInfo());
        String triggerFamily = deriveTriggerFamily(taskInfo.getTriggerInfo());
        RegistrationResult result = taskStore.addTaskIfNotExist(taskName, initialState, scheduleFingerprint, triggerFamily,
                phase);
        if (result == RegistrationResult.DIFFERENT_FP_PARKED) {
            parkedTasks.add(taskName);
        } else {
            resolvePark(taskName);
        }
        return result;
    }

    /**
     * The parked-set removal half of the park lifecycle: readiness names the park, so when the last
     * park on this node resolves (a schedulable doorway result, or the undeploy of a parked artifact)
     * the parked-task condition clears. A park raised concurrently with the last resolution can be
     * transiently cleared; the per-cycle retry re-raises it on its next parked result.
     */
    private void resolvePark(String taskName) {

        if (parkedTasks.remove(taskName) && parkedTasks.isEmpty()) {
            TasksDSComponent.getReadinessRegistry().clear(PARKED_TASK_CONDITION);
        }
    }

    /**
     * The Misfire Pin's checked precondition: a coordinated trigger declaring a misfire policy that is
     * not skip-forward-compatible (ignore-misfire / fire-now variants replay old occurrences with
     * stale FIRE_KEYs) raises the misfire-validation readiness condition naming the task.
     */
    private void validateMisfirePolicy(String taskName, TaskInfo.TriggerInfo triggerInfo) {

        TaskConstants.TaskMisfirePolicy misfirePolicy = triggerInfo.getMisfirePolicy();
        if (!SKIP_FORWARD_COMPATIBLE_MISFIRE_POLICIES.contains(misfirePolicy)) {
            TasksDSComponent.getReadinessRegistry().raise(MISFIRE_VALIDATION_CONDITION,
                    CoordinationReadinessRegistry.ConditionClass.CONTINUOUS,
                    "task [" + taskName + "] declares misfire policy [" + misfirePolicy
                            + "], which is not skip-forward-compatible and would replay old occurrences with "
                            + "stale FIRE_KEYs");
        }
    }

    /**
     * The shipped trigger-construction predicate: cronExpression != null -> RECURRING (cron);
     * cronExpression == null && repeatCount == 0 -> ONESHOT (the bare once-only trigger, interval
     * ignored); repeatCount == -1 or > 0 -> RECURRING (interval).
     */
    public static String deriveTriggerFamily(TaskInfo.TriggerInfo triggerInfo) {

        if (triggerInfo.getCronExpression() != null) {
            return TaskQueryHelper.TRIGGER_FAMILY_RECURRING;
        }
        return triggerInfo.getRepeatCount() == 0 ? TaskQueryHelper.TRIGGER_FAMILY_ONESHOT
                : TaskQueryHelper.TRIGGER_FAMILY_RECURRING;
    }

    /**
     * The per-cycle park retry — simply the doorway again, STEADY_STATE always: a non-parked result
     * removes the name and normal scheduling follows on that same cycle.
     */
    public void retryParkedRegistrations() {

        if (!coordinationHardened) {
            return;
        }
        for (String taskName : parkedTasks) {
            try {
                registerThroughDoorway(taskName, CoordinatedTask.States.NONE, RegistrationPhase.STEADY_STATE);
            } catch (Throwable retryFailure) {
                if (log.isDebugEnabled()) {
                    log.debug("Park retry of task [" + taskName + "] did not converge this cycle.", retryFailure);
                }
            }
        }
    }

    /**
     * The RETIRING responder — how live nodes answer a retire wave. Invoked from the existing ~2 s
     * coordinated-task scheduler cycle (the same hook as the parked-registration retry), on every
     * patched node. Per wave, the LOCAL view only, through the single local check
     * hasLocalArtifact(taskName): name absent from both -> the boot-fenced ack (this node holds
     * nothing under that name); name present in either -> the durable dissent (a parked entry counts
     * as presence — deleting under it would strand the park). It answers RETIRING waves only, never
     * OPEN undeploy waves.
     */
    public void answerRetireWaves() {

        if (!coordinationHardened) {
            return;
        }
        List<RDMBSConnector.RetiringWaveRef> waves;
        try {
            waves = taskStore.getUnansweredRetiringWaves(localNodeId);
        } catch (Throwable readFailure) {
            if (log.isDebugEnabled()) {
                log.debug("The RETIRING responder's wave read did not complete this cycle.", readFailure);
            }
            return;
        }
        if (waves.isEmpty()) {
            return;
        }
        for (RDMBSConnector.RetiringWaveRef wave : waves) {
            try {
                if (hasLocalArtifact(wave.getTaskName())) {
                    if (taskStore.refuseRetiringWave(wave.getTaskName(), wave.getGuardUuid())) {
                        log.error("task [" + wave.getTaskName() + "] retire refused: node [" + localNodeId
                                + "] has the artifact deployed — retire withdrawn");
                    }
                } else {
                    taskStore.ackRetiringWave(wave.getTaskName(), wave.getGuardUuid(), localNodeId,
                            System.currentTimeMillis());
                }
            } catch (Throwable answerFailure) {
                log.error("The RETIRING responder could not answer the wave of task [" + wave.getTaskName()
                        + "] guard [" + wave.getGuardUuid() + "]; it retries next cycle.", answerFailure);
            }
        }
    }

    /**
     * The additionFailedTasks retry routes through the doorway and honors its result — never a blind
     * re-add.
     */
    public void retryAdditionFailedTask(TaskEntry taskEntry) throws TaskCoordinationException {

        if (coordinationHardened) {
            registerThroughDoorway(taskEntry.getName(), taskEntry.getState(), RegistrationPhase.STEADY_STATE);
        } else {
            taskStore.addTaskIfNotExist(taskEntry.getName(), taskEntry.getState());
        }
    }

    /**
     * Checks whether the particular task needs coordination or not.
     *
     * @param taskName - Name of the task.
     * @return - Needs coordination or not.
     */
    private boolean isCoordinatedTask(String taskName) {

        if (isTaskPinned(taskName)) {
            // if task is pinned it shouldn't be coordinated as it belongs to this node.
            return false;
        }
        DataHolder dataHolder = DataHolder.getInstance();
        return dataHolder.isCoordinationConfigured();
    }

    /**
     * Check whether pinned server is enabled for this task.
     *
     * @param taskName - The name of the task.
     * @return - whether pinned server enabled.
     */
    private boolean isTaskPinned(String taskName) {

        if (synapseEnvironment == null) {
            synapseEnvironment = MicroIntegratorBaseUtils.getSynapseEnvironment();
            if (synapseEnvironment == null) {
                return false;
            }
        }
        TaskDescription taskDescription =
                synapseEnvironment.getTaskManager().getTaskDescriptionRepository().getTaskDescription(taskName);
        if (taskDescription != null) { // would be null for MPs
            List pinnedServers = taskDescription.getPinnedServers();
            if (pinnedServers != null && !pinnedServers.isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("Pinned server enabled for task " + taskName);
                }
                return true;
            }
        }
        return false;
    }

    /**
     * Schedules the coordinated tasks.
     *
     * @param taskName The name of the task to be scheduled.
     * @throws TaskException Exception
     */
    public void scheduleCoordinatedTask(String taskName) throws TaskException {

        // the scheduling chokepoint: refuse a parked name before any JobDetail is built — defense in depth,
        // covering every caller, present and future
        if (parkedTasks.contains(taskName)) {
            log.warn("Refusing to schedule parked coordinated task [" + taskName
                    + "]; the per-cycle park retry is the doorway itself.");
            return;
        }
        if (coordinationHardened) {
            scheduleCoordinatedTaskWithPublication(taskName);
            return;
        }
        try {
            if (taskStore.updateTaskState(taskName, CoordinatedTask.States.RUNNING, localNodeId)) {
                if (!isPreviouslyScheduled(taskName, getTenantTaskGroup())) {
                    // update the task repo to remove pause
                    scheduleTask(taskName);
                } else {
                    resumeLocalTask(taskName);
                    notifyOnResume(taskName);
                }
                locallyRunningCoordinatedTasks.add(taskName);
            } else {
                log.error("Failed to update state as " + CoordinatedTask.States.RUNNING + " for task [" + taskName + "]"
                                  + ". Hence not scheduling.");
            }
        } catch (TaskCoordinationException e) {
            throw new TaskException(
                    "Exception occurred while updating the state of the task : " + taskName + " to :" + " "
                            + CoordinatedTask.States.RUNNING, TaskException.Code.DATABASE_ERROR, e);
        }
    }

    /**
     * Notifies the relevant components to resume operations associated with the specified task.
     *
     * <p>This method checks whether the given task is associated with a Message Processor or
     * an Inbound Endpoint and triggers the necessary action to resume the respective component.
     * If the task belongs to a Message Processor, the associated Message Processor is resumed
     * remotely. If the task belongs to an Inbound Endpoint, its state is updated to active.
     *
     * @param taskName the name of the task to be resumed
     */
    private void notifyOnResume(String taskName) {
        if (MiscellaneousUtil.isTaskOfMessageProcessor(taskName)) {
            String messageProcessorName = MiscellaneousUtil.getMessageProcessorName(taskName);
            MessageProcessor messageProcessor = synapseEnvironment.getSynapseConfiguration()
                    .getMessageProcessors().get(messageProcessorName);
            if (messageProcessor != null) {
                messageProcessor.resumeRemotely();
            }
            return;
        }
        TaskDescription taskDescription =
                synapseEnvironment.getTaskManager().getTaskDescriptionRepository().getTaskDescription(taskName);
        if (taskDescription.getProperty(TaskUtils.TASK_OWNER_PROPERTY)
                == TaskUtils.TASK_BELONGS_TO_INBOUND_ENDPOINT) {
            InboundEndpoint inboundEndpoint = synapseEnvironment.getSynapseConfiguration()
                    .getInboundEndpoint((String) taskDescription.getProperty(TaskUtils.TASK_OWNER_NAME));
            if (Objects.nonNull(inboundEndpoint)) {
                inboundEndpoint.updateInboundEndpointState(false);
            }
        }
    }

    /**
     * The TARGET scheduling transition (ownership publication (ii)): validate destined-to-me +
     * RUNNING-state update, then the expected-epoch CAS publishing THIS node's own live boot id —
     * commit, THEN build (or for resume paths REBUILD) the Quartz JobDetail from the just-published
     * tuple. A lost CAS means no JobDetail: publication honesty over availability.
     */
    private void scheduleCoordinatedTaskWithPublication(String taskName) throws TaskException {

        TaskInfo taskInfo = this.getTaskRepository().getTask(taskName);
        if (taskInfo == null || taskInfo.getTriggerInfo() == null) {
            throw new TaskException("Non-existing task for scheduling with name: " + taskName,
                    TaskException.Code.NO_TASK_EXISTS);
        }
        String localScheduleFp = ScheduleFingerprintEncoder.encode(taskInfo.getTriggerInfo());
        String localTriggerFamily = deriveTriggerFamily(taskInfo.getTriggerInfo());
        RDMBSConnector.PublishedOwnership published;
        try {
            published = taskStore.publishTargetOwnershipAndRun(taskName, localNodeId, localScheduleFp,
                    localTriggerFamily, clusterCoordinator.getHeartbeatMaxRetryInterval(),
                    System.currentTimeMillis());
        } catch (TaskCoordinationException e) {
            throw new TaskException(
                    "Exception occurred while updating the state of the task : " + taskName + " to :" + " "
                            + CoordinatedTask.States.RUNNING, TaskException.Code.DATABASE_ERROR, e);
        }
        if (published == null) {
            log.error("Failed to publish ownership and update state as " + CoordinatedTask.States.RUNNING
                    + " for task [" + taskName + "]. Hence not scheduling.");
            return;
        }
        JobDataMap fenceData = buildFenceData(taskName, published);
        if (!isPreviouslyScheduled(taskName, getTenantTaskGroup())) {
            if (this.isMyTaskTypeRegistered()) {
                this.scheduleLocalTask(taskName, false, fenceData);
            } else {
                throw new TaskException(
                        "Task type: '" + this.getTaskType() + "' is not registered in the current task node",
                        TaskException.Code.TASK_NODE_NOT_AVAILABLE);
            }
        } else {
            // a resume without a JobDetail rebuild leaves a stale epoch in the job and vetoes the
            // task permanently
            rebuildCoordinatedJobDetail(taskName, fenceData);
            resumeLocalTask(taskName);
            notifyOnResume(taskName);
        }
        locallyRunningCoordinatedTasks.add(taskName);
    }

    private JobDataMap buildFenceData(String taskName, RDMBSConnector.PublishedOwnership published) {

        JobDataMap fenceData = new JobDataMap();
        fenceData.put(CoordinatedTaskTriggerListener.JOB_DATA_COORDINATED, true);
        fenceData.put(CoordinatedTaskTriggerListener.JOB_DATA_TASK_KEY, taskName);
        fenceData.put(CoordinatedTaskTriggerListener.JOB_DATA_OWNER_EPOCH, published.getOwnerEpoch());
        fenceData.put(CoordinatedTaskTriggerListener.JOB_DATA_INCARNATION, published.getIncarnation());
        fenceData.put(CoordinatedTaskTriggerListener.JOB_DATA_OWNER_BOOT_ID, published.getOwnerBootId());
        fenceData.put(CoordinatedTaskTriggerListener.JOB_DATA_SCHEDULE_FP, published.getScheduleFp());
        fenceData.put(CoordinatedTaskTriggerListener.JOB_DATA_TRIGGER_FAMILY, published.getTriggerFamily());
        return fenceData;
    }

    public List<String> getLocallyRunningCoordinatedTasks() {
        return new ArrayList<>(locallyRunningCoordinatedTasks);
    }

    @Override
    public void removeTaskFromLocallyRunningTaskList(String taskName) {
        locallyRunningCoordinatedTasks.remove(taskName);
    }

    public List<String> getAllCoordinatedTasksDeployed() {
        return new ArrayList<>(deployedCoordinatedTasks);
    }

    /**
     * Stops the execution of the task.
     *
     * @param taskName - Name of the task.
     * @throws TaskException - Exception.
     */
    public void stopExecution(String taskName) throws TaskException {
        this.pauseLocalTask(taskName);
        locallyRunningCoordinatedTasks.remove(taskName);
    }

    /**
     * Temporarily stops the execution of the task Since the task should be able to resume after db recovery.
     *
     * @param taskName - Name of the task.
     * @throws TaskException - Exception.
     */
    public void stopExecutionTemporarily(String taskName) throws TaskException {
        this.pauseLocalTaskTemporarily(taskName);
        locallyRunningCoordinatedTasks.remove(taskName);
    }

    /**
     * Pause every locally running coordinated task in a single synchronized pass. Both
     * TaskEventListener.becameUnresponsive (heartbeat thread) and CoordinatedTaskScheduler.run's
     * finally block (polling thread) call this; they serialize on the `this` monitor — same monitor
     * as the inherited pauseLocalTaskTemporarily / scheduleLocalTask methods. The snapshot is taken
     * inside the lock so a concurrent caller sees the post-removal residue (typically empty).
     */
    @Override
    public synchronized void pauseAllLocallyRunningTasks() {
        List<String> tasks = new ArrayList<>(locallyRunningCoordinatedTasks);
        tasks.forEach(task -> {
            try {
                stopExecutionTemporarily(task);
            } catch (Throwable t) {
                log.error("Unable to pause the task " + task, t);
            }
        });
    }

    private void scheduleTask(String taskName) throws TaskException {
        if (this.isMyTaskTypeRegistered()) {
            this.scheduleLocalTask(taskName);
        } else {
            throw new TaskException(
                    "Task type: '" + this.getTaskType() + "' is not registered in the current task node",
                    TaskException.Code.TASK_NODE_NOT_AVAILABLE);
        }
    }

    private void scheduleTaskInPausedMode(String taskName) throws TaskException {
        if (this.isMyTaskTypeRegistered()) {
            this.scheduleLocalTask(taskName, true);
        } else {
            throw new TaskException(
                    "Task type: '" + this.getTaskType() + "' is not registered in the current task node",
                    TaskException.Code.TASK_NODE_NOT_AVAILABLE);
        }
    }

    @Override
    public boolean deleteTask(String taskName) throws TaskException {

        boolean result = this.deleteLocalTask(taskName);

        boolean isCoordinationEnabled = DataHolder.getInstance().isCoordinationEnabledGlobally();
        if (isCoordinationEnabled && hasLocalArtifact(taskName)) {
            // the Failed-Add Purge: undeploy cancels the pending re-add — pure hygiene, correct in
            // all configurations; sits BEFORE the hint check
            if (additionFailedTasks.removeIf(entry -> taskName.equals(entry.getName()))
                    && log.isDebugEnabled()) {
                log.debug("Removed task [" + taskName + "] from the addition-failed retry list on undeploy.");
            }
            if (coordinationHardened) {
                try {
                    adoptCrossNodeSkipWaitHint();
                    if (skipWaitHint) {
                        createAndLocallyAckBarrier(taskName, UUID.randomUUID().toString());
                    } else if (clusterCoordinator.isLeader()) {
                        coordinateDeleteWithClassifiedBarrier(taskName);
                    } else {
                        acknowledgeDeleteBarrierHardened(taskName);
                    }
                } catch (TaskCoordinationException ex) {
                    log.error("This node could not join the delete wave for task [" + taskName + "] — its row "
                            + "may outlive the undeploy until barrier recovery runs. Cause: " + ex.getMessage(), ex);
                }
                deployedCoordinatedTasks.remove(taskName);
                resolvePark(taskName);
                locallyRunningCoordinatedTasks.remove(taskName);
            } else if (taskDeleteBarrierEnabled) {
                try {
                    if (clusterCoordinator.isLeader()) {
                        coordinateDeleteWithBarrier(taskName);
                    } else {
                        acknowledgeDeleteBarrier(taskName);
                    }
                } catch (TaskCoordinationException ex) {
                    log.error("This node could not join the delete wave for task [" + taskName + "] — its row "
                            + "may outlive the undeploy until barrier recovery runs. Cause: " + ex.getMessage(), ex);
                }
                deployedCoordinatedTasks.remove(taskName);
                resolvePark(taskName);
                locallyRunningCoordinatedTasks.remove(taskName);
            } else {
                deleteTaskWithLegacyDelay(taskName);
            }
        }
        return result;
    }

    @Override
    public void handleTaskPause(String taskName) throws TaskException {

        if (hasLocalArtifact(taskName)) {
            if (locallyRunningCoordinatedTasks.contains(taskName)) {
                try {
                    if (coordinationHardened) {
                        writePausedStateFenced(taskName);
                    } else {
                        taskStore.updateTaskState(Collections.singletonList(taskName), CoordinatedTask.States.PAUSED);
                    }
                    stopExecution(taskName);
                } catch (TaskCoordinationException e) {
                    throw new TaskException("Pause failed for task : " + taskName, TaskException.Code.DATABASE_ERROR,
                                            e);
                }
            } else {
                try {
                    taskStore.deactivateTask(taskName);
                    if (coordinationHardened) {
                        // This path is administrator-originated by construction — the task is not
                        // locally held here — so the write is unconditioned but audited.
                        fencedWriteEpisodeLog.episodeWarn(taskName, ADMIN_DEACTIVATE_REASON,
                                "Unconditioned DEACTIVATED write for task [" + taskName + "] issued from node ["
                                        + localNodeId + "] — administrator-originated: the task is not "
                                        + "locally held on this node.");
                    }
                } catch (TaskCoordinationException e) {
                    throw new TaskException("Pause failed for task : " + taskName, TaskException.Code.DATABASE_ERROR,
                                            e);
                }
            }
            return;
        }
        pauseTask(taskName);
    }

    private void pauseTask(String taskName) throws TaskException {
        this.pauseLocalTask(taskName);
        TaskUtils.setTaskPaused(this.getTaskRepository(), taskName, true);
    }

    /**
     * Fenced Writes — the locally running task's own PAUSED transition: the fence tuple is already in
     * the local job's JobDataMap (stamped at scheduling). A job without a stamped tuple falls back to
     * the stock write.
     */
    private void writePausedStateFenced(String taskName) throws TaskCoordinationException {

        JobDataMap data = null;
        try {
            JobDetail jobDetail = getScheduler().getJobDetail(new JobKey(taskName, getTenantTaskGroup()));
            if (jobDetail != null) {
                data = jobDetail.getJobDataMap();
            }
        } catch (SchedulerException e) {
            log.warn("Unable to read the fence tuple of task [" + taskName + "] for the PAUSED state write; "
                    + "falling back to the stock write.", e);
        }
        if (data == null || !data.getBoolean(CoordinatedTaskTriggerListener.JOB_DATA_COORDINATED)) {
            taskStore.updateTaskState(Collections.singletonList(taskName), CoordinatedTask.States.PAUSED);
            return;
        }
        if (!taskStore.updateTaskStateFenced(taskName, CoordinatedTask.States.PAUSED, localNodeId,
                data.getInt(CoordinatedTaskTriggerListener.JOB_DATA_INCARNATION),
                data.getLong(CoordinatedTaskTriggerListener.JOB_DATA_OWNER_EPOCH),
                data.getString(CoordinatedTaskTriggerListener.JOB_DATA_OWNER_BOOT_ID))) {
            fencedWriteEpisodeLog.episodeWarn(taskName, FENCED_WRITE_REASON,
                    "State write [" + CoordinatedTask.States.PAUSED + "] for task [" + taskName
                            + "] matched zero rows — fenced or already-applied.");
        }
    }

    @Override
    public void registerTask(TaskInfo taskInfo) throws TaskException {
        this.registerLocalTask(taskInfo);
    }

    @Override
    public boolean isDeactivated(String taskName) throws TaskException {

        if (hasLocalArtifact(taskName)) {
            boolean isDeactivated = !CoordinatedTask.States.RUNNING.equals(getCoordinatedTaskState(taskName));
            if (log.isDebugEnabled()) {
                log.debug("Task [" + taskName + "] is " + (isDeactivated ? "" : "not") + " in deactivated state.");
            }
            return isDeactivated;
        }
        return !(getTaskState(taskName).equals(TaskState.NORMAL) || getTaskState(taskName).equals(TaskState.BLOCKED));
    }

    @Override
    public boolean isTaskRunning(String taskName) throws TaskException {

        if (hasLocalArtifact(taskName)) {
            boolean isRunning = CoordinatedTask.States.RUNNING.equals( getCoordinatedTaskState(taskName));
            if (log.isDebugEnabled()) {
                log.debug("Task [" + taskName + "] is " + (isRunning ? "" : "not") + " in running state.");
            }
            return isRunning;
        }
        return getTaskState(taskName).equals(TaskState.NORMAL);
    }

    private CoordinatedTask.States getCoordinatedTaskState(String taskName) {

        if (locallyRunningCoordinatedTasks.contains(taskName)) {
            return CoordinatedTask.States.RUNNING;
        }
        try {
            return taskStore.getTaskState(taskName);
        } catch (TaskCoordinationException e) {
            log.error("Error while retrieving state for task : " + taskName, e);
            return null;
        }
    }

    @Override
    public TaskState getTaskState(String taskName) throws TaskException {
        return this.getLocalTaskState(taskName);
    }


    @Override
    public void setMessageProcessorTaskState(String taskName, String taskState) {
        taskStore.addOrUpdateMessageProcessorState(taskName, taskState);
        recentlyUpdatedStates.put(taskName, taskState);
    }

    @Override
    public String getMessageProcessorTaskState(String taskName) {
        return taskStore.getMessageProcessorTaskState(taskName);
    }

    /**
     * Updates the message processor state in the registry.
     *
     * @param taskName  The name of the task
     * @param taskState The state to set
     */
    public void updateMessageProcessorStateInRegistry(String taskName, String taskState) {
        String localCached = recentlyUpdatedStates.get(taskName);
        if (!Objects.equals(taskState, localCached)) {
            // New or changed state; update registry and cache
            initializeRegistry();
            if ("CLEAR".equalsIgnoreCase(taskState)) {
                registry.delete(REG_PROCESSOR_BASE_PATH + taskName);
            } else {
                registry.newNonEmptyResource(REG_PROCESSOR_BASE_PATH + taskName, false, "text/plain", taskState,
                        MP_STATE);
            }
            recentlyUpdatedStates.put(taskName, taskState);
        }
    }

    private void initializeRegistry() {
        if (synapseEnvironment == null) {
            synapseEnvironment = MicroIntegratorBaseUtils.getSynapseEnvironment();
        }
        if (registry == null) {
            registry = synapseEnvironment.getSynapseConfiguration().getRegistry();
        }
    }

    @Override
    public void clearRecentlyUpdatedStates() {
        recentlyUpdatedStates.clear();
    }

    @Override
    public TaskInfo getTask(String taskName) throws TaskException {
        return this.getTaskRepository().getTask(taskName);
    }

    @Override
    public List<TaskInfo> getAllTasks() throws TaskException {
        return this.getTaskRepository().getAllTasks();
    }

    @Override
    public void rescheduleTask(String taskName) throws TaskException {
        // reschedule path parked check: a check site only reads the set and skips
        if (parkedTasks.contains(taskName)) {
            log.warn("Refusing to reschedule parked coordinated task [" + taskName
                    + "]; the per-cycle park retry is the doorway itself.");
            return;
        }
        if (this.isMyTaskTypeRegistered()) {
            this.rescheduleLocalTask(taskName);
        } else {
            throw new TaskException(
                    "Task type: '" + this.getTaskType() + "' is not registered in the current task node",
                    TaskException.Code.TASK_NODE_NOT_AVAILABLE);
        }
    }

    @Override
    public void handleTaskResume(String taskName) throws TaskException {

        if (hasLocalArtifact(taskName)) {
            // resume path parked check: a check site only reads the set and skips
            if (parkedTasks.contains(taskName)) {
                log.warn("Refusing to resume parked coordinated task [" + taskName
                        + "]; the per-cycle park retry is the doorway itself.");
                return;
            }
            resumeCoordinatedTask(taskName);
            return;
        }
        resumeTask(taskName);
    }

    private void resumeCoordinatedTask(String taskName) throws TaskException {
        try {
            taskStore.activateTask(taskName);
        } catch (TaskCoordinationException e) {
            throw new TaskException("Failed to resume task [" + taskName + "]", TaskException.Code.DATABASE_ERROR, e);
        }
    }

    private void resumeTask(String taskName) throws TaskException {
        this.resumeLocalTask(taskName);
        TaskUtils.setTaskPaused(this.getTaskRepository(), taskName, false);
    }

    public static class TaskEntry {
        private String name;
        private CoordinatedTask.States state;

        public TaskEntry(String name, CoordinatedTask.States state) {
            this.name = name;
            this.state = state;
        }

        public String getName() {

            return name;
        }

        public CoordinatedTask.States getState() {

            return state;
        }
    }

    /**
     * Legacy coordinated delete flow used when barrier feature flag is disabled.
     * Non-leader nodes skip DB delete and leader waits heartbeat delay before deleting.
     *
     * @param taskName task name
     */
    private void deleteTaskWithLegacyDelay(String taskName) {
        if (!clusterCoordinator.isLeader()) {
            log.warn("Hot deployment enabled. Hence the task " + taskName
                    + " will be deleted by the coordinator node.");
            return;
        }
        long hotDeploymentDelay = clusterCoordinator.getHeartbeatMaxRetryInterval();
        try {
            log.info("Waiting for " + hotDeploymentDelay + " ms to hotdeployment to settle.");
            try {
                Thread.sleep(hotDeploymentDelay); // Wait for nodes to settle
            } catch (InterruptedException e) {
                // Ignore to preserve legacy behavior
            }
            log.info("Deleting task " + taskName + " from the data base since this is a coordinated task.");
            taskStore.deleteTasks(Collections.singletonList(taskName));
        } catch (TaskCoordinationException ex) {
            log.error("Error while removing tasks.", ex);
        }
        deployedCoordinatedTasks.remove(taskName);
        resolvePark(taskName);
        locallyRunningCoordinatedTasks.remove(taskName);
    }

    /**
     * Sets the skip-wait hint with one ERROR log.
     */
    public static void setSkipWaitHint(String reason) {

        if (!skipWaitHint) {
            skipWaitHint = true;
            log.error("Skip-wait hint set: " + reason + " Further deletions in this sweep skip the barrier "
                    + "waiting; classification is left to the cleaner. The hint removes the waiting, never the "
                    + "evidence.");
        }
    }

    /**
     * Deliberately loose clearing rule: next successfully finalized wave, or restart.
     */
    public static void clearSkipWaitHint() {

        skipWaitHint = false;
    }

    /**
     * Cross-node signal (the grinding node is often not the classifier): on entering deleteTask(),
     * one indexed read of whether an OPEN wave is already past deadline with a live holdout.
     */
    private void adoptCrossNodeSkipWaitHint() {

        if (skipWaitHint) {
            return;
        }
        try {
            long now = System.currentTimeMillis();
            List<RDMBSConnector.DeleteBarrierView> openWaves =
                    taskStore.getDeleteBarriersByStatus(TaskQueryHelper.BARRIER_STATUS_OPEN);
            BarrierLivenessClassifier classifier = barrierLivenessClassifier();
            if (classifier == null) {
                return;
            }
            for (RDMBSConnector.DeleteBarrierView wave : openWaves) {
                if (now < wave.getDeadlineAt()) {
                    continue;
                }
                if (classifier.classifyMissing(wave.getTaskName(), wave.getGuardUuid(), now)
                        .containsValue(BarrierLivenessClassifier.NodeClass.LIVE)) {
                    setSkipWaitHint("an OPEN delete wave for task [" + wave.getTaskName()
                            + "] is past its deadline with a live holdout.");
                    return;
                }
            }
        } catch (Throwable advisoryFailure) {
            // the hint is advisory and JVM-local; a failed read never blocks the delete path
            if (log.isDebugEnabled()) {
                log.debug("Cross-node skip-wait hint read failed.", advisoryFailure);
            }
        }
    }

    private BarrierLivenessClassifier barrierLivenessClassifier() {

        BootLeaseController controller = TasksDSComponent.getBootLeaseController();
        if (!(controller instanceof BootLeaseControllerImpl)) {
            return null;
        }
        return new BarrierLivenessClassifier(taskStore, ((BootLeaseControllerImpl) controller).getGroupId(),
                clusterCoordinator.getHeartbeatMaxRetryInterval());
    }

    /**
     * The shared method for the skip-wait path (the coordinated branch of deleteTask and the worker's
     * acknowledgement path call it while the hint is set) and for the worker bootstrap: create the
     * barrier row if missing — guard stamped via the required read-then-branch (NULL -> stamp;
     * in-flight OPEN/FINALIZING/RETIRING -> join, never restamp; anything else is the guard-orphan
     * corruption class -> refuse + readiness) — write THIS node's own ack (boot-lease-fenced, like
     * every ack write), and return: no waiting, classification left to the cleaner.
     */
    private void createAndLocallyAckBarrier(String taskName, String guardUuid) throws TaskCoordinationException {

        long currentTime = System.currentTimeMillis();
        long deadlineAt = currentTime + clusterCoordinator.getHeartbeatMaxRetryInterval();
        List<String> expectedNodes = clusterCoordinator.getAllNodeIds();
        if (localNodeId != null && !expectedNodes.contains(localNodeId)) {
            expectedNodes.add(localNodeId);
        }
        RDMBSConnector.WaveHandle handle = taskStore.createOrJoinDeleteBarrier(taskName, guardUuid, localNodeId,
                expectedNodes, deadlineAt, currentTime);
        if (handle.isAlreadyCompleted()) {
            // Completed-wave handling: no task row remains — the delete this wave would perform already completed
            // cluster-wide (this node was down at finalize); nothing to acknowledge.
            log.info("Node [" + localNodeId + "] found the delete of task [" + taskName
                    + "] already completed cluster-wide; no wave to create or acknowledge.");
            return;
        }
        log.info("Node [" + localNodeId + "] " + (handle.isCreated() ? "created and acknowledged" : "joined")
                + " delete barrier for task [" + taskName + "] with guard [" + handle.getGuardUuid() + "].");
    }

    /**
     * Hardened leader path: create (or join) the wave, wait for acknowledgements, then run the single
     * guarded finalize — its in-transaction classification decides. All missing nodes consented ->
     * finalized as today. Any LIVE holdout -> ERROR + the skip-wait hint + return: the SAME wave stays
     * OPEN with the acks it already collected and is re-evaluated each cleaner cycle; nothing is
     * written.
     */
    private void coordinateDeleteWithClassifiedBarrier(String taskName) throws TaskCoordinationException {

        long currentTime = System.currentTimeMillis();
        long deadlineAt = currentTime + clusterCoordinator.getHeartbeatMaxRetryInterval();
        List<String> expectedNodes = clusterCoordinator.getAllNodeIds();
        if (localNodeId != null && !expectedNodes.contains(localNodeId)) {
            expectedNodes.add(localNodeId);
        }
        RDMBSConnector.WaveHandle handle = taskStore.createOrJoinDeleteBarrier(taskName,
                UUID.randomUUID().toString(), localNodeId, expectedNodes, deadlineAt, currentTime);
        if (handle.isAlreadyCompleted()) {
            // Completed-wave handling: no task row remains — an earlier wave already finalized this delete; the
            // undeploy has no store work left.
            log.info("Leader flow found the delete of task [" + taskName
                    + "] already completed cluster-wide; no wave to create or finalize.");
            return;
        }
        waitForDeleteBarrier(taskName, handle.getGuardUuid(), deadlineAt);
        RDMBSConnector.FinalizeReport report = taskStore.finalizeDeleteBarrierClassified(taskName,
                handle.getGuardUuid(), System.currentTimeMillis(),
                clusterCoordinator.getHeartbeatMaxRetryInterval());
        switch (report.getOutcome()) {
        case HOLDOUT:
            log.error("task [" + taskName + "] undeployed on [" + localNodeId + "] but node(s) "
                    + report.getHoldoutNodes() + " alive and not acknowledging — " + report.getHoldoutNodes()
                    + " likely still has the artifact; suspected false deployment state on [" + localNodeId
                    + "]");
            setSkipWaitHint("a live holdout was classified on the wave of task [" + taskName + "].");
            return;
        case FINALIZED:
            clearSkipWaitHint();
            log.info("Leader flow finalized delete barrier for task [" + taskName + "] with guard ["
                    + handle.getGuardUuid() + "]. Task row deleted: true");
            return;
        default:
            log.info("Leader flow left delete barrier of task [" + taskName + "] with guard ["
                    + handle.getGuardUuid() + "] in outcome [" + report.getOutcome() + "].");
        }
    }

    /**
     * Hardened worker path: acknowledge the leader-created wave (boot-lease-fenced) within the ack
     * window; if no wave appears, bootstrap one via the same read-then-branch creation — recovery
     * finalizes later.
     */
    private void acknowledgeDeleteBarrierHardened(String taskName) throws TaskCoordinationException {

        if (skipWaitHint) {
            createAndLocallyAckBarrier(taskName, UUID.randomUUID().toString());
            return;
        }
        long ackWindowMillis = clusterCoordinator.getHeartbeatMaxRetryInterval();
        long deadlineAt = System.currentTimeMillis() + ackWindowMillis;
        boolean acked = false;
        while (System.currentTimeMillis() <= deadlineAt) {
            acked = taskStore.acknowledgeOpenDeleteBarrierHardened(taskName, localNodeId,
                    System.currentTimeMillis());
            if (acked) {
                break;
            }
            long remaining = deadlineAt - System.currentTimeMillis();
            if (remaining <= 0) {
                break;
            }
            try {
                Thread.sleep(Math.min(remaining, 200));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        if (!acked) {
            createAndLocallyAckBarrier(taskName, UUID.randomUUID().toString());
        }
        log.info("Barrier acknowledgement for task [" + taskName + "] by node [" + localNodeId + "] : " + acked);
    }

    /**
     * Leader path for coordinated task delete.
     * Creates barrier rows, waits for worker acknowledgements (or deadline), and finalizes task-row deletion.
     *
     * @param taskName task name being hot-deployed
     * @throws TaskCoordinationException when barrier operations fail
     */
    private void coordinateDeleteWithBarrier(String taskName) throws TaskCoordinationException {
        long currentTime = System.currentTimeMillis();
        long hotDeploymentDelay = clusterCoordinator.getHeartbeatMaxRetryInterval();
        long deadlineAt = currentTime + hotDeploymentDelay;
        String guardUuid = UUID.randomUUID().toString();
        List<String> expectedNodes = clusterCoordinator.getAllNodeIds();
        if (localNodeId != null && !expectedNodes.contains(localNodeId)) {
            expectedNodes.add(localNodeId);
        }
        taskStore.createDeleteBarrier(taskName, guardUuid, localNodeId, expectedNodes, deadlineAt, currentTime);
        taskStore.acknowledgeOpenDeleteBarrier(taskName, localNodeId, currentTime);
        waitForDeleteBarrier(taskName, guardUuid, deadlineAt);
        boolean deleted = taskStore.finalizeDeleteBarrier(taskName, guardUuid, System.currentTimeMillis());
        log.info("Leader flow finalized delete barrier for task [" + taskName + "] with guard [" + guardUuid
                + "]. Task row deleted: " + deleted);

    }


    /**
     * Worker path for coordinated task delete.
     * Tries to acknowledge the leader created barrier with a bounded retry window.
     *
     * @param taskName task name being hot deployed
     * @throws TaskCoordinationException when acknowledgement operations fail
     */
    private void acknowledgeDeleteBarrier(String taskName) throws TaskCoordinationException {
        // Barrier can be created by leader slightly after worker reaches delete path.
        // Retry for the full heartbeat max retry interval, aligned with leader barrier deadline.
        long ackWindowMillis = clusterCoordinator.getHeartbeatMaxRetryInterval();
        long deadlineAt = System.currentTimeMillis() + ackWindowMillis;
        boolean acked = false;
        while (System.currentTimeMillis() <= deadlineAt) {
            acked = taskStore.acknowledgeOpenDeleteBarrier(taskName, localNodeId, System.currentTimeMillis());
            if (acked) {
                break;
            }
            long remaining = deadlineAt - System.currentTimeMillis();
            if (remaining <= 0) {
                break;
            }
            try {
                Thread.sleep(Math.min(remaining, 200));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        if (!acked) {
            bootstrapDeleteBarrierIfMissing(taskName);
        }
        log.info("Barrier acknowledgement for task [" + taskName + "] by node [" + localNodeId + "] : " + acked);

    }

    /**
     * Worker create deleteBarrier path when no leader created barrier appears within ACK window.
     * Uses guard compare and set ownership, so only one worker can create barrier rows for a task wave.
     * This path does not finalize deletion, recovery flow will finalize later.
     *
     * @param taskName task name
     * @return true if this worker won CAS and created barrier rows
     * @throws TaskCoordinationException when barrier operations fail
     */
    private boolean bootstrapDeleteBarrierIfMissing(String taskName) throws TaskCoordinationException {
        long currentTime = System.currentTimeMillis();
        long hotDeploymentDelay = clusterCoordinator.getHeartbeatMaxRetryInterval();
        long deadlineAt = currentTime + hotDeploymentDelay;
        String observedGuard = taskStore.getCurrentDeleteGuardUuid(taskName);
        String bootstrapGuard = UUID.randomUUID().toString();
        List<String> expectedNodes = clusterCoordinator.getAllNodeIds();
        if (localNodeId != null && !expectedNodes.contains(localNodeId)) {
            expectedNodes.add(localNodeId);
        }

        boolean wonOwnership = taskStore.tryCreateDeleteBarrierWithGuardCas(taskName, observedGuard, bootstrapGuard,
                localNodeId, expectedNodes, deadlineAt, currentTime);
        if (!wonOwnership) {
            // Another node has already claimed the guard CAS and will drive barrier finalize for this task.
            log.warn("Bootstrap barrier ownership was not acquired by node [" + localNodeId + "] for task ["
                    + taskName + "].");
            return false;
        }

        // Worker bootstrap owner only opens and ACKs barrier state.
        // Final delete is delegated to recovery path in this exception scenario.
        taskStore.acknowledgeOpenDeleteBarrier(taskName, localNodeId, currentTime);
        log.info("Node [" + localNodeId + "] created and acknowledged bootstrap delete barrier for task ["
                + taskName + "] with guard [" + bootstrapGuard + "]. Finalization will be handled by recovery flow.");
        return true;
    }

    /**
     * Waits until all expected nodes acknowledge the delete barrier or the barrier deadline is reached.
     *
     * @param taskName task name
     * @param guardUuid barrier token
     * @param deadlineAt barrier deadline in epoch millis
     * @throws TaskCoordinationException when acknowledgement-check queries fail
     */
    private void waitForDeleteBarrier(String taskName, String guardUuid, long deadlineAt)
            throws TaskCoordinationException {
        while (System.currentTimeMillis() < deadlineAt) {
            if (taskStore.areAllExpectedNodesAcked(taskName, guardUuid)) {
                return;
            }
            long remaining = deadlineAt - System.currentTimeMillis();
            if (remaining <= 0) {
                return;
            }
            try {
                Thread.sleep(Math.min(remaining, 200));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

}

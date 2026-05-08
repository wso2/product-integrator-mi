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
import org.wso2.micro.integrator.coordination.ClusterCoordinator;
import org.wso2.micro.integrator.core.util.MicroIntegratorBaseUtils;
import org.wso2.micro.integrator.ntask.common.TaskException;
import org.wso2.micro.integrator.ntask.coordination.TaskCoordinationException;
import org.wso2.micro.integrator.ntask.coordination.task.CoordinatedTask;
import org.wso2.micro.integrator.ntask.coordination.task.store.TaskStore;
import org.wso2.micro.integrator.ntask.core.TaskInfo;
import org.wso2.micro.integrator.ntask.core.TaskRepository;
import org.wso2.micro.integrator.ntask.core.TaskUtils;
import org.wso2.micro.integrator.ntask.core.impl.AbstractQuartzTaskManager;
import org.wso2.micro.integrator.ntask.core.internal.DataHolder;
import org.wso2.micro.integrator.ntask.core.internal.TaskHandlingConfigUtils;
import org.wso2.micro.integrator.ntask.core.internal.TasksDSComponent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is responsible for handling / scheduling all tasks in Micro Integrator.
 */
public class ScheduledTaskManager extends AbstractQuartzTaskManager {

    private static Log log = LogFactory.getLog(ScheduledTaskManager.class);
    /**
     * The list which holds the list of coordinated tasks deployed in this node.
     */
    private List<String> deployedCoordinatedTasks = new ArrayList<>();

    /**
     * The list of tasks for which the addition failed.
     */
    private List<TaskEntry> additionFailedTasks = new ArrayList<>();

    private List<String> locallyRunningCoordinatedTasks = new ArrayList<>();

    private SynapseEnvironment synapseEnvironment = null;
    private TaskStore taskStore;
    private String localNodeId;
    private ClusterCoordinator clusterCoordinator;
    private final boolean taskDeleteBarrierEnabled;

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
        log.info("Clustered task delete barrier flow is " + (taskDeleteBarrierEnabled ? "enabled" : "disabled")
                + ". Configure [" + TaskHandlingConfigUtils.TASK_DELETE_BARRIER_ENABLED_CONFIG + "] to control it.");
    }

    @Override
    public void initStartupTasks() throws TaskException {
        this.scheduleAllTasks();
    }

    private boolean isMyTaskTypeRegistered() {
        return TasksDSComponent.getTaskService().getRegisteredTaskTypes().contains(this.getTaskType());
    }

    /**
     * Handles the task with given name. Schedule if its not coordinated else update the task data base.
     *
     * @param taskName The name of the task
     * @throws TaskException - Exception
     */
    public void handleTask(String taskName) throws TaskException {

        handleTask(taskName, false);
    }

    public void handleTask(String taskName, boolean scheduledInPausedMode) throws TaskException {
        if (isCoordinatedTask(taskName)) {
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
            try {
                taskStore.addTaskIfNotExist(taskName, state);
            } catch (TaskCoordinationException ex) {
                additionFailedTasks.add(new TaskEntry(taskName, state));
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
        return dataHolder.isCoordinationEnabledGlobally();
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
        if (isCoordinationEnabled && deployedCoordinatedTasks.contains(taskName)) {
            if (taskDeleteBarrierEnabled) {
                try {
                    if (clusterCoordinator.isLeader()) {
                        coordinateDeleteWithBarrier(taskName);
                    } else {
                        acknowledgeDeleteBarrier(taskName);
                    }
                } catch (TaskCoordinationException ex) {
                    log.error("Error while removing tasks.", ex);
                }
                deployedCoordinatedTasks.remove(taskName);
                locallyRunningCoordinatedTasks.remove(taskName);
            } else {
                deleteTaskWithLegacyDelay(taskName);
            }
        }
        return result;
    }

    @Override
    public void handleTaskPause(String taskName) throws TaskException {

        if (deployedCoordinatedTasks.contains(taskName)) {
            if (locallyRunningCoordinatedTasks.contains(taskName)) {
                try {
                    taskStore.updateTaskState(Collections.singletonList(taskName), CoordinatedTask.States.PAUSED);
                    stopExecution(taskName);
                } catch (TaskCoordinationException e) {
                    throw new TaskException("Pause failed for task : " + taskName, TaskException.Code.DATABASE_ERROR,
                                            e);
                }
            } else {
                try {
                    taskStore.deactivateTask(taskName);
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

    @Override
    public void registerTask(TaskInfo taskInfo) throws TaskException {
        this.registerLocalTask(taskInfo);
    }

    @Override
    public boolean isDeactivated(String taskName) throws TaskException {

        if (deployedCoordinatedTasks.contains(taskName)) {
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

        if (deployedCoordinatedTasks.contains(taskName)) {
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

        if (deployedCoordinatedTasks.contains(taskName)) {
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
        locallyRunningCoordinatedTasks.remove(taskName);
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

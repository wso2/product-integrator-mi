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

package org.wso2.micro.integrator.ntask.coordination.task.store.cleaner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.micro.integrator.coordination.ClusterCoordinator;
import org.wso2.micro.integrator.ntask.coordination.TaskCoordinationException;
import org.wso2.micro.integrator.ntask.coordination.task.CoordinatedTask;
import org.wso2.micro.integrator.ntask.coordination.task.store.TaskStore;
import org.wso2.micro.integrator.ntask.coordination.task.store.connector.RDMBSConnector;
import org.wso2.micro.integrator.ntask.coordination.task.util.HotDeploymentWaveWaiter;
import org.wso2.micro.integrator.ntask.core.impl.standalone.ScheduledTaskManager;
import org.wso2.micro.integrator.ntask.core.internal.DataHolder;
import org.wso2.micro.integrator.ntask.core.internal.TaskHandlingConfigUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The class which is responsible for cleaning the task store. This will remove the tasks if they are invalid and
 * will remove the node assignments if they are no more present in the cluster.
 */
public class TaskStoreCleaner {

    private static final Log LOG = LogFactory.getLog(TaskStoreCleaner.class);
    private static final String DELETE_PENDING_STATE = "DELETE_PENDING";

    private DataHolder dataHolder = DataHolder.getInstance();
    private ClusterCoordinator clusterCoordinator = dataHolder.getClusterCoordinator();
    private TaskStore taskStore;
    private ScheduledTaskManager taskManager;
    private final boolean taskDeleteBarrierEnabled;
    // names that qualified for invalid-task removal on the previous sweep. The memory is only valid
    // between CONSECUTIVE sweeps of one leadership term — after a gap (lost leadership, long stall)
    // it is discarded rather than voiding the one-pass grace for a row reopened in the meantime.
    private static final long SWEEP_MEMORY_VALIDITY_MILLIS = 60_000;
    private final Set<String> invalidTaskSweepCandidates = new HashSet<>();
    private long lastInvalidTaskSweepAt;

    /**
     * Constructor.
     *
     * @param taskStore - Task database.
     */
    public TaskStoreCleaner(ScheduledTaskManager taskManager, TaskStore taskStore) {
        this.taskManager = taskManager;
        this.taskStore = taskStore;
        this.taskDeleteBarrierEnabled = TaskHandlingConfigUtils.isTaskDeleteBarrierEnabled();
    }

    /**
     * Cleans the task store. Removes the invalid tasks and invalid nodes ( nodes that are no more in cluster ).
     *
     * @throws TaskCoordinationException When something goes wrong while connecting to the store.
     */
    public void clean() throws TaskCoordinationException {

        LOG.debug("Starting task store cleaning.");
        List<CoordinatedTask> allTasks = taskStore.getAllTaskNames();
        List<String> allNodesAvailableInCluster = clusterCoordinator.getAllNodeIds();
        if (allTasks.isEmpty()) {
            LOG.debug("No tasks found in task database.");
            if (!taskDeleteBarrierEnabled) {
                LOG.debug("Completed task store cleaning.");
                return;
            }
        } else {
            removeInvalidTasksFromStore(allTasks, allNodesAvailableInCluster);
            validateDestinedNodeAndUpdateStore(allNodesAvailableInCluster);
        }
        if (taskDeleteBarrierEnabled) {
            recoverExpiredOrAbandonedDeleteBarriers(allNodesAvailableInCluster);
        }
        LOG.debug("Completed task store cleaning.");
    }

    /**
     * Checks whether the destined node is valid and remove it if it is not.
     *
     * @param allNodesAvailableInCluster - all available nodes in the cluster
     * @throws TaskCoordinationException - When something goes wrong while updating tasks.
     */
    private void validateDestinedNodeAndUpdateStore(List<String> allNodesAvailableInCluster)
            throws TaskCoordinationException {

        List<CoordinatedTask> assignedIncompleteTasks = taskStore.getAllAssignedIncompleteTasks();

        if (allNodesAvailableInCluster.isEmpty()) {
            LOG.warn("No nodes are registered to the cluster successfully yet.");
            return;
        }
        List<String> tasksToBeUpdated = new ArrayList<>();
        assignedIncompleteTasks.forEach(task -> {
            // check whether the node assigned is still valid
            String nodeId = task.getDestinedNodeId();
            if (!allNodesAvailableInCluster.contains(nodeId)) {
                String taskName = task.getTaskName();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("The node [" + nodeId + "] of task [" + taskName + "] is not found in cluster"
                                      + ". Hence the node assignment will be removed.");
                }
                LOG.info("The node [" + nodeId + "] of task [" + taskName + "] is not found in cluster."
                        + " Hence the node assignment will be removed.");
                tasksToBeUpdated.add(taskName);
            }
        });
        if (taskDeleteBarrierEnabled && !tasksToBeUpdated.isEmpty()) {
            try {
                HotDeploymentWaveWaiter.waitForHotDeploymentWaveToSettle(taskStore, clusterCoordinator, LOG,
                        "invalid node unassignment");
            } catch (TaskCoordinationException e) {
                LOG.warn("Unable to wait for hot deployment wave settling before invalid-node unassignment. "
                        + "Proceeding with immediate cleanup.");
            }
        }
        taskStore.unAssignAndUpdateState(tasksToBeUpdated);
    }

    /**
     * From the list of tasks provided removes the invalid tasks entries in the store ( i.e tasks that are not
     * deployed as coordinated task.
     *
     * @param tasksList - The list of tasks to be checked.
     * @param allNodesAvailableInCluster - all available nodes in the cluster
     * @throws TaskCoordinationException - When something goes wrong while updating the store.
     */
    private void removeInvalidTasksFromStore(List<CoordinatedTask> tasksList, List<String> allNodesAvailableInCluster)
            throws TaskCoordinationException {

        List<String> deployedCoordinatedTasks = taskManager.getAllCoordinatedTasksDeployed();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Following list of tasks are found deployed coordinated task list.");
            deployedCoordinatedTasks.forEach(LOG::debug);
        }
        // We first add to list and then to the store  while deploying. So all the tasks retrieved from the store
        // which has valid node ids should be in the list, if not they are invalid entries.
        tasksList.removeIf(task -> allNodesAvailableInCluster.contains(task.getDestinedNodeId()));
        tasksList.removeIf(task -> deployedCoordinatedTasks.contains(task.getTaskName()));
        List<String> tasksToDelete = new ArrayList<>();
        for (CoordinatedTask task : tasksList) {
            tasksToDelete.add(task.getTaskName());
        }
        if (TaskHandlingConfigUtils.isCoordinationHardeningEnabled()) {
            // One pass of grace: an unassigned row absent from this leader's own inventory may be a
            // just-reopened registration on another node that the resolver has not assigned yet.
            // Delete only names that also qualified on the previous sweep — a genuine orphan still
            // qualifies then, while a legitimate row is assigned within a cycle and drops out.
            long now = System.currentTimeMillis();
            if (now - lastInvalidTaskSweepAt > SWEEP_MEMORY_VALIDITY_MILLIS) {
                invalidTaskSweepCandidates.clear();
            }
            lastInvalidTaskSweepAt = now;
            List<String> qualifying = tasksToDelete;
            tasksToDelete = new ArrayList<>(qualifying);
            tasksToDelete.retainAll(invalidTaskSweepCandidates);
            invalidTaskSweepCandidates.clear();
            invalidTaskSweepCandidates.addAll(qualifying);
            if (!tasksToDelete.isEmpty()) {
                LOG.info("Invalid-task sweep removing task row(s) " + tasksToDelete + ": not deployed on this "
                        + "leader and unassigned for two consecutive sweeps.");
            }
        }
        List<String> skippedTasks = taskStore.deleteTasksIfStateNotMatch(tasksToDelete, DELETE_PENDING_STATE);
        for (String skippedTask : skippedTasks) {
            LOG.info("Skipping invalid task cleanup for task [" + skippedTask + "] because it is in ["
                    + DELETE_PENDING_STATE + "] state.");
        }
        if (LOG.isDebugEnabled()) {
            tasksToDelete.forEach(removedTask -> LOG.debug("Removed invalid task :" + removedTask));
        }
    }

    /**
     * Recovers delete barriers owned by dead nodes or expired by deadline.
     *
     * @param allNodesAvailableInCluster currently live nodes
     * @throws TaskCoordinationException when recovery query execution fails
     */
    private void recoverExpiredOrAbandonedDeleteBarriers(List<String> allNodesAvailableInCluster)
            throws TaskCoordinationException {

        if (TaskHandlingConfigUtils.isCoordinationHardeningEnabled()) {
            recoverInFlightDeleteBarriersClassified();
            return;
        }
        List<String> recoveredTaskNames = taskStore.recoverExpiredOrAbandonedDeleteBarriers(allNodesAvailableInCluster,
                System.currentTimeMillis());
        if (recoveredTaskNames.isEmpty()) {
            return;
        }
        List<String> deployedCoordinatedTasks = taskManager.getAllCoordinatedTasksDeployed();
        for (String taskName : recoveredTaskNames) {
            if (!deployedCoordinatedTasks.contains(taskName)) {
                continue;
            }
            taskStore.addTaskIfNotExist(taskName);
            LOG.info("Recovery flow reinitialized coordinated task row for task [" + taskName
                    + "] after delete barrier recovery.");
        }
    }

    /**
     * Hardened recovery: re-classifies every in-flight wave each cycle through the single guarded
     * finalize shared with the leader path (the stock pendingOnly finalize is gone from this flow); a
     * LIVE holdout leaves the SAME wave OPEN with its collected acks. The re-add loop keeps its
     * shipped shape, but the call site is the claim-status-only repair — parked names are not
     * "missing"; the park's retry is the doorway itself, run by the deploying side, which does hold
     * the artifact.
     */
    private void recoverInFlightDeleteBarriersClassified() throws TaskCoordinationException {

        RDMBSConnector.BarrierRecoveryReport report = taskStore.recoverInFlightDeleteBarriersClassified(
                clusterCoordinator.getThisNodeId(), clusterCoordinator.getHeartbeatMaxRetryInterval(),
                System.currentTimeMillis());
        if (report.isLiveHoldoutOnLocalWave()) {
            ScheduledTaskManager.setSkipWaitHint("the cleaner classified a live holdout on a wave this node "
                    + "initiated or joined.");
        }
        List<String> recoveredTaskNames = report.getFinalizedTasks();
        if (recoveredTaskNames.isEmpty()) {
            return;
        }
        ScheduledTaskManager.clearSkipWaitHint();
        List<String> deployedCoordinatedTasks = taskManager.getAllCoordinatedTasksDeployed();
        for (String taskName : recoveredTaskNames) {
            if (!deployedCoordinatedTasks.contains(taskName)) {
                continue;
            }
            if (taskManager.isParked(taskName)) {
                continue;
            }
            taskStore.repairMissingTaskRow(taskName);
            LOG.info("Recovery flow ran the claim-status-only repair for task [" + taskName
                    + "] after delete barrier recovery.");
        }
    }

}

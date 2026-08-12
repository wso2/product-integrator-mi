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

package org.wso2.micro.integrator.ntask.coordination.task;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.micro.integrator.coordination.ClusterCoordinator;
import org.wso2.micro.integrator.coordination.MemberEventListener;
import org.wso2.micro.integrator.coordination.RDBMSMemberEventCallBack;
import org.wso2.micro.integrator.coordination.node.NodeDetail;
import org.wso2.micro.integrator.ntask.coordination.TaskCoordinationException;
import org.wso2.micro.integrator.ntask.coordination.task.resolver.TaskLocationResolver;
import org.wso2.micro.integrator.ntask.coordination.task.scehduler.CoordinatedTaskScheduler;
import org.wso2.micro.integrator.ntask.coordination.task.store.TaskStore;
import org.wso2.micro.integrator.ntask.coordination.task.util.HotDeploymentWaveWaiter;
import org.wso2.micro.integrator.ntask.core.BootLeaseController;
import org.wso2.micro.integrator.ntask.core.impl.standalone.ScheduledTaskManager;
import org.wso2.micro.integrator.ntask.core.internal.BootLeaseControllerImpl;
import org.wso2.micro.integrator.ntask.core.internal.CoordinatedTaskScheduleManager;
import org.wso2.micro.integrator.ntask.core.internal.DataHolder;
import org.wso2.micro.integrator.ntask.core.internal.TaskHandlingConfigUtils;
import org.wso2.micro.integrator.ntask.core.internal.TasksDSComponent;

import java.util.concurrent.ScheduledExecutorService;

/**
 * The event listener which is registered to capture the cluster events and handle scheduled tasks in accordance.
 */
public class TaskEventListener extends MemberEventListener {

    private static final Log LOG = LogFactory.getLog(TaskEventListener.class);

    private DataHolder dataHolder = DataHolder.getInstance();
    private ClusterCoordinator clusterCoordinator = dataHolder.getClusterCoordinator();
    private TaskStore taskStore;
    private TaskLocationResolver locationResolver;
    private ScheduledTaskManager taskManager;
    private final boolean taskDeleteBarrierEnabled;

    public TaskEventListener(ScheduledTaskManager taskManager, TaskStore taskStore,
                             TaskLocationResolver locationResolver) {

        this.taskManager = taskManager;
        this.taskStore = taskStore;
        this.locationResolver = locationResolver;
        this.taskDeleteBarrierEnabled = TaskHandlingConfigUtils.isTaskDeleteBarrierEnabled();
    }

    @Override
    public void memberAdded(NodeDetail nodeDetail) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Cluster member joined: node id [" + nodeDetail.getNodeId() + "]");
        }
        // The Config Fence recheck on memberAdded (cheap: the joiner's row is fresh) — a drifted node
        // joining later is flagged on the incumbent's side too, not only its own log. Detection on the
        // incumbent's side, not a second enforcement point.
        BootLeaseController bootLeaseController = TasksDSComponent.getBootLeaseController();
        if (bootLeaseController instanceof BootLeaseControllerImpl) {
            try {
                ((BootLeaseControllerImpl) bootLeaseController).runConfigFenceComparison();
            } catch (TaskCoordinationException fenceCheckFailure) {
                LOG.warn("The Config Fence recheck on member addition of node [" + nodeDetail.getNodeId()
                        + "] failed.", fenceCheckFailure);
            }
        }
        if (clusterCoordinator.isLeader()) {
            LOG.debug("Current node is leader, hence resolving unassigned tasks upon member addition.");
            ClusterCommunicator clusterCommunicator = new ClusterCommunicator(clusterCoordinator);
            CoordinatedTaskScheduler taskScheduler = new CoordinatedTaskScheduler(taskManager, taskStore,
                                                                                  locationResolver,
                                                                                  clusterCommunicator);
            try {
                taskScheduler.resolveUnassignedNotCompletedTasksAndUpdateStore();
            } catch (TaskCoordinationException taskResolutionFailure) {
                LOG.error("Failed to resolve unassigned tasks after cluster member joined: node id [" + nodeDetail
                        .getNodeId() + "]", taskResolutionFailure);
            }
        }
    }

    @Override
    public void memberRemoved(NodeDetail nodeDetail) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Member removed : " + nodeDetail.getNodeId());
        }
        String nodeId = nodeDetail.getNodeId();

        // Liveness guard against stale MEMBER_REMOVED events. MEMBERSHIP_EVENT_TABLE delivery is async,
        // and because the leader detects "removed" only after heartbeatMaxRetryInterval (~15s) while
        // "added" is detected almost immediately, a queued MEMBER_REMOVED for nodeId can arrive AFTER
        // nodeId has rejoined and re-INSERTed itself into CLUSTER_NODE_STATUS_TABLE. Acting on such a
        // stale event would strip the rejoined node's task assignments while it is actively running
        // them locally — producing orphan JmsConsumers on the broker. Skip the unAssign if the node
        // is currently in the cluster; the leader-only TaskStoreCleaner remains the authoritative
        // cleanup path for genuinely-removed nodes.
        if (clusterCoordinator.getAllNodeIds().contains(nodeId)) {
            LOG.info("Skipping stale memberRemoved for [" + nodeId
                    + "] — node is currently in the cluster.");
            return;
        }

        if (taskDeleteBarrierEnabled) {
            try {
                HotDeploymentWaveWaiter.waitForHotDeploymentWaveToSettle(taskStore, clusterCoordinator, LOG,
                        "removed node [" + nodeId + "] task unassignment");
            } catch (TaskCoordinationException e) {
                LOG.warn("Unable to wait for hot deployment wave settling before member removed cleanup for node ["
                        + nodeId + "]. Proceeding with immediate cleanup.");
            }
        }
        try {
            taskStore.unAssignAndUpdateState(nodeId);
        } catch (TaskCoordinationException e) {
            LOG.error("Error occurred while cleaning the tasks of node " + nodeId, e);
        }
    }

    @Override
    public void coordinatorChanged(NodeDetail nodeDetail) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Coordinator changed : " + nodeDetail.getNodeId());
        }
    }

    @Override
    public void becameUnresponsive(String nodeId) {

        LOG.debug("This node became unresponsive.");
        ScheduledExecutorService taskScheduler = dataHolder.getTaskScheduler();
        if (taskScheduler != null) {
            LOG.info("Shutting down coordinated task scheduler scheduler since the node became unresponsive.");
            // Shutdown the task scheduler forcefully since node became unresponsive and need to avoid task duplication.
            taskScheduler.shutdownNow();
            dataHolder.setTaskScheduler(null);
        }

        // Release JmsConsumers / scheduled-task resources held by this node. Shared synchronized
        // path with the polling thread's run() finally block — the monitor serializes the two
        // callers and the second one sees an empty list because stopExecutionTemporarily removes
        // entries from the running list as it goes.
        taskManager.pauseAllLocallyRunningTasks();

        // unAssignAndUpdateState(nodeId) is intentionally NOT called here. Two other paths already
        // do that work, and adding it here was creating a third concurrent writer racing against
        // the same TaskStore.cleanup lock — under sustained DB stalls this race produced duplicate
        // FATAL stack traces and stalled the coordinator scheduler.
        //   1. reJoined(nodeId) — fires on this node when DB recovers, calls unAssignAndUpdateState
        //      and restarts the scheduler. Now that wasMemberUnresponsive is set BEFORE
        //      notifyUnresponsiveness, this path is guaranteed to fire on recovery.
        //   2. The leader's TaskStoreCleaner — runs every leader iteration against the current
        //      CLUSTER_NODE_STATUS_TABLE and unassigns tasks of any genuinely-absent node.
        // Either path will mark this node's tasks unassigned; doing it a third time here only
        // adds contention and gains nothing.
    }


    @Override
    public void reJoined(String nodeId, RDBMSMemberEventCallBack callBack) {

        LOG.debug("Local node rejoined the coordination group: node id [" + nodeId + "].");
        BootLeaseController bootLeaseController = TasksDSComponent.getBootLeaseController();
        if (bootLeaseController != null) {
            // Legacy restart paths are requests INTO the lease state machine: they may wake the
            // recovery loop early, but they never clear a latch and never start dispatch while the
            // state is not PROVEN.
            bootLeaseController.wakeRecovery();
            // A coordination-database stall makes becameUnresponsive kill the scheduler and
            // pause the local jobs WITHOUT a lease transition (the Pause Watchdog observes JVM
            // freezes, not DB stalls), so no lease exit will ever restart dispatch. The rejoin is
            // that stall's recovery edge — hand this node's surviving RUNNING rows back and restart
            // the scheduler, PROVEN-gated inside the controller.
            if (bootLeaseController instanceof BootLeaseControllerImpl) {
                ((BootLeaseControllerImpl) bootLeaseController).restartAfterMembershipRejoin();
            }
            return;
        }
        try {
            // removing the node id so that it will be resolved and assigned again in case if member removal
            // hasn't happened already or the task hasn't been captured by task cleaning event.
            // this will ensure that the task duplication doesn't occur.
            taskStore.unAssignAndUpdateState(nodeId);
            // start the scheduler again since the node joined cluster successfully.
            CoordinatedTaskScheduleManager scheduleManager = new CoordinatedTaskScheduleManager(taskManager, taskStore,
                    clusterCoordinator, locationResolver);
            scheduleManager.startTaskScheduler(" upon rejoining the cluster");
        } catch (Throwable e) { // catching throwable so that we don't miss starting the scheduler
            LOG.error("Error occurred while cleaning the tasks while rejoining of node " + nodeId, e);
            if (callBack != null) {
                callBack.onExceptionThrown(nodeId, e);
            }
        }
    }

    @Override
    public void messageProcessorStateChanged(String messageProcessorName, String state) {
        taskManager.updateMessageProcessorStateInRegistry(messageProcessorName, state);
    }

}

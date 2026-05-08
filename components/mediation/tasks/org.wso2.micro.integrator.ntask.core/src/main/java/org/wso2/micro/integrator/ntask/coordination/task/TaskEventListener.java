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
import org.wso2.micro.integrator.ntask.common.TaskException;
import org.wso2.micro.integrator.ntask.coordination.TaskCoordinationException;
import org.wso2.micro.integrator.ntask.coordination.task.resolver.TaskLocationResolver;
import org.wso2.micro.integrator.ntask.coordination.task.scehduler.CoordinatedTaskScheduler;
import org.wso2.micro.integrator.ntask.coordination.task.store.TaskStore;
import org.wso2.micro.integrator.ntask.coordination.task.util.HotDeploymentWaveWaiter;
import org.wso2.micro.integrator.ntask.core.impl.standalone.ScheduledTaskManager;
import org.wso2.micro.integrator.ntask.core.internal.CoordinatedTaskScheduleManager;
import org.wso2.micro.integrator.ntask.core.internal.DataHolder;
import org.wso2.micro.integrator.ntask.core.internal.TaskHandlingConfigUtils;

import java.util.List;
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
            LOG.debug("Member added : " + nodeDetail.getNodeId());
        }
        if (clusterCoordinator.isLeader()) {
            LOG.debug("Current node is leader, hence resolving unassigned tasks upon member addition.");
            ClusterCommunicator clusterCommunicator = new ClusterCommunicator(clusterCoordinator);
            CoordinatedTaskScheduler taskScheduler = new CoordinatedTaskScheduler(taskManager, taskStore,
                                                                                  locationResolver,
                                                                                  clusterCommunicator);
            try {
                taskScheduler.resolveUnassignedNotCompletedTasksAndUpdateStore();
            } catch (TaskCoordinationException e) {
                LOG.error("Exception occurred while resolving un assigned tasks upon member addition " + nodeDetail
                        .getNodeId(), e);
            }
        }
    }

    @Override
    public void memberRemoved(NodeDetail nodeDetail) {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Member removed : " + nodeDetail.getNodeId());
        }
        String nodeId = nodeDetail.getNodeId();
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
        List<String> tasks = taskManager.getLocallyRunningCoordinatedTasks();
        // stop all running coordinated tasks.
        tasks.forEach(task -> {
            try {
                taskManager.stopExecutionTemporarily(task);
            } catch (TaskException e) {
                LOG.error("Unable to pause the task " + task, e);
            }
        });

        try {
            // Unassigns tasks from the specified node and updates their state in the task store.
            taskStore.unAssignAndUpdateState(nodeId);
        } catch (TaskCoordinationException e) {
            LOG.error("Error while removing the tasks of this node.", e);
        }
    }


    @Override
    public void reJoined(String nodeId, RDBMSMemberEventCallBack callBack) {

        LOG.debug("This node re-joined the cluster successfully.");
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

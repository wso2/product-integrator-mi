/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.carbon.inbound.endpoint.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.inbound.InboundRequestProcessor;
import org.apache.synapse.startup.quartz.StartUpController;
import org.apache.synapse.task.TaskConstants;
import org.apache.synapse.task.TaskDescription;
import org.apache.synapse.task.TaskManager;
import org.jetbrains.annotations.NotNull;
import org.wso2.carbon.inbound.endpoint.persistence.InboundEndpointsDataStore;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericTask;
import org.wso2.carbon.inbound.endpoint.protocol.jms.JMSTask;
import org.wso2.micro.integrator.mediation.ntask.NTaskTaskManager;
import org.wso2.micro.integrator.ntask.core.TaskUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This class provides the common implementation for polling protocol processors
 */
public abstract class InboundRequestProcessorImpl implements InboundRequestProcessor {

    protected SynapseEnvironment synapseEnvironment;
    protected long interval;
    protected String name;
    protected boolean coordination;
    protected boolean startInPausedMode;
    protected String cronExpression;

    private List<StartUpController> startUpControllersList = new ArrayList<>();
    private static final Log log = LogFactory.getLog(InboundRequestProcessorImpl.class);

    protected final static String COMMON_ENDPOINT_POSTFIX = "--SYNAPSE_INBOUND_ENDPOINT";

    /**
     * Based on the coordination option schedule the task with NTASK or run as a
     * background thread
     *
     * @param task
     * @param endpointPostfix
     */
    protected void start(InboundTask task, String endpointPostfix) {
        log.info("Starting the inbound endpoint [" + name + "]" + (startInPausedMode ? " in suspended mode" : "")
                + ", with coordination " + coordination + ". Interval : " + interval + ". Type : " + endpointPostfix);
        //This only supports Generic tasks for cron expression based scheduling
        if (task instanceof GenericTask)  {
            cronExpression = ((GenericTask) task).getPollingConsumer().getCronExpression();
        }
        handleTask(task, endpointPostfix);
    }

    /**
     * This function is for handling inbound task.
     */
    private void handleTask(InboundTask task, String endpointPostfix) {
        try {
            TaskDescription taskDescription = getTaskDescription(task, endpointPostfix);
            StartUpController startUpController = new StartUpController();
            startUpController.setTaskDescription(taskDescription);
            startUpController.init(synapseEnvironment);
            startUpControllersList.add(startUpController);
            // Register a listener to be notified when the local Cron/Generic task is deleted/paused
            if (task instanceof JMSTask || task instanceof GenericTask) {
                TaskManager taskManagerImpl = synapseEnvironment.getTaskManager().getTaskManagerImpl();
                if (taskManagerImpl instanceof NTaskTaskManager) {
                    NTaskTaskManager ntaskManager = (NTaskTaskManager) taskManagerImpl;
                    if (task instanceof JMSTask) {
                        ntaskManager.registerListener((JMSTask) task, taskDescription.getName());
                    } else if (task instanceof GenericTask) {
                        ntaskManager.registerListener((GenericTask) task, taskDescription.getName());
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error starting the inbound endpoint " + name + ". Unable to schedule the task. " + e
                    .getLocalizedMessage(), e);
        }
    }

    /**
     * Create a TaskDescription object for the given InboundTask and endpoint postfix.
     *
     * @param task The InboundTask for which the TaskDescription is to be created.
     * @param endpointPostfix The postfix to be appended to the task name and group.
     * @return A TaskDescription object populated with the relevant details from the InboundTask.
     */
    @NotNull
    private TaskDescription getTaskDescription(InboundTask task, String endpointPostfix) {
        TaskDescription taskDescription = new TaskDescription();
        taskDescription.setName(name + "-" + endpointPostfix);
        taskDescription.setTaskGroup(endpointPostfix);

        if (cronExpression != null && !cronExpression.isEmpty()) {
            taskDescription.setCronExpression(cronExpression);
        } else {
            if (interval < InboundTask.TASK_THRESHOLD_INTERVAL) {
                taskDescription.setInterval(InboundTask.TASK_THRESHOLD_INTERVAL);
            } else {
                taskDescription.setInterval(interval);
            }
            taskDescription.setIntervalInMs(true);
        }
        taskDescription.addResource(TaskDescription.INSTANCE, task);
        taskDescription.addResource(TaskDescription.CLASSNAME, task.getClass().getName());
        taskDescription.setTaskImplClassName(task.getClass().getName());
        taskDescription.addProperty(TaskUtils.TASK_OWNER_PROPERTY, TaskUtils.TASK_BELONGS_TO_INBOUND_ENDPOINT);
        taskDescription.addProperty(TaskUtils.TASK_OWNER_NAME, name);
        taskDescription.setStartInPausedMode(startInPausedMode);
        taskDescription.addProperty(TaskConstants.SKIP_START_IN_PAUSED_MODE_ASSIGNMENT, "true");
        return taskDescription;
    }
    

    /**
     * Stop the inbound polling processor This will be called when inbound is
     * undeployed/redeployed or when server stop
     */
    public void destroy() {
        log.info("Inbound endpoint " + name + " stopping.");

        if (!startUpControllersList.isEmpty()) {
            for (StartUpController sc : startUpControllersList) {
                sc.destroy();
            }
            startUpControllersList.clear();
        }
    }

    /**
     * Activates the Inbound Endpoint by activating any associated startup controllers.
     *
     * The decision on whether to use startup controllers (task) or inbound runner threads will depend
     * on the coordination enabled or not.
     * - if coordination enabled then startup controller otherwise inbound runner thread
     *
     * <p>This method first checks if there are any startup controllers. If there are, it attempts to activate
     * each controller and sets the success flag accordingly. The method returns a boolean indicating whether
     * the activation was successful.</p>
     *
     * @return {@code true} if at least one associated startup controller was successfully activated;
     *         {@code false} if activation task failed for all the startup controllers.
     */
    @Override
    public boolean activate() {
        log.info("Activating the Inbound Endpoint [" + name + "].");

        boolean isSuccessfullyActivated = false;
        if (!startUpControllersList.isEmpty()) {
            for (StartUpController sc : startUpControllersList) {
                if (sc.activateTask()) {
                    isSuccessfullyActivated = true;
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Failed to activate the consumer: " + sc.getTaskDescription().getName());
                    }
                }
            }
        }
        return isSuccessfullyActivated;
    }

    /**
     * Deactivates the Inbound Endpoint by deactivating any associated startup controllers.
     *
     * <p>This method first checks if there are any startup controllers. If there are, it attempts to deactivate
     * each controller and sets the success flag accordingly. The method returns a boolean indicating whether
     * the deactivation was successful.</p>
     *
     * @return {@code true} if all associated startup controllers were successfully deactivated;
     *         {@code false} if any deactivation task failed.
     */
    @Override
    public boolean deactivate() {
        log.info("Deactivating the Inbound Endpoint [" + name + "].");

        boolean isSuccessfullyDeactivated = true;
        if (!startUpControllersList.isEmpty()) {
            for (StartUpController sc : startUpControllersList) {
                if (!sc.deactivateTask()) {
                    if (log.isDebugEnabled()) {
                        log.debug("Failed to deactivate the consumer: " + sc.getTaskDescription().getName());
                    }
                    isSuccessfullyDeactivated = false;
                }
            }
        }
        return isSuccessfullyDeactivated;
    }

    /**
     * Checks if the Inbound Endpoint is deactivated. This method checks the status of any associated
     * startup controllers. The endpoint is considered deactivated if all
     * startup controllers are inactive and all inbound runner threads are paused.
     *
     * @return {@code true} if all startup controllers are inactive;
     *         {@code false} if any startup controller is active.
     */
    @Override
    public boolean isDeactivated() {
        if (!startUpControllersList.isEmpty()) {
            for (StartUpController sc : startUpControllersList) {
                if (sc.isTaskActive()) {
                    // Inbound Endpoint is considered active if at least one consumer is alive.
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public void pause() {

    }
}

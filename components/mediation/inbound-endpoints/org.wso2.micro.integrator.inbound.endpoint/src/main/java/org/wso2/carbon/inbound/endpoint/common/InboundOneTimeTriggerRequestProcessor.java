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
import org.apache.synapse.inbound.InboundTaskProcessor;
import org.apache.synapse.startup.quartz.StartUpController;
import org.apache.synapse.task.TaskConstants;
import org.apache.synapse.task.TaskDescription;
import org.apache.synapse.task.TaskManager;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericOneTimeTask;
import org.wso2.carbon.inbound.endpoint.protocol.rabbitmq.RabbitMQTask;
import org.wso2.micro.integrator.mediation.ntask.NTaskTaskManager;

import java.util.Objects;

import static org.wso2.carbon.inbound.endpoint.common.Constants.SUPER_TENANT_DOMAIN_NAME;

/**
 * This class provides the common implementation for one time trigger protocol processors
 * Implemented the support if message injection happens in a separate thread. ( using Callbacks )
 * One such requirement is loading the tenant when message is injected if at that moment tenant
 * is unloaded.
 */
public abstract class InboundOneTimeTriggerRequestProcessor implements InboundRequestProcessor, InboundTaskProcessor {

    protected StartUpController startUpController;
    protected SynapseEnvironment synapseEnvironment;
    protected String name;
    protected boolean coordination;
    protected boolean startInPausedMode;

    private static final Log log = LogFactory.getLog(InboundOneTimeTriggerRequestProcessor.class);

    protected final static String COMMON_ENDPOINT_POSTFIX = "--SYNAPSE_INBOUND_ENDPOINT";
    public static final int TASK_THRESHOLD_INTERVAL = 1000;

    /**
     * Based on the coordination option schedule the task with NTASK or run as a
     * background thread
     *
     * @param task
     * @param endpointPostfix
     */
    protected void start(OneTimeTriggerInboundTask task, String endpointPostfix) {
        log.info("Starting the inbound endpoint " + name + ", with coordination " + coordination + ". Type : "
                         + endpointPostfix);
        try {
            TaskDescription taskDescription = new TaskDescription();
            taskDescription.setName(name + "-" + endpointPostfix);
            taskDescription.setTaskGroup(endpointPostfix);
            taskDescription.setInterval(TASK_THRESHOLD_INTERVAL);
            taskDescription.setIntervalInMs(true);
            taskDescription.addResource(TaskDescription.INSTANCE, task);
            taskDescription.addResource(TaskDescription.CLASSNAME, task.getClass().getName());
            taskDescription.addProperty(TaskConstants.SKIP_START_IN_PAUSED_MODE_ASSIGNMENT, "true");
            startUpController = new StartUpController();
            startUpController.setTaskDescription(taskDescription);
            startUpController.init(synapseEnvironment);
            // registering a listener to identify task removal or deletions.
            TaskManager taskManagerImpl = synapseEnvironment.getTaskManager().getTaskManagerImpl();
            if (task instanceof RabbitMQTask) {
                if (taskManagerImpl instanceof NTaskTaskManager) {
                    ((NTaskTaskManager) taskManagerImpl).registerListener((RabbitMQTask) task,
                            taskDescription.getName());
                }
            } else if (task instanceof GenericOneTimeTask) {
                if (taskManagerImpl instanceof NTaskTaskManager) {
                    ((NTaskTaskManager) taskManagerImpl).registerListener((GenericOneTimeTask) task,
                            taskDescription.getName());
                }
            }
        } catch (Exception e) {
            log.error("Error starting the inbound endpoint " + name + ". Unable to schedule the task. " + e
                    .getLocalizedMessage(), e);
        }
    }

    /**
     * Stop the inbound polling processor This will be called when inbound is
     * undeployed/redeployed or when server stop
     */
    public void destroy() {
        destroy(true);
    }

    @Override
    public void destroy(boolean removeTask) {
        log.info("Inbound endpoint " + name + " stopping.");

        if (startUpController != null) {
            startUpController.destroy(removeTask);
        }
    }

    @Override
    public void pause() {

    }

    @Override
    public boolean activate() {
        log.info("Activating the Inbound Endpoint [" + name + "].");

        if (startUpController != null) {
            if (!startUpController.activateTask()) {
                log.error("Failed to activate the consumer task [" + startUpController.getTaskDescription().getName()
                        + "] for the inbound endpoint " + name);
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean deactivate() {
        log.info("Deactivating the Inbound Endpoint [" + name + "].");

        if (startUpController != null) {
            if (!startUpController.deactivateTask()) {
                log.error("Failed to deactivate the consumer task [" + startUpController.getTaskDescription().getName()
                        + "] for the inbound endpoint " + name);
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean isDeactivated() {

        if (Objects.nonNull(startUpController)) {
            return !startUpController.isTaskActive();
        }
        return true;
    }
}

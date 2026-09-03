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
package org.wso2.micro.integrator.ntask.core.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.micro.integrator.ntask.common.TaskException;
import org.wso2.micro.integrator.ntask.coordination.task.store.TaskStore;
import org.wso2.micro.integrator.ntask.core.BootLeaseController;
import org.wso2.micro.integrator.ntask.core.BootPassHandle;
import org.wso2.micro.integrator.ntask.core.TaskManager;
import org.wso2.micro.integrator.ntask.core.TaskManagerFactory;
import org.wso2.micro.integrator.ntask.core.TaskManagerId;
import org.wso2.micro.integrator.ntask.core.impl.standalone.ScheduledTasksManagerFactory;
import org.wso2.micro.integrator.ntask.core.internal.TaskHandlingConfigUtils;
import org.wso2.micro.integrator.ntask.core.internal.TasksDSComponent;
import org.wso2.micro.integrator.ntask.core.service.TaskService;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class represents the TaskService implementation.
 *
 * @see TaskService
 */
public class TaskServiceImpl implements TaskService {

    private static final Log log = LogFactory.getLog(TaskServiceImpl.class);
    private static final int SUPER_TENANT_ID = -1234;
    private Set<String> registeredTaskTypes;
    private boolean serverInit;
    private TaskManagerFactory taskManagerFactory;
    private TaskStore taskStore;

    public TaskServiceImpl(TaskStore taskStore) {

        this.taskStore = taskStore;
        this.registeredTaskTypes = new HashSet<>();
        this.taskManagerFactory = new ScheduledTasksManagerFactory();
        log.info("Starting task service .");
    }

    @Override
    public boolean isServerInit() {
        return serverInit;
    }

    public TaskManagerFactory getTaskManagerFactory() {
        return taskManagerFactory;
    }

    @Override
    public Set<String> getRegisteredTaskTypes() {
        return registeredTaskTypes;
    }

    private void initTaskManagersForType(String taskType) throws TaskException {
        if (log.isDebugEnabled()) {
            log.debug("Initializing task managers [" + taskType + "]");
        }
        // BOOT_PASS is per-type-first-pass, decided by the controller: the holder answers ARMED, the
        // controller (read at call time; null on STOCK/REFUSING) hands out a generation-bound handle
        // on the type's first pass and null thereafter. The handle threads as an explicit parameter.
        BootPassHandle handle = null;
        if (TaskHandlingConfigUtils.isCoordinationHardeningEnabled()) {
            BootLeaseController controller = TasksDSComponent.getBootLeaseController();
            if (controller != null) {
                handle = controller.beginTypeBootPass(taskType);
            }
        }
        try {
            List<TaskManager> startupTms = this.getTaskManagerFactory()
                    .getStartupSchedulingTaskManagersForType(taskType, taskStore);
            for (TaskManager tm : startupTms) {
                tm.initStartupTasks(handle);
            }
        } catch (TaskException | RuntimeException e) {
            // a type-level failure is recorded as type:<taskType> and then RETHROWN — shipped
            // propagation unchanged
            if (handle != null) {
                handle.recordFailure("type:" + taskType, e);
            }
            throw e;
        }
    }

    @Override
    public TaskManager getTaskManager(String taskType) throws TaskException {
        return this.getTaskManagerFactory().getTaskManager(new TaskManagerId(SUPER_TENANT_ID, taskType), taskStore);
    }

    @Override
    public synchronized void registerTaskType(String taskType) throws TaskException {
        this.registeredTaskTypes.add(taskType);
        /* if server has finished initializing, lets initialize the
         * task managers for this type */
        if (this.isServerInit()) {
            this.initTaskManagersForType(taskType);
        }
    }

    @Override
    public synchronized void serverInitialized() {
        try {
            this.serverInit = true;
            for (String taskType : this.getRegisteredTaskTypes()) {
                this.initTaskManagersForType(taskType);
            }
        } catch (TaskException e) {
            String msg = "Error initializing task managers: " + e.getMessage();
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

}

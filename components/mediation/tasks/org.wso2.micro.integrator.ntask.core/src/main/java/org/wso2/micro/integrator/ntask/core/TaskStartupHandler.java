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
package org.wso2.micro.integrator.ntask.core;

import org.wso2.micro.core.ServerStartupObserver;
import org.wso2.micro.integrator.ntask.core.internal.TasksDSComponent;
import org.wso2.micro.integrator.ntask.core.service.TaskService;

/**
 * A server startup handler implementation which is used as a listener to know when to start
 * scheduling the tasks.
 */
public class TaskStartupHandler implements ServerStartupObserver {

    private org.wso2.micro.integrator.ntask.core.service.TaskService taskService;

    public TaskStartupHandler(org.wso2.micro.integrator.ntask.core.service.TaskService taskService) {
        this.taskService = taskService;
    }

    public TaskService getTaskService() {
        return taskService;
    }

    @Override
    public void completingServerStartup() {
        /* do nothing */
    }

    @Override
    public void completedServerStartup() {
        BootLeaseController bootLeaseController = TasksDSComponent.getBootLeaseController();
        if (bootLeaseController == null) {
            // Stock nodes retain the original startup enumeration. Refusing nodes also enumerate local tasks,
            // while the coordinated-task guard in ScheduledTaskManager prevents their registration and scheduling.
            this.getTaskService().serverInitialized();
            return;
        }
        String bootId = bootLeaseController.bootId();  // Bind completion to the generation enumerated below.
        try {
            this.getTaskService().serverInitialized();
        } finally {
            bootLeaseController.completeBootPass(bootId); // A stale generation is logged and ignored.
        }
    }
}

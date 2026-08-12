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

package org.wso2.micro.integrator.ntask.core.internal;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.micro.integrator.coordination.ClusterCoordinator;
import org.wso2.micro.integrator.ntask.coordination.task.ClusterCommunicator;
import org.wso2.micro.integrator.ntask.coordination.task.resolver.TaskLocationResolver;
import org.wso2.micro.integrator.ntask.coordination.task.scehduler.CoordinatedTaskScheduler;
import org.wso2.micro.integrator.ntask.coordination.task.store.TaskStore;
import org.wso2.micro.integrator.ntask.coordination.task.store.cleaner.TaskStoreCleaner;
import org.wso2.micro.integrator.ntask.core.impl.standalone.ScheduledTaskManager;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * The class which manages CoordinatedTaskScheduler.
 *
 * @see CoordinatedTaskScheduler
 */
public class CoordinatedTaskScheduleManager {

    private static final Log LOG = LogFactory.getLog(CoordinatedTaskScheduleManager.class);

    private TaskStore taskStore;
    private ClusterCoordinator clusterCoordinator;
    private TaskLocationResolver resolver;
    private ScheduledTaskManager taskManager;
    // Time period in seconds in which the scheduler runs
    private static int executionPeriod = 2;
    // The frequency at which task resolving need to be done per cleaning.
    private static int resolveFrequency = 5;

    public CoordinatedTaskScheduleManager(ScheduledTaskManager taskManager, TaskStore taskStore,
                                          ClusterCoordinator clusterCoordinator, TaskLocationResolver resolver) {
        this.taskManager = taskManager;
        this.taskStore = taskStore;
        this.clusterCoordinator = clusterCoordinator;
        this.resolver = resolver;
    }

    static void setExecutionPeriod(int executionPeriod) {
        CoordinatedTaskScheduleManager.executionPeriod = executionPeriod;
    }

    static void setResolveFrequency(int resolveFrequency) {
        CoordinatedTaskScheduleManager.resolveFrequency = resolveFrequency;
    }

    /**
     * Spawns new scheduled executor service which is responsible for handling coordinated tasks.
     */
    public void startTaskScheduler(String msg) {

        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("CoordinatedTaskScheduler-%d")
                .build();
        ScheduledExecutorService taskSchedulerExecutor = Executors.newSingleThreadScheduledExecutor(namedThreadFactory);
        TaskStoreCleaner taskStoreCleaner = new TaskStoreCleaner(taskManager, taskStore);
        ClusterCommunicator connector = new ClusterCommunicator(clusterCoordinator);
        CoordinatedTaskScheduler taskScheduler = new CoordinatedTaskScheduler(taskManager, taskStore, resolver,
                                                                              connector, taskStoreCleaner,
                                                                              resolveFrequency);
        int initialDelay = 0; // can start immediately as the task service is already registered.
        LOG.info("Triggering coordinated task scheduler with an initial delay of " + initialDelay + " second(s) and a "
                         + "period of " + executionPeriod + " second(s)" + msg + ".");
        // the Ownership Sweep's completion stamp is initialized at scheduler start
        DataHolder.getInstance().setLastSweepCompletionNanos(System.nanoTime());
        taskSchedulerExecutor.scheduleWithFixedDelay(taskScheduler, initialDelay, executionPeriod, TimeUnit.SECONDS);
        DataHolder.getInstance().setTaskScheduler(taskSchedulerExecutor);
    }

}

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
package org.wso2.micro.integrator.mediation.ntask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.task.SynapseTaskException;
import org.apache.synapse.task.TaskDescription;
import org.apache.synapse.task.TaskManager;
import org.apache.synapse.task.TaskManagerObserver;
import org.wso2.config.mapper.ConfigParser;
import org.wso2.micro.core.ServerStartupHandler;
import org.wso2.micro.integrator.core.util.MicroIntegratorBaseUtils;
import org.wso2.micro.integrator.mediation.ntask.internal.NtaskService;
import org.wso2.micro.integrator.ntask.core.TaskInfo;
import org.wso2.micro.integrator.ntask.core.TaskUtils;
import org.wso2.micro.integrator.ntask.core.impl.LocalTaskActionListener;
import org.wso2.micro.integrator.ntask.core.service.TaskService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class NTaskTaskManager implements TaskManager, TaskServiceObserver, ServerStartupHandler {
    private final Object lock = new Object();

    static final int SUPER_TENANT_ID = -1234;

    private static final Log logger = LogFactory.getLog(NTaskTaskManager.class.getName());

    private String name;

    private boolean initialized = false;

    private org.wso2.micro.integrator.ntask.core.TaskManager taskManager;

    private final Map<String, Object> properties = new HashMap<>(5);

    private final Properties configProperties = new Properties();

    private final List<TaskManagerObserver> observers = new ArrayList<>();

    private final List<TaskDescription> taskQueue = new ArrayList<>();

    private final Object taskQueueLock = new Object();

    private static final String TASK_CONFIG = "task_handling";

    private static final String MP_STATE_PREFIX = "MESSAGE_PROCESSOR_STATE_";

    private static final String PERSIST_MESSAGE_PROCESSOR_STATE = "persist_message_processor_state";

    private static String persistMessageProcessorTaskState = null;

    @Override
    public boolean schedule(TaskDescription taskDescription) {
        if (logger.isDebugEnabled()) {
            logger.debug("#schedule Scheduling task : " + taskId(taskDescription));
        }
        TaskInfo taskInfo;
		try {
			taskInfo = TaskBuilder.buildTaskInfo(taskDescription, properties);
		} catch (Exception e) {
			if (logger.isDebugEnabled()) {
				logger.debug("#schedule Could not build task info object of task : " +taskId(taskDescription)+ ". Error: " +
				                     e.getLocalizedMessage(), e);
			}
			synchronized (lock) {
                queueTask(taskDescription);
			}
			return false;
		}
		if (!isInitialized()) {
			// if cannot schedule yet, put in the pending tasks list.
			synchronized (lock) {
                if (logger.isDebugEnabled()) {
                    logger.debug("#schedule Added pending task : " + taskId(taskDescription));
                }
                queueTask(taskDescription);
			}
			return false;
		}
		try {
			synchronized (lock) {
				if (taskManager == null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("#schedule Could not schedule task " + taskId(taskDescription) +
                                ". Task manager is not available.");
                    }
                    queueTask(taskDescription);
					return false;
				}
                taskManager.registerTask(taskInfo);
                if (NtaskService.getTaskService().isServerInit()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Submitting task [ " + taskId(taskDescription) + " ] to the task manager.");
                    }
                    taskManager.handleTask(taskInfo.getName(), taskDescription.isStartInPausedMode(), null);
                }
                removeTask(taskDescription);
            }
		} catch (Exception e) {
			logger.error("Scheduling task [" + taskId(taskDescription) + "::" +
			                     taskDescription.getTaskGroup() + "] FAILED. Error: " +
			                     e.getLocalizedMessage(), e);
			return false;
		}
		return true;
	}

    @Override
    public boolean reschedule(String taskName, TaskDescription taskDescription) {
		if (!isInitialized()) {
			return false;
		}
		try {
			synchronized (lock) {
				if (taskManager == null) {
					logger.warn("#reschedule Could not reschedule task [" + taskName +
					            "]. Task manager is not available.");
					return false;
				}
				TaskInfo taskInfo = taskManager.getTask(taskName);
				TaskDescription description = TaskBuilder.buildTaskDescription(taskInfo);
				taskInfo = TaskBuilder.buildTaskInfo(description, properties);
				taskManager.registerTask(taskInfo);
                final TaskService taskService = NtaskService.getTaskService();
                if (taskService != null && taskService.isServerInit()) {
                    taskManager.rescheduleTask(taskInfo.getName());
                }
            }
		} catch (Exception e) {
			return false;
		}
		return true;
	}

    @Override
    public boolean delete(String taskName) {
        if (taskName == null) {
            return false;
        }
        String list[] = taskName.split("::");
        String name = list[0];
        if (name == null || "".equals(name)) {
            throw new SynapseTaskException("Task name is null. ", logger);
        }

        String group = null;
        if (list.length > 1) {
            group = list[1];
        }
        if (group == null || "".equals(group)) {
            group = TaskDescription.DEFAULT_GROUP;
            if (logger.isDebugEnabled()) {
                logger.debug("#delete Task group is null or empty , using default group :"
                        + TaskDescription.DEFAULT_GROUP);
            }
        }
        try {
            boolean deleted;
            synchronized (lock) {
                if (!isInitialized()) {
                    checkAndDeleteTaskFromQueue(name, group, taskName);
                    return false;
                }
                if (taskManager == null) {
                    logger.warn("#delete Could not delete task [" + taskName + "]. Task manager is not available.");
                    return false;
                }
                deleted = taskManager.deleteTask(name);
                NTaskAdapter.removeProperty(taskName);
            }

            logger.debug("Deleted task [" + name + "] [" + deleted + "]");
            return deleted;
        } catch (Exception e) {
            logger.error("Cannot delete task [" + taskName + "::" + group + "]. Error: " + e.getLocalizedMessage(), e);
            return false;
        }
    }

    @Override
    public boolean pause(String taskName) {
        if (!isInitialized()) {
            return false;
        }
        try {
            synchronized (lock) {
                if (taskManager == null) {
                    logger.warn("#pause Could not pause task [" + taskName + "]. Task manager is not available.");
                    return false;
                }
                taskManager.handleTaskPause(taskName);
            }
            return true;
        } catch (Exception e) {
            logger.error("Cannot pause task [" + taskName + "]. Error: " + e.getLocalizedMessage(), e);
        }
        return false;
    }

    @Override
    public boolean pauseAll() {
        if (!isInitialized()) {
            return false;
        }
        try {
            synchronized (lock) {
                if (taskManager == null) {
                    logger.warn("#pauseAll Could not pause any task. Task manager is not available.");
                    return false;
                }
                List<TaskInfo> taskList = taskManager.getAllTasks();
                for (TaskInfo taskInfo : taskList) {
                    String taskName = taskInfo.getName();
                    taskManager.handleTaskPause(taskName);
                }
            }
            return true;
        } catch (Exception e) {
            logger.error("Cannot pause all tasks. Error: " + e.getLocalizedMessage(), e);
        }
        return false;
    }

    @Override
    public boolean resume(String taskName) {
        if (!isInitialized()) {
            return false;
        }
        if (taskName == null) {
            return false;
        }
        try {
            synchronized (lock) {
                if (taskManager == null) {
                    logger.warn("#resume Could not resume task [" + taskName + "]. Task manager is not available.");
                    return false;
                }
                taskManager.handleTaskResume(taskName);
            }
        } catch (Exception e) {
            logger.error("Cannot resume task [" + taskName + "]. Error: " + e.getLocalizedMessage(), e);
            return false;
        }
        return true;
    }

    @Override
    public boolean resumeAll() {
        if (!isInitialized()) {
            return false;
        }
        try {
            synchronized (lock) {
                if (taskManager == null) {
                    logger.warn("#resumeAll Could not resume any task. Task manager is not available.");
                    return false;
                }
                List<TaskInfo> taskList = taskManager.getAllTasks();
                for (TaskInfo taskInfo : taskList) {
                    taskManager.handleTaskResume(taskInfo.getName());
                }
            }
            return true;
        } catch (Exception e) {
            logger.error("Cannot resume all tasks. Error: " + e.getLocalizedMessage(), e);
        }
        return false;
    }

    @Override
    public TaskDescription getTask(String taskName) {
        if (!isInitialized()) {
            return null;
        }
        try {
            TaskInfo taskInfo;
            synchronized (lock) {
                if (taskManager == null) {
                    logger.warn("#getTask Could not retrieve task [" + taskName + "]. Task manager is not available.");
                    return null;
                }
                taskInfo = taskManager.getTask(taskName);
            }
            return TaskBuilder.buildTaskDescription(taskInfo);
        } catch (Exception e) {
            logger.error("Cannot return task [" + taskName + "]. Error: " + e.getLocalizedMessage(), e);
            return null;
        }
    }

    @Override
    public String[] getTaskNames() {
        if (!isInitialized()) {
            return new String[0];
        }
        try {
            List<TaskInfo> taskList;
            synchronized (lock) {
                if (taskManager == null) {
                    logger.warn("#getTaskNames Could not query task names. Task manager is not available.");
                    return new String[0];
                }
                taskList = taskManager.getAllTasks();
            }
            List<String> result = new ArrayList<String>();
            for (TaskInfo taskInfo : taskList) {
                result.add(taskInfo.getName());
            }
            return result.toArray(new String[result.size()]);
        } catch (Exception e) {
            logger.error("Cannot return task list. Error: " + e.getLocalizedMessage(), e);
        }
        return new String[0];
    }

    @Override
    public boolean init(Properties properties) {
        synchronized (lock) {
            try {
                TaskService taskService = NtaskService.getTaskService();
                if (taskService == null || NtaskService.getCcServiceInstance() == null) {
                    NtaskService.addObserver(this);
                    return false;
                }
                if ((taskManager = getTaskManager(false)) == null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("#init Could not initialize task manager. " + managerId());
                    }
                    return false;
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("#init Obtained Carbon task manager " + managerId());
                    }
                }

                initialized = true;
                    taskService.registerTaskType(Constants.TASK_TYPE_ESB);
                    updateAndCleanupObservers();


                logger.info("Initialized task manager. Tenant [" + getCurrentTenantId() + "]");
                if (logger.isDebugEnabled()) {
                    logger.debug("#init Initialized task manager : " + managerId());
                    logger.debug("#init Scheduling existing tasks if any. : " + managerId());
                }
                Object[] taskDescriptions = pendingTasks();
                for (Object d : taskDescriptions) {
                    schedule((TaskDescription) d);
                }

                return true;
            } catch (Exception e) {
                logger.error("Cannot initialize task manager. Error: " + e.getLocalizedMessage(), e);
                initialized = false;
            }
        }
        return false;
    }

    @Override
    public boolean update(Map<String, Object> parameters) {
        return init(parameters == null || !parameters.containsKey("init.properties") ? null
                : (Properties) parameters.get("init.properties"));
    }

    @Override
    public boolean isInitialized() {
        synchronized (lock) {
            return initialized;
        }
    }

    @Override
    public boolean start() {
        return isInitialized();
    }

    @Override
    public boolean stop() {
        // Nothing to do here.
        return true;
    }

    @Override
    public int getRunningTaskCount() {
        if (!isInitialized()) {
            return -1;
        }
        String[] names = getTaskNames();
        int count = 0;
        try {
            for (String name : names) {
                synchronized (lock) {
                    if (taskManager == null) {
                        logger.warn("#getRunningTaskCount Could not determine the number of running tasks. Task manager is not available.");
                        return -1;
                    }
                    if (taskManager.getTaskState(name)
                            .equals(org.wso2.micro.integrator.ntask.core.TaskManager.TaskState.NORMAL)) {
                        ++count;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Cannot return running task count. Error: " + e.getLocalizedMessage(), e);
        }
        return count;
    }

    public List<String> getRunningTaskList() {
        if (!isInitialized()) {
            return null;
        }
        String[] names = getTaskNames();
        List<String> runningTaskList = new ArrayList<String>();
        try {
            for (String name : names) {
                synchronized (lock) {
                    if (taskManager.getTaskState(name)
                            .equals(org.wso2.micro.integrator.ntask.core.TaskManager.TaskState.NORMAL)) {
                        runningTaskList.add(name);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Cannot return running task list. Error: " + e.getLocalizedMessage(), e);
        }
        return runningTaskList;
    }


    @Override
    public boolean isTaskRunning(Object o) {
        if (!isInitialized()) {
            return false;
        }
        String taskName;
        if (o instanceof String) {
            taskName = (String) o;
        } else {
            return false;
        }
        return checkTaskRunning(taskName);
    }

    @Override
    public boolean setProperties(Map<String, Object> properties) {
        if (properties == null) {
            return false;
        }
        for (String key : properties.keySet()) {
            synchronized (lock) {
                this.properties.put(key, properties.get(key));
            }
        }
        return true;
    }

    @Override
    public boolean setProperty(String name, Object property) {
        if (name == null) {
            return false;
        }
        initializePersistMPstate();
        if (Boolean.parseBoolean(persistMessageProcessorTaskState)) {
            String[] processorState = extractProcessorNameAndState(name, property);
            if (processorState != null) {
                String processorName = processorState[0];
                String state = processorState[1];
                if (state != null) {
                    taskManager.setMessageProcessorTaskState(processorName, state);
                }
            }
        }
        synchronized (lock) {
            properties.put(name, property);
        }
        return true;
    }

    /**
     * Checks and extracts processor name from a given key-value pair.
     *
     * @param key The property key
     * @param value The associated state value
     * @return Optional string array with [processorName, state], or null if not an MP state key
     */
    protected String[] extractProcessorNameAndState(String key, Object value) {
        if (key != null && !key.isEmpty() && key.startsWith(MP_STATE_PREFIX)) {
            String processorName = key.substring(MP_STATE_PREFIX.length());
            if (!processorName.isEmpty() && value != null) {
                return new String[]{processorName, value.toString()};
            }
        }
        return null;
    }

    protected String extractProcessorName(String key) {
        if (key != null && !key.isEmpty() && key.startsWith(MP_STATE_PREFIX)) {
            String processorName = key.substring(MP_STATE_PREFIX.length());
            if (!processorName.isEmpty()) {
                return processorName;
            }
        }
        return null;
    }

    private void initializePersistMPstate() {
        if (persistMessageProcessorTaskState == null) {
            Map<String, Object> configs = ConfigParser.getParsedConfigs();
            Object persistMPState = configs.get(TASK_CONFIG + "." + PERSIST_MESSAGE_PROCESSOR_STATE);
            if (persistMPState != null) {
                persistMessageProcessorTaskState = persistMPState.toString();
            }
        }
    }

    @Override
    public Object getProperty(String name) {
        if (name == null) {
            return null;
        }
        initializePersistMPstate();
        if (Boolean.parseBoolean(persistMessageProcessorTaskState)) {
            String processorName = extractProcessorName(name);
            if (processorName != null) {
                return taskManager.getMessageProcessorTaskState(processorName);
            }
        }
        synchronized (lock) {
            return properties.get(name);
        }
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getProviderClass() {
        return this.getClass().getName();
    }

    @Override
    public Properties getConfigurationProperties() {
        synchronized (lock) {
            return configProperties;
        }
    }

    @Override
    public void setConfigurationProperties(Properties properties) {
        if (properties == null) {
            return;
        }
        synchronized (lock) {
            configProperties.putAll(properties);
        }
    }

    private org.wso2.micro.integrator.ntask.core.TaskManager getTaskManager(boolean system) throws Exception {
        TaskService taskService = NtaskService.getTaskService();
        if (taskService == null) {
            return null;
        }
        return taskService.getTaskManager(Constants.TASK_TYPE_ESB);
    }

    private int getCurrentTenantId() {
        return SUPER_TENANT_ID;
    }

    public static int tenantId() {
        return SUPER_TENANT_ID;
    }

    @Override
    public void invoke() {
        //Initialize the Task Manager after server is started
        init(null);
    }

    private String taskId(TaskDescription description) {
        return "[NTask::" + getCurrentTenantId() + "::" + description.getName() + "]";
    }

    private String managerId() {
        return "[NTaskTaskManager::" + getCurrentTenantId() + " ::" + this.hashCode() + "]";
    }

    @Override
    public void addObserver(TaskManagerObserver o) {
        if (observers.contains(o)) {
            return;
        }
        observers.add(o);
    }

    @Override
    public boolean isTaskDeactivated(String taskName) {
        if (!isInitialized()) {
            return false;
        }
        synchronized (lock) {
            if (taskManager == null) {
                logger.warn("#isTaskRunning Could not determine the state of the task [" +
                            taskName + "]. Task manager is not available.");
                return false;
            }
            try {
                return taskManager.isDeactivated(taskName);
            } catch (Exception e) {
                /*
                 * This fix was given to avoid error messages printing
                 * while server shutdowns in cluster mode when MP is running.
                 * This is related to the issue ESBJAVA-4061.
                 */
                if (logger.isDebugEnabled()) {
                    logger.debug("Cannot return task status [" + taskName + "]. Error: " +
                            e.getLocalizedMessage(), e);
                }
            }
        }
        return false;
    }

    @Override
    public boolean isTaskBlocked(String taskName) {
        if (!isInitialized()) {
            return false;
        }
        synchronized (lock) {
            if (taskManager == null) {
                logger.warn("#isTaskRunning Could not determine the state of the task [" +
                            taskName + "]. Task manager is not available.");
                return false;
            }
            try {
                return taskManager.getTaskState(taskName)
                                  .equals(org.wso2.micro.integrator.ntask.core.TaskManager.TaskState.BLOCKED);
            } catch (Exception e) {
                logger.error("Cannot return task status [" + taskName + "]. Error: " +
                                     e.getLocalizedMessage(), e);
            }
        }
        return false;
    }

    @Override
    public boolean isTaskRunning(String taskName) {
        if (!isInitialized()) {
            return false;
        }
        return checkTaskRunning(taskName);
    }

    private boolean checkTaskRunning(String taskName) {
        synchronized (lock) {
            if (taskManager == null) {
                logger.warn("#isTaskRunning Could not determine the state of the task [" +
                            taskName + "]. Task manager is not available.");
                return false;
            }
            try {
                return taskManager.isTaskRunning(taskName);
            } catch (Exception e) {
                logger.error("Cannot return task status [" + taskName + "]. Error: " +
                                     e.getLocalizedMessage(), e);
            }
        }
        return false;
    }

    public boolean isTaskExist(String taskName) {
        if (!isInitialized()) {
            return false;
        }
        synchronized (lock) {
            if (taskManager == null) {
                logger.warn("#isTaskExist Could not determine the state of the task [" + taskName +
                            "]. Task manager is not available.");
                return false;
            }
            try {
                return !taskManager.getTaskState(taskName)
                                   .equals(org.wso2.micro.integrator.ntask.core.TaskManager.TaskState.NONE);
            } catch (Exception e) {
                logger.error("Cannot return task status [" + taskName + "]. Error: " +
                                     e.getLocalizedMessage(), e);
            }
        }
        return false;
    }

    private void updateAndCleanupObservers() {
        Iterator<TaskManagerObserver> i = observers.iterator();
        while (i.hasNext()) {
            TaskManagerObserver observer = i.next();
            observer.update();
            i.remove();
        }
    }

    private boolean queueTask(TaskDescription description) {
        synchronized (taskQueueLock) {
            logger.debug("#queueTask Queuing task " + taskId(description)) ;
            if (!taskQueue.contains(description)) {
                return taskQueue.add(description);
            }
        }
        return false;
    }

    private boolean removeTask(TaskDescription description) {
        synchronized (taskQueueLock) {
            logger.debug("#removeTask removing task " + taskId(description)) ;
            return taskQueue.remove(description);
        }
    }

    private Object[] pendingTasks() {
        synchronized (taskQueueLock) {
            return taskQueue.toArray();
        }
    }

    @Override
    public void sendClusterMessage(Callable<Void> callable) {
        //do nothing since we do not support clustering
    }

    /**
     * Registers a listener to the {@link org.wso2.micro.integrator.ntask.core.TaskManager} to be notified when a local task is
     * deleted.
     *
     * @param listener the listener to be notified
     * @param taskName the task name for which the listener is bound
     */
    public void registerListener(final LocalTaskActionListener listener, final String taskName) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (taskManager == null) {
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        //Continue looping to check if task manager is set.
                    }
                }
                taskManager.registerLocalTaskActionListener(listener, taskName);
            }
        }).start();
    }

    private void checkAndDeleteTaskFromQueue(String name, String group, String taskName) {
        synchronized (taskQueueLock) {
            Iterator<TaskDescription> iterator = taskQueue.iterator();
            while (iterator.hasNext()) {
                TaskDescription taskDescription = iterator.next();
                if (taskDescription.getName().equals(name) && taskDescription.getTaskGroup().equals(group)) {
                    iterator.remove();
                    NTaskAdapter.removeProperty(taskName);
                    logger.info("Deleted queued task [" + name + "::" + group + "].");
                }
            }
        }
    }

}

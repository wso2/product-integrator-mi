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
package org.wso2.micro.integrator.ntask.core.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.micro.integrator.ntask.common.TaskException;
import org.wso2.micro.integrator.ntask.core.TaskInfo;
import org.wso2.micro.integrator.ntask.core.TaskManagerId;
import org.wso2.micro.integrator.ntask.core.TaskRepository;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.stream.StreamSource;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Files based task repository implementation.
 */
public class FileBasedTaskRepository implements TaskRepository {
    private static final Log log = LogFactory.getLog(FileBasedTaskRepository.class);

    private static final String REG_TASK_BASE_PATH = "/repository/components/org.wso2.carbon.tasks";
    private static final String REG_TASK_REPO_BASE_PATH = REG_TASK_BASE_PATH + "/" + "definitions";
    private static final char URL_SEPARATOR_CHAR = '/';
    private static final String SYS_GOV_REGISTRY_PROPERTY_NAME = "mi.registry.gov";
    private static final int TASK_DELETE_MAX_ATTEMPTS = 3;
    private static final int TASK_DELETE_DELAY_MS = 50;
    private static String resourcePath = getGovernanceRegistryPath();
    private static Marshaller taskMarshaller;
    private static Unmarshaller taskUnmarshaller;
    private static final XMLInputFactory xmlInputFactory = XMLInputFactory.newFactory();

    static {
        try {
            JAXBContext ctx = JAXBContext.newInstance(org.wso2.micro.integrator.ntask.core.TaskInfo.class);
            taskMarshaller = ctx.createMarshaller();
            taskUnmarshaller = ctx.createUnmarshaller();
            xmlInputFactory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
            xmlInputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        } catch (JAXBException e) {
            throw new RuntimeException("Error creating task marshaller/unmarshaller: " + e.getMessage());
        }
    }

    HashMap<String, Properties> taskMetaPropMap = new HashMap<>();
    private String taskType;
    private int tenantId;

    public FileBasedTaskRepository(int tenantId, String taskType) {
        this.tenantId = tenantId;
        this.taskType = taskType;
    }

    private static String getGovernanceRegistryPath() {

        String defaultGovernanceRegistryPath = getSystemDependentPath(new File(".").getAbsolutePath()) +
                File.separator + "registry" + File.separator + "governance" + File.separator;
        String sysGovernanceRegistryPath = System.getProperty(SYS_GOV_REGISTRY_PROPERTY_NAME);
        if (StringUtils.isNotEmpty(sysGovernanceRegistryPath) && !".".equals(sysGovernanceRegistryPath)) {
            try {
                URI govRegistryUri = new URI(sysGovernanceRegistryPath);
                return Paths.get(govRegistryUri).toString() + File.separator;
            } catch (URISyntaxException e) {
                log.error("The governance registry path: '" + sysGovernanceRegistryPath + "' is not valid. Hence the " +
                        "default governance registry path: '" + defaultGovernanceRegistryPath + "' will be used.");
            }
        }
        return defaultGovernanceRegistryPath;
    }

    private static String getSystemDependentPath(String path) {
        return path.replace(URL_SEPARATOR_CHAR, File.separatorChar);
    }

    private static Marshaller getTaskMarshaller() {
        return taskMarshaller;
    }

    private static Unmarshaller getTaskUnmarshaller() {
        return taskUnmarshaller;
    }

    public static List<org.wso2.micro.integrator.ntask.core.TaskManagerId> getAllTenantTaskManagersForType(
            String taskType) throws TaskException {
        List<org.wso2.micro.integrator.ntask.core.TaskManagerId> tmList = getAvailableTenantTasksInRepo();
        for (Iterator<org.wso2.micro.integrator.ntask.core.TaskManagerId> itr = tmList.iterator(); itr.hasNext(); ) {
            if (!itr.next().getTaskType().equals(taskType)) {
                itr.remove();
            }
        }
        return tmList;
    }

    private static List<org.wso2.micro.integrator.ntask.core.TaskManagerId> getAvailableTenantTasksInRepo()
            throws TaskException {
        List<org.wso2.micro.integrator.ntask.core.TaskManagerId> tmList = new ArrayList<org.wso2.micro.integrator.ntask.core.TaskManagerId>();
        try {
            File file = new File(getSystemDependentPath(resourcePath + REG_TASK_BASE_PATH));
            boolean result = file.exists();
            int tid;
            if (result) {
                if (!(file.isDirectory())) {
                    return tmList;
                }
                if (file.listFiles() != null) {
                    for (File tidPath : file.listFiles()) {
                        if (!(tidPath.isDirectory())) {
                            continue;
                        }
                        if (tidPath.listFiles() != null) {
                            for (File taskTypePath : tidPath.listFiles()) {
                                if (!(taskTypePath.isDirectory())) {
                                    continue;
                                }
                                if (taskTypePath.listFiles() != null && taskTypePath.listFiles().length > 0) {
                                    try {
                                        tid = Integer.parseInt(tidPath.getAbsolutePath().substring(
                                                tidPath.getAbsolutePath().lastIndexOf('/') + 1));
                                        tmList.add(new TaskManagerId(tid, taskTypePath.getAbsolutePath()
                                                .substring(taskTypePath.getAbsolutePath().lastIndexOf('/') + 1)));
                                    } catch (NumberFormatException ignore) {
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new TaskException(e.getMessage(), TaskException.Code.UNKNOWN, e);
        }
        return tmList;
    }

    private String getMyTasksPath() {
        return REG_TASK_REPO_BASE_PATH + "/" + this.getTenantId() + "/" + this.getTasksType();
    }

    @Override
    public List<org.wso2.micro.integrator.ntask.core.TaskInfo> getAllTasks() {

        Set<org.wso2.micro.integrator.ntask.core.TaskInfo> result = new HashSet<>();
        String tasksPath = this.getMyTasksPath();
        File resource = new File(getSystemDependentPath(resourcePath + tasksPath));
        if (resource.exists()) {
            File[] taskPaths = resource.listFiles();
            org.wso2.micro.integrator.ntask.core.TaskInfo taskInfo;
            if (taskPaths != null)
                for (File taskPath : taskPaths) {
                    if (!taskPath.getName().startsWith("_meta_")) {
                        try {
                            taskInfo = this.getTaskInfoRegistryPath(taskPath.getAbsolutePath());
                            result.add(taskInfo);
                        } catch (JAXBException | IOException | XMLStreamException ex) {
                            log.error("Invalid/ corrupted entry found in : " + taskPath.getAbsolutePath(), ex);
                        }
                    }
                }
        }
        return new ArrayList<>(result);
    }

    private org.wso2.micro.integrator.ntask.core.TaskInfo getTaskInfoRegistryPath(String path) throws IOException,
            JAXBException, XMLStreamException {
        InputStream in = null;
        try {
            in = new FileInputStream(path);
            org.wso2.micro.integrator.ntask.core.TaskInfo taskInfo;
            /*
             * the following synchronized block is to avoid
             * "org.xml.sax.SAXException: FWK005" error where the XML parser is
             * not thread safe
             */
            synchronized (getTaskUnmarshaller()) {
                taskInfo =
                        (org.wso2.micro.integrator.ntask.core.TaskInfo) getTaskUnmarshaller().unmarshal(getXMLStreamReader(in));
            }
            in.close();
            taskInfo.getProperties().put(org.wso2.micro.integrator.ntask.core.TaskInfo.TENANT_ID_PROP,
                                         String.valueOf(this.getTenantId()));
            return taskInfo;
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }

    @Override
    public org.wso2.micro.integrator.ntask.core.TaskInfo getTask(String taskName) throws TaskException {
        String tasksPath = this.getMyTasksPath();
        String currentTaskPath = tasksPath + "/" + taskName;
        try {
            File task = new File(getSystemDependentPath(resourcePath + currentTaskPath));
            if (!task.exists()) {
                throw new TaskException("The task '" + taskName + "' does not exist",
                                        TaskException.Code.NO_TASK_EXISTS);
            }
            return this.getTaskInfoRegistryPath(resourcePath + currentTaskPath);
        } catch (TaskException e) {
            throw e;
        } catch (Exception e) {
            throw new TaskException("Error in loading task '" + taskName + "' from registry: " + e.getMessage(),
                                    TaskException.Code.CONFIG_ERROR, e);
        }
    }

    @Override
    public void addTask(TaskInfo taskInfo) throws TaskException {
        String tasksPath = this.getMyTasksPath();
        String currentTaskPath = tasksPath + "/" + taskInfo.getName();
        File file = new File(getSystemDependentPath(resourcePath + tasksPath));
        if (!file.exists()) {
            file.mkdirs();
        }
        try (FileOutputStream fos = new FileOutputStream(getSystemDependentPath(resourcePath + currentTaskPath))) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            getTaskMarshaller().marshal(taskInfo, out);
            fos.write(out.toByteArray());
        } catch (Exception e) {
            throw new TaskException(
                    "Error in adding task '" + taskInfo.getName() + "' to the repository: " + e.getMessage(),
                    TaskException.Code.CONFIG_ERROR, e);
        }
    }

    @Override
    public boolean deleteTask(String taskName) throws TaskException {
        String tasksPath = this.getMyTasksPath();
        String currentTaskPath = tasksPath + "/" + taskName;
        boolean deleteSuccess = false;
        try {
            File file = new File(getSystemDependentPath(resourcePath + currentTaskPath));
            if (file.exists()) {
                if (deleteWithRetry(file)) {
                    deleteSuccess = true;
                }
            }

            File metaFile = new File(getSystemDependentPath(resourcePath + tasksPath + "/_meta_" + taskName));
            if (metaFile.exists()) {
                if (deleteWithRetry(metaFile)) {
                    deleteSuccess = true;
                    taskMetaPropMap.remove(getSystemDependentPath(resourcePath + taskName));
                }
            }
            return deleteSuccess;
        } catch (Exception e) {
            throw new TaskException("Error in deleting task '" + taskName + "' in the repository",
                                    TaskException.Code.CONFIG_ERROR, e);
        }
    }

    @Override
    public String getTasksType() {
        return taskType;
    }

    @Override
    public int getTenantId() {
        return tenantId;
    }

    @Override
    public void setTaskMetadataProp(String taskName, String key, String value) throws TaskException {
        Properties propertyMap = taskMetaPropMap.get(getSystemDependentPath(resourcePath + taskName));
        if (propertyMap == null) {
            propertyMap = new Properties();
            taskMetaPropMap.put(getSystemDependentPath(resourcePath + taskName), propertyMap);
        }
        try {
            propertyMap.put(key, value);
            writeToMetaFile(propertyMap, taskName);
        } catch (Exception e) {
            throw new TaskException("Error in setting task metadata properties: " + e.getMessage(),
                                    TaskException.Code.UNKNOWN, e);
        }
    }

    private void writeToMetaFile(Properties properties, String taskName) throws Exception {
        String tasksPath = this.getMyTasksPath();
        String currentTaskMetaPath = tasksPath + "/_meta_" + taskName;
        try (FileOutputStream fos = new FileOutputStream(getSystemDependentPath(resourcePath + currentTaskMetaPath))) {
            properties.store(fos, null);
        }
    }

    private Properties loadFromMetaFile(String taskName) throws Exception {
        Properties properties = new Properties();
        String tasksPath = this.getMyTasksPath();
        String currentTaskMetaPath = tasksPath + "/_meta_" + taskName;
        try (FileInputStream fis = new FileInputStream(getSystemDependentPath(resourcePath + currentTaskMetaPath))) {
            properties.load(fis);
        } catch (Exception e) {
            log.debug("Retrieving Meta file is failed.");
        }
        return properties;
    }

    @Override
    public String getTaskMetadataProp(String taskName, String key) throws TaskException {
        Properties propertyMap = taskMetaPropMap.get(getSystemDependentPath(resourcePath + taskName));
        try {
            if (propertyMap == null) {
                propertyMap = loadFromMetaFile(taskName);
                taskMetaPropMap.put(getSystemDependentPath(resourcePath + taskName), propertyMap);
            }
            return propertyMap.getProperty(key);
        } catch (Exception e) {
            throw new TaskException("Error in getting task metadata properties: " + e.getMessage(),
                                    TaskException.Code.UNKNOWN, e);
        }
    }

    private static XMLStreamReader getXMLStreamReader(InputStream input) throws XMLStreamException {

        return xmlInputFactory.createXMLStreamReader(new StreamSource(input));
    }

    private boolean deleteWithRetry(File file) {
        if (!file.exists()) {
            return false;
        }

        for (int attempt = 1; attempt <= TASK_DELETE_MAX_ATTEMPTS; attempt++) {
            if (file.delete()) {
                log.debug("File deleted successfully on attempt " + attempt + ": " + file.getAbsolutePath());
                return true;
            } else {
                // Force garbage collection to help release any unclosed file handles on Windows
                // Usually file handle is released after 1-2 attempts
                System.gc();
            }

            try {
                Thread.sleep(TASK_DELETE_DELAY_MS);
            } catch (InterruptedException e) {
                log.warn("Interrupted while waiting to retry file deletion: " + file.getAbsolutePath());
                break;
            }
        }

        log.error("Error occurred while deleting task. Unable to delete after " + TASK_DELETE_MAX_ATTEMPTS +
                " attempts: " + file.getAbsolutePath());
        return false;
    }
}
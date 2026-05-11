/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * you may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.micro.integrator.initializer.deployment.synapse.deployer;

import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.impl.builder.StAXOMBuilder;
import org.apache.axis2.deployment.DeploymentException;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.registry.Registry;
import org.wso2.micro.application.deployer.AppDeployerConstants;
import org.wso2.micro.application.deployer.AppDeployerUtils;
import org.wso2.micro.application.deployer.CarbonApplication;
import org.wso2.micro.application.deployer.config.ApplicationConfiguration;
import org.wso2.micro.application.deployer.config.Artifact;
import org.wso2.micro.application.deployer.config.CappFile;
import org.wso2.micro.application.deployer.config.RegistryConfig;
import org.wso2.micro.application.deployer.handler.AppDeploymentHandler;
import org.wso2.micro.integrator.registry.MicroIntegratorRegistry;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLStreamException;

import static org.wso2.micro.application.deployer.AppDeployerUtils.createRegistryPath;
import static org.wso2.micro.application.deployer.AppDeployerUtils.readChildText;
import static org.wso2.micro.integrator.initializer.deployment.synapse.deployer.SynapseAppDeployerConstants.REGISTRY_RESOURCE_TYPE;

/**
 * Carbon application deployer to deploy registry artifacts to file based registry
 */
public class FileRegistryResourceDeployer implements AppDeploymentHandler {

    private Registry lightweightRegistry;

    public static final Log log = LogFactory.getLog(FileRegistryResourceDeployer.class);
    private static final String[] excludedDirectoryNames = new String[]{".meta"};
    private static final String MEDIA_TYPE = "mediaType";

    public FileRegistryResourceDeployer(Registry lightweightRegistry) {
        this.lightweightRegistry = lightweightRegistry;
    }

    @Override
    public void deployArtifacts(CarbonApplication carbonApplication, AxisConfiguration axisConfiguration)
                                                                                            throws DeploymentException {
        ApplicationConfiguration appConfig = carbonApplication.getAppConfig();
        List<Artifact.Dependency> deps = appConfig.getApplicationArtifact().getDependencies();

        List<Artifact> artifacts = new ArrayList<Artifact>();
        for (Artifact.Dependency dep : deps) {
            if (dep.getArtifact() != null) {
                artifacts.add(dep.getArtifact());
            }
        }
        deployRegistryArtifacts(artifacts, carbonApplication.getAppNameWithVersion());
    }

    @Override
    public void undeployArtifacts(CarbonApplication carbonApplication, AxisConfiguration axisConfiguration) throws DeploymentException {
        ApplicationConfiguration appConfig = carbonApplication.getAppConfig();
        List<Artifact.Dependency> deps = appConfig.getApplicationArtifact().getDependencies();

        List<Artifact> artifacts = new ArrayList<Artifact>();
        for (Artifact.Dependency dep : deps) {
            if (dep.getArtifact() != null) {
                artifacts.add(dep.getArtifact());
            }
        }
        undeployRegistryArtifacts(artifacts, carbonApplication.getAppNameWithVersion());
    }

    /**
     * Deploys registry artifacts recursively. A Registry artifact can exist as a sub artifact in
     * any type of artifact. Therefore, have to search recursively
     *
     * @param artifacts     - list of artifacts to be deployed
     * @param parentAppName - name of the parent cApp
     */
    private void deployRegistryArtifacts(List<Artifact> artifacts, String parentAppName) {
        artifacts.stream().filter(artifact -> REGISTRY_RESOURCE_TYPE.equals(artifact.getType())).forEach(artifact -> {
            if (log.isDebugEnabled()) {
                log.debug("Deploying registry artifact: " + artifact.getName());
            }
            RegistryConfig regConfig = buildRegistryConfig(artifact, parentAppName);
            writeArtifactToRegistry(regConfig);
        });
    }

    /**
     * Deploys registry artifacts recursively. A Registry artifact can exist as a sub artifact in
     * any type of artifact. Therefore, have to search recursively
     *
     * @param artifacts     - list of artifacts to be deployed
     * @param parentAppName - name of the parent cApp
     */
    private void undeployRegistryArtifacts(List<Artifact> artifacts, String parentAppName) {
        artifacts.stream().filter(artifact -> REGISTRY_RESOURCE_TYPE.equals(artifact.getType())).forEach(artifact -> {
            if (log.isDebugEnabled()) {
                log.debug("Undeploying registry artifact: " + artifact.getName());
            }
            RegistryConfig regConfig = buildRegistryConfig(artifact, parentAppName);
            removeArtifactFromRegistry(regConfig);
        });
    }


    /**
     * Registry config file comes bundled inside the Registry/Resource artifact. Hence have to
     * find the file from the extractedPath of the artifact and build the RegistryConfig instance
     * using the contents of that file.
     *
     * @param artifact - Registry/Resource artifact
     * @return - RegistryConfig instance
     */
    private RegistryConfig buildRegistryConfig(Artifact artifact, String appName) {

        RegistryConfig regConfig = null;
        // get the file path of the registry config file
        List<CappFile> files = artifact.getFiles();
        if (files.size() == 1) {
            String fileName = artifact.getFiles().get(0).getName();
            String regConfigPath = artifact.getExtractedPath() + File.separator + fileName;

            File regConfigFile = new File(regConfigPath);
            if (regConfigFile.exists()) {
                // read the reg config file and build the configuration
                InputStream xmlInputStream = null;
                try {
                    xmlInputStream = new FileInputStream(regConfigFile);
                    regConfig = AppDeployerUtils.populateRegistryConfig(
                            new StAXOMBuilder(xmlInputStream).getDocumentElement());
                } catch (Exception e) {
                    log.error("Error while reading file : " + fileName, e);
                } finally {
                    if (xmlInputStream != null) {
                        try {
                            xmlInputStream.close();
                        } catch (IOException e) {
                            log.error("Error while closing input stream.", e);
                        }
                    }
                }

                if (regConfig != null) {
                    regConfig.setAppName(appName);
                    regConfig.setArtifactIdentifier(artifact.getArtifactIdentifier());
                    regConfig.setExtractedPath(artifact.getExtractedPath());
                    regConfig.setParentArtifactName(artifact.getName());
                    regConfig.setConfigFileName(fileName);
                }
            } else {
                log.error("Registry config file not found at : " + regConfigPath);
            }
        } else {
            log.error("Registry/Resource type must have a single file which declares " +
                    "registry configs. But " + files.size() + " files found.");
        }
        return regConfig;
    }


    /**
     * Writes all registry contents (resources) of the given artifact to the registry.
     *
     * @param registryConfig - Artifact instance
     */
    private void writeArtifactToRegistry(RegistryConfig registryConfig){

        // write resources
        List<RegistryConfig.Resourse> resources = registryConfig.getResources();

        for (RegistryConfig.Resourse resource : resources) {
            String filePath = registryConfig.getExtractedPath() + File.separator + AppDeployerConstants.RESOURCES_DIR
                    + File.separator + resource.getFileName();

            // check whether the file exists
            File file = new File(filePath);
            if (!file.exists()) {
                log.error("Specified file to be written as a resource is " + "not found at : " + filePath);
                continue;
            }
            String resourcePath = AppDeployerUtils.computeResourcePath(createRegistryKey(resource), resource.getFileName(), registryConfig);
            String mediaType = resource.getMediaType();
            ((MicroIntegratorRegistry)lightweightRegistry).addNewNonEmptyResource(resourcePath, false, mediaType,
                                                                                  readResourceContent(file),
                                                                                  resource.getProperties());
        }

        List<RegistryConfig.Collection> collections = registryConfig.getCollections();

        for (RegistryConfig.Collection collection : collections) {
            String filePath = registryConfig.getExtractedPath() + File.separator + AppDeployerConstants.RESOURCES_DIR
                    + File.separator + collection.getDirectory();

            // check whether the file exists
            File file = new File(filePath);
            if (!file.exists()) {
                log.error("Specified file to be written as a resource is " + "not found at : " + filePath);
                continue;
            }
            String directoryRegistryPath = createRegistryPath(collection.getPath());
            ((MicroIntegratorRegistry)lightweightRegistry).addNewNonEmptyResource(
                    directoryRegistryPath, true, "", "",
                    collection.getProperties());
            ArrayList<SubFileInfo> fileList = traverseDirectory(file);
            for (SubFileInfo subFileInfo : fileList) {
                Path subFilePath =subFileInfo.file.toPath();
                String subFileRelativePath = directoryRegistryPath + File.separator + file.toPath().relativize(subFilePath);
                String mediaType = subFileInfo.mediaType;
                String content = readResourceContent(new File(subFilePath.toString()));
                ((MicroIntegratorRegistry)lightweightRegistry).addNewNonEmptyResource(
                        subFileRelativePath, false, mediaType, content,
                        collection.getProperties());
            }
        }
    }

    /**
     * Remove all registry contents (resources) of the given artifact from the registry.
     *
     * @param registryConfig - Artifact instance
     */
    private void removeArtifactFromRegistry(RegistryConfig registryConfig){

        // get resources
        List<RegistryConfig.Resourse> resources = registryConfig.getResources();
        for (RegistryConfig.Resourse resource : resources) {
            String filePath = registryConfig.getExtractedPath() + File.separator + AppDeployerConstants.RESOURCES_DIR
                    + File.separator + resource.getFileName();
            // check whether the file exists
            File file = new File(filePath);
            if (!file.exists()) {
                // the file is already deleted.
                continue;
            }
            String resourcePath = AppDeployerUtils.computeResourcePath(createRegistryPath(resource.getPath()),
                                                                       resource.getFileName(), registryConfig);
            lightweightRegistry.delete(resourcePath);
        }

        // get collections
        List<RegistryConfig.Collection> collections = registryConfig.getCollections();
        for (RegistryConfig.Collection collection : collections) {
            String directoryPath = registryConfig.getExtractedPath() + File.separator + AppDeployerConstants.RESOURCES_DIR
                    + File.separator + collection.getDirectory();
            // check whether the file exists
            File file = new File(directoryPath);
            if (!file.exists()) {
                // the file is already deleted.
                continue;
            }
            String directoryRegistryPath = createRegistryPath(collection.getPath());
            lightweightRegistry.delete(directoryRegistryPath);
        }
    }

    /**
     * Function to create registry key from registry path
     *
     * @return
     */
    private String createRegistryKey(RegistryConfig.Resourse resourse) {

        return createRegistryPath(resourse.getPath());
    }

    /**
     * Function to retrieve file content to a string
     *
     * @param file target file
     * @return String containing the content of the file
     */
    private String readResourceContent (File file) {

        try (InputStream is = new FileInputStream(file)) {
            long length = file.length();
            // to ensure that file is not larger than Integer.MAX_VALUE.
            if (length > Integer.MAX_VALUE) {
                // File is too large
                log.error("File " + file.getName() + "is too large.");
            }

            StringBuilder strBuilder = new StringBuilder();
            try (BufferedReader bReader = new BufferedReader(new InputStreamReader(is))) {
                String line;
                while ((line = bReader.readLine()) != null) {
                    strBuilder.append(line).append('\n');
                }
            }
            return strBuilder.toString();

        } catch (FileNotFoundException e) {
            log.error("Unable to find file at: " + file.getAbsolutePath(), e);
        } catch (IOException e) {
            log.error("Error occurred while reading the content of file: " + file.getAbsolutePath());
        }
        return null;
    }

    /**
     Recursively traverses a directory and returns a list of all the files in the directory and its subdirectories.
     @param directory the directory to traverse
     @return an ArrayList of SubFileInfo objects, each representing a file in the directory and its media type
     */
    public static ArrayList<SubFileInfo> traverseDirectory(File directory) {
        ArrayList<SubFileInfo> fileList = new ArrayList<>();
        if (directory.isDirectory()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (isFileIgnored(file)) {
                        continue;
                    }
                    if (file.isDirectory()) {
                        fileList.addAll(traverseDirectory(file));
                    } else {
                        String mediaType = getMediaTypeFromMeta(file);
                        fileList.add(new SubFileInfo(file, mediaType));
                        getMediaTypeFromMeta(file);
                    }
                }
            }
        }
        return fileList;
    }

    /**
     Determines if a file or directory should be ignored based on its name.
     @param file the file to check
     @return true if the file should be ignored, false otherwise
     */
    public static boolean isFileIgnored(File file) {
        if (file.isDirectory()) {
            for (String excludedDirectoryName : excludedDirectoryNames) {
                if (file.getName().equals(excludedDirectoryName)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     Returns the media type for a given file by reading the media type information from its meta file.
     @param file the file for which to determine the media type
     @return the media type for the file, or an empty string if it couldn't be determined
     */
    public static String getMediaTypeFromMeta(File file) {
        try {
            if (!file.isDirectory()) {
                String metaInfoFileName = String.format("%s%s.meta%s~%s.xml",
                        file.getParent(), File.separator, File.separator, file.getName());
                InputStream xmlInputStream = new FileInputStream(metaInfoFileName);
                OMElement resourcesElement = new StAXOMBuilder(xmlInputStream).getDocumentElement();
                String mediaType = readChildText(resourcesElement, MEDIA_TYPE);
                return mediaType;
            }
        } catch (FileNotFoundException | XMLStreamException e) {
            log.error("Error while determining media type for the file: " + file.getAbsolutePath(), e);
        }
        return "";
    }

    /**
     * A pojo class to store the file and media type.
     */
    public static class SubFileInfo {
        private final File file;
        private final String mediaType;

        public SubFileInfo(File file, String mediaType) {
            this.file = file;
            this.mediaType = mediaType;
        }

        public File getFile() {
            return file;
        }

        public String getMediaType() {
            return mediaType;
        }
    }
}



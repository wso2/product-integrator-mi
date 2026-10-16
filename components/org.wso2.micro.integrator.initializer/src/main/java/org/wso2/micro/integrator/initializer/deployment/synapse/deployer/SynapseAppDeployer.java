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

import com.google.gson.JsonObject;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMException;
import org.apache.axiom.om.impl.builder.StAXOMBuilder;
import org.apache.axiom.om.util.StAXUtils;
import org.apache.axis2.AxisFault;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.deployment.Deployer;
import org.apache.axis2.deployment.DeploymentEngine;
import org.apache.axis2.deployment.DeploymentException;
import org.apache.axis2.deployment.repository.util.DeploymentFileData;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.Startup;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.SynapseException;
import org.apache.synapse.api.API;
import org.apache.synapse.config.Entry;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.config.SynapsePropertiesLoader;
import org.apache.synapse.config.xml.EntryFactory;
import org.apache.synapse.config.xml.SynapseImportFactory;
import org.apache.synapse.config.xml.SynapseImportSerializer;
import org.apache.synapse.config.xml.XMLConfigConstants;
import org.apache.synapse.core.axis2.ProxyService;
import org.apache.synapse.deployers.APIDeployer;
import org.apache.synapse.deployers.AbstractSynapseArtifactDeployer;
import org.apache.synapse.deployers.EndpointDeployer;
import org.apache.synapse.deployers.InboundEndpointDeployer;
import org.apache.synapse.deployers.LibraryArtifactDeployer;
import org.apache.synapse.deployers.LocalEntryDeployer;
import org.apache.synapse.deployers.MessageProcessorDeployer;
import org.apache.synapse.deployers.MessageStoreDeployer;
import org.apache.synapse.deployers.ProxyServiceDeployer;
import org.apache.synapse.deployers.SequenceDeployer;
import org.apache.synapse.deployers.SynapseArtifactDeploymentStore;
import org.apache.synapse.deployers.TaskDeployer;
import org.apache.synapse.deployers.TemplateDeployer;
import org.apache.synapse.endpoints.Endpoint;
import org.apache.synapse.endpoints.Template;
import org.apache.synapse.inbound.InboundEndpoint;
import org.apache.synapse.libraries.imports.SynapseImport;
import org.apache.synapse.libraries.model.Library;
import org.apache.synapse.libraries.util.LibDeployerUtils;
import org.apache.synapse.mediators.base.SequenceMediator;
import org.apache.synapse.mediators.template.TemplateMediator;
import org.apache.synapse.message.processor.MessageProcessor;
import org.apache.synapse.message.store.MessageStore;
import org.apache.synapse.transport.customlogsetter.CustomLogSetter;
import org.wso2.micro.application.deployer.AppDeployerConstants;
import org.wso2.micro.application.deployer.AppDeployerUtils;
import org.wso2.micro.application.deployer.CarbonApplication;
import org.wso2.micro.application.deployer.config.Artifact;
import org.wso2.micro.application.deployer.config.CappFile;
import org.wso2.micro.application.deployer.handler.AppDeploymentHandler;
import org.wso2.micro.core.util.StringUtils;
import org.wso2.micro.integrator.core.util.MicroIntegratorBaseUtils;
import org.wso2.micro.integrator.initializer.ServiceBusConstants;
import org.wso2.micro.integrator.initializer.deployment.application.deployer.CappDeployer;
import org.wso2.micro.integrator.initializer.ServiceBusUtils;
import org.wso2.micro.integrator.initializer.persistence.MediationPersistenceManager;
import org.wso2.micro.integrator.initializer.utils.ConfigurationHolder;
import org.wso2.micro.integrator.initializer.utils.Constants;
import org.wso2.micro.integrator.initializer.utils.DeployerUtil;
import org.wso2.micro.integrator.initializer.utils.LocalEntryUtil;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;

public class SynapseAppDeployer implements AppDeploymentHandler {

    private static final Log log = LogFactory.getLog(SynapseAppDeployer.class);

    private Map<String, Boolean> acceptanceList = null;

    private static String MAIN_XML="<sequence xmlns=\"http://ws.apache.org/ns/synapse\" name=\"main\"/>";
    private static String FAULT_XML="<sequence xmlns=\"http://ws.apache.org/ns/synapse\" name=\"fault\"/>";
    private static String MAIN_SEQ_REGEX = "main-\\d+\\.\\d+\\.\\d+\\.xml";
    private static String FAULT_SEQ_REGEX = "fault-\\d+\\.\\d+\\.\\d+\\.xml";

    private HashMap<String, Deployer> synapseDeployers = new HashMap<>();

    public SynapseAppDeployer(){
        initializeDefaultSynapseDeployers();
    }

    /**
     * Deploy the artifacts which can be deployed through this deployer (endpoints, sequences,
     * proxy service etc.).
     *
     * @param carbonApp  - CarbonApplication instance to check for artifacts
     * @param axisConfig - AxisConfiguration of the current tenant
     */
    public void deployArtifacts(CarbonApplication carbonApp, AxisConfiguration axisConfig)
            throws DeploymentException{
        List<Artifact.Dependency> artifacts = carbonApp.getAppConfig().getApplicationArtifact()
                .getDependencies();

        deployClassMediators(carbonApp, artifacts, axisConfig);
        deployConnectorDependencies(carbonApp, artifacts, axisConfig);
        deploySynapseLibrary(carbonApp, artifacts, axisConfig);
        Map<String, List<Artifact.Dependency>> artifactTypeMap = getOrderedArtifactsMap(artifacts);

        //deploy artifacts
        for (String artifactType : artifactTypeMap.keySet()) {
            deployArtifactType(artifactTypeMap.get(artifactType), carbonApp, axisConfig);
        }
    }

    /**
     * Add the artifacts in a CAPP to a dependency based orders list
     */
    private Map<String, List<Artifact.Dependency>> getOrderedArtifactsMap(List<Artifact.Dependency> artifacts) {
        //lists to hold different artifact types considering the deployment order
        Map<String, List<Artifact.Dependency>> artifactTypeMap = new LinkedHashMap<>();
        artifactTypeMap.put(SynapseAppDeployerConstants.MEDIATOR_TYPE, new ArrayList<Artifact.Dependency>());
        artifactTypeMap.put(SynapseAppDeployerConstants.SYNAPSE_LIBRARY_TYPE, new ArrayList<Artifact.Dependency>());
        artifactTypeMap.put(SynapseAppDeployerConstants.LOCAL_ENTRY_TYPE, new ArrayList<Artifact.Dependency>());
        artifactTypeMap.put(SynapseAppDeployerConstants.ENDPOINT_TYPE, new ArrayList<Artifact.Dependency>());
        artifactTypeMap.put(SynapseAppDeployerConstants.SEQUENCE_TYPE, new ArrayList<Artifact.Dependency>());
        artifactTypeMap.put(SynapseAppDeployerConstants.MESSAGE_STORE_TYPE, new ArrayList<Artifact.Dependency>());
        artifactTypeMap.put(SynapseAppDeployerConstants.TEMPLATE_TYPE, new ArrayList<Artifact.Dependency>());
        artifactTypeMap.put(SynapseAppDeployerConstants.PROXY_SERVICE_TYPE, new ArrayList<Artifact.Dependency>());
        artifactTypeMap.put(SynapseAppDeployerConstants.TASK_TYPE, new ArrayList<Artifact.Dependency>());
        artifactTypeMap.put(SynapseAppDeployerConstants.EVENT_SOURCE_TYPE, new ArrayList<Artifact.Dependency>());
        artifactTypeMap.put(SynapseAppDeployerConstants.MESSAGE_PROCESSOR_TYPE, new ArrayList<Artifact.Dependency>());
        artifactTypeMap.put(SynapseAppDeployerConstants.API_TYPE, new ArrayList<Artifact.Dependency>());
        artifactTypeMap.put(SynapseAppDeployerConstants.INBOUND_ENDPOINT_TYPE, new ArrayList<Artifact.Dependency>());
        artifactTypeMap.put(SynapseAppDeployerConstants.OTHER_TYPE, new ArrayList<Artifact.Dependency>());

        //Categorize artifacts based on the artifact type
        for (Artifact.Dependency dep : artifacts) {
            switch (dep.getArtifact().getType()) {
                case SynapseAppDeployerConstants.MEDIATOR_TYPE:
                    artifactTypeMap.get(SynapseAppDeployerConstants.MEDIATOR_TYPE).add(dep);
                    break;
                case SynapseAppDeployerConstants.SYNAPSE_LIBRARY_TYPE:
                    artifactTypeMap.get(SynapseAppDeployerConstants.SYNAPSE_LIBRARY_TYPE).add(dep);
                    break;
                case SynapseAppDeployerConstants.LOCAL_ENTRY_TYPE:
                    artifactTypeMap.get(SynapseAppDeployerConstants.LOCAL_ENTRY_TYPE).add(dep);
                    break;
                case SynapseAppDeployerConstants.ENDPOINT_TYPE:
                    artifactTypeMap.get(SynapseAppDeployerConstants.ENDPOINT_TYPE).add(dep);
                    break;
                case SynapseAppDeployerConstants.SEQUENCE_TYPE:
                    artifactTypeMap.get(SynapseAppDeployerConstants.SEQUENCE_TYPE).add(dep);
                    break;
                case SynapseAppDeployerConstants.MESSAGE_STORE_TYPE:
                    artifactTypeMap.get(SynapseAppDeployerConstants.MESSAGE_STORE_TYPE).add(dep);
                    break;
                case SynapseAppDeployerConstants.TEMPLATE_TYPE:
                    artifactTypeMap.get(SynapseAppDeployerConstants.TEMPLATE_TYPE).add(dep);
                    break;
                case SynapseAppDeployerConstants.PROXY_SERVICE_TYPE:
                    artifactTypeMap.get(SynapseAppDeployerConstants.PROXY_SERVICE_TYPE).add(dep);
                    break;
                case SynapseAppDeployerConstants.TASK_TYPE:
                    artifactTypeMap.get(SynapseAppDeployerConstants.TASK_TYPE).add(dep);
                    break;
                case SynapseAppDeployerConstants.EVENT_SOURCE_TYPE:
                    artifactTypeMap.get(SynapseAppDeployerConstants.EVENT_SOURCE_TYPE).add(dep);
                    break;
                case SynapseAppDeployerConstants.MESSAGE_PROCESSOR_TYPE:
                    artifactTypeMap.get(SynapseAppDeployerConstants.MESSAGE_PROCESSOR_TYPE).add(dep);
                    break;
                case SynapseAppDeployerConstants.API_TYPE:
                    artifactTypeMap.get(SynapseAppDeployerConstants.API_TYPE).add(dep);
                    break;
                case SynapseAppDeployerConstants.INBOUND_ENDPOINT_TYPE:
                    artifactTypeMap.get(SynapseAppDeployerConstants.INBOUND_ENDPOINT_TYPE).add(dep);
                    break;
                default:
                    artifactTypeMap.get(SynapseAppDeployerConstants.OTHER_TYPE).add(dep);
            }
        }
        return artifactTypeMap;
    }

    /**
     * Un-deploys Synapse artifacts found in this application. Just delete the files from the
     * hot folders. Synapse hot deployer will do the rest..
     *
     * @param carbonApplication - CarbonApplication instance
     * @param axisConfig - AxisConfiguration of the current tenant
     */
    public void undeployArtifacts(CarbonApplication carbonApplication, AxisConfiguration axisConfig)
            throws DeploymentException{

        List<Artifact.Dependency> artifacts = carbonApplication.getAppConfig()
                .getApplicationArtifact().getDependencies();
        // Reverse artifacts dependency order should be used for un deployment, EI-1737
        Map<String, List<Artifact.Dependency>> artifactTypeMap = getOrderedArtifactsMap(artifacts);
        List<String> artifactTypesList = new ArrayList<String>(artifactTypeMap.keySet());
        Collections.reverse(artifactTypesList);

        for (String artifactType : artifactTypesList) {
            undeployArtifactType(carbonApplication, axisConfig, artifactTypeMap.get(artifactType));
        }
    }

    /**
     * Un deploy a given artifact type
     * @param carbonApplication
     * @param axisConfig
     * @param artifacts
     */
    private void undeployArtifactType(CarbonApplication carbonApplication, AxisConfiguration axisConfig,
            List<Artifact.Dependency> artifacts) {

        for (Artifact.Dependency dep : artifacts) {

            Artifact artifact = dep.getArtifact();

            if (!validateArtifact(artifact)) {
                continue;
            }

            Deployer deployer;
            String artifactDir = null;

            if (SynapseAppDeployerConstants.MEDIATOR_TYPE.endsWith(artifact.getType())) {
                deployer = getClassMediatorDeployer(axisConfig);
            } else if(SynapseAppDeployerConstants.SYNAPSE_LIBRARY_TYPE.equals(artifact.getType())) {
                deployer = getSynapseLibraryDeployer(axisConfig);
            } else {
                String artifactType = artifact.getType();
                String artifactDirName = getArtifactDirName(artifactType);
                if (artifactDirName == null) {
                    continue;
                }

                deployer = getDeployer(artifact.getType());
                artifactDir = getArtifactDirPath(axisConfig, artifactDirName);
            }

            String fileName = artifact.getFiles().get(0).getName();
            String artifactPath = null;
            if (!StringUtils.isEmpty(fileName)) {
                artifactPath = artifact.getExtractedPath() + File.separator + fileName;
            }

            if (deployer != null && AppDeployerConstants.DEPLOYMENT_STATUS_DEPLOYED.
                                            equals(artifact.getDeploymentStatus())) {

                String artifactName = artifact.getName();
                if (carbonApplication.getAppConfig().isVersionedDeployment()) {
                    if (artifact.getType().equals("synapse/inbound-endpoint")) {
                        // skip versioning for inbound endpoints as they don't support versioning
                    } else if (artifact.getType().equals("synapse/proxy-service")) {
                        if (SynapsePropertiesLoader.getBooleanProperty(SynapseConstants.EXPOSE_VERSIONED_SERVICES, false)) {
                            artifactName = carbonApplication.getAppConfig().getAppArtifactIdentifier() + "/" + artifactName;
                        }
                    } else {
                        artifactName = carbonApplication.getAppConfig().getAppArtifactIdentifier() + Constants.DOUBLE_UNDERSCORE + artifactName;
                    }
                }
                File artifactInRepo = new File(artifactDir + File.separator + fileName);

                try {
                    if (SynapseAppDeployerConstants.MEDIATOR_TYPE.endsWith(artifact.getType())) {

                        if (deployer instanceof AbstractSynapseArtifactDeployer) {
                            ((AbstractSynapseArtifactDeployer) deployer).setCustomLog(carbonApplication.getAppName(),
                                    AppDeployerUtils.getTenantIdLogString(AppDeployerUtils.getTenantId()));
                        }
                        deployer.undeploy(artifactPath);
                    } else if (SynapseAppDeployerConstants.SYNAPSE_LIBRARY_TYPE.equals(artifact.getType())){
                        if (isLibraryUsedByOtherCApps(artifact.getName(), carbonApplication)) {
                            log.info("Skipping undeployment of synapse library '" + artifact.getName()
                                    + "' since it is still used by another deployed Carbon Application");
                        } else {
                            String libQName = getArtifactName(artifactPath, axisConfig);
                            deleteImport(libQName, axisConfig);
                            deployer.undeploy(artifactPath);
                        }
                    } else if (SynapseAppDeployerConstants.SEQUENCE_TYPE.equals(artifact.getType())
                               && handleMainFaultSeqUndeployment(artifact, axisConfig)) {
                        log.debug("Handling main and fault sequence un-deployment");
                    } else if (artifactInRepo.exists()) {
                        log.info("Deleting artifact at " + artifactInRepo.getAbsolutePath());
                        if (!artifactInRepo.delete()) {
                            log.error("Unable to delete " + artifactInRepo.getAbsolutePath());
                        }
                    } else {
                        // use reflection to avoid having synapse as a dependency

                        // If API we are using the API name instead of artifact name to support API versioning.
                        if (artifact.getType().equals("synapse/api")) {
                            OMElement apiElement =
                                    new StAXOMBuilder(new FileInputStream(new File(artifactPath))).getDocumentElement();
                            API api = DeployerUtil.partiallyBuildAPI(apiElement);
                            artifactName = api.getName();
                            if (SynapsePropertiesLoader.getBooleanProperty(SynapseConstants.EXPOSE_VERSIONED_SERVICES, false) &&
                                    carbonApplication.getAppConfig().isVersionedDeployment()) {
                                artifactName = carbonApplication.getAppConfig().getAppArtifactIdentifier() + Constants.DOUBLE_UNDERSCORE + artifactName;
                            }
                        }

                        Class[] paramString = new Class[1];
                        paramString[0] = String.class;
                        Method method = deployer.getClass().getDeclaredMethod("undeploySynapseArtifact", paramString);
                        method.invoke(deployer, artifactName);
                    }

                    artifact.setDeploymentStatus(AppDeployerConstants.DEPLOYMENT_STATUS_PENDING);
                    File artifactFile = new File(artifactPath);
                    if (artifactFile.exists() && !artifactFile.delete()) {
                        log.warn("Couldn't delete App artifact file : " + artifactPath);
                    }
                } catch (Exception e) {
                    artifact.setDeploymentStatus(AppDeployerConstants.DEPLOYMENT_STATUS_FAILED);
                    log.error("Error occured while trying to un deploy : "+ artifactName);
                }
            }
        }
    }

    /**
     * Deploy class mediators contains in the CApp
     *
     * @param artifacts List of Artifacts contains in the capp
     * @param axisConfig AxisConfiguration of the current tenant
     * @throws DeploymentException if something goes wrong while deployment
     */
    private void deployClassMediators(CarbonApplication carbonApp, List<Artifact.Dependency> artifacts,
                                      AxisConfiguration axisConfig)
            throws DeploymentException {
        for (Artifact.Dependency dependency : artifacts) {

            Artifact artifact = dependency.getArtifact();
            if (!validateArtifact(artifact)) {
                continue;
            }

            if (SynapseAppDeployerConstants.MEDIATOR_TYPE.endsWith(artifact.getType())) {

                Deployer deployer = getClassMediatorDeployer(axisConfig);

                if (deployer != null) {
                    artifact.setRuntimeObjectName(artifact.getName());
                    String fileName = artifact.getFiles().get(0).getName();
                    String artifactPath = artifact.getExtractedPath() + File.separator + fileName;

                    try {
                        deployer.deploy(new DeploymentFileData(new File(artifactPath), deployer,
                                carbonApp.getAppConfig().getAppArtifactIdentifier(), carbonApp.getAppConfig().getCAppDependencies()));
                        artifact.setDeploymentStatus(AppDeployerConstants.DEPLOYMENT_STATUS_DEPLOYED);
                    } catch (DeploymentException e) {
                        artifact.setDeploymentStatus(AppDeployerConstants.DEPLOYMENT_STATUS_FAILED);
                        throw e;
                    }
                }
            }
        }
    }

    /**
     * Deploy the connector dependencies contains in the CApp
     *
     * @param artifacts List of Artifacts contains in the capp
     * @param axisConfig  AxisConfiguration of the current tenant
     * @throws DeploymentException
     */
    private void deployConnectorDependencies(CarbonApplication carbonApp, List<Artifact.Dependency> artifacts,
                                             AxisConfiguration axisConfig)
            throws DeploymentException {

        for (Artifact.Dependency dependency : artifacts) {

            Artifact artifact = dependency.getArtifact();
            if (!validateArtifact(artifact)) {
                continue;
            }
            if (SynapseAppDeployerConstants.CONNECTOR_DEPENDENCY_TYPE.equals(artifact.getType())) {
                Deployer deployer = getSynapseLibraryDeployer(axisConfig);
                if (deployer != null) {
                    artifact.setRuntimeObjectName(artifact.getName());
                    String fileName = artifact.getFiles().get(0).getName();
                    String artifactPath = artifact.getExtractedPath() + File.separator + fileName;

                    try {
                        if (deployer instanceof LibraryArtifactDeployer) {
                            ((LibraryArtifactDeployer) deployer).deployLibraryDependency(
                                    new DeploymentFileData(new File(artifactPath), deployer,
                                            carbonApp.getAppConfig().getAppArtifactIdentifier(),
                                            carbonApp.getAppConfig().getCAppDependencies()), artifact.getConnector());
                        }
                        artifact.setDeploymentStatus(AppDeployerConstants.DEPLOYMENT_STATUS_DEPLOYED);
                    } catch (DeploymentException e) {
                        artifact.setDeploymentStatus(AppDeployerConstants.DEPLOYMENT_STATUS_FAILED);
                        throw e;
                    }
                }
            }
        }
    }

    /**
     * Deploy synapse libraries contains in the CApp
     *
     * @param artifacts  List of Artifacts contains in the capp
     * @param axisConfig AxisConfiguration of the current tenant
     * @throws DeploymentException if something goes wrong while deployment
     */
    private void deploySynapseLibrary(CarbonApplication carbonApp, List<Artifact.Dependency> artifacts,
                                      AxisConfiguration axisConfig)
            throws DeploymentException {
        for (Artifact.Dependency dependency : artifacts) {

            Artifact artifact = dependency.getArtifact();
            if (!validateArtifact(artifact)) {
                continue;
            }

            if (SynapseAppDeployerConstants.SYNAPSE_LIBRARY_TYPE.equals(artifact.getType())) {

                Deployer deployer = getSynapseLibraryDeployer(axisConfig);

                if (deployer != null) {
                    artifact.setRuntimeObjectName(artifact.getName());
                    String fileName = artifact.getFiles().get(0).getName();
                    String artifactPath = artifact.getExtractedPath() + File.separator + fileName;
                    String artifactDir = getArtifactDirPath(axisConfig, SynapseAppDeployerConstants.SYNAPSE_LIBS);
                    File artifactInRepo = new File(artifactDir + File.separator + fileName);
                    if (artifactInRepo.exists()) {
                        log.warn("Synapse Library " + fileName + " already found in " + artifactInRepo.getAbsolutePath() +
                                ". Ignoring CAPP's artifact");
                        artifact.setDeploymentStatus(AppDeployerConstants.DEPLOYMENT_STATUS_DEPLOYED);
                    } else {
                        try {
                            deployer.deploy(new DeploymentFileData(new File(artifactPath), deployer,
                                    carbonApp.getAppConfig().getAppArtifactIdentifier(), carbonApp.getAppConfig().getCAppDependencies()));
                            artifact.setDeploymentStatus(AppDeployerConstants.DEPLOYMENT_STATUS_DEPLOYED);
                            try {
                                String artifactName = getArtifactName(artifactPath, axisConfig);
                                SynapseConfiguration configuration = getSynapseConfiguration(axisConfig);
                                if (artifactName != null) {
                                    if (configuration.getSynapseImports().get(artifactName) == null) {
                                        String libName = artifactName.substring(artifactName.lastIndexOf("}") + 1);
                                        String libraryPackage = artifactName.substring(1, artifactName.lastIndexOf("}"));
                                        updateStatus(artifactName, libName, libraryPackage, ServiceBusConstants.ENABLED, axisConfig);
                                    }
                                }
                            } catch (AxisFault axisFault) {
                                log.error("Unable to update status for the synapse library : " + axisFault.getMessage());
                            } catch (NullPointerException nullException) {
                                log.error("Error while getting qualified name of the synapse library : " + nullException.getMessage());
                            }
                        } catch (DeploymentException e) {
                            artifact.setDeploymentStatus(AppDeployerConstants.DEPLOYMENT_STATUS_FAILED);
                            log.error("Error while deploying the synapse library : " + e.getMessage());
                            throw e;
                        }
                    }
                }
            }
        }
    }

    /**
     *
     * Get the library artifact name
     * @param axisConfig AxisConfiguration of the current tenant
     * @throws DeploymentException if something goes wrong while deployment
     * */
    public String getArtifactName(String filePath, AxisConfiguration axisConfig) throws DeploymentException {
        SynapseArtifactDeploymentStore deploymentStore;
        deploymentStore = getSynapseConfiguration(axisConfig).getArtifactDeploymentStore();
        return deploymentStore.getArtifactNameForFile(filePath);
    }

    /**
     * Helper method to retrieve the Synapse configuration from the relevant axis configuration
     *
     * @param axisConfig AxisConfiguration of the current tenant
     * @return extracted SynapseConfiguration from the relevant AxisConfiguration
     */
    protected SynapseConfiguration getSynapseConfiguration(AxisConfiguration axisConfig) {
        return (SynapseConfiguration) axisConfig.getParameter(
                SynapseConstants.SYNAPSE_CONFIG).getValue();
    }

    /**
     * Performing the action of enabling/disabling the given meidation library
     *
     * @param libName
     * @param packageName
     * @param status
     * @param axisConfig AxisConfiguration of the current tenant
     * @throws AxisFault
     */
    public boolean updateStatus(String libQName, String libName, String packageName, String status, AxisConfiguration axisConfig)
            throws AxisFault {
        try {
            SynapseConfiguration synapseConfiguration = getSynapseConfiguration(axisConfig);
            SynapseImport synapseImport = synapseConfiguration.getSynapseImports().get(libQName);
            if (synapseImport == null && libName != null && packageName != null) {
                addImport(libName, packageName, axisConfig);
                synapseImport = synapseConfiguration.getSynapseImports().get(libQName);
            }
            Library synLib = synapseConfiguration.getSynapseLibraries().get(libQName);
            if (libQName != null && synLib != null) {
                if (ServiceBusConstants.ENABLED.equals(status)) {
                    synapseImport.setStatus(true);
                    synLib.setLibStatus(true);
                    synLib.loadLibrary();
                    deployingLocalEntries(synLib, synapseConfiguration, axisConfig);
                } else {
                    synapseImport.setStatus(false);
                    synLib.setLibStatus(false);
                    synLib.unLoadLibrary();
                    undeployingLocalEntries(synLib, synapseConfiguration, axisConfig);
                }
            }

        } catch (Exception e) {
            String message = "Unable to update status for :  " + libQName;
            handleException(log, message, e);
        }
        return true;
    }

    /**
     * Performing the action of importing the given meidation library
     *
     * @param libName
     * @param packageName
     * @param axisConfig AxisConfiguration of the current tenant
     * @throws AxisFault
     */
    public void addImport(String libName, String packageName, AxisConfiguration axisConfig) throws AxisFault {
        SynapseImport synImport = new SynapseImport();
        synImport.setLibName(libName);
        synImport.setLibPackage(packageName);
        OMElement impEl = SynapseImportSerializer.serializeImport(synImport);
        if (impEl != null) {
            try {
                addImport(impEl.toString(), axisConfig);
            } catch (AxisFault axisFault) {
                handleException(log, "Could not add Synapse Import", axisFault);
            }
        } else {
            handleException(log,
                    "Could not add Synapse Import. Invalid import params for libName : " +
                            libName + " packageName : " + packageName, null);
        }
    }

    /**
     *
     * Undeploy the local entries deployed from the lib
     *
     * @param axisConfig AxisConfiguration of the current tenant
     * */
    private void undeployingLocalEntries(Library library, SynapseConfiguration config, AxisConfiguration axisConfig) {
        if (log.isDebugEnabled()) {
            log.debug("Start : Removing Local registry entries from the configuration");
        }
        for (Map.Entry<String, Object> libararyEntryMap : library.getLocalEntryArtifacts()
                .entrySet()) {
            File localEntryFileObj = (File) libararyEntryMap.getValue();
            OMElement document = LocalEntryUtil.getOMElement(localEntryFileObj);
            deleteEntry(document.toString(), axisConfig);
        }
        if (log.isDebugEnabled()) {
            log.debug("End : Removing Local registry entries from the configuration");
        }
    }

    /**
     * Get an XML configuration element for a message processor from the FE and
     * creates and add the MessageStore to the synapse configuration.
     *
     * @param xml
     *            string that contain the message processor configuration.
     * @param axisConfig AxisConfiguration of the current tenant
     * @throws AxisFault
     *             if some thing goes wrong when creating a MessageProcessor
     *             with the given xml.
     */
    private void addImport(String xml, AxisConfiguration axisConfig) throws AxisFault {
        try {
            OMElement imprtElem = createElement(xml);
            SynapseImport synapseImport = SynapseImportFactory.createImport(imprtElem, null);
            if (synapseImport != null && synapseImport.getName() != null) {
                SynapseConfiguration synapseConfiguration = getSynapseConfiguration(axisConfig);
                String fileName = ServiceBusUtils.generateFileName(synapseImport.getName());
                synapseImport.setFileName(fileName);
                synapseConfiguration.addSynapseImport(synapseImport.getName(), synapseImport);
                String synImportQualfiedName = LibDeployerUtils.getQualifiedName(synapseImport);
                Library synLib =
                        synapseConfiguration.getSynapseLibraries()
                                .get(synImportQualfiedName);
                if (synLib != null) {
                    LibDeployerUtils.loadLibArtifacts(synapseImport, synLib);
                }
            } else {
                String message = "Unable to create a Synapse Import for :  " + xml;
                handleException(log, message, null);
            }

        } catch (XMLStreamException e) {
            String message = "Unable to create a Synapse Import for :  " + xml;
            handleException(log, message, e);
        }

    }

    /**
     * Creates an <code>OMElement</code> from the given string
     *
     * @param str
     *            the XML string
     * @return the <code>OMElement</code> representation of the given string
     * @throws XMLStreamException
     *             if building the <code>OmElement</code> is unsuccessful
     */
    private OMElement createElement(String str) throws XMLStreamException {
        InputStream in = new ByteArrayInputStream(str.getBytes());
        return new StAXOMBuilder(in).getDocumentElement();
    }

    private void handleException(Log log, String message, Exception e) throws AxisFault {

        if (e == null) {

            AxisFault exception = new AxisFault(message);
            log.error(message, exception);
            throw exception;

        } else {
            message = message + " :: " + e.getMessage();
            log.error(message, e);
            throw new AxisFault(message, e);
        }
    }

    /**
     * Helper method to get the persistence manger
     * @param axisConfig AxisConfiguration of the current tenant
     * @return persistence manager for this configuration context
     */
    protected MediationPersistenceManager getMediationPersistenceManager(AxisConfiguration axisConfig) {
        return ServiceBusUtils.getMediationPersistenceManager(axisConfig);
    }

    /**
     * Deploy the local entries from lib
     *
     * @param axisConfig AxisConfiguration of the current tenant
     * */
    private void deployingLocalEntries(Library library, SynapseConfiguration config, AxisConfiguration axisConfig) {
        if (log.isDebugEnabled()) {
            log.debug("Start : Adding Local registry entries to the configuration");
        }
        for (Map.Entry<String, Object> libararyEntryMap : library.getLocalEntryArtifacts()
                .entrySet()) {
            File localEntryFileObj = (File) libararyEntryMap.getValue();
            OMElement document = LocalEntryUtil.getOMElement(localEntryFileObj);
            addEntry(document.toString(), axisConfig);
        }
        if (log.isDebugEnabled()) {
            log.debug("End : Adding Local registry entries to the configuration");
        }
    }

    /**
     * Add the local entry
     *
     * @param ele
     * @param axisConfig AxisConfiguration of the current tenant
     * */
    private boolean addEntry(String ele, AxisConfiguration axisConfig) {
        final Lock lock = getLock(axisConfig);
        try {
            lock.lock();
            OMElement elem;
            try {
                elem = LocalEntryUtil.nonCoalescingStringToOm(ele);
            } catch (XMLStreamException e) {
                log.error("Error while converting the file content : " + e.getMessage());
                return false;
            }

            if (elem.getQName().getLocalPart().equals(XMLConfigConstants.ENTRY_ELT.getLocalPart())) {

                String entryKey = elem.getAttributeValue(new QName("key"));
                entryKey = entryKey.trim();
                SynapseConfiguration synapseConfiguration = getSynapseConfiguration(axisConfig);
                if (log.isDebugEnabled()) {
                    log.debug("Adding local entry with key : " + entryKey);
                }
                if (synapseConfiguration.getLocalRegistry().containsKey(entryKey)) {
                    log.error("An Entry with key " + entryKey +
                            " is already used within the configuration");
                } else {
                    Entry entry =
                            EntryFactory.createEntry(elem,
                                    synapseConfiguration.getProperties());
                    entry.setFileName(ServiceBusUtils.generateFileName(entry.getKey()));
                    synapseConfiguration.addEntry(entryKey, entry);
                    MediationPersistenceManager pm =
                            ServiceBusUtils.getMediationPersistenceManager(axisConfig);
                    pm.saveItem(entry.getKey(), ServiceBusConstants.ITEM_TYPE_ENTRY);
                }
                if (log.isDebugEnabled()) {
                    log.debug("Local registry entry : " + entryKey + " added to the configuration");
                }
                return true;
            } else {
                log.warn("Error adding local entry. Invalid definition");
            }
        } catch (SynapseException syne) {
            log.error("Unable to add local entry ", syne);
        } catch (OMException e) {
            log.error("Unable to add local entry. Invalid XML ", e);
        } catch (Exception e) {
            log.error("Unable to add local entry. Invalid XML ", e);
        } finally {
            lock.unlock();
        }
        return false;
    }

    /**
     * Remove the local entry
     *
     * @param ele
     * @param axisConfig AxisConfiguration of the current tenant
     * */
    public boolean deleteEntry(String ele, AxisConfiguration axisConfig) {

        final Lock lock = getLock(axisConfig);
        String entryKey = null;
        try {
            lock.lock();
            OMElement elem;
            try {
                elem = LocalEntryUtil.nonCoalescingStringToOm(ele);
            } catch (XMLStreamException e) {
                log.error("Error while converting the file content : " + e.getMessage());
                return false;
            }

            if (elem.getQName().getLocalPart().equals(XMLConfigConstants.ENTRY_ELT.getLocalPart())) {

                entryKey = elem.getAttributeValue(new QName("key"));
                entryKey = entryKey.trim();
                log.debug("Adding local entry with key : " + entryKey);

                SynapseConfiguration synapseConfiguration = getSynapseConfiguration(axisConfig);
                Entry entry = synapseConfiguration.getDefinedEntries().get(entryKey);
                if (entry != null) {
                    synapseConfiguration.removeEntry(entryKey);
                    MediationPersistenceManager pm =
                            ServiceBusUtils.getMediationPersistenceManager(axisConfig);
                    pm.deleteItem(entryKey, entry.getFileName(),
                            ServiceBusConstants.ITEM_TYPE_ENTRY);
                    if (log.isDebugEnabled()) {
                        log.debug("Deleted local entry with key : " + entryKey);
                    }
                    return true;
                } else {
                    log.warn("No entry exists by the key : " + entryKey);
                    return false;
                }
            }
        } catch (SynapseException syne) {
            log.error("Unable to delete the local entry : " + entryKey, syne);
        } catch (Exception e) {
            log.error("Unable to delete the local entry : " + entryKey, e);
        } finally {
            lock.unlock();
        }
        return false;
    }

    /**
     * Acquires the lock
     *
     * @param axisConfig AxisConfiguration instance
     * @return Lock instance
     */
    protected Lock getLock(AxisConfiguration axisConfig) {
        Parameter p = axisConfig.getParameter(ServiceBusConstants.SYNAPSE_CONFIG_LOCK);
        if (p != null) {
            return (Lock) p.getValue();
        } else {
            log.warn(ServiceBusConstants.SYNAPSE_CONFIG_LOCK + " is null, Recreating a new lock");
            Lock lock = new ReentrantLock();
            try {
                axisConfig.addParameter(ServiceBusConstants.SYNAPSE_CONFIG_LOCK, lock);
                return lock;
            } catch (AxisFault axisFault) {
                log.error("Error while setting " + ServiceBusConstants.SYNAPSE_CONFIG_LOCK);
            }
        }

        return null;
    }

    /**
     * Delete the SynapseImport instance with given importQualifiedName in the
     * synapse configuration
     *
     * @param importQualifiedName
     *            of the MessageProcessor to be deleted
     * @param axisConfig AxisConfiguration of the current tenant
     * @throws AxisFault
     *             if Message processor does not exist
     */
    public void deleteImport(String importQualifiedName, AxisConfiguration axisConfig) throws AxisFault {
        try {
            SynapseConfiguration configuration = getSynapseConfiguration(axisConfig);

            assert configuration != null;
            if (configuration.getSynapseImports().containsKey(importQualifiedName)) {
                SynapseImport synapseImport = configuration.removeSynapseImport(importQualifiedName);
                String fileName = synapseImport.getFileName();
                // get corresponding library for un-loading this import
                Library synLib =
                        configuration.getSynapseLibraries()
                                .get(importQualifiedName);
                if (synLib != null) {
                    // this is a important step -> we need to unload what ever the
                    // components loaded thru this import
                    synLib.unLoadLibrary();
                    undeployingLocalEntries(synLib, configuration, axisConfig);
                }
            }
        } catch (Exception e) {
            log.warn("Error occured while deleting the synapse library import");
        }
    }

    /**
     * Handle main and fault sequence deployment.
     * Since main.xml and fault.xml is already in filesystem, we only can update those.
     * NO direct deployer call and sync deployment
     *
     * @param artifact Sequence Artifact
     * @param axisConfig AxisConfiguration of the current tenant
     * @return whether main or fault sequence is handled
     */
    private boolean handleMainFaultSeqDeployment(Artifact artifact,
                                                 AxisConfiguration axisConfig, Deployer deployer) throws DeploymentException {

        String fileName = artifact.getFiles().get(0).getName();
        String artifactPath = artifact.getExtractedPath() + File.separator + fileName;
        boolean isMainOrFault = false;

        if (fileName.matches(MAIN_SEQ_REGEX) || fileName.matches(SynapseAppDeployerConstants.MAIN_SEQ_FILE)) {
            isMainOrFault = true;
            try {
                String mainXMLPath = getMainXmlPath(axisConfig);
                log.info("Copying main sequence to " + mainXMLPath);
                FileUtils.copyFile(new File(artifactPath), new File(mainXMLPath));

                if (!MicroIntegratorBaseUtils.getCarbonAxisConfigurator().isHotDeploymentEnabled()) {
                    deployer.deploy(new DeploymentFileData(new File(mainXMLPath), deployer));
                }
                artifact.setDeploymentStatus(AppDeployerConstants.DEPLOYMENT_STATUS_DEPLOYED);
            } catch (DeploymentException e) {
                artifact.setDeploymentStatus(AppDeployerConstants.DEPLOYMENT_STATUS_FAILED);
                throw e;
            } catch (IOException e) {
                log.error("Error copying main.xml to sequence directory", e);
            } catch (Throwable throwable) {
                artifact.setDeploymentStatus(AppDeployerConstants.DEPLOYMENT_STATUS_FAILED);
                // Since there can be different deployers, they can throw any error.
                // So need to handle unhandled exception has occurred during deployement. Hence catch all and
                // wrap it with DeployementException and throw it
                throw new DeploymentException(throwable);
            } finally {
                //clear the log appender once deployment is finished to avoid appending the
                //same log to other classes.
                setCustomLogContent(deployer, null);
                CustomLogSetter.getInstance().clearThreadLocalContent();
            }
        } else if (fileName.matches(FAULT_SEQ_REGEX) || fileName.matches(SynapseAppDeployerConstants.FAULT_SEQ_FILE)) {
            isMainOrFault = true;
            try {
                String faultXMLPath = getFaultXmlPath(axisConfig);
                log.info("Copying fault sequence to " + faultXMLPath);
                FileUtils.copyFile(new File(artifactPath), new File(faultXMLPath));

                if (!MicroIntegratorBaseUtils.getCarbonAxisConfigurator().isHotDeploymentEnabled()) {
                    deployer.deploy(new DeploymentFileData(new File(faultXMLPath), deployer));
                }
                artifact.setDeploymentStatus(AppDeployerConstants.DEPLOYMENT_STATUS_DEPLOYED);
            } catch (DeploymentException e) {
                artifact.setDeploymentStatus(AppDeployerConstants.DEPLOYMENT_STATUS_FAILED);
                throw e;
            } catch (IOException e) {
                log.error("Error copying main.xml to sequence directory", e);
            } catch (Throwable throwable) {
                artifact.setDeploymentStatus(AppDeployerConstants.DEPLOYMENT_STATUS_FAILED);
                // Since there can be different deployers, they can throw any error.
                // So need to handle unhandled exception has occurred during deployement. Hence catch all and
                // wrap it with DeployementException and throw it
                throw new DeploymentException(throwable);
            } finally {
                //clear the log appender once deployment is finished to avoid appending the
                //same log to other classes.
                setCustomLogContent(deployer, null);
                CustomLogSetter.getInstance().clearThreadLocalContent();
            }
        }
        return isMainOrFault;
    }

    /**
     * Handle main and fault sequence un-deployment.
     * Since main.xml and fault.xml is already in filesystem, we only can update those.
     * NO direct deployer call
     *
     * @param artifact Sequence Artifact
     * @param axisConfig AxisConfiguration of the current tenant
     * @return whether main or fault sequence is handled
     * @throws IOException
     */
    private boolean handleMainFaultSeqUndeployment(Artifact artifact,
                                                     AxisConfiguration axisConfig)
            throws IOException {

        boolean isMainOrFault = false;
        String fileName = artifact.getFiles().get(0).getName();
        if (fileName.matches(MAIN_SEQ_REGEX) || fileName.matches(SynapseAppDeployerConstants.MAIN_SEQ_FILE)) {
            isMainOrFault = true;
            String mainXMLPath = getMainXmlPath(axisConfig);
            FileUtils.deleteQuietly(new File(mainXMLPath));
            FileUtils.writeStringToFile(new File(mainXMLPath), MAIN_XML);

        } else if (fileName.matches(FAULT_SEQ_REGEX) || fileName.matches(SynapseAppDeployerConstants.FAULT_SEQ_FILE)) {
            isMainOrFault = true;
            String faultXMLPath = getFaultXmlPath(axisConfig);
            FileUtils.deleteQuietly(new File(faultXMLPath));
            FileUtils.writeStringToFile(new File(faultXMLPath), FAULT_XML);
        }

        return isMainOrFault;
    }

    /**
     * Check whether a particular artifact type can be accepted for deployment. If the type doesn't
     * exist in the acceptance list, we assume that it doesn't require any special features to be
     * installed in the system. Therefore, that type is accepted.
     * If the type exists in the acceptance list, the acceptance value is returned.
     *
     * @param serviceType - service type to be checked
     * @return true if all features are there or entry is null. else false
     */
    private boolean isAccepted(String serviceType) {
        // TODO: check this
        /*if (acceptanceList == null) {
            acceptanceList = AppDeployerUtils.buildAcceptanceList(SynapseAppDeployerDSComponent
                    .getRequiredFeatures());
        }
        Boolean acceptance = acceptanceList.get(serviceType);
        return (acceptance == null || acceptance);*/
        return true;
    }

    /**
     * Validate artifact
     *
     * @param artifact artifact to be validated
     * @return validation passed or not
     */
    private boolean validateArtifact(Artifact artifact) {
        if (artifact == null) {
            return false;
        }

        if (!isAccepted(artifact.getType())) {
            log.warn("Can't deploy artifact : " + artifact.getName() + " of type : " +
                     artifact.getType() + ". Required features are not installed in the system");
            return false;
        }

        List<CappFile> files = artifact.getFiles();
        if (files.size() != 1) {
            log.error("Synapse artifact types must have a single file to " +
                      "be deployed. But " + files.size() + " files found.");
            return false;
        }

        return true;
    }

    /**
     * Get the deployer for the Class Mediators
     *
     * @param axisConfig AxisConfiguration instance
     * @return Deployer instance
     */
    private Deployer getClassMediatorDeployer(AxisConfiguration axisConfig) {
        DeploymentEngine deploymentEngine = (DeploymentEngine) axisConfig.getConfigurator();
        String classMediatorPath = axisConfig.getRepository().getPath() +
                                   File.separator + SynapseAppDeployerConstants.MEDIATORS_FOLDER;
        return deploymentEngine.
                getDeployer(classMediatorPath, ServiceBusConstants.CLASS_MEDIATOR_EXTENSION);
    }

    /**
     * Get the deployer for the Synapse Library
     *
     * @param axisConfig AxisConfiguration instance
     * @return Deployer instance
     */
    private Deployer getSynapseLibraryDeployer(AxisConfiguration axisConfig) {
        try {
            String synapseLibraryPath = axisConfig.getRepository().getPath() +
                    SynapseAppDeployerConstants.SYNAPSE_LIBS;
            DeploymentEngine deploymentEngine = (DeploymentEngine) axisConfig.getConfigurator();
            deploymentEngine.addDeployer(new LibraryArtifactDeployer(), synapseLibraryPath, ServiceBusConstants.SYNAPSE_LIBRARY_EXTENSION);

            return deploymentEngine.
                    getDeployer(synapseLibraryPath, ServiceBusConstants.SYNAPSE_LIBRARY_EXTENSION);
        } catch (Exception e) {
            log.error("Error occured while getting the deployer");
            return null;
        }
    }

    /**
     * Get the artifact directory name for the artifact type
     *
     * @param artifactType  type of the synapse artifact
     * @return artifact directory
     */
    private String getArtifactDirName(String artifactType) {

        if (SynapseAppDeployerConstants.SEQUENCE_TYPE.equals(artifactType)) {
            return SynapseAppDeployerConstants.SEQUENCES_FOLDER;
        } else if (SynapseAppDeployerConstants.ENDPOINT_TYPE.equals(artifactType)) {
            return SynapseAppDeployerConstants.ENDPOINTS_FOLDER;
        } else if (SynapseAppDeployerConstants.PROXY_SERVICE_TYPE.equals(artifactType)) {
            return SynapseAppDeployerConstants.PROXY_SERVICES_FOLDER;
        } else if (SynapseAppDeployerConstants.LOCAL_ENTRY_TYPE.equals(artifactType)) {
            return SynapseAppDeployerConstants.LOCAL_ENTRIES_FOLDER;
        } else if (SynapseAppDeployerConstants.EVENT_SOURCE_TYPE.equals(artifactType)) {
            return SynapseAppDeployerConstants.EVENTS_FOLDER;
        } else if (SynapseAppDeployerConstants.TASK_TYPE.equals(artifactType)) {
            return SynapseAppDeployerConstants.TASKS_FOLDER;
        } else if (SynapseAppDeployerConstants.MESSAGE_STORE_TYPE.endsWith(artifactType)) {
            return SynapseAppDeployerConstants.MESSAGE_STORE_FOLDER;
        } else if (SynapseAppDeployerConstants.MESSAGE_PROCESSOR_TYPE.endsWith(artifactType)) {
            return SynapseAppDeployerConstants.MESSAGE_PROCESSOR_FOLDER;
        } else if (SynapseAppDeployerConstants.API_TYPE.endsWith(artifactType)) {
            return SynapseAppDeployerConstants.APIS_FOLDER;
        } else if (SynapseAppDeployerConstants.TEMPLATE_TYPE.endsWith(artifactType)) {
            return SynapseAppDeployerConstants.TEMPLATES_FOLDER;
        } else if (SynapseAppDeployerConstants.INBOUND_ENDPOINT_TYPE.endsWith(artifactType)) {
           return SynapseAppDeployerConstants.INBOUND_ENDPOINT_FOLDER;
        } else if (SynapseAppDeployerConstants.SYNAPSE_LIBRARY_TYPE.equals(artifactType)) {
            return SynapseAppDeployerConstants.SYNAPSE_LIBS;
        }
        return null;
    }

    /**
     * Get the absolute path of the artifact directory
     *
     * @param axisConfiguration axis configuration
     * @param artifactDirName synapse artifact directory name
     * @return absolute path of artifact directory
     */
    private String getArtifactDirPath(AxisConfiguration axisConfiguration, String artifactDirName) {
        if (artifactDirName.equals(SynapseAppDeployerConstants.SYNAPSE_LIBS)) {
            return axisConfiguration.getRepository().getPath() +
                    SynapseAppDeployerConstants.SYNAPSE_LIBS;
        } else {
            return axisConfiguration.getRepository().getPath() +
                    SynapseAppDeployerConstants.SYNAPSE_CONFIGS +
                    File.separator + SynapseAppDeployerConstants.DEFAULT_DIR +
                    File.separator + artifactDirName;
        }
    }

    private String getMainXmlPath(AxisConfiguration axisConfig) {
        return axisConfig.getRepository().getPath() +
               SynapseAppDeployerConstants.SYNAPSE_CONFIGS +
               File.separator + SynapseAppDeployerConstants.DEFAULT_DIR +
               File.separator + SynapseAppDeployerConstants.SEQUENCES_FOLDER +
               File.separator + SynapseAppDeployerConstants.MAIN_SEQ_FILE;
    }

    private String getFaultXmlPath(AxisConfiguration axisConfig) {
        return axisConfig.getRepository().getPath() +
               SynapseAppDeployerConstants.SYNAPSE_CONFIGS +
               File.separator + SynapseAppDeployerConstants.DEFAULT_DIR +
               File.separator + SynapseAppDeployerConstants.SEQUENCES_FOLDER +
               File.separator + SynapseAppDeployerConstants.FAULT_SEQ_FILE;
    }

    /**
     * Set the custom log content if the per Artifact container logging is enabled
     *
     * @param deployer Application Deployer
     * @param carbonApp carbon application
     */
    public void setCustomLogContent (Deployer deployer, CarbonApplication carbonApp) {
        if ((deployer instanceof AbstractSynapseArtifactDeployer)) {
            if (carbonApp != null) {
                ((AbstractSynapseArtifactDeployer) deployer).setCustomLog(carbonApp.getAppName(),
                        AppDeployerUtils.getTenantIdLogString(AppDeployerUtils.getTenantId()));
            } else {
                ((AbstractSynapseArtifactDeployer) deployer).setCustomLog(null, null);
            }
        }
    }

    /**
     * This deploys artifacts when a list of artifacts is provided
     *
     * @param artifacts - List of artifacts which should be deployed
     * @param carbonApp  - CarbonApplication instance to check for artifacts
     * @param axisConfig - AxisConfiguration of the current tenant
     * @throws DeploymentException if some error occurs while deployment
     */
    public void deployArtifactType(List<Artifact.Dependency> artifacts, CarbonApplication carbonApp,
                                AxisConfiguration axisConfig) throws DeploymentException {
        for (Artifact.Dependency dep : artifacts) {
            Artifact artifact = dep.getArtifact();
            String artifactType = artifact.getType();
            String artifactDirName = getArtifactDirName(artifactType);

            if (!validateArtifact(artifact) || artifactDirName == null) {
                continue;
            }

            Deployer deployer = getDeployer(artifact.getType());
            String artifactDir = getArtifactDirPath(axisConfig, artifactDirName);

            artifact.setRuntimeObjectName(artifact.getName());

            String fileName = artifact.getFiles().get(0).getName();
            String artifactPath = null;
            if (!StringUtils.isEmpty(fileName)) {
                artifactPath = artifact.getExtractedPath() + File.separator + fileName;
            }

            if (deployer != null) {
                File artifactInRepo = new File(artifactDir + File.separator + fileName);

                if (SynapseAppDeployerConstants.SEQUENCE_TYPE.equals(artifact.getType()) &&
                        handleMainFaultSeqDeployment(artifact, axisConfig, deployer)) {
                    log.debug("Handling main and fault sequence deployment");
                } else if (artifactInRepo.exists()) {
                    log.warn("Artifact " + fileName + " already found in " + artifactInRepo.getAbsolutePath() +
                            ". Ignoring CAPP's artifact");
                    artifact.setDeploymentStatus(AppDeployerConstants.DEPLOYMENT_STATUS_DEPLOYED);
                } else {
                    try {
                        setCustomLogContent(deployer, carbonApp);
                        deployer.deploy(new DeploymentFileData(new File(artifactPath), deployer,
                                carbonApp.getAppConfig().getAppArtifactIdentifier(), carbonApp.getAppConfig().getCAppDependencies()));
                        artifact.setDeploymentStatus(AppDeployerConstants.DEPLOYMENT_STATUS_DEPLOYED);
                    } catch (DeploymentException e) {
                        artifact.setDeploymentStatus(AppDeployerConstants.DEPLOYMENT_STATUS_FAILED);
                        throw e;
                    } catch (Throwable throwable) {
                        artifact.setDeploymentStatus(AppDeployerConstants.DEPLOYMENT_STATUS_FAILED);
                        // Since there can be different deployers, they can throw any error.
                        // So need to handle unhandled exception has occurred during deployement. Hence catch all and
                        // wrap it with DeployementException and throw it
                        throw new DeploymentException(throwable);
                    } finally {
                        //clear the log appender once deployment is finished to avoid appending the
                        //same log to other classes.
                        setCustomLogContent(deployer, null);
                        CustomLogSetter.getInstance().clearThreadLocalContent();
                    }
                }
            }
        }
    }

    /**
     * Function to initialize deployers with default deployers. Need to invoke this before adding custom implementations
     */
    private void initializeDefaultSynapseDeployers() {
        addSynapseDeployer(SynapseAppDeployerConstants.LOCAL_ENTRY_TYPE, new LocalEntryDeployer());
        addSynapseDeployer(SynapseAppDeployerConstants.ENDPOINT_TYPE, new EndpointDeployer());
        addSynapseDeployer(SynapseAppDeployerConstants.SEQUENCE_TYPE, new SequenceDeployer());
        addSynapseDeployer(SynapseAppDeployerConstants.TEMPLATE_TYPE, new TemplateDeployer());
        addSynapseDeployer(SynapseAppDeployerConstants.TASK_TYPE, new TaskDeployer());
        addSynapseDeployer(SynapseAppDeployerConstants.MESSAGE_STORE_TYPE, new MessageStoreDeployer());
        addSynapseDeployer(SynapseAppDeployerConstants.MESSAGE_PROCESSOR_TYPE, new MessageProcessorDeployer());
        addSynapseDeployer(SynapseAppDeployerConstants.INBOUND_ENDPOINT_TYPE, new InboundEndpointDeployer());
        addSynapseDeployer(SynapseAppDeployerConstants.API_TYPE, new APIDeployer());
        addSynapseDeployer(SynapseAppDeployerConstants.PROXY_SERVICE_TYPE, new ProxyServiceDeployer());
    }

    /**
     * Function to retrieve related deployer for the given artifact type
     *
     * @param type artifact type
     * @return related deployer, returns null if no deployer registered for given artifact type
     */
    private Deployer getDeployer(String type) {
        return synapseDeployers.get(type);
    }

    /**
     * Function to add synapse deployer
     *
     * @param type     artifact type that deployed by the deployer
     * @param deployer deployer implementation
     */
    private void addSynapseDeployer(String type, Deployer deployer) {
        ConfigurationContext configContext =
                ConfigurationHolder.getInstance().getAxis2ConfigurationContextService().getServerConfigContext();
        if (deployer == null) {
            log.error("Failed to add Deployer : deployer is null");
            return;
        }
        if (configContext != null) {
            // Initialize and register Deployer
            deployer.init(configContext);
            registerSynapseDeployer(configContext.getAxisConfiguration(), type, deployer);
        } else {
            log.warn("ConfigurationContext has not been set. Deployer: " +
                     deployer.getClass() + "is not initialized");
        }
        synapseDeployers.put(type, deployer);
    }

    /**
     * Register synapse deployers to the deployment engine.
     *
     * @param axisConfig   - axisConfiguration to which this deployer belongs
     * @param artifactType - type of the artifact
     * @param deployer     - related deployer
     */
    private void registerSynapseDeployer(AxisConfiguration axisConfig, String artifactType, Deployer deployer) {

        DeploymentEngine deploymentEngine = (DeploymentEngine) axisConfig.getConfigurator();
        String artifactDirName = getArtifactDirName(artifactType);

        if (artifactDirName != null) {
            SynapseArtifactDeploymentStore deploymentStore = getSynapseConfiguration(axisConfig).getArtifactDeploymentStore();
            SynapseConfiguration synCfg = getSynapseConfiguration(axisConfig);
            String artifactDir = getArtifactDirPath(axisConfig, artifactDirName);

            switch (artifactType) {
                case SynapseAppDeployerConstants.SEQUENCE_TYPE:
                    for (SequenceMediator seq : synCfg.getDefinedSequences().values()) {
                        if (seq.getFileName() != null) {
                            deploymentStore.addRestoredArtifact(artifactDir + File.separator + seq.getFileName());
                        }
                    }
                    break;
                case SynapseAppDeployerConstants.ENDPOINT_TYPE:
                    for (Endpoint ep : synCfg.getDefinedEndpoints().values()) {
                        if (ep.getFileName() != null) {
                            deploymentStore.addRestoredArtifact(artifactDir + File.separator + ep.getFileName());
                        }
                    }
                    break;
                case SynapseAppDeployerConstants.PROXY_SERVICE_TYPE:
                    for (ProxyService proxyService : synCfg.getProxyServices()) {
                        if (proxyService.getFileName() != null) {
                            deploymentStore.addRestoredArtifact(artifactDir + File.separator + proxyService.getFileName());
                        }
                    }
                    break;
                case SynapseAppDeployerConstants.LOCAL_ENTRY_TYPE:
                    for (Entry entry : synCfg.getDefinedEntries().values()) {
                        if (entry.getFileName() != null) {
                            deploymentStore.addRestoredArtifact(artifactDir + File.separator + entry.getFileName());
                        }
                    }
                    break;
                case SynapseAppDeployerConstants.TASK_TYPE:
                    for (Startup stp : synCfg.getStartups()) {
                        if (stp.getFileName() != null) {
                            deploymentStore.addRestoredArtifact(artifactDir + File.separator + stp.getFileName());
                        }
                    }
                    break;
                case SynapseAppDeployerConstants.MESSAGE_STORE_TYPE:
                    for (MessageStore messageStore : synCfg.getMessageStores().values()) {
                        if (messageStore.getFileName() != null) {
                            deploymentStore.addRestoredArtifact(artifactDir + File.separator + messageStore.getFileName());
                        }
                    }
                    break;
                case SynapseAppDeployerConstants.MESSAGE_PROCESSOR_TYPE:
                    for (MessageProcessor processor : synCfg.getMessageProcessors().values()) {
                        if (processor.getFileName() != null) {
                            deploymentStore.addRestoredArtifact(artifactDir + File.separator + processor.getFileName());
                        }
                    }
                    break;
                case SynapseAppDeployerConstants.API_TYPE:
                    for (API api : synCfg.getAPIs()) {
                        if (api.getFileName() != null) {
                            deploymentStore.addRestoredArtifact(artifactDir + File.separator + api.getFileName());
                        }
                    }
                    break;
                case SynapseAppDeployerConstants.TEMPLATE_TYPE:
                    for (TemplateMediator seqTempl : synCfg.getSequenceTemplates().values()) {
                        if (seqTempl.getFileName() != null) {
                            deploymentStore.addRestoredArtifact(artifactDir + File.separator + seqTempl.getFileName());
                        }
                    }
                    for (Template epTempl : synCfg.getEndpointTemplates().values()) {
                        if (epTempl.getFileName() != null) {
                            deploymentStore.addRestoredArtifact(artifactDir + File.separator + epTempl.getFileName());
                        }
                    }
                    break;
                case SynapseAppDeployerConstants.INBOUND_ENDPOINT_TYPE:
                    for (InboundEndpoint inboundEndpoint : synCfg.getInboundEndpoints()) {
                        if (inboundEndpoint.getFileName() != null) {
                            deploymentStore.addRestoredArtifact(artifactDir + File.separator + inboundEndpoint.getFileName());
                        }
                    }
                    break;
                default:
                    // do nothing

            }
            deploymentEngine.addDeployer(deployer, artifactDir, ServiceBusConstants.ARTIFACT_EXTENSION);
        }
    }

    private String getConnectorName(String artifactName) {
        return artifactName.substring(0, artifactName.lastIndexOf("-connector"));
    }

    /**
     * Checks whether a synapse library artifact is still declared as a dependency by another
     * currently deployed Carbon Application, so a shared connector library is not removed while
     * another CApp is still using it.
     */
    private boolean isLibraryUsedByOtherCApps(String libraryName, CarbonApplication currentApp) {

        for (CarbonApplication otherApp : CappDeployer.getCarbonApps()) {
            if (otherApp.getAppNameWithVersion().equals(currentApp.getAppNameWithVersion())) {
                continue;
            }
            for (Artifact.Dependency dep : otherApp.getAppConfig().getApplicationArtifact().getDependencies()) {
                Artifact otherArtifact = dep.getArtifact();
                if (otherArtifact == null || !AppDeployerConstants.DEPLOYMENT_STATUS_DEPLOYED
                        .equals(otherArtifact.getDeploymentStatus())) {
                    continue;
                }
                if (SynapseAppDeployerConstants.SYNAPSE_LIBRARY_TYPE.equals(otherArtifact.getType())
                        && libraryName.equals(otherArtifact.getName())) {
                    return true;
                }
                if (SynapseAppDeployerConstants.CONNECTOR_DEPENDENCY_TYPE.equals(otherArtifact.getType())
                        && libraryName.equals(otherArtifact.getConnector())) {
                    return true;
                }
            }
        }
        return false;
    }

    private String getTemplateName(String artifactPath, String name) {
        try {
            FileInputStream in = FileUtils.openInputStream(new File(artifactPath));
            OMElement artifactConfig = (new StAXOMBuilder(StAXUtils.createXMLStreamReader(in))).getDocumentElement();
            OMElement element = artifactConfig.getFirstChildWithName
                    (new QName("http://ws.apache.org/ns/synapse", "endpoint"));
            if (null != element) {
                return "endpoint_".concat(name);
            } else {
                element = artifactConfig.getFirstChildWithName
                        (new QName("http://ws.apache.org/ns/synapse", "sequence"));
                if (null != element) {
                    return "sequence_".concat(name);
                }
            }
        } catch (IOException | XMLStreamException e) {
            log.error("Error occurred while creating name of template located at "+ artifactPath, e);
        }
        return name;
    }
}



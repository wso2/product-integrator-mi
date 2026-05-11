/*
 *  Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.micro.integrator.initializer.deployment.application.deployer;

import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMException;
import org.apache.axiom.om.impl.builder.StAXOMBuilder;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.deployment.AbstractDeployer;
import org.apache.axis2.deployment.Deployer;
import org.apache.axis2.deployment.DeploymentException;
import org.apache.axis2.deployment.repository.util.DeploymentFileData;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.SynapseException;
import org.apache.synapse.config.SynapseConfigUtils;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.api.API;
import org.apache.synapse.config.SynapsePropertiesLoader;
import org.wso2.carbon.securevault.SecretCallbackHandlerService;
import org.wso2.config.mapper.ConfigParser;
import org.wso2.micro.application.deployer.AppDeployerUtils;
import org.wso2.micro.application.deployer.CarbonApplication;
import org.wso2.micro.application.deployer.config.ApplicationConfiguration;
import org.wso2.micro.application.deployer.config.Artifact;
import org.wso2.micro.application.deployer.handler.AppDeploymentHandler;
import org.wso2.micro.core.CarbonAxisConfigurator;
import org.wso2.micro.core.util.CarbonException;
import org.wso2.micro.core.util.FileManipulator;
import org.wso2.micro.integrator.initializer.deployment.DuplicateCAppDescriptorException;
import org.wso2.micro.integrator.initializer.serviceCatalog.ServiceCatalogDeployer;
import org.wso2.micro.integrator.initializer.utils.Constants;
import org.wso2.micro.integrator.initializer.utils.DeployerUtil;
import org.wso2.micro.integrator.initializer.utils.ServiceCatalogUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;

import static org.wso2.micro.core.Constants.SUPER_TENANT_DOMAIN_NAME;
import static org.wso2.micro.integrator.initializer.deployment.synapse.deployer.SynapseAppDeployerConstants.API_TYPE;
import static org.wso2.micro.integrator.initializer.deployment.synapse.deployer.SynapseAppDeployerConstants.MEDIATOR_TYPE;
import static org.wso2.micro.integrator.initializer.deployment.synapse.deployer.SynapseAppDeployerConstants.REGISTRY_RESOURCE_TYPE;
import static org.wso2.micro.integrator.initializer.deployment.synapse.deployer.SynapseAppDeployerConstants.SYNAPSE_LIBRARY_TYPE;
import static org.wso2.micro.integrator.initializer.utils.Constants.CAPP_FOLDER_NAME;
import static org.wso2.micro.integrator.initializer.utils.Constants.CAR_FILE_EXTENSION;
import static org.wso2.micro.integrator.initializer.utils.Constants.HTTP_CONNECTOR_NAME;
import static org.wso2.micro.integrator.initializer.utils.DeployerUtil.getCAppsWithDescriptorCount;
import static org.wso2.micro.integrator.initializer.utils.DeployerUtil.getCAppProcessingOrder;
import static org.wso2.micro.integrator.registry.MicroIntegratorRegistryConstants.REG_DEP_FAILURE_IDENTIFIER;

public class CappDeployer extends AbstractDeployer {

    private static final Log log = LogFactory.getLog(CappDeployer.class);

    private AxisConfiguration axisConfig;

    private List<AppDeploymentHandler> appDeploymentHandlers = new ArrayList<>();
    private static ArrayList<CarbonApplication> cAppMap = new ArrayList<>();
    private static ArrayList<CarbonApplication> faultyCAppObjects = new ArrayList<>();
    private static ArrayList<String> faultyCapps = new ArrayList<>();
    private final Object lock = new Object();

    private static final String ERROR_MESSAGE = "errorMessage";
    private static final String SWAGGER_SUBSTRING = "_swagger";
    private static final String METADATA_FOLDER_NAME = "metadata";
    private static final String ARTIFACT_FILE = "artifact.xml";

    /**
     * Counts the number of retry passes executed so far this startup cycle.
     * Compared against {@link #getMaxRetryCount()} to bound the total number of retries.
     * Reset in {@link #cleanup()} so that unit tests that re-use the same JVM get a clean state.
     */
    private static volatile int retryPassCount = 0;

    /**
     * True while {@link #retryFaultyCApps()} is executing. Guards the retry trigger in
     * {@link #deployCarbonApps(String)} so that the recursive {@code deployCarbonApps} calls
     * made during retries do not re-enter the retry logic; the explicit loop inside
     * {@code retryFaultyCApps} drives all subsequent passes instead.
     */
    private static volatile boolean isRetrying = false;

    /**
     * Number of high-priority CApps discovered during {@link #sort}. Used to detect when
     * the high-priority deployment phase is complete so faulty ones can be retried before
     * any low-priority CApp is deployed. -1 means sort() has not run yet (e.g. priority
     * deployment is disabled or hot-deploy path).
     */
    private static volatile int highPriorityCAppCount = -1;

    private static final Set<String> HIGH_PRIORITY_TYPES = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(MEDIATOR_TYPE, SYNAPSE_LIBRARY_TYPE, REGISTRY_RESOURCE_TYPE)));

    private static final XMLInputFactory SECURE_XML_INPUT_FACTORY = createSecureXMLInputFactory();

    private static XMLInputFactory createSecureXMLInputFactory() {
        XMLInputFactory factory = XMLInputFactory.newInstance();
        setXMLInputFactoryProperty(factory, XMLInputFactory.SUPPORT_DTD, Boolean.FALSE);
        setXMLInputFactoryProperty(factory, XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, Boolean.FALSE);
        return factory;
    }

    private static void setXMLInputFactoryProperty(XMLInputFactory factory, String property, Object value) {
        try {
            factory.setProperty(property, value);
        } catch (IllegalArgumentException e) {
            if (log.isDebugEnabled()) {
                log.debug("XMLInputFactory implementation does not support property '" + property + "'.", e);
            }
        }
    }

    /**
     * {@code [server]} section key in deployment.toml that enables priority-based CApp sorting
     * and faulty-CApp retry. When absent or false the deployer falls back to the default
     * alphabetical ordering and skips the retry pass.
     */
    private static final String PRIORITY_DEPLOYMENT_CONFIG_KEY = "server.enable_priority_deployment";

    /**
     * {@code [server]} section key in deployment.toml that controls how many retry passes are
     * performed after the high-priority phase. Defaults to {@code 1} when absent or invalid.
     * Set to {@code 0} to disable retries entirely.
     */
    private static final String PRIORITY_DEPLOYMENT_RETRY_COUNT_CONFIG_KEY =
            "server.priority_deployment_retry_count";

    /**
     * Carbon application repository directory.
     */
    private String cAppDir;

    private String appsDir;

    /**
     * Carbon application file extension (i.e. 'car').
     */
    private String extension;

    /**
     * Service Catalog Executor threads for publishing Services to Service Catalog.
     */
    private ExecutorService serviceCatalogExecutor;

    /**
     * Execution Tracker to track Service Catalog Execution at server startup.
     * initialServiceCatalogExecutor will be executed when the first carbon application get deployed and
     * this is a one time process.
     */
    private boolean isServiceCatalogStartupExecutionPending = true;
    private ExecutorService initialServiceCatalogExecutor;

    /**
     * Map object to store Service Catalog configuration
     */
    private Map serviceCatalogConfiguration;

    /**
     * SecretCallbackHandlerService to read Service Catalog Configs
     */
    private SecretCallbackHandlerService secretCallbackHandlerService;

    public void init(ConfigurationContext configurationContext) {

        if (log.isDebugEnabled()) {
            log.debug("Initializing Capp Deployer..");
        }
        this.axisConfig = configurationContext.getAxisConfiguration();

        //delete the older extracted capps for this tenant.
        String appUnzipDir = AppDeployerUtils.getAppUnzipDir() + File.separator +
                AppDeployerUtils.getTenantIdString();
        FileManipulator.deleteDir(appUnzipDir);

        if (ServiceCatalogUtils.isServiceCatalogEnabled()) {
            serviceCatalogConfiguration = ServiceCatalogUtils.readConfiguration(secretCallbackHandlerService);
            serviceCatalogExecutor = Executors.newSingleThreadExecutor();
            initialServiceCatalogExecutor = Executors.newSingleThreadExecutor();
        }
    }

    public void setDirectory(String cAppDir) {

        this.cAppDir = cAppDir;
    }

    public void setAppsDirectory(String appsDir) {

        this.appsDir = appsDir;
    }

    public void setExtension(String extension) {

        this.extension = extension;
    }

    /**
     * Axis2 deployment engine will call this method when a .car archive is deployed. So we only have to call the
     * cAppDeploymentManager to deploy it using the absolute path of the deployed .car file.
     *
     * @param deploymentFileData - info about the deployed file
     * @throws DeploymentException - error while deploying cApp
     */
    public void deploy(DeploymentFileData deploymentFileData) throws DeploymentException {
        String artifactPath = deploymentFileData.getAbsolutePath();
        try {
            deployCarbonApps(artifactPath, deploymentFileData.isEmbeddedCAR());
        } catch (Exception e) {
            log.error("Error while deploying carbon application " + artifactPath, e);
        }

        super.deploy(deploymentFileData);
    }

    /**
     * Deploy synapse artifacts in a .car file.
     *
     * @param artifactPath - file path to be processed
     * @throws CarbonException - error while building
     */
    private void deployCarbonApps(String artifactPath, boolean isEmbeddedCAR) throws CarbonException {

        File cAppDirectory = new File(this.cAppDir);

        String archPathToProcess = AppDeployerUtils.formatPath(artifactPath);
        String cAppName = archPathToProcess.substring(archPathToProcess.lastIndexOf('/') + 1);

        if (!isCAppArchiveFile(cAppName)) {
            log.warn("Only .car files are processed. Hence " + cAppName + " will be ignored");
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug("Carbon Application detected : " + cAppName);
        }

        String targetCAppPath = cAppDirectory + File.separator + cAppName;
        if (isEmbeddedCAR) {
            String parentCApp = extractParentCAppName(archPathToProcess);
            targetCAppPath = cAppDirectory + File.separator + parentCApp + File.separator + "dependencies" + File.separator + cAppName;
        }
        String extractedPath = extractCarbonApplication(targetCAppPath);
        deployCarbonApplications(cAppName, targetCAppPath, extractedPath, isEmbeddedCAR);
    }

    public static String extractParentCAppName(String filePath) {
        Path p = Paths.get(filePath);
        if (p.getParent() != null && p.getParent().getParent() != null) {
            return p.getParent().getParent().getFileName().toString();
        }
        return null;
    }

    /**
     * Deploy synapse artifacts in a carbon application under apps directory.
     *
     * @param artifactPath - file path to be processed
     * @throws CarbonException - error while building
     */
    public void deployApps(String artifactPath) throws CarbonException {

        String archPathToProcess = AppDeployerUtils.formatPath(artifactPath);
        String cAppName = archPathToProcess.substring(archPathToProcess.lastIndexOf('/') + 1);
        String targetCAppPath = artifactPath.endsWith(File.separator) ? artifactPath : artifactPath + File.separator;
        deployCarbonApplications(cAppName, targetCAppPath, targetCAppPath, false);
    }

    private void deployCarbonApplications(String cAppName, String cAppPath, String targetCAppPath, boolean isEmbeddedCAR) throws CarbonException {

        CarbonApplication currentApp = null;
        try {
            currentApp = buildCarbonApplication(cAppPath, targetCAppPath, cAppName, axisConfig);
            if (currentApp != null) {
                // deploy sub artifacts of this cApp
                this.searchArtifacts(currentApp.getExtractedPath(), currentApp);

                if (isArtifactReadyToDeploy(currentApp.getAppConfig().getApplicationArtifact())) {
                    // Now ready to deploy
                    // send the CarbonApplication instance through the handler chain
                    for (AppDeploymentHandler appDeploymentHandler : appDeploymentHandlers) {
                        appDeploymentHandler.deployArtifacts(currentApp, axisConfig);
                    }
                } else {
                    log.error("Some dependencies were not satisfied in cApp:" + currentApp.getAppNameWithVersion() +
                            "Check whether all dependent artifacts are included in cApp file: " +
                            targetCAppPath);

                    deleteExtractedCApp(currentApp.getExtractedPath());
                    // Validate synapse config to remove half added swagger definitions in the case of a faulty CAPP.
                    SynapseConfigUtils.getSynapseConfiguration(SUPER_TENANT_DOMAIN_NAME).validateSwaggerTable();
                    return;
                }

                // Deployment Completed
                currentApp.setDeploymentCompleted(true);
                this.addCarbonApp(currentApp);
                log.info("Successfully Deployed Carbon Application : " + currentApp.getAppNameWithVersion() +
                        AppDeployerUtils.getTenantIdLogString(AppDeployerUtils.getTenantId()));
            }
        } catch (DeploymentException e) {
            handleDeployException(e, cAppName, currentApp, isEmbeddedCAR);
        } catch (SynapseException e) {
            // Handel SynapseException thrown by MicroIntegratorRegistry
            if (e.getMessage() != null && e.getMessage().startsWith(REG_DEP_FAILURE_IDENTIFIER)){
                handleDeployException(e, cAppName, currentApp, isEmbeddedCAR);
            }
            throw e;
        } finally {
            // Skip priority-deployment retry logic entirely when the feature is disabled.
            if (isCAppPriorityDeploymentEnabled()) {
                // Once every high-priority CApp has been processed (either successfully or as
                // faulty), retry the failed ones before any low-priority CApp is deployed.
                // This gives high-priority CApps (connectors, class mediators, registry
                // resources) another chance while low-priority CApps can still depend on them.
                // Using finally ensures this fires on all paths: success, DeploymentException
                // (which is consumed by the catch), and SynapseException (which re-throws).
                // The == rather than >= condition fires exactly once: on the last high-priority
                // CApp of the initial deployment pass. isRetrying suppresses re-entry from the
                // deployCarbonApps calls made inside retryFaultyCApps(); that method drives all
                // subsequent passes via an explicit loop.
                boolean isHighPriorityPhaseComplete = highPriorityCAppCount > 0
                        && (cAppMap.size() + faultyCapps.size()) == highPriorityCAppCount;
                if (isHighPriorityPhaseComplete && !isRetrying && !faultyCapps.isEmpty()
                        && ServiceCatalogUtils.isServerInStartupMode()) {
                    isRetrying = true;
                    try {
                        retryFaultyCApps();
                    } finally {
                        isRetrying = false;
                    }
                }
            }
        }

        boolean isAllCAppsDeployed = getCAppFileList().length == cAppMap.size() + faultyCapps.size();

        // Initial execution of Service catalog Deployer at server startup when last CApp get deployed.
        // isServiceCatalogStartupExecutionPending guards against multiple submissions.
        if (isServiceCatalogStartupExecutionPending && serviceCatalogConfiguration != null && isAllCAppsDeployed) {
            ServiceCatalogDeployer serviceDeployer = new ServiceCatalogDeployer(null,
                    ((CarbonAxisConfigurator) axisConfig.getAxisConfiguration().getConfigurator()).getRepoLocation(),
                    serviceCatalogConfiguration, false);
            initialServiceCatalogExecutor.execute(serviceDeployer);
            isServiceCatalogStartupExecutionPending = false;
        }

        // Execution of Service catalog Deployer at each CApp hot deployment
        if (serviceCatalogConfiguration != null && !faultyCapps.contains(cAppName) &&
                !ServiceCatalogUtils.isServerInStartupMode()) {
            ServiceCatalogDeployer serviceDeployer = new ServiceCatalogDeployer(cAppName,
                    ((CarbonAxisConfigurator) axisConfig.getAxisConfiguration().getConfigurator()).getRepoLocation(),
                    serviceCatalogConfiguration, true);
            serviceCatalogExecutor.execute(serviceDeployer);
        }
    }

    public void deployCarbonAppsDirectory() {

        File cAppDirectory = new File(this.appsDir);
        File[] cAppFiles = cAppDirectory.listFiles();
        if (cAppFiles.length == 0) {
            return;
        }
        for (File cAppFile : cAppFiles) {
            if (cAppFile.isDirectory()) {
                try {
                    deployApps(cAppFile.getAbsolutePath());
                } catch (CarbonException e) {
                    log.error("Error while deploying carbon application " + cAppFile.getAbsolutePath(), e);
                }
            }
        }
    }

    /**
     * Returns true when the {@code server.enable_priority_deployment} key in
     * deployment.toml is present and set to {@code true}. Defaults to false when absent.
     */
    private boolean isCAppPriorityDeploymentEnabled() {
        Object value = ConfigParser.getParsedConfigs().get(PRIORITY_DEPLOYMENT_CONFIG_KEY);
        return value != null && Boolean.parseBoolean(value.toString());
    }

    /**
     * Returns the maximum number of retry passes to perform after the high-priority phase.
     * Read from {@code server.priority_deployment_retry_count} in deployment.toml.
     * Defaults to {@code 1} when the key is absent or the value is not a valid integer.
     * Negative values are treated as {@code 0} (no retries).
     */
    private int getMaxRetryCount() {
        Object value = ConfigParser.getParsedConfigs().get(PRIORITY_DEPLOYMENT_RETRY_COUNT_CONFIG_KEY);
        if (value == null) {
            return 1;
        }
        try {
            return Math.max(0, Integer.parseInt(value.toString()));
        } catch (NumberFormatException e) {
            log.warn("Invalid value for '" + PRIORITY_DEPLOYMENT_RETRY_COUNT_CONFIG_KEY + "': " + value
                    + ". Using default retry count of 1.");
            return 1;
        }
    }

    /**
     * Retries deployment of all CApps that failed during the high-priority phase.
     * Executes up to {@link #getMaxRetryCount()} passes; exits early when no faults remain.
     * Faulty CApp lists are snapshotted and cleared before each pass so each CApp gets
     * a clean attempt — on success it lands in {@code cAppMap}, on failure it is
     * re-added to the faulty lists and considered in the next pass.
     */
    private void retryFaultyCApps() {
        final int maxRetryCount = getMaxRetryCount();
        while (retryPassCount < maxRetryCount && !faultyCapps.isEmpty()) {
            retryPassCount++;
            List<CarbonApplication> appsToRetry = new ArrayList<>(faultyCAppObjects);
            List<String> toRetry = new ArrayList<>(faultyCapps);
            faultyCAppObjects.clear();
            faultyCapps.clear();
            log.info("Retry pass " + retryPassCount + " of " + maxRetryCount
                    + ": Retrying deployment of " + toRetry.size() + " failed CApp(s): " + toRetry);
            for (int i = 0; i < toRetry.size(); i++) {
                CarbonApplication faultyApp = i < appsToRetry.size() ? appsToRetry.get(i) : null;
                String artifactPath = faultyApp != null ? faultyApp.getAppFilePath()
                        : cAppDir + File.separator + toRetry.get(i);
                boolean isEmbedded = faultyApp != null && faultyApp.isEmbeddedCAR();
                try {
                    deployCarbonApps(artifactPath, isEmbedded);
                } catch (Exception e) {
                    log.error("Error while retrying deployment of carbon application: " + artifactPath, e);
                }
            }
        }
    }

    /**
     * Extracts the carbon application to the tmp/carbonapps directory.
     *
     * @param targetCAppPath - path of the carbon application
     * @return - path to the extracted carbon application
     * @throws CarbonException - error while extracting
     */
    private String extractCarbonApplication(String targetCAppPath) throws CarbonException {

        return AppDeployerUtils.extractCarbonApp(targetCAppPath);
    }

    private void handleDeployException(Exception e, String cAppName, CarbonApplication currentApp, boolean isEmbeddedCAR) {
        log.error("Error occurred while deploying the Carbon application: " + cAppName
                + ". Reverting successfully deployed artifacts in the CApp.", e);
        undeployCarbonApp(currentApp, axisConfig);
        // Validate synapse config to remove half added swagger definitions in the case of a faulty CAPP.
        SynapseConfigUtils.getSynapseConfiguration(SUPER_TENANT_DOMAIN_NAME).validateSwaggerTable();
        currentApp.setErrorMessage(e.getMessage());
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        currentApp.setFaultStackTrace(sw.toString());
        currentApp.setEmbeddedCAR(isEmbeddedCAR);
        faultyCAppObjects.add(currentApp);
        faultyCapps.add(cAppName);
    }

    /**
     * Builds the carbon application from app configuration created using the artifacts.xml path.
     *
     * @param cappPath             - path of the carbon application
     * @param extractedCAppDirectory - path to extracted carbon application
     * @param cAppName             - name of the carbon application
     * @param axisConfig           - AxisConfiguration instance
     * @return - CarbonApplication instance if successfull. otherwise null..
     * @throws CarbonException - error while building
     */
    private CarbonApplication buildCarbonApplication(String cappPath, String extractedCAppDirectory, String cAppName,
                                                     AxisConfiguration axisConfig) throws CarbonException {

        // Build the app configuration by providing the artifacts.xml path
        ApplicationConfiguration appConfig = new ApplicationConfiguration(extractedCAppDirectory);

        // If we don't have features (artifacts) for this server, ignore
        if (appConfig.getApplicationArtifact().getDependencies().isEmpty()) {
            log.warn("No artifacts found to be deployed in this server. " +
                    "Ignoring Carbon Application : " + cAppName);
            return null;
        }

        CarbonApplication carbonApplication = new CarbonApplication();
        carbonApplication.setAppFilePath(cappPath);
        carbonApplication.setExtractedPath(extractedCAppDirectory);
        carbonApplication.setAppConfig(appConfig);

        // Set App Name
        String appName = appConfig.getAppName();
        if (appName == null) {
            log.warn("No application name found in Carbon Application : " + cAppName + ". Using " +
                    "the file name as the application name");
            appName = cAppName.substring(0, cAppName.lastIndexOf('.'));
        }
        // to support multiple capp versions, we check app name with version
        if (appExists(appConfig.getAppNameWithVersion(), axisConfig)) {
            String msg =
                    "Carbon Application : " + appConfig.getAppNameWithVersion() + " already exists. Two applications " +
                            "can't have the same Id. Deployment aborted.";
            log.error(msg);
            throw new CarbonException(msg);
        }
        carbonApplication.setAppName(appName);

        // Set App Version
        String appVersion = appConfig.getAppVersion();
        if (appVersion != null && !("").equals(appVersion)) {
            carbonApplication.setAppVersion(appVersion);
        }
        String mainSeq = appConfig.getMainSequence();
        if (mainSeq != null && !("").equals(mainSeq)) {
            carbonApplication.setMainSequence(mainSeq);
        }
        return carbonApplication;
    }

    /**
     * Check whether there is an already existing Carbon application with the given name. Use app name with version to
     * support multiple capp versions
     *
     * @param newAppNameWithVersion - name of the new app
     * @param axisConfig            - AxisConfiguration instance
     * @return - true if exits
     */
    private boolean appExists(String newAppNameWithVersion, AxisConfiguration axisConfig) {
        CarbonApplication appToRemove = null;
        for (CarbonApplication carbonApp : getCarbonApps()) {
            if (newAppNameWithVersion.equals(carbonApp.getAppNameWithVersion())) {
                if (carbonApp.isDeploymentCompleted()) {
                    return true;
                } else {
                    appToRemove = carbonApp;
                    break;
                }
            }
        }
        if (appToRemove != null) {
            undeployCarbonApp(appToRemove, axisConfig);
        }
        return false;
    }

    /**
     * Deletes a directory given it's path.
     *
     * @param path the path of the directory to be deleted
     */
    private void deleteExtractedCApp(String path) {
        try {
            FileUtils.deleteDirectory(new File(path));
        } catch (IOException e) {
            log.warn("Unable to locate: " + path);
        }
    }

    /**
     * Add a new carbon application to cAppMap.
     *
     * @param carbonApp - CarbonApplication instance
     */
    private void addCarbonApp(CarbonApplication carbonApp) {
        synchronized (lock) {
            cAppMap.add(carbonApp);
        }
    }

    /**
     * Get the list of CarbonApplications. If the list is null, return an empty ArrayList.
     *
     * @return - list of cApps
     */
    public static List<CarbonApplication> getCarbonApps() {
        return Collections.unmodifiableList(cAppMap);
    }

    public static CarbonApplication getCarbonAppByName(String cAppName) {
        for (CarbonApplication capp : cAppMap) {
            if (cAppName.equals(capp.getAppName())) {
                return capp;
            }
        }
        return null;
    }

    public static CarbonApplication getCarbonAppByFullyQualifiedName(String cAppName) {
        for (CarbonApplication capp : cAppMap) {
            if (cAppName.equals(capp.getAppNameWithVersion())) {
                return capp;
            }
        }
        return null;
    }

    /**
     * Checks whether a given file is a jar or an aar file.
     *
     * @param filename file to check
     * @return Returns boolean.
     */
    private boolean isCAppArchiveFile(String filename) {
        return (filename.endsWith(".car"));
    }

    /**
     * Function to register application deployers.
     *
     * @param handler - app deployer which implements the AppDeploymentHandler interface
     */
    public synchronized void registerDeploymentHandler(AppDeploymentHandler handler) {
        appDeploymentHandlers.add(handler);
    }

    /**
     * Deploys all artifacts under a root artifact.
     *
     * @param rootDirPath - root dir of the extracted artifact
     * @param parentApp   - capp instance
     * @throws org.wso2.micro.core.util.CarbonException - on error
     */
    private void searchArtifacts(String rootDirPath, CarbonApplication parentApp) throws CarbonException {
        SynapseConfiguration synapseConfiguration =
                SynapseConfigUtils.getSynapseConfiguration(SUPER_TENANT_DOMAIN_NAME);
        // For each CAPP initiate again.
        Map<String, String> swaggerTable = new HashMap<String, String>();
        Map<String, String> apiArtifactMap = new HashMap<String, String>();
        File extractedDir = new File(rootDirPath);
        File[] allFiles = extractedDir.listFiles();
        if (allFiles == null) {
            return;
        }

        // list to keep all artifacts
        List<Artifact> allArtifacts = new ArrayList<Artifact>();

        // search for all directories under the extracted path
        for (File artifactDirectory : allFiles) {
            if (!artifactDirectory.isDirectory()) {
                continue;
            }

            String directoryPath = AppDeployerUtils.formatPath(artifactDirectory.getAbsolutePath());
            String artifactXmlPath = directoryPath + File.separator + Artifact.ARTIFACT_XML;

            File f = new File(artifactXmlPath);
            // if the artifact.xml not found, ignore this dir
            if (!f.exists()) {
                // Add swagger files to the synapse configuration context.
                if (directoryPath.endsWith(METADATA_FOLDER_NAME)) {
                    File[] metadataFiles = new File(directoryPath).listFiles();
                    for (File metaFile : metadataFiles) {
                        if (metaFile.isDirectory()) {
                            try {
                                InputStream xmlInputStream =
                                        new FileInputStream(new File(metaFile, ARTIFACT_FILE));
                                Artifact artifact = this.buildAppArtifact(parentApp, xmlInputStream);
                                Artifact parentArtifact = parentApp.getAppConfig().getApplicationArtifact();
                                // Removing metadata dependencies from the CAPP parent artifact
                                boolean removed =
                                        parentArtifact.getDependencies()
                                                .removeIf(c -> c.getName().equals(artifact.getName()));
                                if (removed)
                                    parentArtifact.unresolvedDepCount--;

                                if (metaFile.getName().contains(SWAGGER_SUBSTRING)) {
                                    File swaggerFile = new File(metaFile, artifact.getFiles().get(0).getName());
                                    byte[] bytes = Files.readAllBytes(Paths.get(swaggerFile.getPath()));
                                    String artifactName = artifact.getName()
                                            .substring(0, artifact.getName().indexOf(SWAGGER_SUBSTRING));
                                    if (SynapsePropertiesLoader.getBooleanProperty(SynapseConstants.EXPOSE_VERSIONED_SERVICES, false)
                                            && parentApp.getAppConfig().isVersionedDeployment()) {
                                        artifactName = artifact.getFullyQualifiedName()
                                                .substring(0, artifact.getFullyQualifiedName().indexOf(SWAGGER_SUBSTRING));
                                    }
                                    swaggerTable.put(artifactName, new String(bytes));
                                }
                            } catch (FileNotFoundException e) {
                                log.error("Could not find the Artifact.xml file for the metadata", e);
                            } catch (IOException e) {
                                log.error("Error occurred while reading the swagger file from metadata", e);
                            }
                        }
                    }
                }
                continue;
            }

            Artifact artifact = null;
            InputStream xmlInputStream = null;
            try {
                xmlInputStream = new FileInputStream(f);
                artifact = this.buildAppArtifact(parentApp, xmlInputStream);
                // If artifact is an API, add apiMapping to the synapse configuration.
                if (artifact.getType().equals(API_TYPE)) {
                    String apiXmlPath = directoryPath + File.separator + artifact.getFiles().get(0).getName();
                    String apiName = getApiNameFromFile(new FileInputStream(apiXmlPath));
                    if (!StringUtils.isEmpty(apiName)) {
                        // Re-constructing swagger table with API name since artifact name is not unique
                        if (SynapsePropertiesLoader.getBooleanProperty(SynapseConstants.EXPOSE_VERSIONED_SERVICES, false)
                                && parentApp.getAppConfig().isVersionedDeployment()) {
                            apiName = parentApp.getAppConfig().getAppArtifactIdentifier() + Constants.DOUBLE_UNDERSCORE + apiName;
                            apiArtifactMap.put(artifact.getFullyQualifiedName(), apiName);
                        } else {
                            apiArtifactMap.put(artifact.getName(),apiName);
                        }
                    }
                }
            } catch (FileNotFoundException e) {
                handleException("artifacts.xml File cannot be loaded from " + artifactXmlPath, e);
            } finally {
                if (xmlInputStream != null) {
                    try {
                        xmlInputStream.close();
                    } catch (IOException e) {
                        log.error("Error while closing input stream.", e);
                    }
                }
            }

            if (artifact == null) {
                return;
            }
            artifact.setExtractedPath(directoryPath);
            allArtifacts.add(artifact);
        }
        Artifact appArtifact = parentApp.getAppConfig().getApplicationArtifact();
        buildDependencyTree(appArtifact, allArtifacts);
        for (String artifactName : swaggerTable.keySet()) {
            String apiname = apiArtifactMap.get(artifactName);
            if (!StringUtils.isEmpty(apiname)) {
                synapseConfiguration.addSwaggerDefinition(apiname, swaggerTable.get(artifactName),
                        parentApp.getAppConfig().isVersionedDeployment());
            }
        }
    }

    /**
     * Builds the artifact from the given input steam. Then adds it as a dependency in the provided parent carbon
     * application.
     *
     * @param parentApp         - parent application
     * @param artifactXmlStream - xml input stream of the artifact.xml
     * @return - Artifact instance if successfull. otherwise null..
     * @throws CarbonException - error while building
     */
    private Artifact buildAppArtifact(CarbonApplication parentApp, InputStream artifactXmlStream)
            throws CarbonException {
        Artifact artifact = null;
        try {
            OMElement artElement = new StAXOMBuilder(artifactXmlStream).getDocumentElement();

            if (Artifact.ARTIFACT.equals(artElement.getLocalName())) {
                artifact = AppDeployerUtils.populateArtifact(parentApp, artElement);
            } else {
                log.error("artifact.xml is invalid. Parent Application : "
                        + parentApp.getAppNameWithVersion());
                return null;
            }
        } catch (XMLStreamException e) {
            handleException("Error while parsing the artifact.xml file ", e);
        }

        if (artifact == null || artifact.getName() == null) {
            log.error("Invalid artifact found in Carbon Application : " + parentApp.getAppNameWithVersion());
            return null;
        }
        return artifact;
    }

    /**
     * Checks whether the given cApp artifact is complete with all it's dependencies. Recursively checks all it's
     * dependent artifacts as well..
     *
     * @param rootArtifact - artifact to check
     * @return true if ready, else false
     */
    private boolean isArtifactReadyToDeploy(Artifact rootArtifact) {
        if (rootArtifact == null) {
            return false;
        }
        boolean isReady = true;
        for (Artifact.Dependency dep : rootArtifact.getDependencies()) {
            isReady = isArtifactReadyToDeploy(dep.getArtifact());
            if (!isReady) {
                return false;
            }
        }
        if (rootArtifact.unresolvedDepCount > 0) {
            isReady = false;
        }
        return isReady;
    }

    /**
     * If the given artifact is a dependent artifact for the rootArtifact, include it as the actual dependency. The
     * existing one is a dummy one. So remove it. Do this recursively for the dependent artifacts as well..
     *
     * @param rootArtifact - root to start search
     * @param allArtifacts - all artifacts found under current cApp
     */
    private void buildDependencyTree(Artifact rootArtifact, List<Artifact> allArtifacts) {
        for (Artifact.Dependency dep : rootArtifact.getDependencies()) {
            for (Artifact temp : allArtifacts) {
                if (dep.getName().equals(temp.getName())) {
                    String depVersion = dep.getVersion();
                    String attVersion = temp.getVersion();
                    if ((depVersion == null && attVersion == null) ||
                            (depVersion != null && depVersion.equals(attVersion))) {
                        dep.setArtifact(temp);
                        rootArtifact.unresolvedDepCount--;
                        break;
                    }
                }
            }

            // if we've found the dependency, check for it's dependencies as well..
            if (dep.getArtifact() != null) {
                buildDependencyTree(dep.getArtifact(), allArtifacts);
            }
        }
    }

    private void handleException(String msg, Exception e) throws CarbonException {
        log.error(msg, e);
        throw new CarbonException(msg, e);
    }

    /**
     * Undeploys the cApp from system when the .car file is deleted from the repository. Find the relevant cApp using
     * the file path and call the undeploy method on applicationManager.
     *
     * @param filePath - deleted .car file path
     * @throws DeploymentException - error while un-deploying cApp
     */
    public void undeploy(String filePath) throws DeploymentException {
        CarbonApplication existingApp = null;
        for (CarbonApplication carbonApp : getCarbonApps()) {
            if (filePath.equals(carbonApp.getAppFilePath())) {
                existingApp = carbonApp;
                break;
            }
        }
        if (existingApp != null) {
            undeployCarbonApp(existingApp, axisConfig);
            if (existingApp.getAppConfig().isFatCAR()) {
                for (String dependencyIdentifier : existingApp.getAppConfig().getCAppDependencies().keySet()) {
                    String dependencyName = dependencyIdentifier + Constants.DOUBLE_UNDERSCORE + existingApp.getAppConfig().getCAppDependencies().get(dependencyIdentifier);
                    CarbonApplication dependentCAR = getCarbonAppByFullyQualifiedName(dependencyName);
                    if (dependentCAR != null) {
                        undeploy(dependentCAR.getAppFilePath());
                    }
                }

            }
        } else {
            log.info("Undeploying Faulty Carbon Application On : " + filePath);
            removeFaultyCarbonApp(filePath);
        }
        super.undeploy(filePath);
    }

    /**
     * Undeploy the provided carbon App by sending it through the registered undeployment handler chain.
     *
     * @param carbonApp  - CarbonApplication instance
     * @param axisConfig - AxisConfiguration of the current tenant
     */
    private void undeployCarbonApp(CarbonApplication carbonApp,
                                   AxisConfiguration axisConfig) {
        log.info("Undeploying Carbon Application : " + carbonApp.getAppNameWithVersion() + "...");
        // Call the undeployer handler chain
        try {
            for (int handlerIndex = appDeploymentHandlers.size() - 1; handlerIndex >= 0; handlerIndex--) {
                AppDeploymentHandler handler = appDeploymentHandlers.get(handlerIndex);
                handler.undeployArtifacts(carbonApp, axisConfig);
            }
            // Remove the app from cAppMap list
            removeCarbonApp(carbonApp);

            // Remove the app from registry
            // removing the extracted CApp form tmp/carbonapps/
            FileManipulator.deleteDir(carbonApp.getExtractedPath());
            log.info("Successfully undeployed Carbon Application : " + carbonApp.getAppNameWithVersion()
                    + AppDeployerUtils.getTenantIdLogString(AppDeployerUtils.getTenantId()));
        } catch (Exception e) {
            log.error("Error occurred while trying to unDeploy  : " + carbonApp.getAppNameWithVersion(), e);
        }
    }

    /**
     * Remove a carbon application from cAppMap.
     *
     * @param carbonApp - CarbonApplication instance
     */
    private void removeCarbonApp(CarbonApplication carbonApp) {
        synchronized (lock) {
            cAppMap.remove(carbonApp);
        }
    }

    /**
     * Remove a faulty cApp from faultyCapps.
     *
     * @param appFilePath - file path to faulty carbon application
     */
    void removeFaultyCarbonApp(String appFilePath) {
        synchronized (lock) {
            String cAppName = appFilePath.substring(appFilePath.lastIndexOf(File.separator) + 1);
            faultyCapps.remove(cAppName);
            for (CarbonApplication application : faultyCAppObjects) {
                if (application.getAppFilePath().equals(appFilePath)) {
                    faultyCAppObjects.remove(application);
                    break;
                }
            }
        }
    }

    /**
     * Get a list of faulty CAPPs in the server.
     *
     * @return list of faulty CAPPs
     */
    public static List<String> getFaultyCapps() {
        return Collections.unmodifiableList(faultyCapps);
    }

    /**
     * Get a list of faulty cApp objects in the server.
     *
     * @return list of faulty cApp objects
     */
    public static List<CarbonApplication> getFaultyCAppObjects() {
        return Collections.unmodifiableList(faultyCAppObjects);
    }

    public void cleanup() {
        //cleanup the capp list during the unload
        cAppMap.clear();
        faultyCapps.clear();
        faultyCAppObjects.clear();
        retryPassCount = 0;
        isRetrying = false;
        highPriorityCAppCount = -1;
    }

    public void setSecretCallbackHandlerService(SecretCallbackHandlerService secretCallbackHandlerService) {
        this.secretCallbackHandlerService = secretCallbackHandlerService;
    }

    /**
     * Partially building the API to get the API name
     *
     * @param apiXmlStream input stream of the API file.
     * @return name of the API.
     */
    private String getApiNameFromFile(InputStream apiXmlStream) {

        try {
            OMElement apiElement = new StAXOMBuilder(apiXmlStream).getDocumentElement();
            API api = DeployerUtil.partiallyBuildAPI(apiElement);
            return api.getName();
        } catch (XMLStreamException | OMException e) {
            // Cannot find the API file or API is faulty.
            // This error is properly handled later in the deployers and CAPP will go faulty.
            // Hence the exception is not propagated from here.
            return null;
        }
    }

    /**
     * Sorts the sub-range [startIndex, toIndex) of filesToDeploy.
     *
     * <p>Delegates to {@link #sortByPriority} when {@code server.enable_priority_deployment} is
     * {@code true}, otherwise to {@link #sortByDependencyOrderWithFallback}.
     *
     * @param filesToDeploy - list of all deployment file data
     * @param startIndex    - start index (inclusive) of the range to sort
     * @param toIndex       - end index (exclusive) of the range to sort
     */
    @Override
    public void sort(List<DeploymentFileData> filesToDeploy, int startIndex, int toIndex) {
        if (isCAppPriorityDeploymentEnabled()) {
            sortByPriority(filesToDeploy, startIndex, toIndex);
        } else {
            sortByDependencyOrderWithFallback(filesToDeploy, startIndex, toIndex);
        }
    }

    /**
     * Sorts the sub-range [startIndex, toIndex) of filesToDeploy with priority-based ordering.
     *
     * <p>Each .car file is inspected to determine whether it contains any high-priority artifact
     * type ({@code lib/synapse/mediator}, {@code synapse/lib}, {@code registry/resource}).
     * High-priority CApps are placed before low-priority ones; within each group the order is
     * alphabetical by file name. {@link #highPriorityCAppCount} is set to the number of
     * high-priority CApps so the retry pass in {@link #deployCarbonApplications} fires at the
     * correct moment.
     *
     * @param filesToDeploy - list of all deployment file data
     * @param startIndex    - start index (inclusive) of the range to sort
     * @param toIndex       - end index (exclusive) of the range to sort
     */
    private void sortByPriority(List<DeploymentFileData> filesToDeploy, int startIndex, int toIndex) {
        Comparator<DeploymentFileData> byFileName =
                (a, b) -> a.getFile().getName().compareTo(b.getFile().getName());
        if (log.isDebugEnabled()) {
            log.debug("Sorting CApp files with priority order in range [" + startIndex + ", " + toIndex + ")");
        }

        List<DeploymentFileData> subList = new ArrayList<>(filesToDeploy.subList(startIndex, toIndex));

        List<DeploymentFileData> highPriorityCApps = new ArrayList<>();
        List<DeploymentFileData> lowPriorityCApps = new ArrayList<>();

        for (DeploymentFileData fileData : subList) {
            File carFile = new File(fileData.getAbsolutePath());
            if (isHighPriorityCApp(carFile)) {
                if (log.isDebugEnabled()) {
                    log.debug("CApp classified as high priority: " + carFile.getName());
                }
                highPriorityCApps.add(fileData);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("CApp classified as low priority: " + carFile.getName());
                }
                lowPriorityCApps.add(fileData);
            }
        }

        highPriorityCAppCount = highPriorityCApps.size();

        highPriorityCApps.sort(byFileName);
        lowPriorityCApps.sort(byFileName);

        if (log.isDebugEnabled()) {
            log.debug("High priority CApps (" + highPriorityCApps.size() + "): " +
                    highPriorityCApps.stream()
                            .map(f -> new File(f.getAbsolutePath()).getName())
                            .collect(Collectors.joining(", ")));
            log.debug("Low priority CApps (" + lowPriorityCApps.size() + "): " +
                    lowPriorityCApps.stream()
                            .map(f -> new File(f.getAbsolutePath()).getName())
                            .collect(Collectors.joining(", ")));
        }

        List<DeploymentFileData> sorted = new ArrayList<>();
        sorted.addAll(highPriorityCApps);
        sorted.addAll(lowPriorityCApps);

        for (int i = 0; i < sorted.size(); i++) {
            filesToDeploy.set(startIndex + i, sorted.get(i));
        }

        if (log.isDebugEnabled()) {
            log.debug("CApp deployment order sorted: " + highPriorityCApps.size() + " high-priority CApp(s) followed by "
                    + lowPriorityCApps.size() + " low-priority CApp(s).");
        }
    }

    /**
     * Attempts to sort the sub-range [startIndex, toIndex) of filesToDeploy by CApp dependency
     * order derived from {@code descriptor.xml} files inside each .car archive.
     *
     * <p>Falls back to alphabetical ordering (via {@code super.sort}) when:
     * <ul>
     *   <li>No CApp has a descriptor.xml</li>
     *   <li>Only some CApps have a descriptor.xml</li>
     *   <li>Duplicate descriptors are detected</li>
     *   <li>The dependency graph cannot be resolved (e.g. cycles or missing artifacts)</li>
     *   <li>The input range is invalid</li>
     * </ul>
     *
     * @param filesToDeploy - list of all deployment file data
     * @param startIndex    - start index (inclusive) of the range to sort
     * @param toIndex       - end index (exclusive) of the range to sort
     */
    private void sortByDependencyOrderWithFallback(List<DeploymentFileData> filesToDeploy, int startIndex, int toIndex) {
        File cAppDirFile = new File(this.cAppDir);
        File[] cAppFiles = cAppDirFile.listFiles((dir, name) -> name.endsWith(CAR_FILE_EXTENSION));

        ArrayList<File> dependentCAppFiles = new ArrayList<>();

        for (File cAppFile : cAppFiles) {
            // Scan for nested CARs under dependencies/
            try (ZipFile zipFile = new ZipFile(cAppFile)) {
                zipFile.stream()
                        .filter(e -> !e.isDirectory())
                        .filter(e -> e.getName().startsWith(AppDeployerUtils.DEPENDENCIES_DIR))
                        .filter(e -> e.getName().toLowerCase(Locale.ROOT).endsWith(".car"))
                        .forEach(entry -> {
                            dependentCAppFiles.add(new File(cAppFile.getAbsolutePath() + File.separator + entry.getName()));
                        });
            } catch (Exception ude) {
                // Since we deploy CApps in alphabetical order when there is any error, this can be ignored.
            }
        }
        cAppFiles = Arrays.copyOf(cAppFiles, cAppFiles.length + dependentCAppFiles.size());
        for (int i = 0; i < dependentCAppFiles.size(); i++) {
            cAppFiles[cAppFiles.length - dependentCAppFiles.size() + i] = dependentCAppFiles.get(i);
        }
        if (cAppFiles == null || filesToDeploy == null || filesToDeploy.isEmpty() || startIndex < 0 ||
                toIndex > filesToDeploy.size() || startIndex >= toIndex) {
            super.sort(filesToDeploy, startIndex, toIndex);
            return;
        }

        int cAppsWithDescriptorCount = getCAppsWithDescriptorCount(cAppFiles);
        if (cAppsWithDescriptorCount == 0) {
            super.sort(filesToDeploy, startIndex, toIndex);
        } else if (cAppsWithDescriptorCount < cAppFiles.length) {
            log.warn(
                    "Some or all CApps are missing descriptor.xml file. Hence, Dependency-based ordering will be " +
                            "skipped, and all CApps will be deployed in alphabetical order.");
            super.sort(filesToDeploy, startIndex, toIndex);
        } else {
            try {
                File[] orderedAllCApps = getCAppProcessingOrder(cAppFiles);

                Map<String, Integer> cAppOrderMap = new HashMap<>();
                for (int i = 0; i < orderedAllCApps.length; i++) {
                    cAppOrderMap.put(orderedAllCApps[i].getName(), i);
                }

                List<DeploymentFileData> subList = filesToDeploy.subList(startIndex, toIndex);
                subList.sort(Comparator.comparingInt(dfd -> {
                    String name = dfd.getFile().getName();
                    return cAppOrderMap.getOrDefault(name, Integer.MAX_VALUE);
                }));
            } catch (DuplicateCAppDescriptorException e) {
                log.warn("Duplicate CApp descriptors found while determining the CApp processing order: " + e.getMessage());
                super.sort(filesToDeploy, startIndex, toIndex);
            } catch (DeploymentException e) {
                log.warn("Unable to determine the CApp processing order based on dependencies. " +
                                "CApps will be deployed in alphabetical order instead. " + e.getMessage());
                super.sort(filesToDeploy, startIndex, toIndex);
            }
        }
    }

    /**
     * Determines whether a .car file is a high-priority CApp by inspecting the {@code type} attribute
     * of each {@code <artifact>} element inside the archive's {@code artifact.xml} files.
     *
     * <p>A CApp is considered high priority if it contains any artifact with one of the types:
     * <ul>
     *   <li>{@code lib/synapse/mediator}  - class mediator</li>
     *   <li>{@code synapse/lib}           - connector</li>
     *   <li>{@code registry/resource}     - registry resource</li>
     * </ul>
     *
     * <p>If {@code carFile} is not a regular file on disk (e.g., a path of the form
     * {@code A.car/dependencies/B.car} representing a .car embedded inside another .car),
     * the check is delegated to {@link #isHighPriorityEmbeddedCApp(File)}.
     *
     * @param carFile the .car file to inspect
     * @return {@code true} if the CApp contains at least one high-priority artifact type, {@code false} otherwise
     */
    private boolean isHighPriorityCApp(File carFile) {
        if (!carFile.isFile()) {
            // Path like /path/A.car/dependencies/B.car — nested car inside another car
            return isHighPriorityEmbeddedCApp(carFile);
        }
        try (ZipInputStream zipIn = new ZipInputStream(new FileInputStream(carFile))) {
            return containsHighPriorityArtifact(zipIn, carFile.getName());
        } catch (IOException e) {
            log.warn("Error reading CApp file: " + carFile.getName() + ". Treating as low priority.", e);
        }
        return false;
    }

    /**
     * Determines whether a .car file embedded inside another .car is a high-priority CApp.
     *
     * <p>Handles virtual paths of the form {@code /path/to/A.car/dependencies/B.car}, where
     * {@code B.car} is a zip entry inside {@code A.car} rather than a standalone file on disk.
     * The outer archive is opened, the inner entry is located by name, and its contents are
     * scanned via {@link #containsHighPriorityArtifact(ZipInputStream, String)}.
     *
     * @param embeddedCarFile a {@link File} whose path encodes the outer .car and the inner entry name
     * @return {@code true} if the embedded CApp contains at least one high-priority artifact type,
     *         {@code false} if the entry cannot be resolved or contains no high-priority artifacts
     */
    private boolean isHighPriorityEmbeddedCApp(File embeddedCarFile) {
        String absPath = embeddedCarFile.getAbsolutePath();
        // Outer car ends at the first ".car" segment; remainder is the entry path inside it.
        String marker = ".car" + File.separator;
        int markerIdx = absPath.indexOf(marker);
        if (markerIdx < 0) {
            log.warn("CApp not found and path does not match nested pattern: " + absPath + ". Treating as low priority.");
            return false;
        }
        File outerCar = new File(absPath.substring(0, markerIdx + 4)); // +4 for ".car"
        // Zip entries always use '/' regardless of OS separator
        String innerEntryName = absPath.substring(markerIdx + marker.length()).replace(File.separatorChar, '/');

        if (!outerCar.isFile()) {
            log.warn("Outer CApp not found: " + outerCar.getAbsolutePath() + ". Treating as low priority.");
            return false;
        }
        try (ZipFile outerZip = new ZipFile(outerCar)) {
            ZipEntry innerCarEntry = outerZip.getEntry(innerEntryName);
            if (innerCarEntry == null) {
                log.warn("Nested CApp entry '" + innerEntryName + "' not found in: "
                        + outerCar.getName() + ". Treating as low priority.");
                return false;
            }
            try (InputStream innerCarStream = outerZip.getInputStream(innerCarEntry);
                 ZipInputStream innerZip = new ZipInputStream(innerCarStream)) {
                return containsHighPriorityArtifact(innerZip, embeddedCarFile.getName());
            }
        } catch (IOException e) {
            log.warn("Error reading nested CApp: " + absPath + ". Treating as low priority.", e);
        }
        return false;
    }

    /**
     * Scans a {@link ZipInputStream} sequentially for {@code artifact.xml} entries and returns
     * {@code true} as soon as one declares a high-priority artifact type.
     *
     * <p>Only entries whose name ends with {@code /<artifact-dir>/artifact.xml} are inspected;
     * the top-level {@code artifacts.xml} descriptor is excluded by the leading {@code "/"} check.
     *
     * @param zipIn    the zip stream to scan; the caller is responsible for closing it
     * @param cappName the CApp name used in log messages
     * @return {@code true} if a high-priority artifact type is found, {@code false} otherwise
     * @throws IOException if the stream cannot be read
     */
    private boolean containsHighPriorityArtifact(ZipInputStream zipIn, String cappName) throws IOException {
        ZipEntry entry;
        while ((entry = zipIn.getNextEntry()) != null) {
            // Look only for artifact.xml files inside artifact directories (e.g., <artifact-dir>/artifact.xml).
            // The top-level descriptor is named "artifacts.xml" so the "/" prefix check correctly excludes it.
            if (!entry.isDirectory() && entry.getName().endsWith("/" + ARTIFACT_FILE)) {
                try {
                    OMElement artElement = secureXmlBuilder(zipIn).getDocumentElement();
                    if (Artifact.ARTIFACT.equals(artElement.getLocalName())) {
                        String artifactType = artElement.getAttributeValue(new QName(Artifact.TYPE));
                        if (SYNAPSE_LIBRARY_TYPE.equals(artifactType) && HTTP_CONNECTOR_NAME.equals(
                                artElement.getAttributeValue(new QName(Artifact.NAME)))) {
                            continue;
                        }
                        if (artifactType != null && HIGH_PRIORITY_TYPES.contains(artifactType)) {
                            if (log.isDebugEnabled()) {
                                log.debug("Found high-priority artifact type '" + artifactType
                                        + "' in CApp: " + cappName
                                        + " [entry: " + entry.getName() + "]");
                            }
                            return true;
                        }
                    }
                } catch (XMLStreamException | OMException e) {
                    log.warn("Error parsing artifact.xml entry '" + entry.getName()
                            + "' in CApp: " + cappName + ". Skipping entry.", e);
                }
            }
            zipIn.closeEntry();
        }
        return false;
    }

    /**
     * Creates a {@link StAXOMBuilder} for the given input stream using a hardened StAX parser
     * with DTD support and external entity resolution disabled to prevent XXE attacks.
     *
     * @param in the input stream containing XML content
     * @return a {@link StAXOMBuilder} ready to parse the document
     * @throws XMLStreamException if the input stream cannot be read as valid XML
     */
    private static StAXOMBuilder secureXmlBuilder(InputStream in) throws XMLStreamException {
        return new StAXOMBuilder(SECURE_XML_INPUT_FACTORY.createXMLStreamReader(in));
    }

    /**
     * Retrieves a list of Carbon Application (CApp) files from the CApps directory.
     *
     * This method scans the CApps folder within the Carbon repository location
     * and returns all files with the ".car" extension (Carbon Archive files).
     *
     * @return an array of File objects representing all .car files found in the
     *         CApps directory.
     */
    private File[] getCAppFileList() {
        FilenameFilter CAPP_FILTER = (f, name) -> name.endsWith(".car");

        File cappFolder = new File(((CarbonAxisConfigurator) axisConfig.getAxisConfiguration().getConfigurator())
                .getRepoLocation(), CAPP_FOLDER_NAME);
        return cappFolder.listFiles(CAPP_FILTER);
    }
}

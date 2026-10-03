/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.micro.integrator.initializer;

import java.util.Map;
import javax.xml.stream.XMLInputFactory;
import org.apache.axiom.om.util.StAXUtils;
import org.apache.axis2.AxisFault;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.description.AxisServiceGroup;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.axis2.engine.Phase;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.AbstractExtendedSynapseHandler;
import org.apache.synapse.MessageContext;
import org.apache.synapse.ServerConfigurationInformation;
import org.apache.synapse.ServerConfigurationInformationFactory;
import org.apache.synapse.ServerContextInformation;
import org.apache.synapse.ServerManager;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.SynapseHandler;
import org.apache.synapse.api.API;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.ProxyService;
import org.apache.synapse.debug.SynapseDebugInterface;
import org.apache.synapse.debug.SynapseDebugManager;
import org.apache.synapse.inbound.InboundEndpoint;
import org.apache.synapse.mediators.base.SequenceMediator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.wso2.carbon.securevault.SecretCallbackHandlerService;
import org.wso2.config.mapper.ConfigParser;
import org.wso2.micro.application.deployer.CarbonApplication;
import org.wso2.micro.core.Constants;
import org.wso2.micro.core.ServerShutdownHandler;
import org.wso2.integration.transaction.counter.TransactionCountHandler;
import org.wso2.micro.integrator.core.services.Axis2ConfigurationContextService;
import org.wso2.micro.integrator.core.services.CarbonServerConfigurationService;
import org.wso2.micro.integrator.core.util.MicroIntegratorBaseUtils;
import org.wso2.micro.integrator.initializer.deployment.application.deployer.CappDeployer;
import org.wso2.micro.integrator.initializer.handler.ProxyLogHandler;
import org.wso2.micro.integrator.initializer.handler.SynapseExternalPropertyConfigurator;
import org.wso2.micro.integrator.initializer.persistence.MediationPersistenceManager;
import org.wso2.micro.integrator.initializer.services.SynapseConfigurationService;
import org.wso2.micro.integrator.initializer.services.SynapseConfigurationServiceImpl;
import org.wso2.micro.integrator.initializer.services.SynapseEnvironmentService;
import org.wso2.micro.integrator.initializer.services.SynapseEnvironmentServiceImpl;
import org.wso2.micro.integrator.initializer.services.SynapseRegistrationsService;
import org.wso2.micro.integrator.initializer.services.SynapseRegistrationsServiceImpl;
import org.wso2.micro.integrator.initializer.utils.ConfigurationHolder;
import org.wso2.micro.integrator.initializer.utils.SynapseArtifactInitUtils;
import org.wso2.micro.integrator.ndatasource.core.DataSourceService;
import org.wso2.micro.integrator.ntask.core.service.TaskService;
import org.wso2.micro.integrator.observability.metric.handler.DSMetricHandler;
import org.wso2.micro.integrator.observability.metric.handler.MetricHandler;
import org.wso2.securevault.SecurityConstants;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@SuppressWarnings({"JavaDoc", "UnusedDeclaration"})
@Component(name = "org.wso2.micro.integrator.initializer.ServiceBusInitializer", immediate = true)
public class ServiceBusInitializer {

    private static final Log log = LogFactory.getLog(ServiceBusInitializer.class);

    // Artifact type reported by InboundEndpointDeployer on deployment
    private static final String INBOUND_ENDPOINT_ARTIFACT_TYPE = "inbound-endpoint";

    // private static RegistryService registryService;
    private static ServerConfigurationInformation configurationInformation;

    private static String configPath;

    private Axis2ConfigurationContextService configCtxSvc;

    // private SynapseRegistryService synRegSvc;
    // private DataSourceInformationRepositoryService dataSourceInformationRepositoryService;

    private SecretCallbackHandlerService secretCallbackHandlerService;

    private ServerManager serverManager;

    private TaskService taskService;

    private DataSourceService dataSourceService;

    @Activate
    protected void activate(ComponentContext ctxt) {

        if (log.isDebugEnabled()) {
            log.debug(ServiceBusInitializer.class.getName() + "#activate() BEGIN - " + System.currentTimeMillis());
        }
        log.debug("Activating Micro Integrator...");

        // Check XMLInputFactory properties for XXE safe configuration
        checkInputFactoryProperties();

        if (taskService != null && !taskService.isServerInit()) {
            log.debug("Initialize Task Service");
            taskService.serverInitialized();
        }

        // clean up temp folder created for connector class loader reference
        String javaTempDir = System.getProperty("java.io.tmpdir");
        String APP_UNZIP_DIR = javaTempDir.endsWith(File.separator) ? javaTempDir + "libs" : javaTempDir + File
                .separator + "libs";
        cleanupTempDirectory(APP_UNZIP_DIR);
        try {
            BundleContext bndCtx = ctxt.getBundleContext();
            ConfigurationHolder.getInstance().setBundleContext(bndCtx);
            // initialize the lock
            Lock lock = new ReentrantLock();
            configCtxSvc.getServerConfigContext().getAxisConfiguration().addParameter(ServiceBusConstants
                    .SYNAPSE_CONFIG_LOCK, lock);
            // first check which configuration should be active
            // UserRegistry registry = registryService.getConfigSystemRegistry();
            // init the multiple configuration tracker
            //ConfigurationManager configurationManager = new ConfigurationManager(configCtxSvc.getServerConfigContext());
            //configurationManager.init();
            // set the event broker as a property
            /*
            if (eventBroker != null) {
                configCtxSvc.getServerConfigContext().setProperty("mediation.event.broker", eventBroker);
            }
*/
            // Initialize Synapse
            ServerContextInformation contextInfo = initESB("default");
            ServiceRegistration synCfgRegistration = null;
            ServiceRegistration synEnvRegistration = null;
            if (contextInfo.getSynapseConfiguration() != null) {
                // Properties props = new Properties();
                SynapseConfigurationService synCfgSvc = new SynapseConfigurationServiceImpl(contextInfo
                        .getSynapseConfiguration(), Constants.SUPER_TENANT_ID, configCtxSvc
                        .getServerConfigContext());
                // Update ConfigurationHolder before registering the service
                ConfigurationHolder.getInstance().setSynapseConfigurationService(synCfgSvc);
                synCfgRegistration = bndCtx.registerService(SynapseConfigurationService.class.getName(), synCfgSvc,
                        null);

                bndCtx.registerService(ServerShutdownHandler.class.getName(), new MPMShutdownHandler(synCfgSvc
                        .getSynapseConfiguration().getAxisConfiguration()), null);
                initPersistence(synCfgSvc, "default");
                if (log.isDebugEnabled()) {
                    log.debug("SynapseConfigurationService Registered");
                }
            } else {
                handleFatal("Couldn't register the SynapseConfigurationService, " + "SynapseConfiguration not found");
            }
            SynapseEnvironment synapseEnvironment = contextInfo.getSynapseEnvironment();
            List handlers = synapseEnvironment.getSynapseHandlers();
            String prometheusApiEnabled = System.getProperty(ServiceBusConstants.ENABLE_PROMETHEUS_API_PROPERTY);
            if ("false".equals(prometheusApiEnabled)) {
                handlers.remove(handlers.stream().filter(c -> c instanceof MetricHandler).findFirst().orElse(null));
            } else if ("true".equals(prometheusApiEnabled)) {
                if (!handlers.stream().anyMatch(c -> c instanceof MetricHandler)) {
                    handlers.add(new MetricHandler());
                }
                AxisConfiguration axisConfig = configCtxSvc.getServerConfigContext().getAxisConfiguration();
                for (Phase inPhase : axisConfig.getInFlowPhases()) {
                    if (ServiceBusConstants.DISPATCH_PHASE_NAME.equals(inPhase.getPhaseName())) {
                        if (!inPhase.getHandlers().stream().anyMatch(c -> c instanceof DSMetricHandler)) {
                            inPhase.addHandler(new DSMetricHandler());
                        }
                    }
                }
                for (Phase outPhase : axisConfig.getOutFlowPhases()) {
                    if (ServiceBusConstants.MESSAGE_OUT_PHASE_NAME.equals(outPhase.getPhaseName())) {
                        if (!outPhase.getHandlers().stream().anyMatch(c -> c instanceof DSMetricHandler)) {
                            outPhase.addHandler(new DSMetricHandler());
                        }
                    }
                }
                for (Phase faultPhase : axisConfig.getOutFaultFlowPhases()) {
                    if (ServiceBusConstants.MESSAGE_OUT_PHASE_NAME.equals(faultPhase.getPhaseName())) {
                        if (!faultPhase.getHandlers().stream().anyMatch(c -> c instanceof DSMetricHandler)) {
                            faultPhase.addHandler(new DSMetricHandler());
                        }
                    }
                }
            }
            Iterator<SynapseHandler> iterator = handlers.iterator();
            while (iterator.hasNext()) {
                SynapseHandler handler = iterator.next();
                if ((handler instanceof AbstractExtendedSynapseHandler)) {
                    AbstractExtendedSynapseHandler abstractExtendedSynapseHandler =
                            (AbstractExtendedSynapseHandler) handler;
                    if (abstractExtendedSynapseHandler.handleServerInit()) {
                        notifyLoadedArtifacts(abstractExtendedSynapseHandler, contextInfo.getSynapseConfiguration());
                    } else {
                        log.warn("Server initialization not executed in the server init path of the " +
                                "AbstractExtendedSynapseHandler");
                    }
                }
            }
            if (synapseEnvironment != null) {
                // Properties props = new Properties();
                SynapseEnvironmentService synEnvSvc = new SynapseEnvironmentServiceImpl(synapseEnvironment, Constants.SUPER_TENANT_ID, configCtxSvc.getServerConfigContext());
                // Update ConfigurationHolder before registering the service
                ConfigurationHolder.getInstance().setSynapseEnvironmentService(synEnvSvc);
                synEnvRegistration = bndCtx.registerService(SynapseEnvironmentService.class.getName(), synEnvSvc, null);

                synapseEnvironment.registerSynapseHandler(new SynapseExternalPropertyConfigurator());
                synapseEnvironment.registerSynapseHandler(new ProxyLogHandler());

                // Register transaction counter handler
                if (isTransactionCounterEnabled()) {
                    synapseEnvironment.registerSynapseHandler(new TransactionCountHandler());
                    log.debug("Transaction Count Handler registered");
                }
                if (log.isDebugEnabled()) {
                    log.debug("SynapseEnvironmentService Registered");
                }
            } else {
                handleFatal("Couldn't register the SynapseEnvironmentService, " + "SynapseEnvironment not found");
            }
            // Properties props = new Properties();
            SynapseRegistrationsService synRegistrationsSvc = new SynapseRegistrationsServiceImpl(synCfgRegistration,
                    synEnvRegistration, Constants.SUPER_TENANT_ID, configCtxSvc.getServerConfigContext());
            // Update ConfigurationHolder before registering the service
            ConfigurationHolder.getInstance().setSynapseRegistrationsService(synRegistrationsSvc);
            bndCtx.registerService(SynapseRegistrationsService.class.getName(), synRegistrationsSvc, null);
            /*configCtxSvc.getServerConfigContext().setProperty(ConfigurationManager.CONFIGURATION_MANAGER,
                    configurationManager);*/
            String injectCarName = System.getProperty(ServiceBusConstants.AUTOMATION_MODE_CAR_NAME_SYSTEM_PROPERTY);
            if (injectCarName != null && !injectCarName.isEmpty()) {
                String sequenceName = getMainSequenceName(injectCarName);
                if (sequenceName != null) {
                    MessageContext synCtx = synapseEnvironment.createMessageContext();
                    SequenceMediator seq = (SequenceMediator) synapseEnvironment.getSynapseConfiguration().
                            getSequence(sequenceName);
                    synapseEnvironment.getSequenceObservers().add(new MicroIntegratorSequenceController());
                    synCtx.setProperty(ServiceBusConstants.AUTOMATION_MODE_MAIN_SEQ_PROPERTY, sequenceName);
                    synCtx.getEnvironment().injectMessage(synCtx, seq);
                }
            }
        } catch (Exception e) {
            handleFatal("Couldn't initialize the ESB...", e);
        } catch (Throwable t) {
            log.fatal("Failed to initialize ESB due to a fatal error", t);
        }
        if (log.isDebugEnabled()) {
            log.debug(ServiceBusInitializer.class.getName() + "#activate() COMPLETED - " + System.currentTimeMillis());
        }
    }

    private String getMainSequenceName(String cappName) {
        CarbonApplication capp = CappDeployer.getCarbonAppByName(cappName);
        if (capp == null) {
            log.error("Invalid cApp name. cApp name: " + cappName + " not found");
            return null;
        }
        String mainSeq = capp.getMainSequence();
        if (mainSeq == null) {
            log.error("Invalid main sequence name. Main sequence: " + mainSeq + " not found");
            return null;
        }
        return capp.getMainSequence();
    }

    @Deactivate
    protected void deactivate(ComponentContext ctxt) {
        List handlers = serverManager.getServerContextInformation().getSynapseEnvironment().getSynapseHandlers();
        Iterator<SynapseHandler> iterator = handlers.iterator();
        while (iterator.hasNext()) {
            SynapseHandler handler = iterator.next();
            if ((handler instanceof AbstractExtendedSynapseHandler)) {
                AbstractExtendedSynapseHandler abstractExtendedSynapseHandler =
                        (AbstractExtendedSynapseHandler) handler;
                if (!abstractExtendedSynapseHandler.handleServerShutDown()) {
                    log.warn("Server shut down not executed in the server shut down path of the " +
                            "AbstractExtendedSynapseHandler");
                }
            }
        }
        serverManager.stop();
        serverManager.shutdown();
    }

    private void initPersistence(SynapseConfigurationService synCfgSvc, String configName) throws AxisFault {
        // Initialize the mediation persistence manager if required
        CarbonServerConfigurationService serverConf = CarbonServerConfigurationService.getInstance();
        String persistence = serverConf.getFirstProperty(ServiceBusConstants.PERSISTENCE);
        // Check whether persistence is disabled
        if (!ServiceBusConstants.DISABLED.equals(persistence)) {
            // Check the worker interval is set or not
            String interval = serverConf.getFirstProperty(ServiceBusConstants.WORKER_INTERVAL);
            long intervalInMillis = 5000L;
            if (interval != null && !"".equals(interval)) {
                try {
                    intervalInMillis = Long.parseLong(interval);
                } catch (NumberFormatException e) {
                    log.error("Invalid value " + interval + " specified for the mediation " + "persistence worker " +
                            "interval, Using defaults", e);
                }
            }
            // Finally init the persistence manager
            MediationPersistenceManager pm = new MediationPersistenceManager(configurationInformation
                    .getSynapseXMLLocation(), synCfgSvc.getSynapseConfiguration(), intervalInMillis, configName);
            configCtxSvc.getServerConfigContext().getAxisConfiguration().addParameter(new Parameter
                    (ServiceBusConstants.PERSISTENCE_MANAGER, pm));
        } else {
            log.info("Persistence for mediation configuration is disabled");
        }
    }

    private ServerContextInformation initESB(String name) throws AxisFault {

        if (configCtxSvc != null) {
            ConfigurationContext configContext = configCtxSvc.getServerConfigContext();
            log.debug("Initializing Apache Synapse...");
            configurationInformation = ServerConfigurationInformationFactory.createServerConfigurationInformation
                    (configContext.getAxisConfiguration());
            // ability to specify the SynapseServerName as a system property
            if (System.getProperty("SynapseServerName") != null) {
                configurationInformation.setServerName(System.getProperty("SynapseServerName"));
            }
            // for now we override the default configuration location with the value in registry
            String synapseConfigsLocation = configurationInformation.getSynapseXMLLocation();
            if (synapseConfigsLocation != null) {
                configurationInformation.setSynapseXMLLocation(synapseConfigsLocation + File.separator + name);
            } else {
                configurationInformation.setSynapseXMLLocation(ServiceBusConstants.DEFAULT_SYNAPSE_CONFIGS_LOCATION +
                        name);
            }
            configurationInformation.setCreateNewInstance(false);
            configurationInformation.setServerControllerProvider(CarbonSynapseController.class.getName());
            if (isRunningSamplesMode()) {
                if (System.getProperty(ServiceBusConstants.ESB_SAMPLE_SYSTEM_PROPERTY) != null) {
                    configurationInformation.setSynapseXMLLocation("repository" + File.separator + "samples" + File
                            .separator + "synapse_sample_" + System.getProperty(ServiceBusConstants
                            .ESB_SAMPLE_SYSTEM_PROPERTY) + ".xml");
                } else {
                    configurationInformation.setSynapseXMLLocation("samples" + File.separator + "service-bus" + File
                            .separator + "synapse_sample_" + System.getProperty(ServiceBusConstants
                            .EI_SAMPLE_SYSTEM_PROPERTY) + ".xml");
                }
            }
            serverManager = new ServerManager();
            ServerContextInformation contextInfo = new ServerContextInformation(configContext,
                    configurationInformation);

            if (secretCallbackHandlerService != null) {
                contextInfo.addProperty(SecurityConstants.PROP_SECRET_CALLBACK_HANDLER, secretCallbackHandlerService
                        .getSecretCallbackHandler());
            }
            AxisConfiguration axisConf = configContext.getAxisConfiguration();
            axisConf.addParameter(new Parameter(ServiceBusConstants.SYNAPSE_CURRENT_CONFIGURATION, name));
            if (isRunningDebugMode()) {
                log.debug("Micro Integrator started in Debug mode for super tenant");
                createSynapseDebugEnvironment(contextInfo);
            }
            boolean initConnectors = !Boolean.valueOf(System.getProperty(ServiceBusConstants
                    .DISABLE_CONNECTOR_INIT_SYSTEM_PROPERTY));
            if (initConnectors) {
                // Enable connectors
                SynapseArtifactInitUtils.initializeConnectors(axisConf);
            }
            serverManager.init(configurationInformation, contextInfo);
            serverManager.start();
            AxisServiceGroup serviceGroup = axisConf.getServiceGroup(SynapseConstants.SYNAPSE_SERVICE_NAME);
            serviceGroup.addParameter("hiddenService", "true");
            return contextInfo;
        } else {
            handleFatal("Couldn't initialize Synapse, " + "ConfigurationContext service or SynapseRegistryService is " +
                    "not available");
        }
        // never executes, but keeps the compiler happy
        return null;
    }

    public static boolean isRunningSamplesMode() {

        return System.getProperty(ServiceBusConstants.ESB_SAMPLE_SYSTEM_PROPERTY) != null || System.getProperty
                (ServiceBusConstants.EI_SAMPLE_SYSTEM_PROPERTY) != null;
    }

    public static boolean isRunningDebugMode() {

        String debugMode = System.getProperty(ServiceBusConstants.ESB_DEBUG_SYSTEM_PROPERTY);
        return "true".equals(debugMode) || "super".equals(debugMode);
    }

    /**
     * creates Synapse debug environment
     * creates TCP channels using command and event ports which initializes the interface to outer debugger
     * set the relevant information in the server configuration so that it can be used when Synapse environment
     * initializes
     *
     * @param contextInfo Server Context Information
     */
    public void createSynapseDebugEnvironment(ServerContextInformation contextInfo) {

        try {
            String carbonHome = System.getProperty(Constants.CARBON_HOME);
            File synapseProperties = Paths.get(MicroIntegratorBaseUtils.getCarbonConfigDirPath(), "synapse.properties").toFile();
            Properties properties = new Properties();
            InputStream inputStream = new FileInputStream(synapseProperties);
            properties.load(inputStream);
            inputStream.close();
            int event_port = Integer.parseInt(properties.getProperty(ServiceBusConstants.ESB_DEBUG_EVENT_PORT));
            int command_port = Integer.parseInt(properties.getProperty(ServiceBusConstants.ESB_DEBUG_COMMAND_PORT));
            SynapseDebugInterface debugInterface = SynapseDebugInterface.getInstance();
            debugInterface.init(command_port, event_port);
            contextInfo.setServerDebugModeEnabled(true);
            contextInfo.setSynapseDebugInterface(debugInterface);
            SynapseDebugManager debugManager = SynapseDebugManager.getInstance();
            contextInfo.setSynapseDebugManager(debugManager);
            log.debug("Synapse debug Environment created successfully");
        } catch (IOException ex) {
            log.error("Error while creating Synapse debug environment ", ex);
        } catch (InterruptedException ex) {
            log.error("Error while creating Synapse debug environment ", ex);
        }
    }

    @Reference(
            name = "config.context.service",
            service = Axis2ConfigurationContextService.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unsetConfigurationContextService")
    protected void setConfigurationContextService(Axis2ConfigurationContextService configurationContextService) {

        if (log.isDebugEnabled()) {
            log.debug("ConfigurationContextService bound to the ESB initialization process");
            log.debug("configurationContextService : " + configurationContextService);
        }
        this.configCtxSvc = configurationContextService;
        ConfigurationHolder.getInstance().setAxis2ConfigurationContextService(configurationContextService);
    }

    protected void unsetConfigurationContextService(Axis2ConfigurationContextService configurationContextService) {

        if (log.isDebugEnabled()) {
            log.debug("ConfigurationContextService unbound from the ESB environment");
        }
        this.configCtxSvc = null;
    }

    @Reference(
            name = "org.wso2.micro.integrator.core.services.CarbonServerConfigurationService",
            service = CarbonServerConfigurationService.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unsetServerConfigurationService")
    protected void setServerConfigurationService(CarbonServerConfigurationService serverConfigurationService) {
        ConfigurationHolder.getInstance().setCarbonServerConfigurationService(serverConfigurationService);
    }

    protected void unsetServerConfigurationService(CarbonServerConfigurationService serverConfigurationService) {
        //nothing to do here since we put this reference to populate the ConfigurationHolder
    }

    @Reference(
            name = "secret.callback.handler.service",
            service = org.wso2.carbon.securevault.SecretCallbackHandlerService.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unsetSecretCallbackHandlerService")
    protected void setSecretCallbackHandlerService(SecretCallbackHandlerService secretCallbackHandlerService) {

        if (log.isDebugEnabled()) {
            log.debug("SecretCallbackHandlerService bound to the ESB initialization process");
        }
        this.secretCallbackHandlerService = secretCallbackHandlerService;
    }

    protected void unsetSecretCallbackHandlerService(SecretCallbackHandlerService secretCallbackHandlerService) {

        if (log.isDebugEnabled()) {
            log.debug("SecretCallbackHandlerService unbound from the ESB environment");
        }
        this.secretCallbackHandlerService = null;
    }

    public static ServerConfigurationInformation getConfigurationInformation() {

        return configurationInformation;
    }

    protected static ServerConfigurationInformation getServerConfigurationInformation() {

        return configurationInformation;
    }

    private void handleFatal(String message) {

        log.fatal(message);
        // Do not do this -- throw new RuntimeException(message);
        // it causes the OSGi environment to reinitialize synapse which will result in a looping
    }

    private void handleFatal(String message, Exception e) {

        log.fatal(message, e);
        // Do not do this -- throw new RuntimeException(message, e);
        // it causes the OSGi environment to reinitialize synapse which will result in a looping
    }

    public class MPMShutdownHandler implements ServerShutdownHandler {

        private AxisConfiguration configuration;

        public MPMShutdownHandler(AxisConfiguration configuration) {

            this.configuration = configuration;
        }

        public void invoke() {

            Parameter p = configuration.getParameter(ServiceBusConstants.PERSISTENCE_MANAGER);
            if (p != null && p.getValue() instanceof MediationPersistenceManager) {
                ((MediationPersistenceManager) p.getValue()).destroy();
            }
        }
    }

    /**
     * Clean up temp files
     *
     * @param appUnzipDir
     */
    private static void cleanupTempDirectory(String appUnzipDir) {

        File tempDirector = new File(appUnzipDir);
        if (tempDirector.isDirectory()) {
            File[] entries = tempDirector.listFiles();
            int size = entries.length;
            for (int i = 0; i < size; i++) {
                try {
                    FileUtils.deleteDirectory(entries[i]);
                } catch (IOException e) {
                    log.warn("Could not build lib artifact for path : " + entries[i].getAbsolutePath());
                }
            }
        }
    }

    /**
     * Register for inbound hot depoyment
     */
    /*private void registerInboundDeployer(AxisConfiguration axisConfig, SynapseEnvironment synEnv) {

        DeploymentEngine deploymentEngine = (DeploymentEngine) axisConfig.getConfigurator();
        SynapseArtifactDeploymentStore deploymentStore = synEnv.getSynapseConfiguration().getArtifactDeploymentStore();
        String synapseConfigPath = ServiceBusUtils.getSynapseConfigAbsPath(synEnv.getServerContextInformation());
        String inboundDirPath = synapseConfigPath + File.separator + MultiXMLConfigurationBuilder.INBOUND_ENDPOINT_DIR;
        for (InboundEndpoint inboundEndpoint : synEnv.getSynapseConfiguration().getInboundEndpoints()) {
            if (inboundEndpoint.getFileName() != null) {
                deploymentStore.addRestoredArtifact(inboundDirPath + File.separator + inboundEndpoint.getFileName());
            }
        }
        deploymentEngine.addDeployer(new InboundEndpointDeployer(), inboundDirPath, ServiceBusConstants
                .ARTIFACT_EXTENSION);
    }*/

//    @Reference(
//            name = "inbound.endpoint.persistence.service",
//            service = org.wso2.carbon.inbound.endpoint.persistence.service.InboundEndpointPersistenceService.class,
//            cardinality = ReferenceCardinality.OPTIONAL,
//            policy = ReferencePolicy.DYNAMIC,
//            unbind = "unsetInboundPersistenceService")
    /*protected void setInboundPersistenceService(InboundEndpointPersistenceService inboundEndpoint) {
        // This service is just here to make sure that ServiceBus is not getting initialized
        // before the Inbound Endpoint Persistence component is up and running
    }

    protected void unsetInboundPersistenceService(InboundEndpointPersistenceService inboundEndpoint) {

    }*/

    @Reference(
            name = "org.wso2.micro.integrator.ntask.core.service.TaskService",
            service = org.wso2.micro.integrator.ntask.core.service.TaskService.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unsetTaskService")
    protected void setTaskService(TaskService taskService) {

        log.debug("Set Task Service");
        this.taskService = taskService;
    }

    protected void unsetTaskService(TaskService taskService) {

        this.taskService = null;
    }

    @Reference(name = "osgi.configadmin.service",
            service = ConfigurationAdmin.class,
            unbind = "unsetConfigAdminService",
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC)
    public void setConfigAdminService(ConfigurationAdmin configAdminService) {
        log.debug("Setting ConfigurationAdmin Service");
        ConfigurationHolder.getInstance().setConfigAdminService(configAdminService);
    }

    public void unsetConfigAdminService(ConfigurationAdmin configAdminService) {
        log.debug("Unsetting ConfigurationAdmin Service");
    }

    @Reference(name = "org.wso2.carbon.ndatasource",
               service = DataSourceService.class,
               cardinality = ReferenceCardinality.MANDATORY,
               policy = ReferencePolicy.DYNAMIC,
               unbind = "unsetDatasourceHandlerService")
    protected void setDatasourceHandlerService(DataSourceService dataSourceService) {
        this.dataSourceService = dataSourceService;
    }

    protected void unsetDatasourceHandlerService(DataSourceService dataSourceService) {
        this.dataSourceService = null;
    }

    private boolean isTransactionCounterEnabled() {
        boolean transactionCounterEnabled = false;
        Object object = ConfigParser.getParsedConfigs().get("integration.transaction_counter.enable");
        if (Objects.nonNull(object)) {
            transactionCounterEnabled = Boolean.parseBoolean(object.toString());
        }
        return transactionCounterEnabled;
    }

    /**
     * Notify the handler about artifacts loaded from synapse-configs at startup. These artifacts are built directly
     * into the SynapseConfiguration and marked as restored, so the deployers never fire the artifact deployment
     * event for them. Artifacts deployed later (CApps, hot deployment) are notified by the deployers as usual.
     *
     * @param handler       handler that completed server initialization
     * @param synapseConfig configuration built at startup
     */
    private void notifyLoadedArtifacts(AbstractExtendedSynapseHandler handler, SynapseConfiguration synapseConfig) {
        if (synapseConfig == null) {
            return;
        }
        String startTime = String.valueOf(System.currentTimeMillis());
        for (ProxyService proxy : synapseConfig.getProxyServices()) {
            notifyArtifactDeployment(handler, proxy.getName(), SynapseConstants.PROXY_SERVICE_TYPE, startTime);
        }
        for (API api : synapseConfig.getAPIs()) {
            notifyArtifactDeployment(handler, api.getName(), SynapseConstants.FAIL_SAFE_MODE_API, startTime);
        }
        for (InboundEndpoint inboundEndpoint : synapseConfig.getInboundEndpoints()) {
            notifyArtifactDeployment(handler, inboundEndpoint.getName(), INBOUND_ENDPOINT_ARTIFACT_TYPE, startTime);
        }
    }

    /**
     * Notify the handler about a single deployed artifact. Failures are logged so that one artifact or handler
     * cannot interrupt the server startup.
     */
    private void notifyArtifactDeployment(AbstractExtendedSynapseHandler handler, String artifactName,
                                          String artifactType, String startTime) {
        try {
            if (!handler.handleArtifactDeployment(artifactName, artifactType, startTime)) {
                log.warn("Artifact deployment not executed in the startup path of the AbstractExtendedSynapseHandler: "
                        + handler.getName() + " for " + artifactType + ": " + artifactName);
            }
        } catch (Exception e) {
            log.warn("Error while executing the AbstractExtendedSynapseHandler: " + handler.getName()
                    + " for the " + artifactType + ": " + artifactName + " at startup", e);
        }
    }

    /**
     * Check XMLInputFactory properties for XXE safe configuration.
     * Shutdown the server if unsafe properties are found.
     */
    private void checkInputFactoryProperties() {
        log.debug("Checking XMLInputFactory properties for unsafe configurations");
        Map<?, ?> props = StAXUtils.loadFactoryProperties("XMLInputFactory.properties");
        boolean propsFound = false;
        if (props != null) {
            log.debug("Found XMLInputFactory properties file, checking for unsafe configurations");
            for (Map.Entry<?, ?> entry : props.entrySet()) {
                String key = String.valueOf(entry.getKey());
                String value = String.valueOf(entry.getValue());
                if ((XMLInputFactory.SUPPORT_DTD.equals(key) && Boolean.parseBoolean(value)) ||
                    (XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES.equals(key)
                        && Boolean.parseBoolean(value))) {
                    propsFound = true;
                    break;
                }
            }
        }
        if (propsFound) {
            log.error(
                "The XMLInputFactory.properties file contains properties which can lead to "
                    + "XXE attacks. Please remove the properties '" + XMLInputFactory.SUPPORT_DTD
                    + "' and '" + XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES
                    + "' from the file and restart the server.");
            System.exit(1);
        }
    }
}

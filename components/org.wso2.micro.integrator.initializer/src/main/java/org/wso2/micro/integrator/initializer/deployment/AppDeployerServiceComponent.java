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
package org.wso2.micro.integrator.initializer.deployment;

import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.deployment.DeploymentEngine;
import org.apache.axis2.engine.AxisConfigurator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.wso2.carbon.securevault.SecretCallbackHandlerService;
import org.wso2.micro.application.deployer.handler.DefaultAppDeployer;
import org.wso2.micro.core.CarbonAxisConfigurator;
import org.wso2.micro.integrator.core.UserStoreTemporaryService;
import org.wso2.micro.integrator.dataservices.core.DBDeployer;
import org.wso2.micro.integrator.initializer.StartupFinalizer;
import org.wso2.micro.integrator.initializer.dashboard.HeartBeatComponent;
import org.wso2.micro.integrator.initializer.deployment.application.deployer.CappDeployer;
import org.wso2.micro.integrator.initializer.deployment.config.deployer.ConfigDeployer;
import org.wso2.micro.integrator.initializer.deployment.synapse.deployer.FileRegistryResourceDeployer;
import org.wso2.micro.integrator.initializer.deployment.synapse.deployer.SynapseAppDeployer;
import org.wso2.micro.integrator.initializer.deployment.user.store.deployer.UserStoreDeployer;
import org.wso2.micro.integrator.initializer.services.SynapseEnvironmentService;
import org.wso2.micro.integrator.initializer.utils.ConfigurationHolder;
import org.wso2.micro.integrator.ndatasource.capp.deployer.DataSourceCappDeployer;

@Component(name = "org.wso2.micro.integrator.initializer.deployment.AppDeployerServiceComponent", immediate = true)
public class AppDeployerServiceComponent {

    private static volatile SecretCallbackHandlerService secretCallbackHandlerService;
    private static UserStoreTemporaryService userStoreTemporaryService;
    private static final Log log = LogFactory.getLog(AppDeployerServiceComponent.class);

    private ConfigurationContext configCtx;
    private SynapseEnvironmentService synapseEnvironmentService;
    private StartupFinalizer startupFinalizer;

    @Activate
    protected void activate(ComponentContext ctxt) {

        if (log.isDebugEnabled()) {
            log.debug(
                    AppDeployerServiceComponent.class.getName() + "#activate() BEGIN - " + System.currentTimeMillis());
            log.debug("Activating AppDeployerServiceComponent");
        }

        // ConfigurationHolder is updated by org.wso2.micro.integrator.initializer.ServiceBusInitializer
        configCtx = ConfigurationHolder.getInstance().getAxis2ConfigurationContextService().getServerConfigContext();

        // Initialize deployers in micro integrator
        initializeDeployers();

        // Invoke all registered deployers to deploy services
        invokeRegisteredDeployers();

        if (HeartBeatComponent.isDashboardConfigured()) {
            log.info("Dashboard is configured. Initiating heartbeat component.");
            HeartBeatComponent.invokeHeartbeatExecutorService();
        }

        // Finalize server startup
        startupFinalizer = new StartupFinalizer(configCtx, ctxt.getBundleContext());
        startupFinalizer.finalizeStartup();

        if (log.isDebugEnabled()) {
            log.debug(AppDeployerServiceComponent.class.getName() + "#activate() COMPLETE - " +
                              System.currentTimeMillis());
        }
    }

    @Deactivate
    protected void deactivate(ComponentContext ctxt) {
        log.debug("Deactivating AppDeployerServiceComponent");
        HeartBeatComponent.shutdownHeartbeatExecutorService();
        startupFinalizer.cleanup();
    }

    // TODO :- uncomment when satisfied
  /*  @Reference(
            name = "ntask.service",
            service = org.wso2.carbon.ntask.core.service.TaskService.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unsetTaskService")
    protected void setTaskService(TaskService taskService) {
        this.taskService = taskService;
    }

    protected void unsetTaskService(TaskService taskService) {
    }*/

    /**
     * Receive an event about the creation of a SynapseEnvironment. If this is
     * SuperTenant we have to wait until all the other constraints are met and actual
     * initialization is done in the activate method. Otherwise we have to do the activation here.
     *
     * @param synapseEnvironmentService SynapseEnvironmentService which contains information
     *                                  about the new Synapse Instance
     */
    @Reference(
            name = "synapse.env.service",
            service = org.wso2.micro.integrator.initializer.services.SynapseEnvironmentService.class,
            cardinality = ReferenceCardinality.AT_LEAST_ONE,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unsetSynapseEnvironmentService")
    protected void setSynapseEnvironmentService(SynapseEnvironmentService synapseEnvironmentService) {
        this.synapseEnvironmentService = synapseEnvironmentService;
        DataHolder.getInstance().setSynapseEnvironmentService(this.synapseEnvironmentService);
    }

    /**
     * Receive an event about Destroying a SynapseEnvironment. This can be the super tenant
     * destruction or a tenant destruction.
     *
     * @param synapseEnvironmentService synapseEnvironment
     */
    protected void unsetSynapseEnvironmentService(SynapseEnvironmentService synapseEnvironmentService) {
        this.synapseEnvironmentService = null;
    }

    /**
     * Function to initialize deployers.
     */
    private void initializeDeployers() {
        DeploymentEngine deploymentEngine = (DeploymentEngine) configCtx.getAxisConfiguration().getConfigurator();

        // Register data services deployer in DeploymentEngine
        addDBDeployer(deploymentEngine);

        // Register CappDeployer in DeploymentEngine (required for CApp hot deployment)
        addCAppDeployer(deploymentEngine);

        // Register user-store deployer
        if (!Boolean.parseBoolean(System.getProperty("NonUserCoreMode"))) {
            addUserStoreDeployer(deploymentEngine);
        }

    }

    /**
     * Initialize and add the CappDeployer to the Deployment Engine.
     */
    private void addCAppDeployer(DeploymentEngine deploymentEngine) {
        String artifactRepoPath = configCtx.getAxisConfiguration().getRepository().getPath();

        // Initialize CApp deployer here
        CappDeployer cappDeployer = new CappDeployer();
        cappDeployer.setDirectory(artifactRepoPath + DeploymentConstants.CAPP_DIR_NAME);
        cappDeployer.setAppsDirectory(artifactRepoPath + DeploymentConstants.APPS_DIR_NAME);
        cappDeployer.setSecretCallbackHandlerService(secretCallbackHandlerService);
        cappDeployer.init(configCtx);

        // Register application deployment handlers
        cappDeployer.registerDeploymentHandler(new ConfigDeployer());
        cappDeployer.registerDeploymentHandler(new FileRegistryResourceDeployer(
                synapseEnvironmentService.getSynapseEnvironment().getSynapseConfiguration().getRegistry()));
        cappDeployer.registerDeploymentHandler(new DataSourceCappDeployer());
        cappDeployer.registerDeploymentHandler(new DefaultAppDeployer());
        cappDeployer.registerDeploymentHandler(new SynapseAppDeployer());
        // deploy capps in the apps directory during the server startup
        cappDeployer.deployCarbonAppsDirectory();

        //Add the deployer to deployment engine. This should be done after registering the deployment handlers.
        deploymentEngine.addDeployer(cappDeployer, artifactRepoPath + DeploymentConstants.CAPP_DIR_NAME,
                                     DeploymentConstants.CAPP_TYPE_EXTENSION);
        if (log.isDebugEnabled()) {
            log.debug("Successfully registered CappDeployer");
        }
    }

    /**
     * Initialize and add the Data Service Deployer to the Deployment Engine.
     */
    private void addDBDeployer(DeploymentEngine deploymentEngine) {
        String artifactRepoPath = configCtx.getAxisConfiguration().getRepository().getPath();

        // Create data services deployer
        DBDeployer dbDeployer = new DBDeployer();
        dbDeployer.setDirectory(artifactRepoPath + DeploymentConstants.DSS_DIR_NAME);
        dbDeployer.setExtension(DeploymentConstants.DSS_TYPE_EXTENSION);

        // Register deployer in DeploymentEngine
        deploymentEngine.addDeployer(dbDeployer, DeploymentConstants.DSS_DIR_NAME, DeploymentConstants.DSS_TYPE_DBS);

        if (log.isDebugEnabled()) {
            log.debug("Successfully registered Data Service Deployer");
        }
    }

    private void addUserStoreDeployer(DeploymentEngine deploymentEngine) {
        String artifactRepoPath = configCtx.getAxisConfiguration().getRepository().getPath();

        // Create user store deployer
        UserStoreDeployer userStoreDeployer = new UserStoreDeployer();
        userStoreDeployer.setDirectory(artifactRepoPath + DeploymentConstants.USER_STORE_DIR_NAME);
        userStoreDeployer.setExtension(DeploymentConstants.XML_TYPE_EXTENSION);

        // Register user store deployer in DeploymentEngine
        deploymentEngine.addDeployer(userStoreDeployer, DeploymentConstants.USER_STORE_DIR_NAME, DeploymentConstants.XML_TYPE_EXTENSION);

        if (log.isDebugEnabled()) {
            log.debug("Successfully registered UserStore Deployer");
        }
    }

    /**
     * Invoke all registered deployers.
     */
    private void invokeRegisteredDeployers() {
        AxisConfigurator axisConfigurator = configCtx.getAxisConfiguration().getConfigurator();
        if (axisConfigurator instanceof CarbonAxisConfigurator) {
            ((CarbonAxisConfigurator) axisConfigurator).deployServices();
        }
    }

    // Waiting for this reference to change the bundle startup order
    // Waiting for user store mgt service component.
    @Reference(name = "user.temporaryService.default", cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC, unbind = "unsetTemporaryService")
    protected void setTemporaryService(UserStoreTemporaryService userStoreTemporaryService) {
        this.userStoreTemporaryService = userStoreTemporaryService;
    }

    protected void unsetTemporaryService(UserStoreTemporaryService userStoreTemporaryService) {
        userStoreTemporaryService = null;
    }

    @Reference(
            name = "secret.callback.handler.service",
            service = SecretCallbackHandlerService.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unsetSecretCallbackHandlerService")
    protected void setSecretCallbackHandlerService(SecretCallbackHandlerService secretCallbackHandlerService) {
        log.debug("SecretCallbackHandlerService bound to the ESB initialization process");
        this.secretCallbackHandlerService = secretCallbackHandlerService;
    }

    protected void unsetSecretCallbackHandlerService(SecretCallbackHandlerService secretCallbackHandlerService) {
        log.debug("SecretCallbackHandlerService unbound from the ESB environment");
        this.secretCallbackHandlerService = null;
    }

    /**
     * Returns the SecretCallbackHandlerService instance bound by OSGi.
     *
     * @return the SecretCallbackHandlerService instance, or null if not bound yet
     */
    public static SecretCallbackHandlerService getSecretCallbackHandlerService() {
        return secretCallbackHandlerService;
    }
}

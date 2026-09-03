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
package org.wso2.micro.integrator.ntask.core.internal;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.quartz.Scheduler;
import org.quartz.impl.StdSchedulerFactory;
import org.wso2.carbon.securevault.SecretCallbackHandlerService;
import org.wso2.config.mapper.ConfigParser;
import org.wso2.micro.core.ServerStartupObserver;
import org.wso2.micro.integrator.coordination.ClusterCoordinator;
import org.wso2.micro.integrator.coordination.ClusterEventListener;
import org.wso2.micro.integrator.coordination.exception.ClusterCoordinationException;
import org.wso2.micro.integrator.coordination.util.RDBMSConstantUtils;
import org.wso2.micro.integrator.core.util.MicroIntegratorBaseUtils;
import org.wso2.micro.integrator.ndatasource.common.DataSourceException;
import org.wso2.micro.integrator.ndatasource.core.CarbonDataSource;
import org.wso2.micro.integrator.ndatasource.core.DataSourceService;
import org.wso2.micro.integrator.ntask.common.TaskException;
import org.wso2.micro.integrator.ntask.coordination.TaskCoordinationException;
import org.wso2.micro.integrator.ntask.coordination.task.TaskEventListener;
import org.wso2.micro.integrator.ntask.core.BootLeaseController;
import org.wso2.micro.integrator.ntask.core.CoordinationReadinessRegistry;
import org.wso2.micro.integrator.ntask.coordination.task.resolver.ActivePassiveResolver;
import org.wso2.micro.integrator.ntask.coordination.task.resolver.TaskLocationResolver;
import org.wso2.micro.integrator.ntask.coordination.task.store.TaskStore;
import org.wso2.micro.integrator.ntask.core.TaskStartupHandler;
import org.wso2.micro.integrator.ntask.core.TaskUtils;
import org.wso2.micro.integrator.ntask.core.impl.CoordinatedTaskTriggerListener;
import org.wso2.micro.integrator.ntask.core.impl.PauseWatchdog;
import org.wso2.micro.integrator.ntask.core.impl.QuartzCachedThreadPool;
import org.wso2.micro.integrator.ntask.core.impl.standalone.ScheduledTaskManager;
import org.wso2.micro.integrator.ntask.core.service.TaskService;
import org.wso2.micro.integrator.ntask.core.service.impl.TaskServiceImpl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;

/**
 * This class represents the Tasks declarative service component.
 */
@Component(name = "org.wso2.micro.integrator.ntask.core.internal.TasksDSComponent",
        immediate = true)
public class TasksDSComponent {

    private static final String QUARTZ_PROPERTIES_FILE_NAME = "quartz.properties";
    private static final String TASK_CONFIG = "task_handling";
    private static final String RESOLVER_CLASS = "resolver_class";
    private static final String RESOLVING_PERIOD = "resolving_period";
    private static final String RESOLVING_FREQUENCY = "resolving_frequency";
    private static final String TASK_RESOLVER = "task_resolver";
    private static final String COORDINATION_HARDENING_CONFIG = TASK_CONFIG + ".coordination_hardening";
    private static final String SYNCHRONOUS_INJECTION_CONFIG = TASK_CONFIG + ".synchronous_injection";
    private static final String HARDENING_DISABLED_CONDITION = "hardening-disabled";
    private static final String HARDENING_CONFIG_CONTRADICTION_CONDITION = "hardening-config-contradiction";
    private static final String HARDENING_PROFILE_INVALID_CONDITION = "hardening-profile-invalid";
    private static final String SCHEMA_PRESENT_CONDITION = "schema-present";
    private static final String MISFIRE_THRESHOLD_PROPERTY = "org.quartz.jobStore.misfireThreshold";

    private final Log log = LogFactory.getLog(TasksDSComponent.class);
    private static Scheduler scheduler;

    private static SecretCallbackHandlerService secretCallbackHandlerService;

    private static TaskService taskService;

    // JVM-lifetime, like the bootstrap state: conditions raised at the first activation's bootstrap decision
    // must survive a component reactivation.
    private static final CoordinationReadinessRegistry readinessRegistry = new CoordinationReadinessRegistryImpl();

    // The controller reference exists for one component activation, while the bootstrap state and configuration
    // snapshot live for the lifetime of the JVM. Atomic compare-and-set operations prevent two activations from
    // publishing controllers concurrently and prevent an older deactivation from clearing its successor. Stock and
    // refusing nodes never install a controller; consumers therefore resolve it at the moment they need it.
    private static final AtomicReference<BootLeaseController> CONTROLLER = new AtomicReference<>();

    private static ExecutorService executor = Executors.newCachedThreadPool();
    private static DataSourceService dataSourceService;
    private Object coordinationDatasourceObject;
    private DataHolder dataHolder = DataHolder.getInstance();
    private TaskStore taskStore;
    private TaskLocationResolver resolver;
    private ClusterCoordinator clusterCoordinator;

    // The hardened activation sequence's per-activation retry state: completed steps are never redone
    // on a lease-backoff retry, and OSGi service registrations happen exactly once.
    private ScheduledExecutorService hardenedSequenceExecutor;
    private long hardenedRetryDelayMillis;
    private boolean hardenedReadinessServiceRegistered;
    private boolean hardenedCoordinatorStarted;
    private boolean hardenedServicesRegistered;
    private boolean hardenedListenersWired;
    private PauseWatchdog pauseWatchdog;

    @Activate
    protected void activate(ComponentContext ctx) {

        try {
            if (executor.isShutdown()) {
                executor = Executors.newCachedThreadPool();
            }
            String quartzConfigFilePath =
                    MicroIntegratorBaseUtils.getCarbonConfigDirPath() + File.separator + "etc" + File.separator
                            + QUARTZ_PROPERTIES_FILE_NAME;
            StdSchedulerFactory fac;
            if (new File(quartzConfigFilePath).exists()) {
                fac = new StdSchedulerFactory(quartzConfigFilePath);
                warnIfMisfireThresholdOverrideExceedsTakeoverWindow(quartzConfigFilePath);
            } else {
                fac = new StdSchedulerFactory(this.getStandardQuartzProps());
            }
            TasksDSComponent.scheduler = fac.getScheduler();
            TasksDSComponent.getScheduler().start();

            if (!TaskHandlingConfigUtils.isBootstrapStateInstalled()) {
                decideCoordinationBootstrapState();
            }

            // The coordinated-vs-local doorway decision is a static config fact, evaluated in all
            // three bootstrap states — never the bootstrap side-state clusterCoordinator != null.
            boolean coordinationConfigured = isCoordinationDataSourceAvailable();
            dataHolder.setCoordinationConfigured(coordinationConfigured);

            if (TaskHandlingConfigUtils.isCoordinationHardeningEnabled()) {
                // ARMED: the fail-closed activation sequence — one guarded sequence, retried on the
                // lease backoff loop. The stock path below never runs on an ARMED node.
                runHardenedActivationSequence(ctx.getBundleContext());
                return;
            }

            boolean isCoordinationEnabled =
                    TaskHandlingConfigUtils.isStockCoordination() && coordinationConfigured;
            if (isCoordinationEnabled) {
                log.info("Initializing task coordination.");
                DataSource coordinationDataSource = (DataSource) coordinationDatasourceObject;
                clusterCoordinator = new ClusterCoordinator(coordinationDataSource);
                clusterCoordinator.setDuplicateNode(clusterCoordinator.checkDuplicateNodeExistence());
                dataHolder.setClusterCoordinator(clusterCoordinator);
                // initialize task data base.
                taskStore = new TaskStore(coordinationDataSource);
                dataHolder.setTaskStore(taskStore);
                // removing all tasks assigned to this node since this node hasn't even  joined the cluster yet and
                // cannot have tasks assigned to it already. Will be useful in case of static node ids
                try {
                    taskStore.deleteTasks(clusterCoordinator.getThisNodeId());
                } catch (TaskCoordinationException e) {
                    log.error("Error while removing the tasks of this node.", e);
                }
                // initialize task location resolver
                resolver = getResolver();
            }

            if (getTaskService() == null) {
                taskService = new TaskServiceImpl(taskStore);
            }

            BundleContext bundleContext = ctx.getBundleContext();
            bundleContext.registerService(ServerStartupObserver.class.getName(), new TaskStartupHandler(taskService),
                                          null);
            bundleContext.registerService(TaskService.class.getName(), getTaskService(), null);
            bundleContext.registerService(CoordinationReadinessRegistry.class.getName(), readinessRegistry, null);

            if (isCoordinationEnabled) {
                clusterCoordinator.registerListener(new ClusterEventListener(clusterCoordinator.getThisNodeId()));
                ScheduledTaskManager scheduledTaskManager = dataHolder.getTaskManager();
                if (scheduledTaskManager == null) {
                    throw new AssertionError("Task Manager is not initialized properly.");
                }
                clusterCoordinator.registerListener(new TaskEventListener(scheduledTaskManager, taskStore, resolver));
                // the task scheduler should be started after registering task service.
                CoordinatedTaskScheduleManager coordinatedTaskScheduleManager = new CoordinatedTaskScheduleManager(
                        scheduledTaskManager, taskStore, clusterCoordinator, resolver);
                // join cluster
                clusterCoordinator.startCoordinator();
                setSchedulerProperties();
                coordinatedTaskScheduleManager.startTaskScheduler("");
            }
        } catch (Throwable initializationFailure) {
            log.error("Error in initializing Tasks component: " + initializationFailure.getMessage(), initializationFailure);
        }
    }

    /**
     * Initializes an armed node in fail-closed order. Dependencies and the configuration fingerprint are created
     * before the controller is installed; the lease is acquired before membership starts; startup handback completes
     * before task services and listeners are exposed; and the global claim listener is registered before the
     * configuration fence is checked. Existing tasks are enumerated later by the normal startup callbacks. Until that
     * boot pass publishes ELIGIBLE and the controller adopts PROVEN, coordinated scheduling remains stopped. A failed
     * step is retried with backoff without repeating completed wiring.
     */
    private void runHardenedActivationSequence(BundleContext bundleContext) {

        try {
            BootLeaseControllerImpl controller = (BootLeaseControllerImpl) getBootLeaseController();
            if (controller == null) {
                if (!isCoordinationDataSourceAvailable()) {
                    throw new TaskCoordinationException("Coordination hardening requires the "
                            + RDBMSConstantUtils.COORDINATION_DB_NAME + " datasource, which is not configured.");
                }
                verifyHardeningSchemaPresent((DataSource) coordinationDatasourceObject);
                CoordinationHardeningConfig config = TaskHandlingConfigUtils.getConfig();
                String configFingerprint = ConfigFingerprint.build(config);
                DataSource coordinationDataSource = (DataSource) coordinationDatasourceObject;
                clusterCoordinator = new ClusterCoordinator(coordinationDataSource);
                dataHolder.setClusterCoordinator(clusterCoordinator);
                taskStore = new TaskStore(coordinationDataSource);
                dataHolder.setTaskStore(taskStore);
                resolver = getResolver();
                if (getTaskService() == null) {
                    taskService = new TaskServiceImpl(taskStore);
                }
                if (!hardenedReadinessServiceRegistered) {
                    bundleContext.registerService(CoordinationReadinessRegistry.class.getName(), readinessRegistry,
                            null);
                    hardenedReadinessServiceRegistered = true;
                }
                controller = new BootLeaseControllerImpl(taskStore, taskService, config, clusterCoordinator,
                        resolver, resolveLocalGroupId(), clusterCoordinator.getThisNodeId(), configFingerprint);
                installController(controller);
                if (pauseWatchdog == null) {
                    pauseWatchdog = new PauseWatchdog(clusterCoordinator);
                    pauseWatchdog.start();
                }
            }
            if (controller.bootId() == null) {
                controller.acquire();
            }
            if (!hardenedCoordinatorStarted) {
                clusterCoordinator.registerHeartbeatHook(controller);
                clusterCoordinator.startCoordinator();
                hardenedCoordinatorStarted = true;
            }
            controller.runGuardedStartupHandback();
            if (!hardenedServicesRegistered) {
                bundleContext.registerService(ServerStartupObserver.class.getName(),
                        new TaskStartupHandler(taskService), null);
                bundleContext.registerService(TaskService.class.getName(), getTaskService(), null);
                hardenedServicesRegistered = true;
            }
            if (!hardenedListenersWired) {
                clusterCoordinator.registerListener(new ClusterEventListener(clusterCoordinator.getThisNodeId()));
                ScheduledTaskManager scheduledTaskManager = dataHolder.getTaskManager();
                if (scheduledTaskManager == null) {
                    throw new TaskCoordinationException("Task Manager is not initialized properly.");
                }
                clusterCoordinator.registerListener(new TaskEventListener(scheduledTaskManager, taskStore,
                        resolver));
                setSchedulerProperties();
                getScheduler().getListenerManager().addTriggerListener(
                        new CoordinatedTaskTriggerListener(taskStore, clusterCoordinator.getThisNodeId(),
                                controller));
                hardenedListenersWired = true;
            }
            controller.runConfigFenceComparison();
            if (getTaskService() != null && getTaskService().isServerInit()) {
                // server initialization was already dispatched (a late acquisition / reactivation):
                // this generation runs its own boot-pass bracket, completing in a finally
                String bootId = controller.bootId();
                try {
                    getTaskService().serverInitialized();
                } finally {
                    controller.completeBootPass(bootId);
                }
            }
        } catch (IllegalStateException ex) {
            log.error("The hardened activation sequence aborted; coordinated scheduling will not start on this "
                    + "node.", ex);
        } catch (Throwable activationFailure) {
            log.error("A hardened activation step failed; coordinated scheduling stays un-started and the "
                    + "sequence retries on the lease backoff loop. " + activationFailure.getMessage(), activationFailure);
            scheduleHardenedActivationRetry(bundleContext);
        }
    }

    private void scheduleHardenedActivationRetry(final BundleContext bundleContext) {

        if (hardenedSequenceExecutor == null) {
            hardenedSequenceExecutor = Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder().setNameFormat("HardenedActivationSequence-%d").build());
        }
        CoordinationHardeningConfig config = TaskHandlingConfigUtils.getConfig();
        long interval = config.getHeartbeatIntervalMillis() > 0 ? config.getHeartbeatIntervalMillis() : 5000L;
        long window = interval * Math.max(1, config.getHeartbeatMaxRetries());
        if (hardenedRetryDelayMillis <= 0) {
            hardenedRetryDelayMillis = interval;
        }
        long delay = hardenedRetryDelayMillis;
        hardenedRetryDelayMillis = Math.min(window, Math.max(interval, delay * 2));
        hardenedSequenceExecutor.schedule(() -> runHardenedActivationSequence(bundleContext), delay,
                TimeUnit.MILLISECONDS);
    }

    /**
     * The schema-present GATE: an ARMED node's activation sequence proves the two hardening tables
     * exist before the boot lease touches them — a probe failure raises the condition (blocking the
     * ALL-GREEN activation runbook) and refuses the attempt onto the lease backoff loop; a later
     * successful probe clears it.
     */
    private void verifyHardeningSchemaPresent(DataSource coordinationDataSource) throws TaskCoordinationException {

        String[][] probes = {{"COORDINATED_TASK_CLAIM", "TASK_NAME"}, {"NODE_ADVERTISEMENT", "NODE_ID"}};
        List<String> missing = new ArrayList<>();
        String failure = null;
        for (String[] tableAndKey : probes) {
            try (Connection connection = coordinationDataSource.getConnection();
                    PreparedStatement probe = connection.prepareStatement(
                            "SELECT " + tableAndKey[1] + " FROM " + tableAndKey[0] + " WHERE " + tableAndKey[1]
                                    + " = ?")) {
                probe.setString(1, "");
                probe.executeQuery().close();
            } catch (SQLException ex) {
                missing.add(tableAndKey[0]);
                failure = ex.getMessage();
            }
        }
        if (missing.isEmpty()) {
            readinessRegistry.clear(SCHEMA_PRESENT_CONDITION);
            return;
        }
        readinessRegistry.raise(SCHEMA_PRESENT_CONDITION, CoordinationReadinessRegistry.ConditionClass.GATE,
                "the hardening schema could not be proven present — unreadable table(s) " + missing
                        + ": " + failure + ". Apply the coordination-hardening DDL to the coordination DB.");
        throw new TaskCoordinationException("The hardening schema is not provably present (table(s) " + missing
                + "); the activation sequence refuses and retries on the lease backoff loop.");
    }

    private static String resolveLocalGroupId() {

        String localGroupId = System.getProperty(RDBMSConstantUtils.LOCAL_GROUP_ID);
        if (localGroupId == null || localGroupId.isEmpty()) {
            localGroupId = System.getenv(RDBMSConstantUtils.LOCAL_GROUP_ID);
        }
        if (localGroupId == null || localGroupId.isEmpty()) {
            localGroupId = RDBMSConstantUtils.DEFAULT_LOCAL_GROUP_ID;
        }
        return localGroupId;
    }

    /**
     * The master-parse / strict-parse step of the first activation: decides the coordination bootstrap
     * tri-state once per JVM and installs it before either bootstrap starts. Master OFF (default) -> STOCK;
     * master unparseable -> STOCK plus the continuous readiness condition hardening-config-contradiction
     * (fail-stock-and-loud); master ON with an invalid hardened profile -> REFUSING_PROFILE_INVALID plus the
     * GATE readiness condition hardening-profile-invalid; master ON with a valid profile -> ARMED with the
     * immutable startup snapshot installed.
     */
    private void decideCoordinationBootstrapState() {

        Map<String, Object> configs = ConfigParser.getParsedConfigs();
        String masterRaw = readRawConfigValue(configs, COORDINATION_HARDENING_CONFIG);
        String barrierRaw = readRawConfigValue(configs, TaskHandlingConfigUtils.TASK_DELETE_BARRIER_ENABLED_CONFIG);
        String syncInjectRaw = readRawConfigValue(configs, SYNCHRONOUS_INJECTION_CONFIG);

        if (masterRaw == null || "false".equalsIgnoreCase(masterRaw.trim())) {
            TaskHandlingConfigUtils.install(CoordinationBootstrapState.STOCK, null);
            readinessRegistry.raise(HARDENING_DISABLED_CONDITION, CoordinationReadinessRegistry.ConditionClass.GATE,
                    "coordination hardening disabled — stock task handling");
            log.info("coordination hardening disabled — stock task handling");
            return;
        }
        if (!"true".equalsIgnoreCase(masterRaw.trim())) {
            TaskHandlingConfigUtils.install(CoordinationBootstrapState.STOCK, null);
            readinessRegistry.raise(HARDENING_DISABLED_CONDITION, CoordinationReadinessRegistry.ConditionClass.GATE,
                    "coordination hardening disabled — stock task handling");
            readinessRegistry.raise(HARDENING_CONFIG_CONTRADICTION_CONDITION,
                    CoordinationReadinessRegistry.ConditionClass.CONTINUOUS,
                    "[" + COORDINATION_HARDENING_CONFIG + "] value [" + masterRaw + "] is unparseable; reading OFF");
            log.error("Unparseable [" + COORDINATION_HARDENING_CONFIG + "] value [" + masterRaw
                    + "]. Coordination hardening reads OFF (stock task handling); readiness condition ["
                    + HARDENING_CONFIG_CONTRADICTION_CONDITION + "] raised.");
            return;
        }
        try {
            CoordinationHardeningConfig snapshot = CoordinationHardeningConfig
                    .parse(masterRaw, barrierRaw, syncInjectRaw, resolveHeartBeatIntervalMillis(),
                           resolveHeartBeatMaxRetries());
            TaskHandlingConfigUtils.install(CoordinationBootstrapState.ARMED, snapshot);
        } catch (TaskCoordinationException e) {
            TaskHandlingConfigUtils.install(CoordinationBootstrapState.REFUSING_PROFILE_INVALID, null);
            readinessRegistry.raise(HARDENING_PROFILE_INVALID_CONDITION,
                    CoordinationReadinessRegistry.ConditionClass.GATE, e.getMessage());
            log.error("Coordination hardening profile is invalid; this node refuses to arm. Coordinated "
                    + "scheduling will not start on this node in either mode; local tasks are unaffected. "
                    + "Fix the deployment.toml and restart. " + e.getMessage());
        }
    }

    private static String readRawConfigValue(Map<String, Object> configs, String key) {

        if (configs == null) {
            return null;
        }
        Object value = configs.get(key);
        return value == null ? null : value.toString();
    }

    private static long resolveHeartBeatIntervalMillis() {

        String heartBeatInterval = System.getProperty(RDBMSConstantUtils.HEART_BEAT_INTERVAL);
        if (heartBeatInterval == null) {
            heartBeatInterval = System.getenv(RDBMSConstantUtils.HEART_BEAT_INTERVAL);
        }
        if (heartBeatInterval == null) {
            return RDBMSConstantUtils.DEFAULT_HEART_BEAT_INTERVAL;
        }
        try {
            return Integer.parseInt(heartBeatInterval);
        } catch (NumberFormatException e) {
            return RDBMSConstantUtils.DEFAULT_HEART_BEAT_INTERVAL;
        }
    }

    private static int resolveHeartBeatMaxRetries() {

        String heartBeatMaxRetry = System.getProperty(RDBMSConstantUtils.HEART_BEAT_MAX_RETRY);
        if (heartBeatMaxRetry == null) {
            heartBeatMaxRetry = System.getenv(RDBMSConstantUtils.HEART_BEAT_MAX_RETRY);
        }
        if (heartBeatMaxRetry == null) {
            return RDBMSConstantUtils.DEFAULT_HEART_BEAT_MAX_RETRY;
        }
        try {
            return Integer.parseInt(heartBeatMaxRetry);
        } catch (NumberFormatException e) {
            return RDBMSConstantUtils.DEFAULT_HEART_BEAT_MAX_RETRY;
        }
    }

    private void setSchedulerProperties() {

        Map<String, Object> configs = ConfigParser.getParsedConfigs();
        Object exePeriod = configs.get(TASK_CONFIG + "." + RESOLVING_PERIOD);
        if (exePeriod != null) {
            int executionPeriod = Integer.parseInt(exePeriod.toString());
            if (executionPeriod < 1) {
                log.warn(
                        RESOLVING_PERIOD + " should be greater than or equal to 1 seconds. The value " + executionPeriod
                                + " will be ignored and default 2 will be used.");
            } else {
                CoordinatedTaskScheduleManager.setExecutionPeriod(executionPeriod);
            }
        }
        Object resolvingFreq = configs.get(TASK_CONFIG + "." + RESOLVING_FREQUENCY);
        if (resolvingFreq != null) {
            int resolvingFrequency = Integer.parseInt(resolvingFreq.toString());
            if (resolvingFrequency < 1) {
                log.warn(RESOLVING_FREQUENCY + " should be greater than or equal to 1. The value " + resolvingFrequency
                                 + " will be" + " ignored and default 5 will be used.");
            } else {
                CoordinatedTaskScheduleManager.setResolveFrequency(resolvingFrequency);
            }
        }
    }

    /**
     * Initializes the task resolver.
     *
     * @return - TaskLocationResolver
     */
    @SuppressWarnings(value = "unchecked")
    private TaskLocationResolver getResolver()
            throws ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException,
                   InvocationTargetException, TaskException {

        Map<String, Object> configs = ConfigParser.getParsedConfigs();
        Object resolverClass = configs.get(TASK_CONFIG + "." + RESOLVER_CLASS);
        if (resolverClass == null) {
            return getDefaultResolver();
        }
        String resolverClassName = resolverClass.toString();
        if (log.isDebugEnabled()) {
            log.debug("Task resolver class : " + resolverClassName);
        }
        TaskLocationResolver taskLocationResolver = (TaskLocationResolver) Class.forName(resolverClassName)
                .getDeclaredConstructor().newInstance();
        List<Object> resolverProperties = (List<Object>) configs.get(TASK_RESOLVER);
        if (resolverProperties != null && !resolverProperties.isEmpty()) {
            Map<String, String> properties = (Map<String, String>) resolverProperties.get(0);
            if (log.isDebugEnabled()) {
                properties.forEach((key, value) -> log.debug("Task resolver property :: " + key + ":" + value));
            }
            taskLocationResolver.init(properties);
        }
        return taskLocationResolver;
    }

    /**
     * Get the default resolver.
     *
     * @return - Default Resolver.
     */
    private TaskLocationResolver getDefaultResolver() {
        if (log.isDebugEnabled()) {
            log.debug("Task resolver class : " + ActivePassiveResolver.class.getName());
        }
        return new ActivePassiveResolver();
    }

    /**
     * Check whether the WSO2_COORDINATION_DB is defined.
     *
     * @return - true if datasource defined.
     * @throws DataSourceException
     */
    private boolean isCoordinationDataSourceAvailable() throws DataSourceException {

        CarbonDataSource dataSource = dataSourceService.getDataSource(RDBMSConstantUtils.COORDINATION_DB_NAME);
        if (dataSource == null) {
            return false;
        }
        coordinationDatasourceObject = dataSource.getDSObject();
        if (!(coordinationDatasourceObject instanceof DataSource)) {
            throw new DataSourceException("DataSource is not an RDBMS data source.");
        }
        return true;
    }

    private Properties getStandardQuartzProps() {

        Properties result = new Properties();
        result.put("org.quartz.scheduler.skipUpdateCheck", "true");
        result.put("org.quartz.threadPool.class", QuartzCachedThreadPool.class.getName());
        // Misfire Pin: same as the effective RAMJobStore default; makes the value explicit, survives
        // a Quartz upgrade or store swap (the JDBC store's default is 60 s), shows up in config diffs.
        result.put(MISFIRE_THRESHOLD_PROPERTY, "5000");
        return result;
    }

    /**
     * A quartz.properties full-override with a misfire threshold above the takeover window silently
     * reintroduces a replay band — worth a startup WARN.
     */
    private void warnIfMisfireThresholdOverrideExceedsTakeoverWindow(String quartzConfigFilePath) {

        Properties overrides = new Properties();
        try (FileInputStream in = new FileInputStream(quartzConfigFilePath)) {
            overrides.load(in);
        } catch (IOException e) {
            return;
        }
        String value = overrides.getProperty(MISFIRE_THRESHOLD_PROPERTY);
        if (value == null) {
            return;
        }
        long takeoverWindow = resolveHeartBeatIntervalMillis() * resolveHeartBeatMaxRetries();
        try {
            if (Long.parseLong(value.trim()) > takeoverWindow) {
                log.warn("The quartz.properties override sets " + MISFIRE_THRESHOLD_PROPERTY + " = " + value.trim()
                        + "ms, above the takeover window (" + takeoverWindow + "ms). A woken node may replay "
                        + "missed coordinated fires inside that band.");
            }
        } catch (NumberFormatException ignored) {
        }
    }

    @Deactivate
    protected void deactivate(ComponentContext ctx) {

        // The controller and its lease generation live per component activation: stop it (lease
        // release, BOOT_ID-conditioned) and clear the slot atomically and identity-conditionally so a
        // successor activation's controller is never clobbered.
        BootLeaseController bootLeaseController = getBootLeaseController();
        if (bootLeaseController != null) {
            if (bootLeaseController instanceof BootLeaseControllerImpl) {
                ((BootLeaseControllerImpl) bootLeaseController).stop();
            }
            clearController(bootLeaseController);
        }
        if (hardenedSequenceExecutor != null) {
            hardenedSequenceExecutor.shutdownNow();
        }
        if (pauseWatchdog != null) {
            pauseWatchdog.stop();
            pauseWatchdog = null;
        }

        ScheduledExecutorService executorService = dataHolder.getTaskScheduler();
        if (executorService != null) {
            log.info("Shutting down coordinated task scheduler.");
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(TaskUtils.getMaxWaitTimeInSeconds()
                        , TimeUnit.SECONDS)) {
                    log.warn("Timeout while waiting for coordinated task scheduler. Forcing shutdown.");
                    executorService.shutdownNow();
                } else {
                    log.info("Coordinated task scheduler shut down cleanly.");
                }
            } catch (InterruptedException e) {
                log.warn("Interrupted during coordinated task scheduler shutdown. Forcing shutdown.");
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        if (TasksDSComponent.getScheduler() != null) {
            try {
                TasksDSComponent.getScheduler().shutdown(true);
            } catch (Exception e) {
                log.error(e);
            }
        }
        if (executor != null) {
            log.info("Shutting down global executor.");
            executor.shutdown();
            try {
                if (!executor.awaitTermination(TaskUtils.getMaxWaitTimeInSeconds(), TimeUnit.SECONDS)) {
                    log.warn("Global executor shutdown timed out. Forcing shutdown.");
                    executor.shutdownNow();
                } else {
                    log.info("Global executor shut down cleanly.");
                }
            } catch (InterruptedException e) {
                log.warn("Interrupted during global executor shutdown. Forcing shutdown.");
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        taskService = null;
    }

    public static BootLeaseController getBootLeaseController() {
        return CONTROLLER.get();
    }

    private static void installController(BootLeaseController controller) {
        if (!CONTROLLER.compareAndSet(null, controller)) {
            // A second live controller indicates overlapping component activations. Abort before lease
            // acquisition or worker startup instead of silently reusing or replacing that controller.
            throw new IllegalStateException("BootLeaseController already installed");
        }
    }

    private static void clearController(BootLeaseController controller) {
        // A failed identity-conditioned clear means the slot is already empty or belongs to a newer activation.
        // Neither case requires action, and the newer controller must remain untouched.
        CONTROLLER.compareAndSet(controller, null);
    }

    public static TaskService getTaskService() {

        return taskService;
    }

    public static CoordinationReadinessRegistry getReadinessRegistry() {

        return readinessRegistry;
    }

    public static Scheduler getScheduler() {

        return scheduler;
    }

    public static SecretCallbackHandlerService getSecretCallbackHandlerService() {

        return TasksDSComponent.secretCallbackHandlerService;
    }

    @Reference(name = "secret.callback.handler.service",
            service = org.wso2.carbon.securevault.SecretCallbackHandlerService.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unsetSecretCallbackHandlerService")
    protected void setSecretCallbackHandlerService(SecretCallbackHandlerService secretCallbackHandlerService) {

        TasksDSComponent.secretCallbackHandlerService = secretCallbackHandlerService;
    }

    protected void unsetSecretCallbackHandlerService(SecretCallbackHandlerService secretCallbackHandlerService) {

        TasksDSComponent.secretCallbackHandlerService = null;
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

}

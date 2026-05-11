/*
 * Copyright (c) 2026, WSO2 LLC (http://www.wso2.com).
 *
 * WSO2 LLC licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.carbon.esb.car.deployment.test;

import org.wso2.carbon.automation.engine.context.AutomationContext;
import org.wso2.esb.integration.common.extensions.carbonserver.CarbonServerExtension;
import org.wso2.esb.integration.common.utils.CarbonLogReader;
import org.wso2.esb.integration.common.utils.ESBIntegrationTest;
import org.wso2.esb.integration.common.utils.ESBTestConstant;
import org.wso2.esb.integration.common.utils.common.ServerConfigurationManager;
import org.wso2.esb.integration.common.utils.common.TestConfigurationProvider;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/**
 * Base class for priority-based CApp deployment integration tests.
 *
 * <p>Provides shared constants (CApp names, log prefixes, deployment.toml paths), the
 * server lifecycle helpers {@link #startServer} / {@link #stopServer}, and small assertion
 * utilities used across every scenario subclass.
 *
 * <p>CApp naming conventions used across all subclasses:
 * <ul>
 *   <li>{@code Z_*} — high-priority (contain {@code lib/synapse/mediator}, {@code synapse/lib},
 *       or {@code registry/resource} artifacts). Alphabetically last, so without priority
 *       ordering they would deploy after {@code A_*}.</li>
 *   <li>{@code A_*} — low-priority (plain APIs / proxy services). Alphabetically first.</li>
 *   <li>{@code Redeploy_A_*} / {@code Redeploy_B_*} — high-priority pair used to exercise
 *       the automatic retry: {@code A} sorts before {@code B}, causing it to fail on the
 *       first pass and succeed on the retry once {@code B}'s mediator is available.</li>
 * </ul>
 */
public abstract class CAppPriorityDeploymentTestBase extends ESBIntegrationTest {

    // -------------------------------------------------------------------------
    // Resource paths
    // -------------------------------------------------------------------------

    protected static final String CAR_DIR =
            TestConfigurationProvider.getResourceLocation(ESBTestConstant.ESB_PRODUCT_GROUP)
                    + File.separator + "car" + File.separator;

    protected static final String PRIORITY_ENABLED_TOML =
            CAR_DIR + "priorityDeployment" + File.separator + "deployment.toml";
    protected static final String PRIORITY_DISABLED_TOML =
            CAR_DIR + "priorityDeploymentDisabled" + File.separator + "deployment.toml";
    protected static final String NO_PRIORITY_CONFIG_TOML =
            CAR_DIR + "noPriorityConfig" + File.separator + "deployment.toml";
    protected static final String PRIORITY_RETRY_COUNT_ZERO_TOML =
            CAR_DIR + "priorityDeploymentRetryCountZero" + File.separator + "deployment.toml";
    protected static final String PRIORITY_RETRY_COUNT_ONE_TOML =
            CAR_DIR + "priorityDeploymentRetryCountOne" + File.separator + "deployment.toml";
    protected static final String PRIORITY_RETRY_COUNT_MULTIPLE_TOML =
            CAR_DIR + "priorityDeploymentRetryCountMultiple" + File.separator + "deployment.toml";

    // -------------------------------------------------------------------------
    // High-priority CApp names (alphabetical within the group)
    // -------------------------------------------------------------------------

    protected static final String CLASS_MEDIATOR_CAPP      = "Z_A_ClassMediatorCApp_1.0.0";
    protected static final String SYNAPSE_LIB_A_CAPP       = "Z_B_SynapseLibACApp_1.0.0";
    protected static final String SYNAPSE_LIB_B_CAPP       = "Z_C_SynapseLibBCApp_1.0.0";
    protected static final String REGISTRY_RESOURCE_A_CAPP = "Z_D_RegistryResourceACApp_1.0.0";
    protected static final String REGISTRY_RESOURCE_B_CAPP = "Z_E_RegistryResourceBCApp_1.0.0";

    // -------------------------------------------------------------------------
    // Low-priority CApp names
    // -------------------------------------------------------------------------

    /** Low-priority proxy whose class mediator is provided by {@link #CLASS_MEDIATOR_CAPP}. */
    protected static final String DEPENDENT_PROXY_CAPP = "A_A_DependentProxyCApp_1.0.0";
    protected static final String PLAIN_API_A_CAPP     = "A_B_PlainApiACApp_1.0.0";
    protected static final String PLAIN_API_B_CAPP     = "A_C_PlainApiBCApp_1.0.0";

    // -------------------------------------------------------------------------
    // Redeploy-scenario CApp names (both high-priority)
    // -------------------------------------------------------------------------

    protected static final String REDEPLOY_DEPENDENT_CAPP = "Redeploy_A_DependentCApp_1.0.0";
    protected static final String REDEPLOY_MEDIATOR_CAPP  = "Redeploy_B_ClassMediatorCApp_1.0.0";

    // -------------------------------------------------------------------------
    // Log message fragments
    // -------------------------------------------------------------------------

    protected static final String DEPLOYED_LOG_PREFIX = "Successfully Deployed Carbon Application : ";
    protected static final String FAILED_LOG_PREFIX   = "Error occurred while deploying the Carbon application: ";
    protected static final String SERVER_STARTED_LOG  = "WSO2 Micro Integrator started in";

    // -------------------------------------------------------------------------
    // Shared infrastructure
    // -------------------------------------------------------------------------

    protected ServerConfigurationManager serverConfigurationManager;
    protected CarbonLogReader carbonLogReader;
    protected String startupLogs;

    /**
     * Shuts down MI, applies {@code toml} as the active deployment configuration, copies each
     * named CApp into the carbonapps directory, starts MI, waits for the startup-complete log,
     * then captures all startup log lines into {@link #startupLogs}.
     *
     * <p>CApps are copied <em>after</em> the server is shut down intentionally: copying a
     * class-mediator CApp to a running server triggers hot-deploy, and a concurrent shutdown
     * can deadlock the Carbon deployer, causing the server to hang indefinitely.
     */
    protected void startServer(File toml, String... cAppNames) throws Exception {
        super.init();
        serverConfigurationManager = new ServerConfigurationManager(new AutomationContext());
        serverConfigurationManager.applyMIConfiguration(toml);

        // carbon.home is cleared on successful shutdown, so capture it first.
        String carbonHome = ServerConfigurationManager.getCarbonHome();
        CarbonServerExtension.shutdownServerSkipCoverage();

        // Copy CApps while the server is stopped to avoid a hot-deploy/shutdown race.
        File carbonappsDir = new File(carbonHome,
                "repository" + File.separator + "deployment" + File.separator + "server" + File.separator
                        + "carbonapps");
        for (String capp : cAppNames) {
            Files.copy(new File(CAR_DIR + capp + ".car").toPath(),
                    new File(carbonappsDir, capp + ".car").toPath(),
                    StandardCopyOption.REPLACE_EXISTING);
        }

        carbonLogReader = new CarbonLogReader();
        carbonLogReader.start();
        CarbonServerExtension.startServer();
        carbonLogReader.checkForLog(SERVER_STARTED_LOG, 120);
        carbonLogReader.stop();
        startupLogs = carbonLogReader.getLogs();
        super.init();
    }

    /**
     * Shuts down MI (without coverage collection), restores the previous deployment.toml, and
     * removes each named CApp's .car file from the carbonapps directory.
     *
     * <p>CApps are removed <em>after</em> the server is shut down intentionally: removing a
     * class-mediator CApp from a running server triggers hot-undeploy, and a concurrent shutdown
     * can deadlock the Carbon deployer, causing the server to hang indefinitely.
     */
    protected void stopServer(String... cAppNames) throws Exception {
        // carbon.home is cleared on successful shutdown, so capture it first.
        String carbonHome = ServerConfigurationManager.getCarbonHome();
        CarbonServerExtension.shutdownServerSkipCoverage();
        serverConfigurationManager.restoreToLastConfiguration(false);

        // Remove CApp files while the server is stopped to avoid a hot-undeploy/shutdown race.
        File carbonappsDir = new File(carbonHome,
                "repository" + File.separator + "deployment" + File.separator + "server" + File.separator
                        + "carbonapps");
        for (String capp : cAppNames) {
            File carFile = new File(carbonappsDir, capp + ".car");
            if (carFile.exists() && !carFile.delete()) {
                log.warn("Failed to delete CApp file during teardown: " + carFile.getAbsolutePath());
            }
        }
    }

    // -------------------------------------------------------------------------
    // Utilities
    // -------------------------------------------------------------------------

    /** Returns the largest value among the supplied integers. */
    protected static int maxOf(int first, int... rest) {
        int max = first;
        for (int v : rest) {
            if (v > max) max = v;
        }
        return max;
    }

    /** Returns the smallest non-negative value among the supplied integers, or -1 if all are negative. */
    protected static int minOf(int first, int... rest) {
        int min = first;
        for (int v : rest) {
            if (v >= 0 && (min < 0 || v < min)) min = v;
        }
        return min;
    }

    /** Returns the number of non-overlapping occurrences of {@code pattern} in {@code text}. */
    protected static int countOccurrences(String text, String pattern) {
        int count = 0;
        int idx = 0;
        while ((idx = text.indexOf(pattern, idx)) != -1) {
            count++;
            idx += pattern.length();
        }
        return count;
    }
}

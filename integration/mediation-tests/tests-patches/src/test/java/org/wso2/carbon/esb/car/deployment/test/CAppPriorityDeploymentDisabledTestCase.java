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

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;

/**
 * Verifies CApp deployment behaviour when priority-based deployment is explicitly disabled
 * via {@code enable_priority_deployment = false} in the {@code [server]} section of
 * {@code deployment.toml}.
 *
 * <p>When the feature is disabled, all CApps are deployed in plain alphabetical order.
 * {@code A_A_DependentProxyCApp} therefore deploys before {@code Z_A_ClassMediatorCApp}
 * and fails because the mediator class is not yet on the classpath.
 */
public class CAppPriorityDeploymentDisabledTestCase extends CAppPriorityDeploymentTestBase {

    // =========================================================================
    // Setup / Teardown
    // =========================================================================

    /**
     * Applies a {@code deployment.toml} that explicitly sets
     * {@code enable_priority_deployment = false} in the {@code [server]} section, copies the
     * eight main ordering CApps, and restarts MI. Without priority deployment, CApps are
     * processed in plain alphabetical order: {@code A_A_DependentProxyCApp} is attempted first
     * and fails because {@code Z_A_ClassMediatorCApp} has not deployed yet.
     */
    @BeforeClass(alwaysRun = true)
    public void setUp() throws Exception {
        startServer(new File(PRIORITY_DISABLED_TOML),
                CLASS_MEDIATOR_CAPP, SYNAPSE_LIB_A_CAPP, SYNAPSE_LIB_B_CAPP,
                REGISTRY_RESOURCE_A_CAPP, REGISTRY_RESOURCE_B_CAPP,
                DEPENDENT_PROXY_CAPP, PLAIN_API_A_CAPP, PLAIN_API_B_CAPP);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown() throws Exception {
        stopServer(CLASS_MEDIATOR_CAPP, SYNAPSE_LIB_A_CAPP, SYNAPSE_LIB_B_CAPP,
                REGISTRY_RESOURCE_A_CAPP, REGISTRY_RESOURCE_B_CAPP,
                DEPENDENT_PROXY_CAPP, PLAIN_API_A_CAPP, PLAIN_API_B_CAPP);
    }

    // =========================================================================
    // Test 1 – Server started successfully
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "Server must complete startup even when a CApp fails to deploy " +
            "(StartupFinalizer log must be present)")
    public void testServerStartedSuccessfully() {
        Assert.assertTrue(startupLogs.contains(SERVER_STARTED_LOG),
                "Server startup log not found — MI may not have started correctly. " +
                        "Expected to find: \"" + SERVER_STARTED_LOG + "\"");
    }

    // =========================================================================
    // Test 2 – CApps deployed in plain alphabetical order (no priority)
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "Without priority deployment, A_* CApps must be attempted before Z_* CApps " +
            "(plain alphabetical order)",
            dependsOnMethods = "testServerStartedSuccessfully")
    public void testCAppsDeployedInPlainAlphabeticalOrder() {
        int dependentProxyAttemptIdx = startupLogs.indexOf(FAILED_LOG_PREFIX + DEPENDENT_PROXY_CAPP);
        int classMediatorSuccessIdx  = startupLogs.indexOf(DEPLOYED_LOG_PREFIX + CLASS_MEDIATOR_CAPP);

        Assert.assertTrue(dependentProxyAttemptIdx >= 0,
                DEPENDENT_PROXY_CAPP + " should have been attempted (and failed) before " +
                        CLASS_MEDIATOR_CAPP + " was deployed");
        Assert.assertTrue(classMediatorSuccessIdx >= 0,
                CLASS_MEDIATOR_CAPP + " should have deployed successfully");
        Assert.assertTrue(dependentProxyAttemptIdx < classMediatorSuccessIdx,
                "A_A_DependentProxyCApp must be attempted before Z_A_ClassMediatorCApp when " +
                        "priority deployment is disabled (plain alphabetical order). " +
                        "dependentProxyAttemptIdx=" + dependentProxyAttemptIdx +
                        ", classMediatorSuccessIdx=" + classMediatorSuccessIdx);
    }

    // =========================================================================
    // Test 3 – Dependent proxy fails because class mediator is not deployed first
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "A_A_DependentProxyCApp must fail to deploy because its class mediator has not " +
            "been deployed yet (priority deployment is disabled)",
            dependsOnMethods = "testServerStartedSuccessfully")
    public void testDependentProxyFailsWithoutPriorityDeployment() {
        Assert.assertTrue(startupLogs.contains(FAILED_LOG_PREFIX + DEPENDENT_PROXY_CAPP),
                DEPENDENT_PROXY_CAPP + " should have failed to deploy because the class mediator " +
                        "deploys after it in alphabetical order when priority deployment is disabled");
    }

    // =========================================================================
    // Test 4 – Dependent proxy CApp has no success log entry
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "A_A_DependentProxyCApp must not have a success log entry because it failed to deploy",
            dependsOnMethods = "testDependentProxyFailsWithoutPriorityDeployment")
    public void testDependentProxyNotAccessible() {
        Assert.assertFalse(startupLogs.contains(DEPLOYED_LOG_PREFIX + DEPENDENT_PROXY_CAPP),
                "'" + DEPENDENT_PROXY_CAPP + "' should not appear in the success deployment log — " +
                        "the CApp should have failed when priority deployment is disabled");
    }

    // =========================================================================
    // Test 5 – High-priority CApps still deploy successfully
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "All Z_* CApps must deploy successfully regardless of the priority deployment flag",
            dependsOnMethods = "testServerStartedSuccessfully")
    public void testHighPriorityCAppsStillDeploySuccessfully() {
        String[] highPriorityCApps = {
                CLASS_MEDIATOR_CAPP, SYNAPSE_LIB_A_CAPP, SYNAPSE_LIB_B_CAPP,
                REGISTRY_RESOURCE_A_CAPP, REGISTRY_RESOURCE_B_CAPP
        };
        for (String capp : highPriorityCApps) {
            Assert.assertTrue(startupLogs.contains(DEPLOYED_LOG_PREFIX + capp),
                    "CApp should have deployed successfully regardless of priority flag: " + capp);
        }
    }
}

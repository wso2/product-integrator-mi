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
 * Verifies priority-based CApp deployment when <em>only</em> low-priority CApps are present.
 *
 * <p>{@code A_B_PlainApiACApp} and {@code A_C_PlainApiBCApp} (plain APIs with no cross-CApp
 * dependencies) are deployed with {@code enable_priority_deployment = true} and no high-priority
 * CApps present. When no high-priority CApps exist, {@code highPriorityCAppCount} is zero so
 * the retry phase is never triggered. Both CApps must deploy successfully in alphabetical order.
 */
public class CAppPriorityOnlyLowPriorityTestCase extends CAppPriorityDeploymentTestBase {

    // =========================================================================
    // Setup / Teardown
    // =========================================================================

    @BeforeClass(alwaysRun = true)
    public void setUp() throws Exception {
        startServer(new File(PRIORITY_ENABLED_TOML), PLAIN_API_A_CAPP, PLAIN_API_B_CAPP);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown() throws Exception {
        stopServer(PLAIN_API_A_CAPP, PLAIN_API_B_CAPP);
    }

    // =========================================================================
    // Test 1 – Server started successfully
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "Server must complete startup when only low-priority CApps are present")
    public void testServerStartedSuccessfully() {
        Assert.assertTrue(startupLogs.contains(SERVER_STARTED_LOG),
                "Server startup log not found — MI may not have started correctly. " +
                        "Expected to find: \"" + SERVER_STARTED_LOG + "\"");
    }

    // =========================================================================
    // Test 2 – Both low-priority CApps deployed successfully
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "Both low-priority CApps must deploy successfully when no high-priority CApps are present",
            dependsOnMethods = "testServerStartedSuccessfully")
    public void testAllLowPriorityCAppsDeployed() {
        Assert.assertTrue(startupLogs.contains(DEPLOYED_LOG_PREFIX + PLAIN_API_A_CAPP),
                "CApp was not deployed successfully: " + PLAIN_API_A_CAPP);
        Assert.assertTrue(startupLogs.contains(DEPLOYED_LOG_PREFIX + PLAIN_API_B_CAPP),
                "CApp was not deployed successfully: " + PLAIN_API_B_CAPP);
    }

    // =========================================================================
    // Test 3 – Deployed in alphabetical order
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "Low-priority CApps must be deployed in alphabetical order (A_B_* before A_C_*)",
            dependsOnMethods = "testAllLowPriorityCAppsDeployed")
    public void testLowPriorityCAppsDeployedInAlphabeticalOrder() {
        int idxA = startupLogs.indexOf(DEPLOYED_LOG_PREFIX + PLAIN_API_A_CAPP);
        int idxB = startupLogs.indexOf(DEPLOYED_LOG_PREFIX + PLAIN_API_B_CAPP);

        Assert.assertTrue(idxA < idxB,
                PLAIN_API_A_CAPP + " must deploy before " + PLAIN_API_B_CAPP +
                        ". idxA=" + idxA + ", idxB=" + idxB);
    }

    // =========================================================================
    // Test 4 – No deployment failures
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "No deployment failures must occur when only independent low-priority CApps are present",
            dependsOnMethods = "testServerStartedSuccessfully")
    public void testNoDeploymentFailures() {
        Assert.assertFalse(startupLogs.contains(FAILED_LOG_PREFIX + PLAIN_API_A_CAPP),
                "Unexpected deployment failure for: " + PLAIN_API_A_CAPP);
        Assert.assertFalse(startupLogs.contains(FAILED_LOG_PREFIX + PLAIN_API_B_CAPP),
                "Unexpected deployment failure for: " + PLAIN_API_B_CAPP);
    }
}

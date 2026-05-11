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
 * Verifies priority-based CApp deployment when <em>only</em> high-priority CApps are present.
 *
 * <p>All five {@code Z_*} CApps (class mediator, two synapse libs, two registry resources) are
 * deployed with {@code enable_priority_deployment = true} and no low-priority CApps present.
 * The deployer must classify all of them as high-priority, sort them alphabetically, and deploy
 * them all successfully in a single pass with no retry needed.
 */
public class CAppPriorityOnlyHighPriorityTestCase extends CAppPriorityDeploymentTestBase {

    // =========================================================================
    // Setup / Teardown
    // =========================================================================

    @BeforeClass(alwaysRun = true)
    public void setUp() throws Exception {
        startServer(new File(PRIORITY_ENABLED_TOML),
                CLASS_MEDIATOR_CAPP, SYNAPSE_LIB_A_CAPP, SYNAPSE_LIB_B_CAPP,
                REGISTRY_RESOURCE_A_CAPP, REGISTRY_RESOURCE_B_CAPP);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown() throws Exception {
        stopServer(CLASS_MEDIATOR_CAPP, SYNAPSE_LIB_A_CAPP, SYNAPSE_LIB_B_CAPP,
                REGISTRY_RESOURCE_A_CAPP, REGISTRY_RESOURCE_B_CAPP);
    }

    // =========================================================================
    // Test 1 – Server started successfully
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "Server must complete startup when only high-priority CApps are present")
    public void testServerStartedSuccessfully() {
        Assert.assertTrue(startupLogs.contains(SERVER_STARTED_LOG),
                "Server startup log not found — MI may not have started correctly. " +
                        "Expected to find: \"" + SERVER_STARTED_LOG + "\"");
    }

    // =========================================================================
    // Test 2 – All high-priority CApps deployed successfully
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "All high-priority CApps must deploy successfully when no low-priority CApps are present",
            dependsOnMethods = "testServerStartedSuccessfully")
    public void testAllHighPriorityCAppsDeployed() {
        String[] cApps = {
                CLASS_MEDIATOR_CAPP, SYNAPSE_LIB_A_CAPP, SYNAPSE_LIB_B_CAPP,
                REGISTRY_RESOURCE_A_CAPP, REGISTRY_RESOURCE_B_CAPP
        };
        for (String capp : cApps) {
            Assert.assertTrue(startupLogs.contains(DEPLOYED_LOG_PREFIX + capp),
                    "CApp was not deployed successfully: " + capp);
        }
    }

    // =========================================================================
    // Test 3 – Deployed in strict alphabetical order
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "High-priority CApps must be deployed in alphabetical order (Z_A → Z_B → Z_C → Z_D → Z_E)",
            dependsOnMethods = "testAllHighPriorityCAppsDeployed")
    public void testHighPriorityCAppsDeployedInAlphabeticalOrder() {
        int idxA = startupLogs.indexOf(DEPLOYED_LOG_PREFIX + CLASS_MEDIATOR_CAPP);
        int idxB = startupLogs.indexOf(DEPLOYED_LOG_PREFIX + SYNAPSE_LIB_A_CAPP);
        int idxC = startupLogs.indexOf(DEPLOYED_LOG_PREFIX + SYNAPSE_LIB_B_CAPP);
        int idxD = startupLogs.indexOf(DEPLOYED_LOG_PREFIX + REGISTRY_RESOURCE_A_CAPP);
        int idxE = startupLogs.indexOf(DEPLOYED_LOG_PREFIX + REGISTRY_RESOURCE_B_CAPP);

        Assert.assertTrue(idxA < idxB,
                CLASS_MEDIATOR_CAPP + " must deploy before " + SYNAPSE_LIB_A_CAPP +
                        ". idxA=" + idxA + ", idxB=" + idxB);
        Assert.assertTrue(idxB < idxC,
                SYNAPSE_LIB_A_CAPP + " must deploy before " + SYNAPSE_LIB_B_CAPP +
                        ". idxB=" + idxB + ", idxC=" + idxC);
        Assert.assertTrue(idxC < idxD,
                SYNAPSE_LIB_B_CAPP + " must deploy before " + REGISTRY_RESOURCE_A_CAPP +
                        ". idxC=" + idxC + ", idxD=" + idxD);
        Assert.assertTrue(idxD < idxE,
                REGISTRY_RESOURCE_A_CAPP + " must deploy before " + REGISTRY_RESOURCE_B_CAPP +
                        ". idxD=" + idxD + ", idxE=" + idxE);
    }

    // =========================================================================
    // Test 4 – No failures
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "No CApp deployment failures must occur when only high-priority CApps are present",
            dependsOnMethods = "testServerStartedSuccessfully")
    public void testNoDeploymentFailures() {
        String[] cApps = {
                CLASS_MEDIATOR_CAPP, SYNAPSE_LIB_A_CAPP, SYNAPSE_LIB_B_CAPP,
                REGISTRY_RESOURCE_A_CAPP, REGISTRY_RESOURCE_B_CAPP
        };
        for (String capp : cApps) {
            Assert.assertFalse(startupLogs.contains(FAILED_LOG_PREFIX + capp),
                    "Unexpected deployment failure for: " + capp);
        }
    }
}

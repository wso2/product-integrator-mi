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
 * Verifies that a low-priority CApp that fails on the initial deployment pass is <em>not</em>
 * retried.
 *
 * <p>Setup: {@code Z_B_SynapseLibACApp} (high-priority) and {@code A_A_DependentProxyCApp}
 * (low-priority) are deployed with priority deployment enabled.
 * {@code Z_A_ClassMediatorCApp} — which provides the class mediator that
 * {@code A_A_DependentProxyCApp}'s proxy depends on — is intentionally absent.
 *
 * <p>Expected sequence:
 * <ol>
 *   <li>{@code Z_B_SynapseLibACApp} deploys successfully (high-priority, no dependencies).</li>
 *   <li>The retry pass fires after the high-priority phase: {@code faultyCapps} is empty so
 *       nothing is retried.</li>
 *   <li>{@code A_A_DependentProxyCApp} deploys as low-priority and fails — the mediator class
 *       is not on the classpath.</li>
 *   <li>No further retry occurs: the retry pass already completed and low-priority CApps are
 *       never retried.</li>
 * </ol>
 *
 * <p>The key assertion is that the failure log entry for {@code A_A_DependentProxyCApp} appears
 * <em>exactly once</em>, proving no retry was attempted.
 */
public class CAppPriorityLowPriorityNotRetriedTestCase extends CAppPriorityDeploymentTestBase {

    // =========================================================================
    // Setup / Teardown
    // =========================================================================

    @BeforeClass(alwaysRun = true)
    public void setUp() throws Exception {
        // Z_B is high-priority (synapse/lib); A_A is low-priority (proxy-service).
        // Z_A (the class-mediator dependency of A_A) is intentionally absent so A_A fails.
        startServer(new File(PRIORITY_ENABLED_TOML), SYNAPSE_LIB_A_CAPP, DEPENDENT_PROXY_CAPP);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown() throws Exception {
        stopServer(SYNAPSE_LIB_A_CAPP, DEPENDENT_PROXY_CAPP);
    }

    // =========================================================================
    // Test 1 – Server started successfully
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "Server must complete startup even when a low-priority CApp fails to deploy")
    public void testServerStartedSuccessfully() {
        Assert.assertTrue(startupLogs.contains(SERVER_STARTED_LOG),
                "Server startup log not found — MI may not have started correctly. " +
                        "Expected to find: \"" + SERVER_STARTED_LOG + "\"");
    }

    // =========================================================================
    // Test 2 – High-priority CApp deployed successfully
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "The high-priority CApp must deploy successfully",
            dependsOnMethods = "testServerStartedSuccessfully")
    public void testHighPriorityCAppDeployedSuccessfully() {
        Assert.assertTrue(startupLogs.contains(DEPLOYED_LOG_PREFIX + SYNAPSE_LIB_A_CAPP),
                SYNAPSE_LIB_A_CAPP + " should have deployed successfully");
        Assert.assertFalse(startupLogs.contains(FAILED_LOG_PREFIX + SYNAPSE_LIB_A_CAPP),
                SYNAPSE_LIB_A_CAPP + " should not appear in the failure log");
    }

    // =========================================================================
    // Test 3 – Low-priority CApp fails
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "The low-priority CApp that is missing its dependency must fail to deploy",
            dependsOnMethods = "testServerStartedSuccessfully")
    public void testLowPriorityCAppFails() {
        Assert.assertTrue(startupLogs.contains(FAILED_LOG_PREFIX + DEPENDENT_PROXY_CAPP),
                DEPENDENT_PROXY_CAPP + " should have failed to deploy — its class mediator is absent");
        Assert.assertFalse(startupLogs.contains(DEPLOYED_LOG_PREFIX + DEPENDENT_PROXY_CAPP),
                DEPENDENT_PROXY_CAPP + " must not appear in the success log");
    }

    // =========================================================================
    // Test 4 – Low-priority CApp is NOT retried
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "The failure log entry for the low-priority CApp must appear exactly once — " +
            "low-priority CApps do not receive a retry pass",
            dependsOnMethods = "testLowPriorityCAppFails")
    public void testLowPriorityCAppNotRetried() {
        int failureCount = countOccurrences(startupLogs, FAILED_LOG_PREFIX + DEPENDENT_PROXY_CAPP);
        Assert.assertEquals(failureCount, 1,
                DEPENDENT_PROXY_CAPP + " must appear in the failure log exactly once. " +
                        "More than one occurrence means the low-priority CApp was retried unexpectedly. " +
                        "Actual failure count: " + failureCount);
    }
}

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
 * Verifies that setting {@code priority_deployment_retry_count = 0} in the {@code [server]}
 * section of {@code deployment.toml} completely disables the faulty-CApp retry pass.
 *
 * <p>Setup: {@code Redeploy_A_DependentCApp} (high-priority, depends on the class mediator
 * provided by {@code Redeploy_B_ClassMediatorCApp}) and {@code Redeploy_B_ClassMediatorCApp}
 * (high-priority, provides the mediator) are deployed with priority deployment enabled but
 * the retry count set to 0.
 *
 * <p>{@code Redeploy_A_*} sorts alphabetically before {@code Redeploy_B_*}, so it is attempted
 * first. Its mediator dependency is not on the classpath yet, causing it to fail. After
 * {@code Redeploy_B_*} deploys successfully the mediator is available — but with
 * {@code priority_deployment_retry_count = 0} no retry pass is triggered.
 *
 * <p>Expected sequence:
 * <ol>
 *   <li>{@code Redeploy_A_DependentCApp} — fails (mediator not yet deployed).</li>
 *   <li>{@code Redeploy_B_ClassMediatorCApp} — succeeds; mediator now on classpath.</li>
 *   <li>No retry pass fires — {@code Redeploy_A_DependentCApp} remains faulty.</li>
 * </ol>
 *
 * <p>Key assertions:
 * <ul>
 *   <li>{@code Redeploy_A_*} has exactly one failure log entry (no retry).</li>
 *   <li>{@code Redeploy_A_*} has no success log entry.</li>
 *   <li>{@code Redeploy_B_*} deploys successfully.</li>
 * </ul>
 */
public class CAppPriorityRetryCountZeroTestCase extends CAppPriorityDeploymentTestBase {

    // =========================================================================
    // Setup / Teardown
    // =========================================================================

    @BeforeClass(alwaysRun = true)
    public void setUp() throws Exception {
        startServer(new File(PRIORITY_RETRY_COUNT_ZERO_TOML),
                REDEPLOY_DEPENDENT_CAPP, REDEPLOY_MEDIATOR_CAPP);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown() throws Exception {
        stopServer(REDEPLOY_DEPENDENT_CAPP, REDEPLOY_MEDIATOR_CAPP);
    }

    // =========================================================================
    // Test 1 – Server started successfully
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "Server must complete startup even when a high-priority CApp remains faulty")
    public void testServerStartedSuccessfully() {
        Assert.assertTrue(startupLogs.contains(SERVER_STARTED_LOG),
                "Server startup log not found — MI may not have started correctly. " +
                        "Expected to find: \"" + SERVER_STARTED_LOG + "\"");
    }

    // =========================================================================
    // Test 2 – Mediator CApp deployed successfully
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "The CApp providing the class mediator must deploy successfully",
            dependsOnMethods = "testServerStartedSuccessfully")
    public void testMediatorCAppDeployedSuccessfully() {
        Assert.assertTrue(startupLogs.contains(DEPLOYED_LOG_PREFIX + REDEPLOY_MEDIATOR_CAPP),
                REDEPLOY_MEDIATOR_CAPP + " should have deployed successfully");
        Assert.assertFalse(startupLogs.contains(FAILED_LOG_PREFIX + REDEPLOY_MEDIATOR_CAPP),
                REDEPLOY_MEDIATOR_CAPP + " should not appear in the failure log");
    }

    // =========================================================================
    // Test 3 – Dependent CApp fails on the initial pass
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "The dependent CApp must fail on the initial deployment pass because the mediator " +
            "has not been deployed yet when it is first attempted",
            dependsOnMethods = "testServerStartedSuccessfully")
    public void testDependentCAppFailsOnFirstPass() {
        Assert.assertTrue(startupLogs.contains(FAILED_LOG_PREFIX + REDEPLOY_DEPENDENT_CAPP),
                REDEPLOY_DEPENDENT_CAPP + " should have failed on the first deployment pass");
    }

    // =========================================================================
    // Test 4 – No retry: dependent CApp is never retried
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "With priority_deployment_retry_count = 0 the failure log entry must appear exactly " +
            "once — no retry pass is triggered",
            dependsOnMethods = "testDependentCAppFailsOnFirstPass")
    public void testDependentCAppNotRetried() {
        int failureCount = countOccurrences(startupLogs, FAILED_LOG_PREFIX + REDEPLOY_DEPENDENT_CAPP);
        Assert.assertEquals(failureCount, 1,
                REDEPLOY_DEPENDENT_CAPP + " must appear in the failure log exactly once. " +
                        "More than one occurrence means a retry was triggered despite retry_count=0. " +
                        "Actual failure count: " + failureCount);
    }

    @Test(groups = {"wso2.esb"}, description =
            "The dependent CApp must not appear in the success log — it was never retried",
            dependsOnMethods = "testDependentCAppNotRetried")
    public void testDependentCAppRemainsUndeployed() {
        Assert.assertFalse(startupLogs.contains(DEPLOYED_LOG_PREFIX + REDEPLOY_DEPENDENT_CAPP),
                REDEPLOY_DEPENDENT_CAPP + " must not appear in the success log when retry is disabled");
    }
}

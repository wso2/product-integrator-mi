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
 * Verifies the retry behaviour when {@code priority_deployment_retry_count = 1} is explicitly set
 * in the {@code [server]} section of {@code deployment.toml}.
 *
 * <p>Setup: {@code Redeploy_A_DependentCApp} (high-priority, depends on the class mediator from
 * {@code Redeploy_B_ClassMediatorCApp}) and {@code Redeploy_B_ClassMediatorCApp} (high-priority,
 * provides the mediator) are deployed.
 *
 * <p>{@code Redeploy_A_*} sorts alphabetically before {@code Redeploy_B_*}, so it is attempted
 * first. Its mediator class is not on the classpath yet — it fails. After {@code Redeploy_B_*}
 * deploys successfully the single retry pass fires and {@code Redeploy_A_*} succeeds.
 *
 * <p>Expected sequence:
 * <ol>
 *   <li>{@code Redeploy_A_DependentCApp} — fails (mediator not yet deployed).</li>
 *   <li>{@code Redeploy_B_ClassMediatorCApp} — succeeds; mediator now on classpath.</li>
 *   <li>Retry pass 1 of 1 fires: {@code Redeploy_A_DependentCApp} — succeeds on retry.</li>
 * </ol>
 *
 * <p>Key assertions:
 * <ul>
 *   <li>{@code Redeploy_A_*} has exactly one failure entry and exactly one success entry.</li>
 *   <li>The success entry appears after the failure entry in the log.</li>
 *   <li>{@code Redeploy_B_*} has a success entry and no failure entry.</li>
 * </ul>
 */
public class CAppPriorityRetryCountOneTestCase extends CAppPriorityDeploymentTestBase {

    // =========================================================================
    // Setup / Teardown
    // =========================================================================

    @BeforeClass(alwaysRun = true)
    public void setUp() throws Exception {
        startServer(new File(PRIORITY_RETRY_COUNT_ONE_TOML),
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
            "Server must complete startup with retry_count = 1")
    public void testServerStartedSuccessfully() {
        Assert.assertTrue(startupLogs.contains(SERVER_STARTED_LOG),
                "Server startup log not found — MI may not have started correctly. " +
                        "Expected to find: \"" + SERVER_STARTED_LOG + "\"");
    }

    // =========================================================================
    // Test 2 – Mediator CApp deployed successfully
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "The CApp providing the class mediator must deploy successfully on the first pass",
            dependsOnMethods = "testServerStartedSuccessfully")
    public void testMediatorCAppDeployedSuccessfully() {
        Assert.assertTrue(startupLogs.contains(DEPLOYED_LOG_PREFIX + REDEPLOY_MEDIATOR_CAPP),
                REDEPLOY_MEDIATOR_CAPP + " should have deployed successfully");
        Assert.assertFalse(startupLogs.contains(FAILED_LOG_PREFIX + REDEPLOY_MEDIATOR_CAPP),
                REDEPLOY_MEDIATOR_CAPP + " should not appear in the failure log");
    }

    // =========================================================================
    // Test 3 – Dependent CApp fails on first pass, succeeds on retry
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "The dependent CApp must fail on the first pass (mediator not yet deployed) " +
            "and succeed on the single retry pass",
            dependsOnMethods = "testMediatorCAppDeployedSuccessfully")
    public void testDependentCAppSucceedsOnRetry() {
        int firstFailIdx    = startupLogs.indexOf(FAILED_LOG_PREFIX   + REDEPLOY_DEPENDENT_CAPP);
        int mediatorDeployIdx = startupLogs.indexOf(DEPLOYED_LOG_PREFIX + REDEPLOY_MEDIATOR_CAPP);
        int retrySuccessIdx = startupLogs.indexOf(DEPLOYED_LOG_PREFIX + REDEPLOY_DEPENDENT_CAPP);

        Assert.assertTrue(firstFailIdx >= 0,
                REDEPLOY_DEPENDENT_CAPP + " should have failed on the first deployment pass");
        Assert.assertTrue(mediatorDeployIdx >= 0,
                REDEPLOY_MEDIATOR_CAPP + " should have deployed successfully");
        Assert.assertTrue(retrySuccessIdx >= 0,
                REDEPLOY_DEPENDENT_CAPP + " should have succeeded on the retry pass");

        Assert.assertTrue(firstFailIdx < mediatorDeployIdx,
                REDEPLOY_DEPENDENT_CAPP + " must fail before " + REDEPLOY_MEDIATOR_CAPP + " deploys. " +
                        "failIdx=" + firstFailIdx + ", mediatorIdx=" + mediatorDeployIdx);
        Assert.assertTrue(mediatorDeployIdx < retrySuccessIdx,
                REDEPLOY_DEPENDENT_CAPP + " retry success must appear after " + REDEPLOY_MEDIATOR_CAPP +
                        " deploys. mediatorIdx=" + mediatorDeployIdx + ", retrySuccessIdx=" + retrySuccessIdx);
    }

    // =========================================================================
    // Test 4 – Exactly one failure and one success: no extra retries
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "The dependent CApp must appear in the failure log exactly once (one failed first pass) " +
            "and in the success log exactly once (one successful retry) — no extra retry passes",
            dependsOnMethods = "testDependentCAppSucceedsOnRetry")
    public void testDependentCAppRetriedExactlyOnce() {
        int failureCount = countOccurrences(startupLogs, FAILED_LOG_PREFIX   + REDEPLOY_DEPENDENT_CAPP);
        int successCount = countOccurrences(startupLogs, DEPLOYED_LOG_PREFIX + REDEPLOY_DEPENDENT_CAPP);

        Assert.assertEquals(failureCount, 1,
                REDEPLOY_DEPENDENT_CAPP + " must appear in the failure log exactly once. " +
                        "Actual: " + failureCount);
        Assert.assertEquals(successCount, 1,
                REDEPLOY_DEPENDENT_CAPP + " must appear in the success log exactly once. " +
                        "Actual: " + successCount);
    }
}

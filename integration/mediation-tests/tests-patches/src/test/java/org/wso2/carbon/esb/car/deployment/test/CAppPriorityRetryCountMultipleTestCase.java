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
 * Verifies retry behaviour when {@code priority_deployment_retry_count = 2} is set in
 * {@code deployment.toml}, covering two aspects:
 *
 * <ol>
 *   <li><b>Retry fires when needed:</b> A high-priority CApp that fails on the first pass is
 *       retried and succeeds on retry pass 1 of 2.</li>
 *   <li><b>No spurious retries:</b> Once all faulty CApps have recovered the retry loop stops
 *       early; the second allowed pass does not trigger because {@code faultyCapps} is empty
 *       after the first successful retry.</li>
 * </ol>
 *
 * <p>Setup: {@code Redeploy_A_DependentCApp} (high-priority, depends on the class mediator from
 * {@code Redeploy_B_ClassMediatorCApp}) and {@code Redeploy_B_ClassMediatorCApp} (high-priority,
 * provides the mediator) are deployed with {@code priority_deployment_retry_count = 2}.
 *
 * <p>Expected sequence:
 * <ol>
 *   <li>{@code Redeploy_A_DependentCApp} — fails on the first pass (mediator not yet on
 *       the classpath).</li>
 *   <li>{@code Redeploy_B_ClassMediatorCApp} — succeeds; mediator now available.</li>
 *   <li>Retry pass 1 of 2 fires: {@code Redeploy_A_DependentCApp} — succeeds.
 *       {@code faultyCapps} is now empty, so retry pass 2 is skipped.</li>
 * </ol>
 *
 * <p>Key assertions:
 * <ul>
 *   <li>{@code Redeploy_A_*} appears in the failure log exactly once (initial pass only).</li>
 *   <li>{@code Redeploy_A_*} appears in the success log exactly once (retry pass 1).</li>
 *   <li>The success entry appears after the failure entry in the log.</li>
 *   <li>{@code Redeploy_B_*} has a success entry and no failure entry.</li>
 *   <li>Server starts cleanly.</li>
 * </ul>
 */
public class CAppPriorityRetryCountMultipleTestCase extends CAppPriorityDeploymentTestBase {

    // =========================================================================
    // Setup / Teardown
    // =========================================================================

    @BeforeClass(alwaysRun = true)
    public void setUp() throws Exception {
        startServer(new File(PRIORITY_RETRY_COUNT_MULTIPLE_TOML),
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
            "Server must complete startup with retry_count = 2")
    public void testServerStartedSuccessfully() {
        Assert.assertTrue(startupLogs.contains(SERVER_STARTED_LOG),
                "Server startup log not found — MI may not have started correctly. " +
                        "Expected to find: \"" + SERVER_STARTED_LOG + "\"");
    }

    // =========================================================================
    // Test 2 – Mediator CApp deployed successfully on the first pass
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
    // Test 3 – Dependent CApp fails on first pass, succeeds on first retry
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "The dependent CApp must fail on the initial pass and succeed on the first retry " +
            "pass (pass 1 of the allowed 2)",
            dependsOnMethods = "testMediatorCAppDeployedSuccessfully")
    public void testDependentCAppSucceedsOnFirstRetry() {
        int firstFailIdx      = startupLogs.indexOf(FAILED_LOG_PREFIX   + REDEPLOY_DEPENDENT_CAPP);
        int mediatorDeployIdx = startupLogs.indexOf(DEPLOYED_LOG_PREFIX + REDEPLOY_MEDIATOR_CAPP);
        int retrySuccessIdx   = startupLogs.indexOf(DEPLOYED_LOG_PREFIX + REDEPLOY_DEPENDENT_CAPP);

        Assert.assertTrue(firstFailIdx >= 0,
                REDEPLOY_DEPENDENT_CAPP + " should have failed on the first deployment pass");
        Assert.assertTrue(mediatorDeployIdx >= 0,
                REDEPLOY_MEDIATOR_CAPP + " should have deployed successfully");
        Assert.assertTrue(retrySuccessIdx >= 0,
                REDEPLOY_DEPENDENT_CAPP + " should have succeeded on retry pass 1");

        Assert.assertTrue(firstFailIdx < mediatorDeployIdx,
                REDEPLOY_DEPENDENT_CAPP + " must fail before " + REDEPLOY_MEDIATOR_CAPP + " deploys. " +
                        "failIdx=" + firstFailIdx + ", mediatorIdx=" + mediatorDeployIdx);
        Assert.assertTrue(mediatorDeployIdx < retrySuccessIdx,
                REDEPLOY_DEPENDENT_CAPP + " retry success must appear after " + REDEPLOY_MEDIATOR_CAPP +
                        " deploys. mediatorIdx=" + mediatorDeployIdx + ", retrySuccessIdx=" + retrySuccessIdx);
    }

    // =========================================================================
    // Test 4 – No spurious retries: exactly one failure and one success
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "Exactly one failure (initial pass) and one success (retry pass 1) must appear in the " +
            "log. More than one success would indicate a spurious second retry pass fired even " +
            "though faultyCapps was empty after the first retry",
            dependsOnMethods = "testDependentCAppSucceedsOnFirstRetry")
    public void testNoSpuriousSecondRetry() {
        int failureCount = countOccurrences(startupLogs, FAILED_LOG_PREFIX   + REDEPLOY_DEPENDENT_CAPP);
        int successCount = countOccurrences(startupLogs, DEPLOYED_LOG_PREFIX + REDEPLOY_DEPENDENT_CAPP);

        Assert.assertEquals(failureCount, 1,
                REDEPLOY_DEPENDENT_CAPP + " must appear in the failure log exactly once " +
                        "(initial pass only). Actual: " + failureCount);
        Assert.assertEquals(successCount, 1,
                REDEPLOY_DEPENDENT_CAPP + " must appear in the success log exactly once " +
                        "(retry pass 1 only — retry pass 2 must be skipped because faultyCapps " +
                        "is empty). Actual: " + successCount);
    }
}

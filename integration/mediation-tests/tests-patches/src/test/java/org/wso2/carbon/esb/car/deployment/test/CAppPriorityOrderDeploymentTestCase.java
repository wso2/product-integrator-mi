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
 * Integration tests for priority-based CApp deployment ordering.
 *
 * <p>When {@code enable_priority_deployment = true} is set in the {@code [server]} section of
 * {@code deployment.toml}, CApps containing {@code lib/synapse/mediator}, {@code synapse/lib},
 * or {@code registry/resource} artifacts are classified as <em>high priority</em> and deployed
 * before all low-priority CApps. Within each priority group CApps are deployed in alphabetical
 * order. Failed high-priority CApps are automatically retried after all high-priority CApps have
 * been processed and before any low-priority CApp is deployed.
 */
public class CAppPriorityOrderDeploymentTestCase extends CAppPriorityDeploymentTestBase {

    // =========================================================================
    // Setup / Teardown
    // =========================================================================

    /**
     * Enables priority deployment, copies all ten CApps, and restarts MI so that all CApps are
     * processed in a single startup batch. Expected deployment order:
     * <ol>
     *   <li><b>High-priority (alphabetical):</b>
     *     <ol>
     *       <li>{@code Redeploy_A_DependentCApp} — fails (its mediator has not deployed yet)</li>
     *       <li>{@code Redeploy_B_ClassMediatorCApp} — succeeds; mediator now on classpath</li>
     *       <li>{@code Z_A_ClassMediatorCApp}</li>
     *       <li>{@code Z_B_SynapseLibACApp}</li>
     *       <li>{@code Z_C_SynapseLibBCApp}</li>
     *       <li>{@code Z_D_RegistryResourceACApp}</li>
     *       <li>{@code Z_E_RegistryResourceBCApp}</li>
     *     </ol>
     *   </li>
     *   <li><b>Retry pass</b> (triggered after all high-priority CApps are processed, before
     *       any low-priority CApp is deployed):
     *     <ol>
     *       <li>{@code Redeploy_A_DependentCApp} — succeeds on retry</li>
     *     </ol>
     *   </li>
     *   <li><b>Low-priority (alphabetical):</b>
     *     <ol>
     *       <li>{@code A_A_DependentProxyCApp}</li>
     *       <li>{@code A_B_PlainApiACApp}</li>
     *       <li>{@code A_C_PlainApiBCApp}</li>
     *     </ol>
     *   </li>
     * </ol>
     */
    @BeforeClass(alwaysRun = true)
    public void setUp() throws Exception {
        startServer(new File(PRIORITY_ENABLED_TOML),
                REDEPLOY_DEPENDENT_CAPP, REDEPLOY_MEDIATOR_CAPP,
                CLASS_MEDIATOR_CAPP, SYNAPSE_LIB_A_CAPP, SYNAPSE_LIB_B_CAPP,
                REGISTRY_RESOURCE_A_CAPP, REGISTRY_RESOURCE_B_CAPP,
                DEPENDENT_PROXY_CAPP, PLAIN_API_A_CAPP, PLAIN_API_B_CAPP);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown() throws Exception {
        stopServer(REDEPLOY_DEPENDENT_CAPP, REDEPLOY_MEDIATOR_CAPP,
                CLASS_MEDIATOR_CAPP, SYNAPSE_LIB_A_CAPP, SYNAPSE_LIB_B_CAPP,
                REGISTRY_RESOURCE_A_CAPP, REGISTRY_RESOURCE_B_CAPP,
                DEPENDENT_PROXY_CAPP, PLAIN_API_A_CAPP, PLAIN_API_B_CAPP);
    }

    // =========================================================================
    // Test 1 – All expected CApps are deployed
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "All ten test CApps must appear in the startup log as successfully deployed")
    public void testAllCAppsAreDeployed() {
        String[] allCApps = {
                CLASS_MEDIATOR_CAPP, SYNAPSE_LIB_A_CAPP, SYNAPSE_LIB_B_CAPP,
                REGISTRY_RESOURCE_A_CAPP, REGISTRY_RESOURCE_B_CAPP,
                DEPENDENT_PROXY_CAPP, PLAIN_API_A_CAPP, PLAIN_API_B_CAPP,
                REDEPLOY_DEPENDENT_CAPP, REDEPLOY_MEDIATOR_CAPP
        };
        for (String capp : allCApps) {
            Assert.assertTrue(startupLogs.contains(DEPLOYED_LOG_PREFIX + capp),
                    "CApp was not deployed successfully: " + capp);
        }
    }

    // =========================================================================
    // Test 2 – Server started successfully
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "Server must complete startup (StartupFinalizer log must be present)")
    public void testServerStartedSuccessfully() {
        Assert.assertTrue(startupLogs.contains(SERVER_STARTED_LOG),
                "Server startup log not found — MI may not have started correctly. " +
                        "Expected to find: \"" + SERVER_STARTED_LOG + "\"");
    }

    // =========================================================================
    // Test 3 – Class mediator CApp deployed before the proxy that depends on it
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "Class mediator CApp (Z_A_*) must appear in the deployment log before the proxy CApp (A_A_*) that depends on it",
            dependsOnMethods = "testAllCAppsAreDeployed")
    public void testClassMediatorCAppDeployedBeforeDependentProxy() {
        int mediatorIdx = startupLogs.indexOf(DEPLOYED_LOG_PREFIX + CLASS_MEDIATOR_CAPP);
        int proxyIdx    = startupLogs.indexOf(DEPLOYED_LOG_PREFIX + DEPENDENT_PROXY_CAPP);

        Assert.assertTrue(mediatorIdx >= 0,
                "Class mediator CApp was not deployed: " + CLASS_MEDIATOR_CAPP);
        Assert.assertTrue(proxyIdx >= 0,
                "Dependent proxy CApp was not deployed: " + DEPENDENT_PROXY_CAPP);
        Assert.assertTrue(mediatorIdx < proxyIdx,
                "Class mediator CApp must be deployed before dependent proxy CApp. " +
                        "mediator-log-index=" + mediatorIdx + ", proxy-log-index=" + proxyIdx);
    }

    @Test(groups = {"wso2.esb"}, description =
            "A_A_DependentProxyCApp must have a success log entry, confirming the class mediator was deployed first",
            dependsOnMethods = "testClassMediatorCAppDeployedBeforeDependentProxy")
    public void testDependentProxyCAppDeployedSuccessfully() {
        Assert.assertTrue(startupLogs.contains(DEPLOYED_LOG_PREFIX + DEPENDENT_PROXY_CAPP),
                "'" + DEPENDENT_PROXY_CAPP + "' did not appear in the success deployment log");
        Assert.assertFalse(startupLogs.contains(FAILED_LOG_PREFIX + DEPENDENT_PROXY_CAPP),
                "'" + DEPENDENT_PROXY_CAPP + "' appeared in the failure log");
    }

    // =========================================================================
    // Test 4 – Multiple synapse-lib CApps: deployed and in alphabetical order
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "Both synapse-lib CApps must be deployed and appear in alphabetical order within the high-priority group",
            dependsOnMethods = "testAllCAppsAreDeployed")
    public void testSynapseLibCAppsDeployedInAlphabeticalOrder() {
        int synapseLibAIdx = startupLogs.indexOf(DEPLOYED_LOG_PREFIX + SYNAPSE_LIB_A_CAPP);
        int synapseLibBIdx = startupLogs.indexOf(DEPLOYED_LOG_PREFIX + SYNAPSE_LIB_B_CAPP);

        Assert.assertTrue(synapseLibAIdx >= 0,
                "Synapse-lib CApp A was not deployed: " + SYNAPSE_LIB_A_CAPP);
        Assert.assertTrue(synapseLibBIdx >= 0,
                "Synapse-lib CApp B was not deployed: " + SYNAPSE_LIB_B_CAPP);
        Assert.assertTrue(synapseLibAIdx < synapseLibBIdx,
                "Synapse-lib CApps must be deployed in alphabetical order within the high-priority group " +
                        "(Z_B_* before Z_C_*). synapseLibA-index=" + synapseLibAIdx +
                        ", synapseLibB-index=" + synapseLibBIdx);
    }

    // =========================================================================
    // Test 5 – Multiple registry resource CApps: deployed and in alphabetical order
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "Both registry resource CApps must be deployed and appear in alphabetical order within the high-priority group",
            dependsOnMethods = "testAllCAppsAreDeployed")
    public void testRegistryResourceCAppsDeployedInAlphabeticalOrder() {
        int registryAIdx = startupLogs.indexOf(DEPLOYED_LOG_PREFIX + REGISTRY_RESOURCE_A_CAPP);
        int registryBIdx = startupLogs.indexOf(DEPLOYED_LOG_PREFIX + REGISTRY_RESOURCE_B_CAPP);

        Assert.assertTrue(registryAIdx >= 0,
                "Registry resource CApp A was not deployed: " + REGISTRY_RESOURCE_A_CAPP);
        Assert.assertTrue(registryBIdx >= 0,
                "Registry resource CApp B was not deployed: " + REGISTRY_RESOURCE_B_CAPP);
        Assert.assertTrue(registryAIdx < registryBIdx,
                "Registry resource CApps must be deployed in alphabetical order within the high-priority group " +
                        "(Z_D_* before Z_E_*). registryA-index=" + registryAIdx +
                        ", registryB-index=" + registryBIdx);
    }

    // =========================================================================
    // Test 6 – All high-priority CApps before all low-priority CApps
    // =========================================================================

    @Test(groups = {"wso2.esb"}, description =
            "All high-priority CApps (Z_*) must be deployed before any low-priority CApp (A_*)",
            dependsOnMethods = "testAllCAppsAreDeployed")
    public void testAllHighPriorityCAppsDeployedBeforeLowPriorityCApps() {
        int lastHighPriorityIdx = maxOf(
                startupLogs.indexOf(DEPLOYED_LOG_PREFIX + CLASS_MEDIATOR_CAPP),
                startupLogs.indexOf(DEPLOYED_LOG_PREFIX + SYNAPSE_LIB_A_CAPP),
                startupLogs.indexOf(DEPLOYED_LOG_PREFIX + SYNAPSE_LIB_B_CAPP),
                startupLogs.indexOf(DEPLOYED_LOG_PREFIX + REGISTRY_RESOURCE_A_CAPP),
                startupLogs.indexOf(DEPLOYED_LOG_PREFIX + REGISTRY_RESOURCE_B_CAPP)
        );

        int firstLowPriorityIdx = minOf(
                startupLogs.indexOf(DEPLOYED_LOG_PREFIX + DEPENDENT_PROXY_CAPP),
                startupLogs.indexOf(DEPLOYED_LOG_PREFIX + PLAIN_API_A_CAPP),
                startupLogs.indexOf(DEPLOYED_LOG_PREFIX + PLAIN_API_B_CAPP)
        );

        Assert.assertTrue(lastHighPriorityIdx >= 0,
                "At least one high-priority CApp was not deployed");
        Assert.assertTrue(firstLowPriorityIdx >= 0,
                "At least one low-priority CApp was not deployed");
        Assert.assertTrue(lastHighPriorityIdx < firstLowPriorityIdx,
                "All high-priority CApps must finish deploying before any low-priority CApp starts. " +
                        "last-high-priority-index=" + lastHighPriorityIdx +
                        ", first-low-priority-index=" + firstLowPriorityIdx);
    }

    @Test(groups = {"wso2.esb"}, description =
            "Low-priority CApps must be deployed in alphabetical order within the low-priority group",
            dependsOnMethods = "testAllCAppsAreDeployed")
    public void testLowPriorityCAppsDeployedInAlphabeticalOrder() {
        int plainApiAIdx = startupLogs.indexOf(DEPLOYED_LOG_PREFIX + PLAIN_API_A_CAPP);
        int plainApiBIdx = startupLogs.indexOf(DEPLOYED_LOG_PREFIX + PLAIN_API_B_CAPP);

        Assert.assertTrue(plainApiAIdx >= 0,
                "Plain API CApp A was not deployed: " + PLAIN_API_A_CAPP);
        Assert.assertTrue(plainApiBIdx >= 0,
                "Plain API CApp B was not deployed: " + PLAIN_API_B_CAPP);
        Assert.assertTrue(plainApiAIdx < plainApiBIdx,
                "Low-priority CApps must be deployed in alphabetical order (A_B_* before A_C_*). " +
                        "plainApiA-index=" + plainApiAIdx + ", plainApiB-index=" + plainApiBIdx);
    }

    // =========================================================================
    // Test 7 – Redeploy on failure
    // =========================================================================

    /**
     * Verifies the automatic retry-on-failure triggered after all high-priority CApps are
     * processed and before any low-priority CApp is deployed.
     *
     * <p>{@code Redeploy_A_DependentCApp} sorts before {@code Redeploy_B_ClassMediatorCApp}
     * alphabetically, so it is attempted first. Its proxy depends on the mediator from
     * {@code Redeploy_B_*}, which has not deployed yet — so it fails on the first pass.
     * After {@code Redeploy_B_*} deploys successfully the retry fires, and {@code Redeploy_A_*}
     * succeeds because the mediator class is now on the classpath.
     */
    @Test(groups = {"wso2.esb"}, description =
            "A high-priority CApp that fails on the first pass must be automatically retried and " +
            "succeed once its dependency CApp has been deployed in the same batch")
    public void testRedeployOnFailure() {
        int dependentFailIdx   = startupLogs.indexOf(FAILED_LOG_PREFIX   + REDEPLOY_DEPENDENT_CAPP);
        int mediatorSuccessIdx = startupLogs.indexOf(DEPLOYED_LOG_PREFIX + REDEPLOY_MEDIATOR_CAPP);
        int retrySuccessIdx    = startupLogs.indexOf(DEPLOYED_LOG_PREFIX + REDEPLOY_DEPENDENT_CAPP);

        Assert.assertTrue(dependentFailIdx >= 0,
                REDEPLOY_DEPENDENT_CAPP + " should have failed on the first deployment pass");
        Assert.assertTrue(mediatorSuccessIdx >= 0,
                REDEPLOY_MEDIATOR_CAPP + " should have deployed successfully on the first pass");
        Assert.assertTrue(retrySuccessIdx >= 0,
                REDEPLOY_DEPENDENT_CAPP + " should have succeeded on the automatic retry pass");

        Assert.assertTrue(dependentFailIdx < mediatorSuccessIdx,
                REDEPLOY_DEPENDENT_CAPP + " must fail before " + REDEPLOY_MEDIATOR_CAPP +
                        " deploys. failIdx=" + dependentFailIdx +
                        ", mediatorSuccessIdx=" + mediatorSuccessIdx);
        Assert.assertTrue(mediatorSuccessIdx < retrySuccessIdx,
                REDEPLOY_DEPENDENT_CAPP + " retry success must appear after " +
                        REDEPLOY_MEDIATOR_CAPP + " deploys. mediatorSuccessIdx=" +
                        mediatorSuccessIdx + ", retrySuccessIdx=" + retrySuccessIdx);
    }
}

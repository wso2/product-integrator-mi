/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.micro.integrator.initializer.deployment.application.deployer;

import org.apache.axis2.deployment.repository.util.DeploymentFileData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.wso2.config.mapper.ConfigParser;
import org.wso2.micro.application.deployer.CarbonApplication;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.wso2.micro.integrator.initializer.utils.DeployerUtilTest.writeDescriptorToExistingCarFile;

import org.wso2.micro.integrator.initializer.utils.DeployerUtilTest;

/**
 * Unit tests for {@link CappDeployer}.
 *
 * <h3>Sort / priority ordering ({@link CappDeployer#sort})</h3>
 * <p>A CApp is classified as <b>high priority</b> if it contains any artifact with one of:
 * <ul>
 *   <li>{@code lib/synapse/mediator} – class mediator</li>
 *   <li>{@code synapse/lib}          – connector</li>
 *   <li>{@code registry/resource}    – registry resource</li>
 * </ul>
 * All other CApps are classified as <b>low priority</b>. After sorting, high-priority CApps
 * appear first (alphabetically), followed by low-priority CApps (also alphabetically).
 */
public class CappDeployerTest {

    private static final String PRIORITY_CONFIG_KEY = "server.enable_priority_deployment";
    private static final String RETRY_COUNT_CONFIG_KEY = "server.priority_deployment_retry_count";

    /** Tracks all temporary .car files created during a test so they can be cleaned up. */
    private final List<File> tempFiles = new ArrayList<>();

    /** Dedicated temp directory for dependency-ordering tests; contains only the test CARs. */
    private File tempCAppDir;

    @Before
    public void setUp() throws Exception {
        // ConfigParser.parsedConfigs is null until ConfigParser.parse() is called.
        // Initialize it here so tests can put/remove entries without NPE.
        Field parsedConfigsField = ConfigParser.class.getDeclaredField("parsedConfigs");
        parsedConfigsField.setAccessible(true);
        if (parsedConfigsField.get(null) == null) {
            parsedConfigsField.set(null, new HashMap<>());
        }
        tempCAppDir = File.createTempFile("cappdir", "");
        tempCAppDir.delete();
        tempCAppDir.mkdir();
    }

    @After
    public void tearDown() throws Exception {
        for (File file : tempFiles) {
            Files.deleteIfExists(file.toPath());
        }
        tempFiles.clear();
        if (tempCAppDir != null && tempCAppDir.exists()) {
            File[] entries = tempCAppDir.listFiles();
            if (entries != null) {
                for (File f : entries) {
                    f.delete();
                }
            }
            tempCAppDir.delete();
        }
        // Reset static state that the retry tests may have modified.
        setStaticField("faultyCapps", new ArrayList<>());
        setStaticField("faultyCAppObjects", new ArrayList<>());
        setStaticField("cAppMap", new ArrayList<>());
        // Remove config keys so each test starts from a clean slate.
        ConfigParser.getParsedConfigs().remove(PRIORITY_CONFIG_KEY);
        ConfigParser.getParsedConfigs().remove(RETRY_COUNT_CONFIG_KEY);
        // Reset highPriorityCAppCount to its sentinel value so sort-related tests are isolated.
        setStaticField("highPriorityCAppCount", -1);
        setStaticField("retryPassCount", 0);
    }

    // -------------------------------------------------------------------------
    // Helper methods
    // -------------------------------------------------------------------------

    /**
     * Creates a minimal .car (zip) file containing one artifact directory per supplied type.
     * Each artifact directory contains a single {@code artifact.xml} that sets the given type.
     *
     * @param fileName      name of the .car file to create (e.g. "my-app.car")
     * @param artifactTypes one or more artifact type strings to embed in the archive
     * @return the created temporary {@link File}
     */
    private File createCarFile(String fileName, String... artifactTypes) throws IOException {
        File carFile = new File(tempCAppDir, fileName);
        try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(carFile))) {
            for (int i = 0; i < artifactTypes.length; i++) {
                // Directory entry (artifact dir)
                String dirEntry = "artifact-" + i + "_1.0.0/";
                zos.putNextEntry(new ZipEntry(dirEntry));
                zos.closeEntry();
                // artifact.xml entry inside the directory
                zos.putNextEntry(new ZipEntry(dirEntry + "artifact.xml"));
                String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                        + "<artifact name=\"artifact-" + i + "\" version=\"1.0.0\""
                        + " type=\"" + artifactTypes[i] + "\""
                        + " serverRole=\"EnterpriseIntegrator\">\n"
                        + "    <file>artifact-" + i + ".zip</file>\n"
                        + "</artifact>";
                zos.write(xml.getBytes(StandardCharsets.UTF_8));
                zos.closeEntry();
            }
        }
        tempFiles.add(carFile);
        return carFile;
    }

    /** Wraps a {@link File} in a {@link DeploymentFileData} instance. */
    private DeploymentFileData toFileData(File file) {
        return new DeploymentFileData(file);
    }

    /** Creates a {@link CappDeployer} with the shared temp directory set as the CApp directory. */
    private CappDeployer createDeployer() {
        CappDeployer deployer = new CappDeployer();
        deployer.setDirectory(tempCAppDir.getAbsolutePath());
        return deployer;
    }

    /** Extracts just the file name from a {@link DeploymentFileData} for assertion readability. */
    private String nameOf(DeploymentFileData fileData) {
        return new File(fileData.getAbsolutePath()).getName();
    }

    /** Injects the priority deployment flag into ConfigParser so sort() takes the priority path. */
    private static void enablePriorityDeployment() {
        ConfigParser.getParsedConfigs().put(PRIORITY_CONFIG_KEY, true);
    }

    // -------------------------------------------------------------------------
    // Tests: priority classification by artifact type
    // -------------------------------------------------------------------------

    /**
     * A CApp containing a connector artifact ({@code synapse/lib}) must be classified as
     * high priority and placed before a low-priority CApp when sorted.
     */
    @Test
    public void testConnectorArtifactTypeIsHighPriority() throws IOException {
        enablePriorityDeployment();
        File connector = createCarFile("connector-app.car", "synapse/lib");
        File regular   = createCarFile("regular-app.car",   "synapse/api");

        List<DeploymentFileData> files = new ArrayList<>();
        files.add(toFileData(regular));
        files.add(toFileData(connector));

        createDeployer().sort(files, 0, 2);

        assertEquals("connector-app.car should be first (high priority)", "connector-app.car", nameOf(files.get(0)));
        assertEquals("regular-app.car should be second (low priority)",   "regular-app.car",   nameOf(files.get(1)));
    }

    /**
     * A CApp containing a class mediator artifact ({@code lib/synapse/mediator}) must be
     * classified as high priority and placed before a low-priority CApp when sorted.
     */
    @Test
    public void testClassMediatorArtifactTypeIsHighPriority() throws IOException {
        enablePriorityDeployment();
        File classMediator = createCarFile("class-mediator-app.car", "lib/synapse/mediator");
        File regular       = createCarFile("regular-app.car",        "synapse/proxy-service");

        List<DeploymentFileData> files = new ArrayList<>();
        files.add(toFileData(regular));
        files.add(toFileData(classMediator));

        createDeployer().sort(files, 0, 2);

        assertEquals("class-mediator-app.car should be first (high priority)", "class-mediator-app.car", nameOf(files.get(0)));
        assertEquals("regular-app.car should be second (low priority)",         "regular-app.car",         nameOf(files.get(1)));
    }

    /**
     * A CApp containing a registry resource artifact ({@code registry/resource}) must be
     * classified as high priority and placed before a low-priority CApp when sorted.
     */
    @Test
    public void testRegistryResourceArtifactTypeIsHighPriority() throws IOException {
        enablePriorityDeployment();
        File registry = createCarFile("registry-app.car", "registry/resource");
        File regular  = createCarFile("regular-app.car",  "synapse/sequence");

        List<DeploymentFileData> files = new ArrayList<>();
        files.add(toFileData(regular));
        files.add(toFileData(registry));

        createDeployer().sort(files, 0, 2);

        assertEquals("registry-app.car should be first (high priority)", "registry-app.car", nameOf(files.get(0)));
        assertEquals("regular-app.car should be second (low priority)",  "regular-app.car",  nameOf(files.get(1)));
    }

    /**
     * A CApp that contains both a high-priority artifact type and a regular artifact type
     * must still be classified as high priority (any match is sufficient).
     */
    @Test
    public void testCAppWithMixedArtifactTypesIsHighPriority() throws IOException {
        enablePriorityDeployment();
        // This CApp has two artifacts: one connector (high priority) and one API (low priority)
        File mixed   = createCarFile("mixed-app.car",   "synapse/lib", "synapse/api");
        File regular = createCarFile("regular-app.car", "synapse/api");

        List<DeploymentFileData> files = new ArrayList<>();
        files.add(toFileData(regular));
        files.add(toFileData(mixed));

        createDeployer().sort(files, 0, 2);

        assertEquals("mixed-app.car should be first (has a high-priority artifact)", "mixed-app.car", nameOf(files.get(0)));
        assertEquals("regular-app.car should be second (low priority)",              "regular-app.car", nameOf(files.get(1)));
    }

    // -------------------------------------------------------------------------
    // Tests: alphabetical ordering within each priority group
    // -------------------------------------------------------------------------

    /**
     * When all CApps are high priority, they must be sorted alphabetically (case-sensitive).
     */
    @Test
    public void testOnlyHighPriorityCAppsAreSortedAlphabetically() throws IOException {
        enablePriorityDeployment();
        File charlie = createCarFile("charlie-connector.car", "synapse/lib");
        File alpha   = createCarFile("alpha-registry.car",   "registry/resource");
        File bravo   = createCarFile("bravo-mediator.car",   "lib/synapse/mediator");

        List<DeploymentFileData> files = new ArrayList<>();
        files.add(toFileData(charlie));
        files.add(toFileData(alpha));
        files.add(toFileData(bravo));

        createDeployer().sort(files, 0, 3);

        assertEquals("alpha-registry.car",   nameOf(files.get(0)));
        assertEquals("bravo-mediator.car",   nameOf(files.get(1)));
        assertEquals("charlie-connector.car", nameOf(files.get(2)));
    }

    /**
     * When all CApps are low priority, they must be sorted alphabetically (case-sensitive).
     */
    @Test
    public void testOnlyLowPriorityCAppsAreSortedAlphabetically() throws IOException {
        enablePriorityDeployment();
        File charlie = createCarFile("charlie-api.car", "synapse/api");
        File alpha   = createCarFile("alpha-seq.car",   "synapse/sequence");
        File bravo   = createCarFile("bravo-ep.car",    "synapse/endpoint");

        List<DeploymentFileData> files = new ArrayList<>();
        files.add(toFileData(charlie));
        files.add(toFileData(alpha));
        files.add(toFileData(bravo));

        createDeployer().sort(files, 0, 3);

        assertEquals("alpha-seq.car",   nameOf(files.get(0)));
        assertEquals("bravo-ep.car",    nameOf(files.get(1)));
        assertEquals("charlie-api.car", nameOf(files.get(2)));
    }

    // -------------------------------------------------------------------------
    // Tests: combined high + low priority ordering
    // -------------------------------------------------------------------------

    /**
     * When a list contains both high- and low-priority CApps, the final order must be:
     * high-priority CApps sorted alphabetically, followed by low-priority CApps sorted
     * alphabetically.
     */
    @Test
    public void testHighPriorityCAppsAreDeployedBeforeLowPriorityCApps() throws IOException {
        enablePriorityDeployment();
        // Low priority
        File lowAlpha   = createCarFile("alpha-api.car",     "synapse/api");
        File lowCharlie = createCarFile("charlie-proxy.car", "synapse/proxy-service");
        // High priority
        File highBravo  = createCarFile("bravo-registry.car",  "registry/resource");
        File highDelta  = createCarFile("delta-connector.car", "synapse/lib");

        List<DeploymentFileData> files = new ArrayList<>();
        // Intentionally unordered
        files.add(toFileData(lowCharlie));    // 0
        files.add(toFileData(highDelta));     // 1
        files.add(toFileData(lowAlpha));      // 2
        files.add(toFileData(highBravo));     // 3

        createDeployer().sort(files, 0, 4);

        // High-priority group (alphabetical): bravo-registry, delta-connector
        assertEquals("bravo-registry.car",  nameOf(files.get(0)));
        assertEquals("delta-connector.car", nameOf(files.get(1)));
        // Low-priority group (alphabetical): alpha-api, charlie-proxy
        assertEquals("alpha-api.car",     nameOf(files.get(2)));
        assertEquals("charlie-proxy.car", nameOf(files.get(3)));
    }

    // -------------------------------------------------------------------------
    // Tests: sub-range boundary behaviour
    // -------------------------------------------------------------------------

    /**
     * The sort must only reorder elements within [startIndex, toIndex). Elements outside
     * that range must remain at their original positions.
     */
    @Test
    public void testSortOnlyAffectsElementsWithinSpecifiedSubRange() throws IOException {
        enablePriorityDeployment();
        File outsideBefore = createCarFile("z-outside-before.car", "synapse/api");
        File lowB          = createCarFile("bravo-low.car",        "synapse/api");
        File highA         = createCarFile("alpha-connector.car",  "synapse/lib");
        File outsideAfter  = createCarFile("z-outside-after.car",  "synapse/api");

        List<DeploymentFileData> files = new ArrayList<>();
        files.add(toFileData(outsideBefore)); // index 0 – outside range
        files.add(toFileData(lowB));          // index 1 – inside range [1, 3)
        files.add(toFileData(highA));         // index 2 – inside range [1, 3)
        files.add(toFileData(outsideAfter));  // index 3 – outside range

        createDeployer().sort(files, 1, 3);

        // Sentinel elements outside the range are untouched
        assertEquals("z-outside-before.car", nameOf(files.get(0)));
        assertEquals("z-outside-after.car",  nameOf(files.get(3)));

        // Inside the range: high priority (alpha-connector) first, then low priority (bravo-low)
        assertEquals("alpha-connector.car", nameOf(files.get(1)));
        assertEquals("bravo-low.car",       nameOf(files.get(2)));
    }

    /**
     * When startIndex equals toIndex (an empty sub-range) the list must not be modified.
     */
    @Test
    public void testSortWithEmptySubRangeIsNoOp() throws IOException {
        enablePriorityDeployment();
        File fileA = createCarFile("a-connector.car", "synapse/lib");
        File fileB = createCarFile("b-regular.car",   "synapse/api");

        List<DeploymentFileData> files = new ArrayList<>();
        files.add(toFileData(fileB));
        files.add(toFileData(fileA));

        createDeployer().sort(files, 1, 1);

        // Order must be unchanged
        assertEquals("b-regular.car",   nameOf(files.get(0)));
        assertEquals("a-connector.car", nameOf(files.get(1)));
    }

    // -------------------------------------------------------------------------
    // Tests: resilience / edge cases
    // -------------------------------------------------------------------------

    /**
     * A non-existent .car file cannot be opened as a zip archive. The deployer must treat
     * it as low priority (warn and continue) rather than throwing an exception.
     */
    @Test
    public void testNonExistentCarFileTreatedAsLowPriority() throws IOException {
        enablePriorityDeployment();
        // This file is never created on disk; it should be treated as low priority.
        File missing   = new File(tempCAppDir, "missing-app.car");
        File connector = createCarFile("connector-app.car", "synapse/lib");

        List<DeploymentFileData> files = new ArrayList<>();
        files.add(toFileData(missing));
        files.add(toFileData(connector));

        createDeployer().sort(files, 0, 2);

        // connector (high priority) should come first; missing file falls to low priority
        assertEquals("connector-app.car", nameOf(files.get(0)));
        assertEquals("missing-app.car",   nameOf(files.get(1)));
    }

    /**
     * A .car file that contains no artifact.xml entries must be treated as low priority.
     */
    @Test
    public void testCarFileWithNoArtifactXmlTreatedAsLowPriority() throws IOException {
        enablePriorityDeployment();
        // Create an empty .car archive (no entries)
        File empty     = new File(tempCAppDir, "empty-app.car");
        try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(empty))) {
            // intentionally left empty
        }
        tempFiles.add(empty);

        File connector = createCarFile("connector-app.car", "synapse/lib");

        List<DeploymentFileData> files = new ArrayList<>();
        files.add(toFileData(empty));
        files.add(toFileData(connector));

        createDeployer().sort(files, 0, 2);

        assertEquals("connector-app.car", nameOf(files.get(0)));
        assertEquals("empty-app.car",     nameOf(files.get(1)));
    }

    /**
     * A single-element sub-range must remain unchanged regardless of the artifact type.
     */
    @Test
    public void testSortSingleElementSubRangeIsNoOp() throws IOException {
        enablePriorityDeployment();
        File connector = createCarFile("connector-app.car", "synapse/lib");
        File regular   = createCarFile("regular-app.car",   "synapse/api");

        List<DeploymentFileData> files = new ArrayList<>();
        files.add(toFileData(regular));
        files.add(toFileData(connector));

        createDeployer().sort(files, 0, 1);

        // Only index 0 is in range; no reordering possible
        assertEquals("regular-app.car",   nameOf(files.get(0)));
        assertEquals("connector-app.car", nameOf(files.get(1)));
    }

    /**
     * When {@code server.enable_priority_deployment} is absent from deployment.toml,
     * sort() must fall back to a plain filename sort and must NOT apply priority hoisting.
     * A connector CApp placed after a regular CApp must stay after it because no
     * priority classification is performed.
     */
    @Test
    public void testSortFallsBackToPlainFilenameSortWhenPriorityDeploymentConfigIsDisabled() throws IOException {
        // Config key is deliberately absent (not calling enablePriorityDeployment()).
        // Start with a non-alphabetical order so the test distinguishes three outcomes:
        //   no-op            → z-connector.car stays at 0 (FAIL)
        //   priority hoisting → z-connector.car (high-priority) stays at 0 (FAIL)
        //   plain filename sort → a-low-app.car moves to 0 (PASS)
        File lowAlpha       = createCarFile("a-low-app.car",    "synapse/api");
        File highZConnector = createCarFile("z-connector.car", "synapse/lib");

        List<DeploymentFileData> files = new ArrayList<>();
        files.add(toFileData(highZConnector));  // index 0: intentionally non-alphabetical
        files.add(toFileData(lowAlpha));        // index 1

        createDeployer().sort(files, 0, 2);

        // Plain filename sort must reorder to alphabetical; priority hoisting must NOT occur.
        assertEquals("a-low-app.car should be moved to position 0 by plain filename sort",
                     "a-low-app.car", nameOf(files.get(0)));
        assertEquals("z-connector.car should be moved to position 1 with no priority hoisting",
                     "z-connector.car", nameOf(files.get(1)));
    }

    // -------------------------------------------------------------------------
    // Tests: highPriorityCAppCount set by sort()
    //
    // The retry trigger in deployCarbonApps() compares cAppMap.size()+faultyCapps.size()
    // against highPriorityCAppCount to detect when the high-priority phase is complete.
    // These tests verify that sort() records the count correctly.
    // -------------------------------------------------------------------------

    private static int getHighPriorityCAppCount() throws Exception {
        Field field = CappDeployer.class.getDeclaredField("highPriorityCAppCount");
        field.setAccessible(true);
        return (int) field.get(null);
    }

    /**
     * sort() must set highPriorityCAppCount to the exact number of high-priority CApps
     * in the sorted range. The deployCarbonApps() retry trigger relies on this value.
     */
    @Test
    public void testSortSetsHighPriorityCAppCountCorrectly() throws Exception {
        enablePriorityDeployment();
        File high1 = createCarFile("high-a.car", "synapse/lib");
        File high2 = createCarFile("high-b.car", "registry/resource");
        File low1  = createCarFile("low-a.car",  "synapse/api");

        List<DeploymentFileData> files = new ArrayList<>();
        files.add(toFileData(low1));
        files.add(toFileData(high1));
        files.add(toFileData(high2));

        createDeployer().sort(files, 0, 3);

        assertEquals("highPriorityCAppCount must equal the number of high-priority CApps in the range",
                     2, getHighPriorityCAppCount());
    }

    /**
     * When all CApps in the range are low priority, highPriorityCAppCount must be 0.
     * The retry trigger condition (highPriorityCAppCount > 0) will then never fire.
     */
    @Test
    public void testSortSetsHighPriorityCAppCountToZeroWhenAllLowPriority() throws Exception {
        enablePriorityDeployment();
        File low1 = createCarFile("low-a.car", "synapse/api");
        File low2 = createCarFile("low-b.car", "synapse/sequence");

        List<DeploymentFileData> files = new ArrayList<>();
        files.add(toFileData(low1));
        files.add(toFileData(low2));

        createDeployer().sort(files, 0, 2);

        assertEquals("highPriorityCAppCount must be 0 when no high-priority CApps are present",
                     0, getHighPriorityCAppCount());
    }

    /**
     * When priority deployment is disabled, sort() returns early without updating
     * highPriorityCAppCount, so it stays at the sentinel value -1.
     */
    @Test
    public void testSortDoesNotSetHighPriorityCAppCountWhenPriorityDisabled() throws Exception {
        // Priority deployment deliberately not enabled.
        File high = createCarFile("high.car", "synapse/lib");
        File low  = createCarFile("low.car",  "synapse/api");

        List<DeploymentFileData> files = new ArrayList<>();
        files.add(toFileData(high));
        files.add(toFileData(low));

        createDeployer().sort(files, 0, 2);

        assertEquals("highPriorityCAppCount must remain -1 when priority deployment is disabled",
                     -1, getHighPriorityCAppCount());
    }

    /**
     * cleanup() must reset highPriorityCAppCount to -1 so that a server re-deployment
     * (or test re-use of the same JVM) starts with a clean slate.
     */
    @Test
    public void testCleanupResetsHighPriorityCAppCount() throws Exception {
        setStaticField("highPriorityCAppCount", 3);

        createDeployer().cleanup();

        assertEquals("highPriorityCAppCount must be reset to -1 by cleanup()",
                     -1, getHighPriorityCAppCount());
    }

    // -------------------------------------------------------------------------
    // Tests: configurable retry count (getMaxRetryCount)
    // -------------------------------------------------------------------------

    private static int invokeGetMaxRetryCount(CappDeployer deployer) throws Exception {
        java.lang.reflect.Method method = CappDeployer.class.getDeclaredMethod("getMaxRetryCount");
        method.setAccessible(true);
        return (int) method.invoke(deployer);
    }

    private static int getRetryPassCount() throws Exception {
        Field field = CappDeployer.class.getDeclaredField("retryPassCount");
        field.setAccessible(true);
        return (int) field.get(null);
    }

    /**
     * When {@code server.priority_deployment_retry_count} is absent from deployment.toml,
     * getMaxRetryCount() must return the default of 1 (one retry pass).
     */
    @Test
    public void testGetMaxRetryCountDefaultsToOneWhenConfigAbsent() throws Exception {
        // RETRY_COUNT_CONFIG_KEY is deliberately not set.
        assertEquals("default retry count must be 1 when config key is absent",
                     1, invokeGetMaxRetryCount(createDeployer()));
    }

    /**
     * getMaxRetryCount() must return the integer value set in deployment.toml.
     */
    @Test
    public void testGetMaxRetryCountReadsValueFromConfig() throws Exception {
        ConfigParser.getParsedConfigs().put(RETRY_COUNT_CONFIG_KEY, "3");
        assertEquals("retry count must match the configured value",
                     3, invokeGetMaxRetryCount(createDeployer()));
    }

    /**
     * When the configured value is 0, getMaxRetryCount() must return 0 (retries disabled).
     */
    @Test
    public void testGetMaxRetryCountReturnsZeroWhenConfiguredToZero() throws Exception {
        ConfigParser.getParsedConfigs().put(RETRY_COUNT_CONFIG_KEY, "0");
        assertEquals("retry count must be 0 when explicitly set to 0",
                     0, invokeGetMaxRetryCount(createDeployer()));
    }

    /**
     * Negative configured values must be clamped to 0.
     */
    @Test
    public void testGetMaxRetryCountClampsNegativeValueToZero() throws Exception {
        ConfigParser.getParsedConfigs().put(RETRY_COUNT_CONFIG_KEY, "-5");
        assertEquals("negative retry count must be clamped to 0",
                     0, invokeGetMaxRetryCount(createDeployer()));
    }

    /**
     * An invalid (non-integer) configured value must fall back to the default of 1.
     */
    @Test
    public void testGetMaxRetryCountFallsBackToOneForInvalidString() throws Exception {
        ConfigParser.getParsedConfigs().put(RETRY_COUNT_CONFIG_KEY, "not-a-number");
        assertEquals("invalid retry count config must fall back to default of 1",
                     1, invokeGetMaxRetryCount(createDeployer()));
    }

    // -------------------------------------------------------------------------
    // Helpers: reflection utilities for retryFaultyCApps tests
    // -------------------------------------------------------------------------

    private static void setStaticField(String fieldName, Object value) throws Exception {
        Field field = CappDeployer.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(null, value);
    }

    private static void invokeRetryFaultyCApps(CappDeployer deployer) throws Exception {
        Method method = CappDeployer.class.getDeclaredMethod("retryFaultyCApps");
        method.setAccessible(true);
        method.invoke(deployer);
    }

    // -------------------------------------------------------------------------
    // Tests: retryFaultyCApps behaviour
    // -------------------------------------------------------------------------

    /**
     * retryFaultyCApps() must snapshot and clear both faultyCapps and faultyCAppObjects
     * before attempting any retry deployments, so that a CApp that succeeds on retry
     * lands in cAppMap with a clean slate rather than remaining in the faulty lists.
     * <p>
     * Individual deploy attempts during the retry fail silently (invalid files, no
     * axisConfig), so both lists stay empty after the call.
     */
    @Test
    public void testRetryFaultyCappsClearsBothListsBeforeRetry() throws Exception {
        ArrayList<String> faulty = new ArrayList<>(Arrays.asList("app-a.car", "app-b.car"));
        ArrayList<CarbonApplication> faultyObjects = new ArrayList<>();
        faultyObjects.add(null); // as happens in prod when currentApp is null on failure
        setStaticField("faultyCapps", faulty);
        setStaticField("faultyCAppObjects", faultyObjects);

        invokeRetryFaultyCApps(createDeployer());

        assertTrue("faultyCapps must be cleared before retry attempts",
                   CappDeployer.getFaultyCapps().isEmpty());
        assertTrue("faultyCAppObjects must be cleared before retry attempts",
                   CappDeployer.getFaultyCAppObjects().isEmpty());
    }

    /**
     * When there are no faulty CApps, retryFaultyCApps() must complete without
     * throwing an exception and both faulty lists must remain empty.
     */
    @Test
    public void testRetryFaultyCAppsIsNoOpWhenFaultyListIsEmpty() throws Exception {
        invokeRetryFaultyCApps(createDeployer()); // must not throw

        assertTrue("faultyCapps must remain empty", CappDeployer.getFaultyCapps().isEmpty());
        assertTrue("faultyCAppObjects must remain empty", CappDeployer.getFaultyCAppObjects().isEmpty());
    }

    /**
     * retryPassCount must start at 0, meaning no retry pass has been executed yet and
     * the deployer is eligible to run up to getMaxRetryCount() passes on first startup.
     */
    @Test
    public void testRetryPassCountIsZeroForNewDeployer() throws Exception {
        assertEquals("retryPassCount must be 0 on a fresh deployer instance",
                     0, getRetryPassCount());
    }

    /**
     * cleanup() must reset retryPassCount to 0 so that a server re-deployment
     * (or test re-use of the same JVM) starts with a clean retry slate.
     */
    @Test
    public void testCleanupResetsRetryPassCount() throws Exception {
        setStaticField("retryPassCount", 2);

        createDeployer().cleanup();

        assertEquals("retryPassCount must be reset to 0 by cleanup()",
                     0, getRetryPassCount());
    }

    // -------------------------------------------------------------------------
    // Helpers for HTTP connector and embedded CAR tests
    // -------------------------------------------------------------------------

    /**
     * Creates a .car file with artifacts of the given names and types.
     * Each artifact gets its own directory entry named {@code <artifactName>_1.0.0/artifact.xml}.
     */
    private File createCarFileWithNamedArtifacts(String fileName, String[] artifactNames,
                                                  String[] artifactTypes) throws IOException {
        File carFile = new File(tempCAppDir, fileName);
        try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(carFile))) {
            for (int i = 0; i < artifactNames.length; i++) {
                String dirEntry = artifactNames[i] + "_1.0.0/";
                zos.putNextEntry(new ZipEntry(dirEntry));
                zos.closeEntry();
                zos.putNextEntry(new ZipEntry(dirEntry + "artifact.xml"));
                String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                        + "<artifact name=\"" + artifactNames[i] + "\" version=\"1.0.0\""
                        + " type=\"" + artifactTypes[i] + "\""
                        + " serverRole=\"EnterpriseIntegrator\">\n"
                        + "    <file>" + artifactNames[i] + ".zip</file>\n"
                        + "</artifact>";
                zos.write(xml.getBytes(StandardCharsets.UTF_8));
                zos.closeEntry();
            }
        }
        tempFiles.add(carFile);
        return carFile;
    }

    /**
     * Creates an outer .car file on disk with {@code innerEntryPath} embedded as a zip entry
     * (the entry itself is a valid .car containing artifacts of {@code innerArtifactTypes}).
     * Returns the synthetic {@link File} path representing the embedded inner CAR
     * (i.e. {@code <outer.car>/<innerEntryPath>} — not a real file on disk), which is the
     * form that {@link CappDeployer#isHighPriorityCApp} receives for embedded CARs.
     */
    private File createEmbeddedCarPath(String outerFileName, String innerEntryPath,
                                        String... innerArtifactTypes) throws IOException {
        byte[] innerCarBytes;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ZipOutputStream innerZos = new ZipOutputStream(baos)) {
            for (int i = 0; i < innerArtifactTypes.length; i++) {
                String dirEntry = "artifact-" + i + "_1.0.0/";
                innerZos.putNextEntry(new ZipEntry(dirEntry));
                innerZos.closeEntry();
                innerZos.putNextEntry(new ZipEntry(dirEntry + "artifact.xml"));
                String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                        + "<artifact name=\"artifact-" + i + "\" version=\"1.0.0\""
                        + " type=\"" + innerArtifactTypes[i] + "\""
                        + " serverRole=\"EnterpriseIntegrator\">\n"
                        + "    <file>artifact-" + i + ".zip</file>\n"
                        + "</artifact>";
                innerZos.write(xml.getBytes(StandardCharsets.UTF_8));
                innerZos.closeEntry();
            }
            innerZos.finish();
            innerCarBytes = baos.toByteArray();
        }

        File outerCar = new File(tempCAppDir, outerFileName);
        try (ZipOutputStream outerZos = new ZipOutputStream(new FileOutputStream(outerCar))) {
            // Zip entries always use '/' as separator regardless of OS
            outerZos.putNextEntry(new ZipEntry(innerEntryPath.replace(File.separatorChar, '/')));
            outerZos.write(innerCarBytes);
            outerZos.closeEntry();
        }
        tempFiles.add(outerCar);

        // Synthetic path: outer.car/<innerEntryPath> — not a real file, triggers the embedded code path
        return new File(outerCar.getAbsolutePath() + File.separator
                + innerEntryPath.replace('/', File.separatorChar));
    }

    // -------------------------------------------------------------------------
    // Tests: HTTP connector skipping
    // -------------------------------------------------------------------------

    /**
     * A CApp whose only {@code synapse/lib} artifact is the HTTP connector
     * ({@code mi-connector-http}) must be treated as low priority. The HTTP connector
     * is added to projects by default, so its presence must not hoist a CApp to high priority.
     */
    @Test
    public void testHttpConnectorArtifactIsSkippedAndTreatedAsLowPriority() throws IOException {
        enablePriorityDeployment();
        File httpConnector = createCarFileWithNamedArtifacts("http-connector-app.car",
                new String[]{"mi-connector-http"}, new String[]{"synapse/lib"});
        File regular = createCarFile("regular-app.car", "synapse/api");

        List<DeploymentFileData> files = new ArrayList<>();
        files.add(toFileData(httpConnector));
        files.add(toFileData(regular));

        createDeployer().sort(files, 0, 2);

        // Both are low priority; alphabetical order applies: "http-connector-app.car" < "regular-app.car"
        assertEquals("HTTP connector CApp must be treated as low priority",
                     "http-connector-app.car", nameOf(files.get(0)));
        assertEquals("regular-app.car", nameOf(files.get(1)));
    }

    /**
     * When a CApp contains the HTTP connector alongside another {@code synapse/lib} connector,
     * the HTTP connector entry must be skipped but the other connector makes the CApp high priority.
     */
    @Test
    public void testHttpConnectorSkippedButOtherConnectorMakesHighPriority() throws IOException {
        enablePriorityDeployment();
        File mixed = createCarFileWithNamedArtifacts("mixed-connector-app.car",
                new String[]{"mi-connector-http", "my-custom-connector"},
                new String[]{"synapse/lib", "synapse/lib"});
        File regular = createCarFile("regular-app.car", "synapse/api");

        List<DeploymentFileData> files = new ArrayList<>();
        files.add(toFileData(regular));
        files.add(toFileData(mixed));

        createDeployer().sort(files, 0, 2);

        assertEquals("CApp with custom connector must be high priority despite also having HTTP connector",
                     "mixed-connector-app.car", nameOf(files.get(0)));
        assertEquals("regular-app.car", nameOf(files.get(1)));
    }

    /**
     * A {@code synapse/lib} artifact with a name other than {@code mi-connector-http} must
     * still make the CApp high priority — only the HTTP connector is skipped.
     */
    @Test
    public void testNonHttpConnectorSynapseLibIsHighPriority() throws IOException {
        enablePriorityDeployment();
        File customConnector = createCarFileWithNamedArtifacts("custom-connector-app.car",
                new String[]{"mi-connector-custom"}, new String[]{"synapse/lib"});
        File regular = createCarFile("regular-app.car", "synapse/api");

        List<DeploymentFileData> files = new ArrayList<>();
        files.add(toFileData(regular));
        files.add(toFileData(customConnector));

        createDeployer().sort(files, 0, 2);

        assertEquals("Non-HTTP connector must still be high priority",
                     "custom-connector-app.car", nameOf(files.get(0)));
        assertEquals("regular-app.car", nameOf(files.get(1)));
    }

    // -------------------------------------------------------------------------
    // Tests: embedded CAR high-priority detection
    // -------------------------------------------------------------------------

    /**
     * A synthetic path of the form {@code outer.car/dependencies/inner.car} (representing a
     * .car embedded inside a FAT CAR) must be classified as high priority when the inner
     * CAR contains a high-priority artifact. The parent and embedded CARs are treated as
     * separate entries during sorting, so the inner CAR is sorted independently based on
     * its own artifact type.
     */
    @Test
    public void testEmbeddedCarWithHighPriorityArtifactIsHighPriority() throws IOException {
        enablePriorityDeployment();
        File embeddedPath = createEmbeddedCarPath("outer-fat.car",
                "dependencies/inner-connector.car", "synapse/lib");
        File regular = createCarFile("regular-app.car", "synapse/api");

        List<DeploymentFileData> files = new ArrayList<>();
        files.add(toFileData(regular));
        files.add(toFileData(embeddedPath));

        createDeployer().sort(files, 0, 2);

        assertEquals("embedded CAR with connector artifact must be classified as high priority",
                     "inner-connector.car", nameOf(files.get(0)));
        assertEquals("regular-app.car", nameOf(files.get(1)));
    }

    /**
     * A synthetic embedded CAR path whose inner CAR contains only low-priority artifacts
     * must be treated as low priority.
     */
    @Test
    public void testEmbeddedCarWithLowPriorityArtifactIsLowPriority() throws IOException {
        enablePriorityDeployment();
        File embeddedPath = createEmbeddedCarPath("outer-fat.car",
                "dependencies/inner-api.car", "synapse/api");
        File connector = createCarFile("connector-app.car", "synapse/lib");

        List<DeploymentFileData> files = new ArrayList<>();
        files.add(toFileData(embeddedPath));
        files.add(toFileData(connector));

        createDeployer().sort(files, 0, 2);

        assertEquals("connector-app.car must be high priority",
                     "connector-app.car", nameOf(files.get(0)));
        assertEquals("embedded CAR with only low-priority artifacts must be low priority",
                     "inner-api.car", nameOf(files.get(1)));
    }

    /**
     * When the outer .car does not exist on disk, the embedded CAR check must return false
     * (low priority) rather than throwing an exception.
     */
    @Test
    public void testEmbeddedCarWithNonExistentOuterCarIsLowPriority() throws IOException {
        enablePriorityDeployment();
        // Synthetic path where the outer .car file is never created on disk
        File missingEmbedded = new File(tempCAppDir.getAbsolutePath() + File.separator + "nonexistent.car"
                + File.separator + "dependencies" + File.separator + "inner.car");
        File connector = createCarFile("connector-app.car", "synapse/lib");

        List<DeploymentFileData> files = new ArrayList<>();
        files.add(toFileData(missingEmbedded));
        files.add(toFileData(connector));

        createDeployer().sort(files, 0, 2);

        assertEquals("connector-app.car must be high priority",
                     "connector-app.car", nameOf(files.get(0)));
        assertEquals("embedded CAR with missing outer file must be treated as low priority",
                     "inner.car", nameOf(files.get(1)));
    }

    /**
     * When the outer .car exists but does not contain the expected inner entry, the embedded
     * CAR check must return false (low priority) rather than throwing an exception.
     */
    @Test
    public void testEmbeddedCarWithMissingInnerEntryIsLowPriority() throws IOException {
        enablePriorityDeployment();
        File outerCar = new File(tempCAppDir, "empty-outer.car");
        try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(outerCar))) {
            // intentionally empty — no inner entries
        }
        tempFiles.add(outerCar);

        File missingInner = new File(outerCar.getAbsolutePath()
                + File.separator + "dependencies" + File.separator + "missing-inner.car");
        File connector = createCarFile("connector-app.car", "synapse/lib");

        List<DeploymentFileData> files = new ArrayList<>();
        files.add(toFileData(missingInner));
        files.add(toFileData(connector));

        createDeployer().sort(files, 0, 2);

        assertEquals("connector-app.car must be high priority",
                     "connector-app.car", nameOf(files.get(0)));
        assertEquals("embedded CAR with missing inner entry must be treated as low priority",
                     "missing-inner.car", nameOf(files.get(1)));
    }

    /**
     * A path that is not a real file but also does not contain the {@code .car/} marker
     * cannot be parsed as an embedded CAR path and must be treated as low priority.
     */
    @Test
    public void testEmbeddedCarPathWithoutCarMarkerIsLowPriority() throws IOException {
        enablePriorityDeployment();
        // A path that does not exist and has no ".car/" segment
        File badPath = new File(tempCAppDir.getAbsolutePath() + File.separator + "some-dir"
                + File.separator + "inner.car");
        File connector = createCarFile("connector-app.car", "synapse/lib");

        List<DeploymentFileData> files = new ArrayList<>();
        files.add(toFileData(badPath));
        files.add(toFileData(connector));

        createDeployer().sort(files, 0, 2);

        assertEquals("connector-app.car must be high priority",
                     "connector-app.car", nameOf(files.get(0)));
        assertEquals("unresolvable non-file path must be treated as low priority",
                     "inner.car", nameOf(files.get(1)));
    }

    /**
     * An embedded CAR containing only the HTTP connector ({@code mi-connector-http}) must
     * still be treated as low priority — the HTTP connector skip applies inside embedded CARs too.
     */
    @Test
    public void testEmbeddedCarWithHttpConnectorOnlyIsLowPriority() throws IOException {
        enablePriorityDeployment();
        byte[] innerCarBytes;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ZipOutputStream innerZos = new ZipOutputStream(baos)) {
            String dirEntry = "mi-connector-http_1.0.0/";
            innerZos.putNextEntry(new ZipEntry(dirEntry));
            innerZos.closeEntry();
            innerZos.putNextEntry(new ZipEntry(dirEntry + "artifact.xml"));
            String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                    + "<artifact name=\"mi-connector-http\" version=\"1.0.0\""
                    + " type=\"synapse/lib\" serverRole=\"EnterpriseIntegrator\">\n"
                    + "    <file>mi-connector-http.zip</file>\n"
                    + "</artifact>";
            innerZos.write(xml.getBytes(StandardCharsets.UTF_8));
            innerZos.closeEntry();
            innerZos.finish();
            innerCarBytes = baos.toByteArray();
        }
        File outerCar = new File(tempCAppDir, "fat-http-only.car");
        try (ZipOutputStream outerZos = new ZipOutputStream(new FileOutputStream(outerCar))) {
            outerZos.putNextEntry(new ZipEntry("dependencies/inner-http.car"));
            outerZos.write(innerCarBytes);
            outerZos.closeEntry();
        }
        tempFiles.add(outerCar);
        File embeddedPath = new File(outerCar.getAbsolutePath()
                + File.separator + "dependencies" + File.separator + "inner-http.car");

        File connector = createCarFile("connector-app.car", "synapse/lib");

        List<DeploymentFileData> files = new ArrayList<>();
        files.add(toFileData(embeddedPath));
        files.add(toFileData(connector));

        createDeployer().sort(files, 0, 2);

        assertEquals("connector-app.car must be high priority",
                     "connector-app.car", nameOf(files.get(0)));
        assertEquals("embedded CAR with only HTTP connector must be low priority",
                     "inner-http.car", nameOf(files.get(1)));
    }

    // -------------------------------------------------------------------------
    // Tests: CarbonApplication embeddedCAR field
    // -------------------------------------------------------------------------

    /**
     * A newly constructed {@link CarbonApplication} must default {@code embeddedCAR} to
     * {@code false} — regular CApps loaded from the file system are not embedded.
     */
    @Test
    public void testCarbonApplicationEmbeddedCARDefaultIsFalse() {
        CarbonApplication app = new CarbonApplication();
        assertFalse("embeddedCAR must default to false for a new CarbonApplication",
                    app.isEmbeddedCAR());
    }

    /**
     * {@link CarbonApplication#setEmbeddedCAR(boolean)} with {@code true} must make
     * {@link CarbonApplication#isEmbeddedCAR()} return {@code true}.
     */
    @Test
    public void testCarbonApplicationEmbeddedCARCanBeSetToTrue() {
        CarbonApplication app = new CarbonApplication();
        app.setEmbeddedCAR(true);
        assertTrue("isEmbeddedCAR() must return true after setEmbeddedCAR(true)",
                   app.isEmbeddedCAR());
    }

    /**
     * {@link CarbonApplication#setEmbeddedCAR(boolean)} with {@code false} after {@code true}
     * must reset the flag back to {@code false}.
     */
    @Test
    public void testCarbonApplicationEmbeddedCARCanBeResetToFalse() {
        CarbonApplication app = new CarbonApplication();
        app.setEmbeddedCAR(true);
        app.setEmbeddedCAR(false);
        assertFalse("isEmbeddedCAR() must return false after being reset with setEmbeddedCAR(false)",
                    app.isEmbeddedCAR());
    }

    // -------------------------------------------------------------------------
    // Tests: retry handling with embedded CARs
    // -------------------------------------------------------------------------

    /**
     * When {@code faultyCAppObjects} contains a {@link CarbonApplication} marked as an
     * embedded CAR, {@code retryFaultyCApps()} must use the stored {@code appFilePath}
     * and complete without throwing. Both faulty lists must be cleared after the retry pass
     * (the deploy attempt itself fails silently because there is no axis config).
     */
    @Test
    public void testRetryFaultyCAppsHandlesEmbeddedCarObjects() throws Exception {
        CarbonApplication embeddedApp = new CarbonApplication();
        embeddedApp.setAppFilePath(tempCAppDir.getAbsolutePath() + File.separator + "outer.car"
                + File.separator + "dependencies" + File.separator + "inner.car");
        embeddedApp.setEmbeddedCAR(true);

        ArrayList<String> faulty = new ArrayList<>(Arrays.asList("inner.car"));
        ArrayList<CarbonApplication> faultyObjects = new ArrayList<>(Arrays.asList(embeddedApp));
        setStaticField("faultyCapps", faulty);
        setStaticField("faultyCAppObjects", faultyObjects);

        invokeRetryFaultyCApps(createDeployer()); // must not throw

        assertTrue("faultyCapps must be cleared after retrying embedded CARs",
                   CappDeployer.getFaultyCapps().isEmpty());
        assertTrue("faultyCAppObjects must be cleared after retrying embedded CARs",
                   CappDeployer.getFaultyCAppObjects().isEmpty());
    }

    /**
     * When {@code faultyCAppObjects} has fewer entries than {@code faultyCapps}, the retry
     * must fall back to the {@code cAppDir + fileName} path for the unmatched entries and
     * complete without throwing.
     */
    @Test
    public void testRetryFaultyCAppsFallsBackToFileNameWhenObjectsMissing() throws Exception {
        CarbonApplication app = new CarbonApplication();
        app.setAppFilePath(tempCAppDir.getAbsolutePath() + File.separator + "first.car");
        app.setEmbeddedCAR(false);

        // faultyCapps has two entries but faultyCAppObjects has only one
        ArrayList<String> faulty = new ArrayList<>(Arrays.asList("first.car", "second.car"));
        ArrayList<CarbonApplication> faultyObjects = new ArrayList<>(Arrays.asList(app));
        setStaticField("faultyCapps", faulty);
        setStaticField("faultyCAppObjects", faultyObjects);

        invokeRetryFaultyCApps(createDeployer()); // must not throw

        assertTrue("faultyCapps must be cleared", CappDeployer.getFaultyCapps().isEmpty());
        assertTrue("faultyCAppObjects must be cleared", CappDeployer.getFaultyCAppObjects().isEmpty());
    }

    // -------------------------------------------------------------------------
    // Dependency test infrastructure
    // -------------------------------------------------------------------------

    /** Creates a {@link CappDeployer} pointing at {@link #tempCAppDir} for dependency tests. */
    private CappDeployer createDepDeployer() {
        CappDeployer deployer = new CappDeployer();
        deployer.setDirectory(tempCAppDir.getAbsolutePath());
        return deployer;
    }

    /**
     * Creates a minimal .car file inside {@link #tempCAppDir} for dependency-ordering tests.
     * The archive contains {@code artifacts.xml} and {@code metadata.xml} but no
     * {@code descriptor.xml} — call {@link DeployerUtilTest#writeDescriptorToExistingCarFile}
     * to add one afterwards.
     */
    private File createDepCarFile(String name) throws IOException {
        return DeployerUtilTest.createCarFile(tempCAppDir, name);
    }

    // -------------------------------------------------------------------------
    // Tests: dependency-based ordering (sortByDependencyOrderWithFallback)
    // -------------------------------------------------------------------------

    @Test
    public void testSort_AlphabeticalOrderWhenDescriptorMissing() throws IOException {
        File carA = createDepCarFile("a.car");
        File carB = createDepCarFile("b.car");
        File carC = createDepCarFile("c.car");
        CappDeployer deployer = createDepDeployer();

        List<DeploymentFileData> files = new ArrayList<>();
        files.add(new DeploymentFileData(carA, deployer));
        files.add(new DeploymentFileData(carB, deployer));
        files.add(new DeploymentFileData(carC, deployer));

        deployer.sort(files, 0, files.size());

        assertEquals("a.car", files.get(0).getFile().getName());
        assertEquals("b.car", files.get(1).getFile().getName());
        assertEquals("c.car", files.get(2).getFile().getName());
    }

    @Test
    public void testSort_DependencyOrderWhenDescriptorPresentWithoutDependencies() throws Exception {
        File carA = createDepCarFile("a.car");
        File carB = createDepCarFile("b.car");
        File carC = createDepCarFile("c.car");

        writeDescriptorToExistingCarFile(carA, "group", "a", "1.0.0");
        writeDescriptorToExistingCarFile(carB, "group", "b", "1.0.0");
        writeDescriptorToExistingCarFile(carC, "group", "c", "1.0.0");

        CappDeployer deployer = createDepDeployer();
        List<DeploymentFileData> files = new ArrayList<>();
        files.add(new DeploymentFileData(carB, deployer));
        files.add(new DeploymentFileData(carA, deployer));
        files.add(new DeploymentFileData(carC, deployer));

        deployer.sort(files, 0, files.size());

        assertEquals("a.car", files.get(0).getFile().getName());
        assertEquals("b.car", files.get(1).getFile().getName());
        assertEquals("c.car", files.get(2).getFile().getName());
    }

    @Test
    public void testSort_DependencyOrderWhenDescriptorPresentWithDependencies() throws Exception {
        File carA = createDepCarFile("a.car");
        File carB = createDepCarFile("b.car");
        File carC = createDepCarFile("c.car");

        // A depends on B, B depends on C, C has no dependencies
        String depB = "<dependency groupId=\"com.example\" artifactId=\"b\" version=\"1.0.0\" type=\"car\"/>";
        String depC = "<dependency groupId=\"com.example\" artifactId=\"c\" version=\"1.0.0\" type=\"car\"/>";

        writeDescriptorToExistingCarFile(carA, "com.example", "a", "1.0.0", depB);
        writeDescriptorToExistingCarFile(carB, "com.example", "b", "1.0.0", depC);
        writeDescriptorToExistingCarFile(carC, "com.example", "c", "1.0.0");

        CappDeployer deployer = createDepDeployer();
        List<DeploymentFileData> files = new ArrayList<>();
        files.add(new DeploymentFileData(carA, deployer));
        files.add(new DeploymentFileData(carB, deployer));
        files.add(new DeploymentFileData(carC, deployer));

        deployer.sort(files, 0, files.size());

        assertEquals("c.car", files.get(0).getFile().getName());
        assertEquals("b.car", files.get(1).getFile().getName());
        assertEquals("a.car", files.get(2).getFile().getName());
    }

    @Test
    public void testSort_DependencyPresentButCarIdDiffersFromFileName() throws Exception {
        File carA = createDepCarFile("a.car");
        File carB = createDepCarFile("b.car");
        File carC = createDepCarFile("c.car");

        String depB = "<dependency groupId=\"com.example\" artifactId=\"carB\" version=\"1.0.0\" type=\"car\"/>";
        String depC = "<dependency groupId=\"com.example\" artifactId=\"carC\" version=\"1.0.0\" type=\"car\"/>";

        writeDescriptorToExistingCarFile(carA, "com.example", "carA", "1.0.0", depB);
        writeDescriptorToExistingCarFile(carB, "com.example", "carB", "1.0.0", depC);
        writeDescriptorToExistingCarFile(carC, "com.example", "carC", "1.0.0");

        CappDeployer deployer = createDepDeployer();
        List<DeploymentFileData> files = new ArrayList<>();
        files.add(new DeploymentFileData(carA, deployer));
        files.add(new DeploymentFileData(carB, deployer));
        files.add(new DeploymentFileData(carC, deployer));

        deployer.sort(files, 0, files.size());

        assertEquals("c.car", files.get(0).getFile().getName());
        assertEquals("b.car", files.get(1).getFile().getName());
        assertEquals("a.car", files.get(2).getFile().getName());
    }

    @Test
    public void testSort_SubsetOfFiles() throws Exception {
        File carA = createDepCarFile("a.car");
        File carB = createDepCarFile("b.car");
        File carC = createDepCarFile("c.car");
        File carD = createDepCarFile("d.car");
        File carE = createDepCarFile("e.car");

        writeDescriptorToExistingCarFile(carA, "group", "a", "1.0.0");
        writeDescriptorToExistingCarFile(carB, "group", "b", "1.0.0");
        writeDescriptorToExistingCarFile(carC, "group", "c", "1.0.0");
        writeDescriptorToExistingCarFile(carD, "group", "d", "1.0.0");
        writeDescriptorToExistingCarFile(carE, "group", "e", "1.0.0");

        CappDeployer deployer = createDepDeployer();
        List<DeploymentFileData> files = new ArrayList<>();
        files.add(new DeploymentFileData(carE, deployer));
        files.add(new DeploymentFileData(carD, deployer));
        files.add(new DeploymentFileData(carC, deployer));
        files.add(new DeploymentFileData(carB, deployer));
        files.add(new DeploymentFileData(carA, deployer));

        // Only sort the middle three (carC, carB, carA)
        deployer.sort(files, 2, 5);

        assertEquals("e.car", files.get(0).getFile().getName());
        assertEquals("d.car", files.get(1).getFile().getName());
        assertEquals("a.car", files.get(2).getFile().getName());
        assertEquals("b.car", files.get(3).getFile().getName());
        assertEquals("c.car", files.get(4).getFile().getName());
    }

    @Test
    public void testSort_SubsetWithDependencies() throws Exception {
        File carA = createDepCarFile("a.car");
        File carB = createDepCarFile("b.car");
        File carC = createDepCarFile("c.car");
        File carD = createDepCarFile("d.car");
        File carE = createDepCarFile("e.car");

        // A depends on B, B depends on C, C/D/E have no dependencies
        String depB = "<dependency groupId=\"com.example\" artifactId=\"b\" version=\"1.0.0\" type=\"car\"/>";
        String depC = "<dependency groupId=\"com.example\" artifactId=\"c\" version=\"1.0.0\" type=\"car\"/>";

        writeDescriptorToExistingCarFile(carA, "com.example", "a", "1.0.0", depB);
        writeDescriptorToExistingCarFile(carB, "com.example", "b", "1.0.0", depC);
        writeDescriptorToExistingCarFile(carC, "com.example", "c", "1.0.0");
        writeDescriptorToExistingCarFile(carD, "com.example", "d", "1.0.0");
        writeDescriptorToExistingCarFile(carE, "com.example", "e", "1.0.0");

        CappDeployer deployer = createDepDeployer();
        List<DeploymentFileData> files = new ArrayList<>();
        files.add(new DeploymentFileData(carE, deployer));
        files.add(new DeploymentFileData(carD, deployer));
        files.add(new DeploymentFileData(carA, deployer));
        files.add(new DeploymentFileData(carB, deployer));
        files.add(new DeploymentFileData(carC, deployer));

        // Only sort the middle three (carA, carB, carC) which have dependencies
        deployer.sort(files, 2, 5);

        assertEquals("e.car", files.get(0).getFile().getName());
        assertEquals("d.car", files.get(1).getFile().getName());
        assertEquals("c.car", files.get(2).getFile().getName());
        assertEquals("b.car", files.get(3).getFile().getName());
        assertEquals("a.car", files.get(4).getFile().getName());
    }

    @Test
    public void testSort_DependencyInDirButNotInFilesList() throws Exception {
        File carA = createDepCarFile("a.car");
        File carB = createDepCarFile("b.car");
        File carC = createDepCarFile("c.car");
        File carD = createDepCarFile("d.car"); // present in dir but not in files list

        // A depends on B, B depends on D (not in files list), C has no dependencies
        String depB = "<dependency groupId=\"com.example\" artifactId=\"b\" version=\"1.0.0\" type=\"car\"/>";
        String depD = "<dependency groupId=\"com.example\" artifactId=\"d\" version=\"1.0.0\" type=\"car\"/>";

        writeDescriptorToExistingCarFile(carA, "com.example", "a", "1.0.0", depB);
        writeDescriptorToExistingCarFile(carB, "com.example", "b", "1.0.0", depD);
        writeDescriptorToExistingCarFile(carC, "com.example", "c", "1.0.0");
        writeDescriptorToExistingCarFile(carD, "com.example", "d", "1.0.0");

        CappDeployer deployer = createDepDeployer();
        List<DeploymentFileData> files = new ArrayList<>();
        files.add(new DeploymentFileData(carA, deployer));
        files.add(new DeploymentFileData(carB, deployer));
        files.add(new DeploymentFileData(carC, deployer));

        deployer.sort(files, 0, files.size());

        // C first (no deps), then B (dep D not in list), then A (depends on B)
        assertEquals("c.car", files.get(0).getFile().getName());
        assertEquals("b.car", files.get(1).getFile().getName());
        assertEquals("a.car", files.get(2).getFile().getName());
    }

    @Test
    public void testSort_MixedDescriptorPresence() throws Exception {
        File carA = createDepCarFile("a.car");
        File carB = createDepCarFile("b.car");
        File carC = createDepCarFile("c.car");

        // Only carA and carC have descriptor.xml; carB does not.
        // Because not all CApps have a descriptor, the sort falls back to alphabetical order.
        String depC = "<dependency groupId=\"group\" artifactId=\"c\" version=\"1.0.0\" type=\"car\"/>";
        writeDescriptorToExistingCarFile(carA, "group", "a", "1.0.0", depC);
        writeDescriptorToExistingCarFile(carC, "group", "c", "1.0.0");

        CappDeployer deployer = createDepDeployer();
        List<DeploymentFileData> files = new ArrayList<>();
        files.add(new DeploymentFileData(carC, deployer));
        files.add(new DeploymentFileData(carB, deployer));
        files.add(new DeploymentFileData(carA, deployer));

        deployer.sort(files, 0, files.size());

        assertEquals("a.car", files.get(0).getFile().getName());
        assertEquals("b.car", files.get(1).getFile().getName());
        assertEquals("c.car", files.get(2).getFile().getName());
    }

    @Test
    public void testSort_CyclicDependency() throws Exception {
        File carA = createDepCarFile("a.car");
        File carB = createDepCarFile("b.car");
        File carC = createDepCarFile("c.car");

        // A→B→C→A (cycle); sort must fall back to alphabetical order
        String depB = "<dependency groupId=\"com.example\" artifactId=\"b\" version=\"1.0.0\" type=\"car\"/>";
        String depC = "<dependency groupId=\"com.example\" artifactId=\"c\" version=\"1.0.0\" type=\"car\"/>";
        String depA = "<dependency groupId=\"com.example\" artifactId=\"a\" version=\"1.0.0\" type=\"car\"/>";

        writeDescriptorToExistingCarFile(carA, "com.example", "a", "1.0.0", depB);
        writeDescriptorToExistingCarFile(carB, "com.example", "b", "1.0.0", depC);
        writeDescriptorToExistingCarFile(carC, "com.example", "c", "1.0.0", depA);

        CappDeployer deployer = createDepDeployer();
        List<DeploymentFileData> files = new ArrayList<>();
        files.add(new DeploymentFileData(carB, deployer));
        files.add(new DeploymentFileData(carC, deployer));
        files.add(new DeploymentFileData(carA, deployer));

        deployer.sort(files, 0, files.size());

        assertEquals("a.car", files.get(0).getFile().getName());
        assertEquals("b.car", files.get(1).getFile().getName());
        assertEquals("c.car", files.get(2).getFile().getName());
    }

    @Test
    public void testSort_MissingDependencyAmongAvailableCApps() throws Exception {
        File carA = createDepCarFile("a.car");
        File carB = createDepCarFile("b.car");
        File carC = createDepCarFile("c.car");

        // A depends on X which does not exist; sort must fall back to alphabetical order
        String depX = "<dependency groupId=\"com.example\" artifactId=\"x\" version=\"1.0.0\" type=\"car\"/>";
        writeDescriptorToExistingCarFile(carA, "com.example", "a", "1.0.0", depX);
        writeDescriptorToExistingCarFile(carB, "com.example", "b", "1.0.0");
        writeDescriptorToExistingCarFile(carC, "com.example", "c", "1.0.0");

        CappDeployer deployer = createDepDeployer();
        List<DeploymentFileData> files = new ArrayList<>();
        files.add(new DeploymentFileData(carA, deployer));
        files.add(new DeploymentFileData(carB, deployer));
        files.add(new DeploymentFileData(carC, deployer));

        deployer.sort(files, 0, files.size());

        assertEquals("a.car", files.get(0).getFile().getName());
        assertEquals("b.car", files.get(1).getFile().getName());
        assertEquals("c.car", files.get(2).getFile().getName());
    }
}

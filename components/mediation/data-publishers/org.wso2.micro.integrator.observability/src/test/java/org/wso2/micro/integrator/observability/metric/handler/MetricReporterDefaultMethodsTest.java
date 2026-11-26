/*
 * Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.micro.integrator.observability.metric.handler;

import org.junit.Assert;
import org.junit.Test;
import org.wso2.micro.integrator.observability.util.MetricConstants;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

public class MetricReporterDefaultMethodsTest {

    private static final String PACKAGE = "org.wso2.micro.integrator.observability.metric.handler";

    // The interface as it was before the three default methods.
    private static final String OLD_INTERFACE = "package " + PACKAGE + ";\n"
            + "public interface MetricReporter {\n"
            + "    void createMetrics(String serviceType, String type, String metricName, String metricHelp,"
            + " String[] properties);\n"
            + "    void initMetrics();\n"
            + "    void initErrorMetrics(String serviceType, String type, String metricName, String metricHelp,"
            + " String[] properties);\n"
            + "    void incrementCount(String metricName, String[] properties);\n"
            + "    void decrementCount(String metricName, String[] properties);\n"
            + "    Object getTimer(String metricName, String[] properties);\n"
            + "    void observeTime(Object timer);\n"
            + "    void serverUp(String host, String port, String javaHome, String javaVersion);\n"
            + "    void serverVersion(String version, String updateLevel);\n"
            + "    void serverDown(String host, String port, String javaVersion, String javaHome);\n"
            + "    void serviceUp(String serviceName, String serviceType);\n"
            + "    void serviceDown(String serviceName, String serviceType);\n"
            + "}\n";

    // A reporter of a customer. Each call it gets adds one to calls.
    private static final String OLD_REPORTER = "package custom.reporter;\n"
            + "import " + PACKAGE + ".MetricReporter;\n"
            + "public class OldReporter implements MetricReporter {\n"
            + "    public int calls;\n"
            + "    public void createMetrics(String s, String t, String n, String h, String[] p) { calls++; }\n"
            + "    public void initMetrics() { calls++; }\n"
            + "    public void initErrorMetrics(String s, String t, String n, String h, String[] p) { calls++; }\n"
            + "    public void incrementCount(String n, String[] p) { calls++; }\n"
            + "    public void decrementCount(String n, String[] p) { calls++; }\n"
            + "    public Object getTimer(String n, String[] p) { calls++; return null; }\n"
            + "    public void observeTime(Object t) { calls++; }\n"
            + "    public void serverUp(String h, String p, String j, String v) { calls++; }\n"
            + "    public void serverVersion(String v, String u) { calls++; }\n"
            + "    public void serverDown(String h, String p, String v, String j) { calls++; }\n"
            + "    public void serviceUp(String n, String t) { calls++; }\n"
            + "    public void serviceDown(String n, String t) { calls++; }\n"
            + "}\n";

    @Test
    public void testAReporterBuiltAgainstTheOldInterfaceAnswersFalse() throws Exception {
        Path sources = Files.createTempDirectory("old_reporter_src");
        Path classes = Files.createTempDirectory("old_reporter_classes");
        Path oldInterface = write(sources.resolve("MetricReporter.java"), OLD_INTERFACE);
        Path oldReporter = write(sources.resolve("OldReporter.java"), OLD_REPORTER);
        JavaCompiler javac = ToolProvider.getSystemJavaCompiler();
        Assert.assertEquals("The old reporter compiles", 0, javac.run(null, null, null, "-d", classes.toString(),
                oldInterface.toString(), oldReporter.toString()));
        byte[] reporterClass = Files.readAllBytes(classes.resolve("custom/reporter/OldReporter.class"));

        // Only the reporter class comes from the old build. The interface is the one of this module.
        ClassLoader loader = new ClassLoader(getClass().getClassLoader()) {
            @Override
            protected Class<?> findClass(String name) throws ClassNotFoundException {
                if ("custom.reporter.OldReporter".equals(name)) {
                    return defineClass(name, reporterClass, 0, reporterClass.length);
                }
                throw new ClassNotFoundException(name);
            }
        };
        Class<?> type = loader.loadClass("custom.reporter.OldReporter");
        Assert.assertSame(MetricReporter.class, type.getInterfaces()[0]);
        MetricReporter reporter = (MetricReporter) type.getDeclaredConstructor().newInstance();

        Assert.assertFalse(reporter.createComponentMetric(MetricConstants.COUNTER, "test_custom_files_total",
                "Files done", new String[]{"kind", "name"}));
        Assert.assertFalse(reporter.createComponentMetric(MetricConstants.GAUGE, "test_custom_open_sessions",
                "Open sessions", new String[]{"kind", "name"}));
        Assert.assertFalse(reporter.setGauge("test_custom_open_sessions", new String[]{"CONNECTION", "c1"}, 1));
        Assert.assertFalse(reporter.initCounter("test_custom_files_total", new String[]{"CONNECTION", "c1"}));
        Assert.assertEquals("The defaults call nothing on the reporter", 0, type.getField("calls").getInt(reporter));
    }

    private static Path write(Path file, String text) throws IOException {
        return Files.write(file, text.getBytes(StandardCharsets.UTF_8));
    }
}

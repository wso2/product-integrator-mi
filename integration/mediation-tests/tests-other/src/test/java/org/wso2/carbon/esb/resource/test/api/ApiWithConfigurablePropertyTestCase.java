/*
 *  Copyright (c) 2024, WSO2 LLC. (https://www.wso2.com).
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.wso2.carbon.esb.resource.test.api;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.automation.engine.frameworkutils.FrameworkPathUtil;
import org.wso2.carbon.automation.test.utils.http.client.HttpRequestUtil;
import org.wso2.carbon.automation.test.utils.http.client.HttpResponse;
import org.wso2.carbon.integration.common.utils.exceptions.AutomationUtilException;
import org.wso2.esb.integration.common.utils.CarbonLogReader;
import org.wso2.esb.integration.common.utils.ESBIntegrationTest;
import org.wso2.esb.integration.common.utils.Utils;
import org.wso2.esb.integration.common.utils.common.ServerConfigurationManager;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ApiWithConfigurablePropertyTestCase extends ESBIntegrationTest {

    private ServerConfigurationManager serverConfigurationManager;
    private CarbonLogReader carbonLogReader;

    @BeforeClass(alwaysRun = true)
    public void init() throws Exception {
        super.init();
        carbonLogReader = new CarbonLogReader();
        carbonLogReader.start();
        serverConfigurationManager = new ServerConfigurationManager(context);
        File capp = new File(getESBResourceLocation() + File.separator + "config.var" + File.separator +
                "testApiWithConfigurationProperty_1.0.0.car");
        serverConfigurationManager.copyToCarbonapps(capp);
        assertTrue(Utils.checkForLog(carbonLogReader, "Successfully Deployed Carbon Application : " +
                        "testApiWithConfigurationProperty_1.0.0", 20), "Did not receive the expected info log");
    }

    @Test(groups = {"wso2.esb"}, description = "Configurable property", priority = 1)
    public void testConfigurablePropertyWithFile() throws IOException {
        Map<String, String> headers = new HashMap<>();
        URL endpoint = new URL(getApiInvocationURL("apiConfig/test_api"));
        HttpResponse httpResponse = HttpRequestUtil.doGet(endpoint.toString(), headers);
        Assert.assertEquals(httpResponse.getResponseCode(), 200);
        Assert.assertEquals(StringUtils.normalizeSpace(httpResponse.getData()),
                StringUtils.normalizeSpace("{ \"name\": \"file\", \"msg\": \"Gd mng\" }"),
                StringUtils.normalizeSpace(httpResponse.getData()));
    }

    @Test(groups = {"wso2.esb"}, description = "Configurable property", priority = 2)
    public void testConfigurablePropertyWithSystemProperty() throws IOException, AutomationUtilException {
        Map<String, String> commands = new HashMap<>();
        commands.put("-Dname", "sys");
        commands.put("-Dmsg", "Hi");
        serverConfigurationManager.restartMicroIntegrator(commands);
        Map<String, String> headers = new HashMap<>();
        URL endpoint = new URL(getApiInvocationURL("apiConfig/test_api"));
        HttpResponse httpResponse = HttpRequestUtil.doGet(endpoint.toString(), headers);
        Assert.assertEquals(httpResponse.getResponseCode(), 200);
        Assert.assertEquals(StringUtils.normalizeSpace(httpResponse.getData()),
                StringUtils.normalizeSpace("{ \"name\": \"sys\", \"msg\": \"Hi\" }"),
                StringUtils.normalizeSpace(httpResponse.getData()));
    }

    @Test(groups = {"wso2.esb"}, description = "Configurable property", priority = 3)
    public void testConfigurablePropertyWithEnvVariable() throws IOException, AutomationUtilException {
        Map<String, String> commands = new HashMap<>();
        commands.put("--env-file", FrameworkPathUtil.getSystemResourceLocation() + "test.env");
        serverConfigurationManager.restartMicroIntegrator(commands);
        Map<String, String> headers = new HashMap<>();
        URL endpoint = new URL(getApiInvocationURL("apiConfig/test_api"));
        HttpResponse httpResponse = HttpRequestUtil.doGet(endpoint.toString(), headers);
        Assert.assertEquals(httpResponse.getResponseCode(), 200);
        Assert.assertEquals(StringUtils.normalizeSpace(httpResponse.getData()),
                StringUtils.normalizeSpace("{ \"name\": \"env\", \"msg\": \"Hello\" }"),
                StringUtils.normalizeSpace(httpResponse.getData()));
    }

    @Test(groups = {"wso2.esb"}, description = "Configurable property", priority = 4)
    public void testConfigurableProperty() throws IOException, AutomationUtilException {
        Map<String, String> commands = new HashMap<>();
        commands.put("-Dname", "sys");
        commands.put("-Dmsg", "Hi");
        commands.put("--env-file", FrameworkPathUtil.getSystemResourceLocation() + "test.env");
        serverConfigurationManager.restartMicroIntegrator(commands);
        Map<String, String> headers = new HashMap<>();
        URL endpoint = new URL(getApiInvocationURL("apiConfig/test_api"));
        HttpResponse httpResponse = HttpRequestUtil.doGet(endpoint.toString(), headers);
        Assert.assertEquals(httpResponse.getResponseCode(), 200);
        Assert.assertEquals(StringUtils.normalizeSpace(httpResponse.getData()),
                StringUtils.normalizeSpace("{ \"name\": \"env\", \"msg\": \"Hello\" }"),
                StringUtils.normalizeSpace(httpResponse.getData()));
    }

    @Test(groups = {"wso2.esb"},
            description = "Regression for product-integrator-mi#4234: the launcher's export_env_file() loop "
                    + "must export the last KEY=VALUE line of an --env-file even when the file lacks a "
                    + "trailing newline.",
            priority = 5)
    public void testConfigurablePropertyWithEnvVariableNoTrailingNewline()
            throws IOException, AutomationUtilException, InterruptedException {
        // Build a .env file whose LAST line is `name=env` and which has NO trailing newline.
        // Pre-fix, the POSIX `read` loop in micro-integrator.sh would silently drop this last line,
        // leaving `name` undefined in the JVM environment and producing a "{name: , msg: Hello}"-style
        // response (and a `ConfigDeployer ... is not found` error in the log).
        Path envFile = Files.createTempFile("issue-4234-no-newline-", ".env");
        envFile.toFile().deleteOnExit();
        String contents = "msg=Hello\nname=env"; // no trailing '\n' — bug's trigger condition
        Files.write(envFile, contents.getBytes(StandardCharsets.UTF_8));

        carbonLogReader.clearLogs();
        Map<String, String> commands = new HashMap<>();
        commands.put("--env-file", envFile.toAbsolutePath().toString());
        serverConfigurationManager.restartMicroIntegrator(commands);

        Map<String, String> headers = new HashMap<>();
        URL endpoint = new URL(getApiInvocationURL("apiConfig/test_api"));
        HttpResponse httpResponse = HttpRequestUtil.doGet(endpoint.toString(), headers);
        Assert.assertEquals(httpResponse.getResponseCode(), 200);
        Assert.assertEquals(StringUtils.normalizeSpace(httpResponse.getData()),
                StringUtils.normalizeSpace("{ \"name\": \"env\", \"msg\": \"Hello\" }"),
                StringUtils.normalizeSpace(httpResponse.getData()));

        // The deployer logs `The value of the key:[<key>] is not found.` for any configurable
        // property that the launcher failed to export. With the fix, neither `name` nor `msg`
        // should hit that branch.
        assertFalse(carbonLogReader.assertIfLogExists("The value of the key:[name] is not found."),
                "ConfigDeployer logged a missing-key error for `name`; the launcher dropped the last "
                        + "line of the no-trailing-newline .env file. See product-integrator-mi#4234.");
        assertFalse(carbonLogReader.assertIfLogExists("The value of the key:[msg] is not found."),
                "ConfigDeployer logged a missing-key error for `msg`.");
    }

    @Test(groups = {"wso2.esb"},
            description = "Regression for product-integrator-mi#4234: a single-line --env-file with no "
                    + "trailing newline must still be exported in full.",
            priority = 6)
    public void testConfigurablePropertyWithSingleLineEnvFileNoTrailingNewline()
            throws IOException, AutomationUtilException, InterruptedException {
        // Edge case from the IA: a 1-line env file with no terminator. The base API needs both
        // `name` and `msg` resolved, so the unsupplied one falls back to a system property here.
        Path envFile = Files.createTempFile("issue-4234-single-line-", ".env");
        envFile.toFile().deleteOnExit();
        Files.write(envFile, "name=env".getBytes(StandardCharsets.UTF_8)); // single line, no '\n'

        carbonLogReader.clearLogs();
        Map<String, String> commands = new HashMap<>();
        commands.put("--env-file", envFile.toAbsolutePath().toString());
        commands.put("-Dmsg", "Hello");
        serverConfigurationManager.restartMicroIntegrator(commands);

        Map<String, String> headers = new HashMap<>();
        URL endpoint = new URL(getApiInvocationURL("apiConfig/test_api"));
        HttpResponse httpResponse = HttpRequestUtil.doGet(endpoint.toString(), headers);
        Assert.assertEquals(httpResponse.getResponseCode(), 200);
        Assert.assertEquals(StringUtils.normalizeSpace(httpResponse.getData()),
                StringUtils.normalizeSpace("{ \"name\": \"env\", \"msg\": \"Hello\" }"),
                StringUtils.normalizeSpace(httpResponse.getData()));
        assertFalse(carbonLogReader.assertIfLogExists("The value of the key:[name] is not found."),
                "ConfigDeployer logged a missing-key error for `name`; the launcher dropped the only "
                        + "line of a single-line, unterminated .env file. See product-integrator-mi#4234.");
    }

    @AfterClass(alwaysRun = true)
    public void destroy() throws Exception {
        serverConfigurationManager.removeFromCarbonapps("testApiWithConfigurationProperty_1.0.0.car");
        super.cleanup();
    }
}

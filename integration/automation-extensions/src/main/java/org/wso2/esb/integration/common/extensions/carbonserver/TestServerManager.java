/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.esb.integration.common.extensions.carbonserver;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.automation.engine.FrameworkConstants;
import org.wso2.carbon.automation.engine.context.AutomationContext;
import org.wso2.carbon.automation.engine.exceptions.AutomationFrameworkException;
import org.wso2.carbon.automation.engine.frameworkutils.FrameworkPathUtil;
import org.wso2.carbon.automation.extensions.ExtensionConstants;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class TestServerManager {

    private static final Log log = LogFactory.getLog(TestServerManager.class);
    private CarbonServerManager carbonServer;
    private String carbonZip;
    private int portOffset;
    private Map<String, String> commandMap;
    protected String carbonHome;

    TestServerManager(AutomationContext context, String carbonZip, Map<String, String> commandMap) {
        carbonServer = new CarbonServerManager(context);
        this.carbonZip = carbonZip;
        if (commandMap.get(ExtensionConstants.SERVER_STARTUP_PORT_OFFSET_COMMAND) != null) {
            this.portOffset = Integer.parseInt(commandMap.get(ExtensionConstants.SERVER_STARTUP_PORT_OFFSET_COMMAND));
        } else {
            throw new IllegalArgumentException("portOffset value must be set in command list");
        }
        this.commandMap = commandMap;
    }

    public String getCarbonHome() {
        return carbonHome;
    }

    private void configureServer(String deploymentDirectory, String registryDir) throws IOException {

        if ("ESB".equalsIgnoreCase(System.getProperty("server.list"))) {
            //copying the files before server start. Ex: synapse artifacts, conf, etc...
            ServerUtils.copyResources("ESB", this.getCarbonHome(), deploymentDirectory, registryDir);
            String resourceHome =
                    FrameworkPathUtil.getSystemResourceLocation() + File.separator + "artifacts" + File.separator
                            + "ESB" + File.separator + "server";
            File dropinsDirectorySource = new File(resourceHome + File.separator + "dropins");
            File dropinsDestination = new File(this.getCarbonHome() + File.separator + "dropins");
            if (dropinsDirectorySource.exists() && dropinsDirectorySource.isDirectory()) {
                try {
                    log.info("Copying " + dropinsDirectorySource.getPath() + " to " + dropinsDestination.getPath());
                    FileUtils.copyDirectory(dropinsDirectorySource, dropinsDestination);
                } catch (IOException e) {
                    log.error("Error while copying lib directory.", e);
                }
            }
        } else if ("DSS".equalsIgnoreCase(System.getProperty("server.list"))) {
            ServerUtils.copyResources("DSS", this.getCarbonHome(), deploymentDirectory, registryDir);
        }
    }

    public Map<String, String> getCommands() {
        return commandMap;
    }

    public String startServer() throws AutomationFrameworkException, IOException {
        return startServer(null, null);
    }

    /**
     * This method is called for starting a Carbon server in preparation for execution of a
     * TestSuite
     * <p/>
     * Add the @BeforeSuite TestNG annotation in the method overriding this method
     *
     * @return The CARBON_HOME
     * @throws IOException If an error occurs while copying the deployment artifacts into the
     *                     Carbon server
     */
    public String startServer(String deploymentDirectory, String registryDir)
            throws AutomationFrameworkException, IOException {
        if (carbonHome == null) {
            setUpCarbonHome(deploymentDirectory, registryDir);
        }
        log.info("Carbon Home - " + carbonHome);
        if (commandMap.get(ExtensionConstants.SERVER_STARTUP_PORT_OFFSET_COMMAND) != null) {
            this.portOffset = Integer.parseInt(commandMap.get(ExtensionConstants.SERVER_STARTUP_PORT_OFFSET_COMMAND));
        } else {
            this.portOffset = 0;
        }
        carbonServer.startServerUsingCarbonHome(carbonHome, commandMap);
        return carbonHome;
    }

    private void setUpCarbonHome(String deploymentDirectory, String registryDir)
            throws IOException, AutomationFrameworkException {
        if (carbonZip == null) {
            carbonZip = System.getProperty(FrameworkConstants.SYSTEM_PROPERTY_CARBON_ZIP_LOCATION);
        }
        if (carbonZip == null) {
            throw new IllegalArgumentException("carbon zip file cannot find in the given location");
        }
        carbonHome = carbonServer.setUpCarbonHome(carbonZip, commandMap.get("startupScript"));
        configureServer(deploymentDirectory, registryDir);
    }

    /**
     * Restarting server already started by the method startServer.
     */
    public void restartGracefully() throws AutomationFrameworkException {
        if (carbonHome == null) {
            throw new AutomationFrameworkException(
                    "No Running Server found to restart. " + "Please make sure whether server is started");
        }
        carbonServer.restartGracefully();
    }

    /**
     * This method is called for stopping a Carbon server
     * <p/>
     * Add the @AfterSuite annotation in the method overriding this method
     *
     * @throws AutomationFrameworkException If an error occurs while shutting down the server
     */
    public void stopServer() throws AutomationFrameworkException {
        carbonServer.serverShutdown(portOffset, false);
    }

    public void stopServerSkipCoverage() throws AutomationFrameworkException {
        log.info("Stopping server with coverage skip enabled");
        carbonServer.serverShutdown(portOffset, true);
    }

    void restartServer() throws AutomationFrameworkException {

        log.info("Preparing to restart the server ...");
        carbonServer.serverShutdown(portOffset, true);
        carbonServer.startServerUsingCarbonHome(carbonHome, commandMap);
        log.info("Server restarted successfully ...");
    }

    void restartServer(Map<String, String> commands) throws AutomationFrameworkException {
        log.info("Preparing to restart the server ...");
        commandMap.putAll(commands);
        carbonServer.serverShutdown(portOffset, true);
        carbonServer.startServerUsingCarbonHome(carbonHome, commandMap);
        log.info("Server restarted successfully ...");
    }

    void startMIServer() throws AutomationFrameworkException {

        log.info("Preparing to start the MI server ...");
        carbonServer.startServerUsingCarbonHome(carbonHome, commandMap);
        log.info("Server restarted successfully ...");
    }

}

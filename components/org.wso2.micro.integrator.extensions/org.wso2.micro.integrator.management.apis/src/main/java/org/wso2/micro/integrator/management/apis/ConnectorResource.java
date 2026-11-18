/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.micro.integrator.management.apis;

import com.google.gson.JsonObject;
import org.apache.axis2.AxisFault;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.libraries.imports.SynapseImport;
import org.apache.synapse.libraries.model.Library;
import org.apache.synapse.libraries.model.SynapseLibrary;

import org.json.JSONArray;
import org.json.JSONObject;
import org.wso2.carbon.connector.core.ConnectionHealthRegistry;
import org.wso2.micro.core.util.AuditLogger;
import org.wso2.micro.integrator.initializer.ServiceBusUtils;
import org.wso2.micro.integrator.initializer.persistence.MediationPersistenceManager;
import org.wso2.micro.integrator.initializer.deployment.synapse.deployer.SynapseAppDeployer;
import org.wso2.micro.integrator.management.apis.security.handler.SecurityUtils;
import org.wso2.micro.integrator.security.user.api.UserStoreException;

import java.io.IOException;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.wso2.micro.integrator.management.apis.Constants.ITEM_TYPE_IMPORT;
import static org.wso2.micro.integrator.management.apis.Constants.SEARCH_KEY;
import static org.wso2.micro.integrator.management.apis.Constants.USERNAME_PROPERTY;

/**
 * API Resource to manage connectors deployed
 **/
public class ConnectorResource implements MiApiResource {

    private static final Log LOG = LogFactory.getLog(ConnectorResource.class);

    // Set of HTTP methods allowed by the resource
    Set<String> methods = null;
    // Attributes in json payload
    private static final String NAME_ATTRIBUTE = "name";
    private static final String STATUS_ATTRIBUTE = "status";
    private static final String PACKAGE_ATTRIBUTE = "package";
    private static final String DESCRIPTION_ATTRIBUTE = "description";
    private static final String CONNECTOR_NAME = "connectorName";
    private static final String PACKAGE_NAME = "packageName";
    private static final String CONNECTIONS_ATTRIBUTE = "connections";
    private static final String HEALTH_ATTRIBUTE = "health";

    public ConnectorResource() {

        methods = new HashSet<>();
        methods.add(Constants.HTTP_POST);
        methods.add(Constants.HTTP_GET);
    }

    @Override
    public Set<String> getMethods() {
        return methods;
    }

    @Override
    public boolean invoke(MessageContext messageContext,
                          org.apache.axis2.context.MessageContext axis2MessageContext,
                          SynapseConfiguration synapseConfiguration) {

        String connectorName = Utils.getQueryParameter(messageContext, CONNECTOR_NAME);
        String searchKey = Utils.getQueryParameter(messageContext, SEARCH_KEY);

        if (messageContext.isDoingGET()) {
            if (Objects.nonNull(connectorName)) {
                populateConnectorData(axis2MessageContext, synapseConfiguration, connectorName);
            } else if (Objects.nonNull(searchKey) && !searchKey.trim().isEmpty()) {
                populateSearchResults(messageContext, searchKey.toLowerCase());
            } else {
                populateConnectorList(messageContext, synapseConfiguration);
            }
            axis2MessageContext.removeProperty(Constants.NO_ENTITY_BODY);
        } else {
            String userName = (String) messageContext.getProperty(USERNAME_PROPERTY);
            try {
                if (SecurityUtils.canUserEdit(messageContext, userName)) {
                    if (!JsonUtil.hasAJsonPayload(axis2MessageContext)) {
                        Utils.setJsonPayLoad(axis2MessageContext, Utils.createJsonErrorObject("POST method required json payload"));
                    } else {
                        JsonObject payload = Utils.getJsonPayload(axis2MessageContext);
                        String performedBy = Constants.ANONYMOUS_USER;
                        if (messageContext.getProperty(Constants.USERNAME_PROPERTY) !=  null) {
                            performedBy = messageContext.getProperty(Constants.USERNAME_PROPERTY).toString();
                        }
                        if (payload.has(NAME_ATTRIBUTE) && payload.has(STATUS_ATTRIBUTE) && payload.has(PACKAGE_ATTRIBUTE)) {
                            changeConnectorState(performedBy, axis2MessageContext, payload, synapseConfiguration);
                        } else {
                            Utils.setJsonPayLoad(axis2MessageContext, Utils.createJsonErrorObject("Missing parameters in payload"));
                        }
                    }
                } else {
                    Utils.sendForbiddenFaultResponse(axis2MessageContext);
                }
            } catch (AxisFault axisFault) {
                LOG.error("Error when updating connector status", axisFault);
                Utils.setJsonPayLoad(axis2MessageContext, Utils.createJsonErrorObject("Error when updating connector status"));
            } catch (IOException e) {
                LOG.error("Error when parsing JSON payload", e);
                Utils.setJsonPayLoad(axis2MessageContext, Utils.createJsonErrorObject("Error when parsing JSON payload"));
            } catch (UserStoreException e) {
                LOG.error("Error occurred while retrieving the user data", e);
                Utils.setJsonPayLoad(axis2MessageContext, Utils.createJsonErrorObject("Error occurred while retrieving the user data"));
            }
        }
        return true;
    }
    private List<Library> getSearchResults(MessageContext messageContext, String searchKey){
        SynapseConfiguration configuration = messageContext.getConfiguration();
        return configuration.getSynapseLibraries().values().stream()
                .filter(artifact -> artifact.getFileName().toLowerCase().contains(searchKey))
                .collect(Collectors.toList());
    }
    
    private void populateSearchResults(MessageContext messageContext, String searchKey) {
        List<Library> searchResultList = getSearchResults(messageContext, searchKey);
        setResponseBody(searchResultList, messageContext);
    }

    private void setResponseBody(Collection<Library> libraryList, MessageContext messageContext) {
        org.apache.axis2.context.MessageContext axis2MessageContext =
                ((Axis2MessageContext) messageContext).getAxis2MessageContext();
        JSONObject jsonBody = Utils.createJSONList(libraryList.size());
        for (Library library : libraryList) {
            addToJsonList(jsonBody.getJSONArray(Constants.LIST), (SynapseLibrary) library);
        }
        Utils.setJsonPayLoad(axis2MessageContext, jsonBody);
    }
    private void populateConnectorData(org.apache.axis2.context.MessageContext axis2MessageContext,
                                       SynapseConfiguration synapseConfiguration, String connectorName) {

        Map<String, Library> libraries = synapseConfiguration.getSynapseLibraries();

        for (Map.Entry<String, Library> entry : libraries.entrySet()) {
            if (((SynapseLibrary)entry.getValue()).getName().equals(connectorName)) {
                SynapseLibrary connector = (SynapseLibrary)entry.getValue();
                if (Objects.nonNull(connector)) {
                    Utils.setJsonPayLoad(axis2MessageContext, getConnectorAsJson(connector));
                }
            }
        }
    }

    private Object getConnectorAsJson(SynapseLibrary connector) {
        JSONObject connectorObject  = new JSONObject();
        connectorObject.put(NAME_ATTRIBUTE, connector.getName());
        connectorObject.put(PACKAGE_ATTRIBUTE, connector.getPackage());
        connectorObject.put(DESCRIPTION_ATTRIBUTE, connector.getDescription());
        connectorObject.put(STATUS_ATTRIBUTE, getConnectorState(connector.getLibStatus()));
        addConnections(connectorObject, connector);
        return connectorObject;
    }

    /**
     * Sets the list of all available connectors to the response as json
     *
     * @param messageContext MessageContext
     * @param synapseConfiguration Synapse configuration object
     */
    private void populateConnectorList(MessageContext messageContext,
                                       SynapseConfiguration synapseConfiguration) {
        Map<String, Library> libraryMap = synapseConfiguration.getSynapseLibraries();
        Collection<Library> libraryList = libraryMap.values();
        setResponseBody(libraryList, messageContext);
    }

    /**
     * Adds the provided synapse library to a json list
     * @param jsonList JsonArray
     * @param library Connector
     */
    private void addToJsonList(JSONArray jsonList, SynapseLibrary library) {

        JSONObject connectorObject = new JSONObject();
        connectorObject.put(NAME_ATTRIBUTE, library.getName());
        connectorObject.put(PACKAGE_ATTRIBUTE, library.getPackage());
        connectorObject.put(DESCRIPTION_ATTRIBUTE, library.getDescription());
        connectorObject.put(STATUS_ATTRIBUTE, getConnectorState(library.getLibStatus()));
        addConnections(connectorObject, library);
        jsonList.put(connectorObject);
    }

    /**
     * Adds the list of the connections of a connector that registered their health. A connector with no
     * registered connection gets no list.
     *
     * @param connectorObject the entry of the connector
     * @param library         the deployed connector library
     */
    private void addConnections(JSONObject connectorObject, SynapseLibrary library) {
        String connector = "{" + library.getQName().getNamespaceURI() + "}" + library.getName();
        Map<String, Supplier<Map<String, Object>>> healthByName =
                new TreeMap<>(ConnectionHealthRegistry.getConnections(connector));
        if (healthByName.isEmpty()) {
            return;
        }
        JSONArray connections = new JSONArray();
        for (Map.Entry<String, Supplier<Map<String, Object>>> entry : healthByName.entrySet()) {
            JSONObject connection = new JSONObject();
            connection.put(NAME_ATTRIBUTE, entry.getKey());
            JSONObject health = getConnectionHealth(connector, entry.getKey(), entry.getValue());
            if (health != null) {
                // The status and the health come from the one copy, so both show the same state.
                Object status = health.opt(STATUS_ATTRIBUTE);
                if (status instanceof String) {
                    connection.put(STATUS_ATTRIBUTE, status);
                }
                connection.put(HEALTH_ATTRIBUTE, health);
            }
            connections.put(connection);
        }
        connectorObject.put(CONNECTIONS_ATTRIBUTE, connections);
    }

    private JSONObject getConnectionHealth(String connector, String connection,
                                           Supplier<Map<String, Object>> supplier) {
        try {
            Map<String, Object> health = supplier.get();
            if (health == null) {
                LOG.debug("No health for the connection " + connection + " of the connector " + connector);
                return null;
            }
            return Utils.copyHealth(health);
        } catch (Throwable t) {
            // A failing health call of a connector, or a health map that can not be printed, never breaks the API.
            LOG.debug("Health not available for the connection " + connection + " of the connector " + connector,
                    t);
            return null;
        }
    }

    /**
     * Returns the String representation of the connector state
     *
     * @param status Boolean synapse library status
     * @return status
     */
    private String getConnectorState(Boolean status) {

        if (status) {
            return "enabled";
        }
        return "disabled";
    }

    /**
     * Changes the state of the connector to the specified state in the json payload
     *
     * @param axis2MessageContext axis2 message context
     * @param payload             request json payload
     */
    private void changeConnectorState(String performedBy, org.apache.axis2.context.MessageContext axis2MessageContext,
                                      JsonObject payload, SynapseConfiguration synapseConfiguration) throws AxisFault {

        String connector = payload.get(NAME_ATTRIBUTE).getAsString();
        String packageName = payload.get(PACKAGE_ATTRIBUTE).getAsString();
        String state = payload.get(STATUS_ATTRIBUTE).getAsString();
        AxisConfiguration axisConfiguration = axis2MessageContext.getConfigurationContext().getAxisConfiguration();
        SynapseAppDeployer appDeployer = new SynapseAppDeployer();
        String qualifiedName = getQualifiedName(connector, packageName);
        Boolean updated = appDeployer.
                updateStatus(qualifiedName, connector, packageName, state, axisConfiguration);
        JSONObject jsonResponse = new JSONObject();
        if (updated) {
            persistStatus(axisConfiguration, synapseConfiguration, qualifiedName);
            JSONObject info = new JSONObject();
            info.put(CONNECTOR_NAME, connector);
            info.put(PACKAGE_NAME, packageName);
            info.put(STATUS_ATTRIBUTE, state);
            AuditLogger.logAuditMessage(performedBy, Constants.AUDIT_LOG_TYPE_CONNECTOR,
                                        Constants.AUDIT_LOG_ACTION_CREATED, info);

            jsonResponse.put("Message", "Status updated successfully");
        } else {
            jsonResponse.put("Message", "Status updated failed");
        }
        Utils.setJsonPayLoad(axis2MessageContext, jsonResponse);
    }

    /**
     * Return qualified name of the library
     *
     * @param libName     name of the synapse library
     * @param packageName package of the synapse library
     * @return qualified name
     */
    private String getQualifiedName(String libName, String packageName) {

        return ("{" + packageName.trim() + "}" + libName.trim()).toLowerCase();
    }

    /**
     * Persist the change state of the connector by writing to the import file
     *
     * @param axisConfiguration    axisconfiguration
     * @param synapseConfiguration synapseconfiguration
     * @param qualifiedName        fully qualified name of the library
     */
    private void persistStatus(AxisConfiguration axisConfiguration,
                               SynapseConfiguration synapseConfiguration, String qualifiedName) {
        SynapseImport synapseImport = synapseConfiguration.getSynapseImports().get(qualifiedName);
        MediationPersistenceManager mediationPersistenceManager = ServiceBusUtils.
                getMediationPersistenceManager(axisConfiguration);
        mediationPersistenceManager.saveItem(synapseImport.getName(), ITEM_TYPE_IMPORT);
    }
}

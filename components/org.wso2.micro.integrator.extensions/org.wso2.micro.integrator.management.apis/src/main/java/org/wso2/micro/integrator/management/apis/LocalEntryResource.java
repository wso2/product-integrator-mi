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

import org.apache.axiom.om.OMElement;
import org.apache.axis2.AxisFault;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.config.Entry;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.wso2.micro.integrator.management.apis.Constants.LIST;
import static org.wso2.micro.integrator.management.apis.Constants.SEARCH_KEY;

/**
 * This class provides mechanisms to monitor local entries deployed.
 */
public class LocalEntryResource implements MiApiResource {

    private static final Log LOG = LogFactory.getLog(LocalEntryResource.class);

    // HTTP method types supported by the resource
    Set<String> methods;
    // Constants for local-entry json object
    private static final String TYPE_ATTRIBUTE= "type";
    private static final String VALUE_ATTRIBUTE= "value";
    private static final String DESCRIPTION_ATTRIBUTE= "description";

    public LocalEntryResource() {
        methods = new HashSet<>(1);
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

        String entryKey = Utils.getQueryParameter(messageContext, Constants.NAME);
        String searchKey = Utils.getQueryParameter(messageContext, SEARCH_KEY);

        try {
            if (Objects.nonNull(entryKey)) {
                populateLocalEntryData(axis2MessageContext, synapseConfiguration, entryKey);
            } else if (Objects.nonNull(searchKey) && !searchKey.trim().isEmpty()) {
                populateSearchResults(messageContext, searchKey.toLowerCase());
            } else {
                populateLocalEntries(axis2MessageContext, synapseConfiguration);
            }
            axis2MessageContext.removeProperty(Constants.NO_ENTITY_BODY);
            return true;
        } catch (AxisFault e) {
            LOG.error("Error while populating service: ", e);
            messageContext.setProperty(Constants.HTTP_STATUS_CODE, Constants.INTERNAL_SERVER_ERROR);
        }
        return false;
    }

    private List<Entry> getSearchResults(MessageContext messageContext, String searchKey) throws AxisFault {
        SynapseConfiguration configuration = messageContext.getConfiguration();
        return configuration.getDefinedEntries().values().stream()
                .filter(entry -> entry.getKey().toLowerCase().contains(searchKey))
                .collect(Collectors.toList());
    }

    private void populateSearchResults(MessageContext messageContext, String searchKey) throws AxisFault {
        List<Entry> searchResultList = getSearchResults(messageContext, searchKey);
        setResponseBody(searchResultList, messageContext);
    }

    private void setResponseBody(Collection<Entry> entriesCollection, MessageContext messageContext) {

        org.apache.axis2.context.MessageContext axis2MessageContext =
                ((Axis2MessageContext) messageContext).getAxis2MessageContext();
        JSONObject jsonBody = Utils.createJSONList(entriesCollection.size() - 2);
        for (Entry entry : entriesCollection) {
            JSONObject entryObject =  getLocalEntryAsJson(entry);
            jsonBody.getJSONArray(LIST).put(entryObject);
        }
        Utils.setJsonPayLoad(axis2MessageContext, jsonBody);
    }


    /**
     * Sets the list of all available local entries to the response as json.
     *
     * @param axis2MessageContext axis2 message context
     * @param synapseConfiguration Synapse configuration object
     */
    private void populateLocalEntries(org.apache.axis2.context.MessageContext axis2MessageContext,
                                      SynapseConfiguration synapseConfiguration) {

        Map<String, Entry> definedEntries = synapseConfiguration.getDefinedEntries();
        // (-2) to account for the 2 local entries defined by the server
        JSONObject jsonBody = Utils.createJSONList(definedEntries.size() - 2);
        definedEntries.forEach((key, entry) -> addLocalEntryToJsonList(entry, jsonBody.getJSONArray(Constants.LIST)));
        Utils.setJsonPayLoad(axis2MessageContext, jsonBody);
    }

    /**
     * Adds the provided Entry to the json array provided.
     *
     * @param list  json array
     * @param entry Entry
     */
    private void addLocalEntryToJsonList(Entry entry, JSONArray list) {

        // Escape local entries defined by the server
        String key = entry.getKey();
        if (SynapseConstants.SERVER_IP.equals(key) || SynapseConstants.SERVER_HOST.equals(key)) {
            return;
        }
        String entryType = getEntryType(entry);
        JSONObject entryObject = new JSONObject();
        entryObject.put(Constants.NAME, key);
        entryObject.put(TYPE_ATTRIBUTE, entryType);
        list.put(entryObject);

    }

    /**
     * Returns the type of the specified local entry.
     *
     * @param localEntry local entry
     * @return entry type String
     */
    private String getEntryType(Entry localEntry) {

        String entryType;
        switch (localEntry.getType()) {
            case Entry.REMOTE_ENTRY:
                entryType = "Registry Key";
                break;
            case Entry.INLINE_TEXT:
                entryType = "Inline Text";
                break;
            case Entry.INLINE_XML:
                entryType = "Inline XML";
                break;
            case Entry.URL_SRC:
                entryType = "Source URL";
                break;
            default:
                entryType = "Unknown - " + localEntry.getType();
                break;
        }
        return entryType;
    }

    /**
     * Sets the information of the specified local entry to the response as json.
     *
     * @param axis2MessageContext AXIS2 message context
     * @param synapseConfiguration Synapse configuration object
     * @param entryKey local entry name
     */
    private void populateLocalEntryData(org.apache.axis2.context.MessageContext axis2MessageContext,
                                        SynapseConfiguration synapseConfiguration, String entryKey) {

        Map<String, Entry> definedEntries = synapseConfiguration.getDefinedEntries();
        Entry localEntry = definedEntries.get(entryKey);
        if (localEntry != null) {
            Utils.setJsonPayLoad(axis2MessageContext, getLocalEntryAsJson(localEntry));
        } else {
            LOG.warn("Reference for local entry " + entryKey + " could not be resolved.");
            Utils.setJsonPayLoad(axis2MessageContext, Utils.createJsonError("Reference for " + entryKey + " could not" +
                    " be resolved", axis2MessageContext, Constants.NOT_FOUND));
        }
    }

    /**
     * Returns the json representation of the local entry.
     *
     * @param localEntry local entry
     * @return JSONObject json representation of local entry
     */
    private JSONObject getLocalEntryAsJson(Entry localEntry) {

        JSONObject jsonObject = new JSONObject();
        jsonObject.put(Constants.NAME, localEntry.getKey());
        jsonObject.put(TYPE_ATTRIBUTE, getEntryType(localEntry));
        jsonObject.put(VALUE_ATTRIBUTE, maskLocalEntryValue(localEntry));
        jsonObject.put(DESCRIPTION_ATTRIBUTE, localEntry.getDescription());
        return jsonObject;
    }

    /**
     * Returns the value of the given local entry with its secrets masked. An inline value is masked as a whole
     * configuration, since a connector keeps its connection parameters in a local entry and those carry credentials
     * such as an access token or an api key at any depth.
     *
     * @param localEntry local entry to be inspected
     * @return the masked value of the local entry
     */
    private Object maskLocalEntryValue(Entry localEntry) {

        Object value = localEntry.getValue();
        if (value instanceof OMElement) {
            return Utils.maskSecretsInConfiguration((OMElement) value);
        }
        return Utils.maskSecretValue(localEntry.getKey(), value);
    }
}

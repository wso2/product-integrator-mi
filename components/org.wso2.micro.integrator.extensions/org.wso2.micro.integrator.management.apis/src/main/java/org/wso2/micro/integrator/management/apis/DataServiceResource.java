/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.micro.integrator.management.apis;

import com.google.gson.Gson;
import org.apache.axis2.description.AxisService;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.json.JSONObject;
import org.wso2.carbon.inbound.endpoint.internal.http.api.APIResource;
import org.wso2.micro.integrator.dataservices.common.DBConstants;
import org.wso2.micro.integrator.dataservices.core.DBDeployer;
import org.wso2.micro.integrator.dataservices.core.DBUtils;
import org.wso2.micro.integrator.dataservices.core.FaultyDataService;
import org.wso2.micro.integrator.dataservices.core.description.config.Config;
import org.wso2.micro.integrator.dataservices.core.description.operation.Operation;
import org.wso2.micro.integrator.dataservices.core.description.query.Query;
import org.wso2.micro.integrator.dataservices.core.description.resource.Resource;
import org.wso2.micro.integrator.dataservices.core.engine.DataService;
import org.wso2.micro.integrator.dataservices.core.engine.DataServiceSerializer;
import org.wso2.micro.integrator.management.apis.models.dataServices.DataServiceInfo;
import org.wso2.micro.integrator.management.apis.models.dataServices.DataServiceSummary;
import org.wso2.micro.integrator.management.apis.models.dataServices.DataSourceInfo;
import org.wso2.micro.integrator.management.apis.models.dataServices.OperationInfo;
import org.wso2.micro.integrator.management.apis.models.dataServices.QuerySummary;
import org.wso2.micro.integrator.management.apis.models.dataServices.ResourceInfo;
import org.wso2.micro.service.mgt.ServiceAdmin;
import org.wso2.micro.service.mgt.ServiceMetaData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.wso2.micro.integrator.management.apis.Constants.NOT_FOUND;
import static org.wso2.micro.integrator.management.apis.Constants.SEARCH_KEY;

public class DataServiceResource extends APIResource {

    private static final Log log = LogFactory.getLog(DataServiceResource.class);

    private static final String FAULT_PATH_SUFFIX = "/fault";

    private static ServiceAdmin serviceAdmin = null;

    private final String urlTemplate;

    public DataServiceResource(String urlTemplate) {
        super(urlTemplate);
        this.urlTemplate = urlTemplate;
    }

    @Override
    public Set<String> getMethods() {
        Set<String> methods = new HashSet<>();
        methods.add(Constants.HTTP_GET);
        return methods;
    }

    @Override
    public boolean invoke(MessageContext msgCtx) {
        buildMessage(msgCtx);
        if (serviceAdmin == null) {
            serviceAdmin = Utils.getServiceAdmin(msgCtx);
        }

        org.apache.axis2.context.MessageContext axis2MessageContext =
                ((Axis2MessageContext) msgCtx).getAxis2MessageContext();

        // Fault detail request: GET /management/data-services/{name}/fault
        if (urlTemplate.endsWith(FAULT_PATH_SUFFIX)) {
            String pathParamName = Utils.getPathParameter(msgCtx, "name");
            log.debug("Handling fault detail request for data service: " + pathParamName);
            try {
                populateFaultyDataServiceData(msgCtx, pathParamName);
            } catch (RuntimeException exception) {
                log.error("Error while populating faulty data service: ", exception);
                msgCtx.setProperty(Constants.HTTP_STATUS_CODE, Constants.INTERNAL_SERVER_ERROR);
            }
            axis2MessageContext.removeProperty(Constants.NO_ENTITY_BODY);
            return true;
        }

        String param = Utils.getQueryParameter(msgCtx, "dataServiceName");
        String searchKey = Utils.getQueryParameter(msgCtx, SEARCH_KEY);

        try {
            if (param != null) {
                // data-service specified by name
                populateDataServiceByName(msgCtx, param);
            } else if (Objects.nonNull(searchKey) && !searchKey.trim().isEmpty()) {
                populateSearchResults(msgCtx, searchKey);
            } else {
                // list of all data-services
                populateDataServiceList(msgCtx);
            }
        } catch (Exception exception) {
            log.error("Error while populating service: ", exception);
            msgCtx.setProperty(Constants.HTTP_STATUS_CODE, Constants.INTERNAL_SERVER_ERROR);
        }

        axis2MessageContext.removeProperty(Constants.NO_ENTITY_BODY);
        return true;
    }

    private void populateSearchResults(MessageContext msgCtx, String searchKey) throws Exception {

        org.apache.axis2.context.MessageContext axis2MessageContext =
                ((Axis2MessageContext) msgCtx).getAxis2MessageContext();

        SynapseConfiguration configuration = msgCtx.getConfiguration();
        AxisConfiguration axisConfiguration = configuration.getAxisConfiguration();

        String normalized = searchKey.toLowerCase(Locale.ROOT);

        // Active matches
        List<String> matchedNames = Arrays.stream(DBUtils.getAvailableDS(axisConfiguration))
                .filter(name -> name.toLowerCase(Locale.ROOT).contains(normalized))
                .collect(Collectors.toList());
        List<DataServiceSummary> summaryList = new ArrayList<>();
        for (String dataServiceName : matchedNames) {
            AxisService axisService = axisConfiguration.getServiceForActivation(dataServiceName);
            if (axisService == null) {
                continue;
            }
            DataService dataService = (DataService) axisService.getParameter(DBConstants.DATA_SERVICE_OBJECT).getValue();
            ServiceMetaData serviceMetaData = getServiceMetaData(dataService);
            if (serviceMetaData != null) {
                summaryList.add(new DataServiceSummary(serviceMetaData.getName(), serviceMetaData.getWsdlURLs()));
            }
        }
        // Faulty matches
        List<FaultyDataService> faultyMatches = DBDeployer.getFaultyDataServices().stream()
                .filter(fds -> fds.getServiceName().toLowerCase(Locale.ROOT).contains(normalized))
                .collect(Collectors.toList());
        List<Map<String, String>> faultyEntries = new ArrayList<>();
        for (FaultyDataService fds : faultyMatches) {
            Map<String, String> entry = new LinkedHashMap<>();
            entry.put("name", fds.getServiceName());
            entry.put("errorMessage", fds.getErrorMessage() != null ? fds.getErrorMessage() : "");
            faultyEntries.add(entry);
        }

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("count", summaryList.size());
        response.put("list", summaryList);
        response.put("faultyCount", faultyMatches.size());
        response.put("faultyList", faultyEntries);

        Utils.setJsonPayLoad(axis2MessageContext, new Gson().toJson(response));
    }

    private void populateDataServiceList(MessageContext msgCtx) throws Exception {

        org.apache.axis2.context.MessageContext axis2MessageContext =
                ((Axis2MessageContext) msgCtx).getAxis2MessageContext();

        SynapseConfiguration configuration = msgCtx.getConfiguration();
        AxisConfiguration axisConfiguration = configuration.getAxisConfiguration();
        List<String> dataServicesNames =
                Arrays.stream(DBUtils.getAvailableDS(axisConfiguration)).collect(Collectors.toList());

        // Build active list
        List<DataServiceSummary> summaryList = new ArrayList<>();
        for (String dataServiceName : dataServicesNames) {
            AxisService axisService = axisConfiguration.getServiceForActivation(dataServiceName);
            if (axisService == null) {
                continue;
            }
            DataService dataService = (DataService) axisService.getParameter(DBConstants.DATA_SERVICE_OBJECT).getValue();
            ServiceMetaData serviceMetaData = getServiceMetaData(dataService);
            if (serviceMetaData != null) {
                summaryList.add(new DataServiceSummary(serviceMetaData.getName(), serviceMetaData.getWsdlURLs()));
            }
        }
        Collection<FaultyDataService> faultyCollection = DBDeployer.getFaultyDataServices();
        List<Map<String, String>> faultyEntries = new ArrayList<>();
        for (FaultyDataService fds : faultyCollection) {
            Map<String, String> entry = new LinkedHashMap<>();
            entry.put("name", fds.getServiceName());
            entry.put("errorMessage", fds.getErrorMessage() != null ? fds.getErrorMessage() : "");
            faultyEntries.add(entry);
        }

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("count", summaryList.size());
        response.put("list", summaryList);
        response.put("faultyCount", faultyCollection.size());
        response.put("faultyList", faultyEntries);

        Utils.setJsonPayLoad(axis2MessageContext, new Gson().toJson(response));
    }

    private void populateDataServiceByName(MessageContext msgCtx, String serviceName) throws Exception {
        DataService dataService = getDataServiceByName(msgCtx, serviceName);

        org.apache.axis2.context.MessageContext axis2MessageContext = ((Axis2MessageContext) msgCtx)
                .getAxis2MessageContext();

        if (dataService == null) {
            axis2MessageContext.setProperty(Constants.HTTP_STATUS_CODE, Constants.NOT_FOUND);
        } else {
            ServiceMetaData serviceMetaData = getServiceMetaData(dataService);
            DataServiceInfo dataServiceInfo = null;

            if (serviceMetaData != null) {
                dataServiceInfo = new DataServiceInfo(serviceMetaData.getName(), serviceMetaData.getDescription(),
                                                      serviceMetaData.getServiceGroupName(),
                                                      serviceMetaData.getWsdlURLs(), serviceMetaData.getSwaggerUrl());

                Map<String, Query> queries = dataService.getQueries();
                for (Map.Entry<String, Query> stringQuery : queries.entrySet()) {
                    QuerySummary querySummary = new QuerySummary();
                    querySummary.setId(stringQuery.getKey());
                    querySummary.setNamespace(stringQuery.getValue().getNamespace());
                    querySummary.setConfigId(stringQuery.getValue().getConfigId());
                    dataServiceInfo.addQuery(querySummary);
                }
            }
            dataServiceInfo.setDataSources(getDataSources(dataService));
            dataServiceInfo.setResources(getResources(dataService));
            dataServiceInfo.setOperations(getOperations(dataService));
            dataServiceInfo.setConfiguration(Utils.maskPasswordsInConfiguration(
                    DataServiceSerializer.serializeDataService(dataService, true)).toString());
            String stringPayload = new Gson().toJson(dataServiceInfo);
            Utils.setJsonPayLoad(axis2MessageContext, new JSONObject(stringPayload));
        }
    }

    private void populateFaultyDataServiceData(MessageContext msgCtx, String serviceName) {

        org.apache.axis2.context.MessageContext axis2MessageContext =
                ((Axis2MessageContext) msgCtx).getAxis2MessageContext();

        FaultyDataService faultyDs = DBDeployer.getFaultyDataServices().stream()
                .filter(fds -> serviceName.equals(fds.getServiceName()))
                .findFirst().orElse(null);

        if (faultyDs == null) {
            log.warn("Faulty data service not found for the given name: " + serviceName);
            Utils.setJsonPayLoad(axis2MessageContext,
                    Utils.createJsonError("Faulty data service not found for the given name: " + serviceName,
                            axis2MessageContext, NOT_FOUND));
            return;
        }

        Map<String, String> jsonBody = new LinkedHashMap<>();
        jsonBody.put("serviceName", faultyDs.getServiceName());
        jsonBody.put("errorMessage", faultyDs.getErrorMessage() != null ? faultyDs.getErrorMessage() : "");
        jsonBody.put("faultStackTrace", faultyDs.getFaultStackTrace() != null ? faultyDs.getFaultStackTrace() : "");
        Utils.setJsonPayLoad(axis2MessageContext, new Gson().toJson(jsonBody));
    }

    private DataService getDataServiceByName(MessageContext msgCtx, String serviceName) {
        AxisService axisService = msgCtx.getConfiguration().
                getAxisConfiguration().getServiceForActivation(serviceName);
        DataService dataService = null;
        if (axisService != null) {
            dataService = (DataService) axisService.getParameter(DBConstants.DATA_SERVICE_OBJECT).getValue();
        } else {
            log.debug("DataService {} is null.");
        }
        return dataService;
    }

    private ServiceMetaData getServiceMetaData(DataService dataService) throws Exception {
        if (dataService != null) {
            return serviceAdmin.getServiceData(dataService.getName());
        } else {
            return null;
        }
    }

    private List<ResourceInfo> getResources(DataService dataService) {

        Set<Resource.ResourceID> resourceIDS = dataService.getResourceIds();
        List<ResourceInfo> resourceList = new ArrayList<>();
        for (Resource.ResourceID id : resourceIDS) {
            ResourceInfo resourceInfo = new ResourceInfo();
            Resource resource = dataService.getResource(id);
            resourceInfo.setResourcePath(id.getPath());
            resourceInfo.setResourceMethod(id.getMethod());
            resourceInfo.setResourceQuery(resource.getCallQuery().getQueryId());
            resourceInfo.setQueryParams(resource.getCallQuery().getQuery().getQueryParams());
            resourceList.add(resourceInfo);
        }
        return resourceList;
    }

    private List<OperationInfo> getOperations(DataService dataService) {

        List<OperationInfo> opertionList = new ArrayList<>();
        Set<String> operationNames = dataService.getOperationNames();
        for (String operationName : operationNames) {
            OperationInfo operationInfo = new OperationInfo();
            Operation operation = dataService.getOperation(operationName);
            operationInfo.setOperationName(operationName);
            operationInfo.setQueryName(operation.getCallQuery().getQueryId());
            operationInfo.setQueryParams(operation.getCallQuery().getQuery().getQueryParams());
            opertionList.add(operationInfo);
        }
        return opertionList;
    }

    private List<DataSourceInfo> getDataSources(DataService dataService) {

        Map<String, Config> configs = dataService.getConfigs();
        List<DataSourceInfo> dataSources = new ArrayList<>();
        configs.forEach((name, config) -> {
            DataSourceInfo dataSource = new DataSourceInfo();
            dataSource.setDataSourceId(config.getConfigId());
            dataSource.setDataSourceType(config.getType());
            // maskPasswordParameters returns a map of objects, whereas a data source property is always a string.
            Map<String, String> maskedProperties = new LinkedHashMap<>();
            config.getProperties().forEach((propertyName, value) ->
                    maskedProperties.put(propertyName, (String) Utils.maskSecretValue(propertyName, value)));
            dataSource.setDataSourceProperties(maskedProperties);
            dataSources.add(dataSource);
        });
        return dataSources;
    }
}

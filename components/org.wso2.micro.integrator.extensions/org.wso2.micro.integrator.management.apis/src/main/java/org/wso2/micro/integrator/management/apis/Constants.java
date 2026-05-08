/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import org.wso2.micro.integrator.core.services.CarbonServerConfigurationService;

import javax.xml.namespace.QName;

public class Constants {
    //Constants for the API resource prefixes
    public static final String REST_API_CONTEXT = "/management";
    public static final String PREFIX_APIS = "/apis";
    public static final String PREFIX_CARBON_APPS = "/applications";
    public static final String PATH_PARAM_CARBON_APP_NAME = "/" + "{name}";
    public static final String PATH_PARAM_CARBON_APP_FAULT = PATH_PARAM_CARBON_APP_NAME + "/fault";
    public static final String PREFIX_ENDPOINTS = "/endpoints";
    public static final String PREFIX_INBOUND_ENDPOINTS = "/inbound-endpoints";
    public static final String PREFIX_PROXY_SERVICES = "/proxy-services";
    public static final String PREFIX_REGISTRY_RESOURCES = "/registry-resources";
    public static final String PREFIX_REGISTRY_CONTENT = "/registry-resources/content";
    public static final String PREFIX_REGISTRY_METADATA = "/registry-resources/metadata";
    public static final String PREFIX_REGISTRY_PROPERTIES = "/registry-resources/properties";
    public static final String PREFIX_TASKS = "/tasks";
    public static final String PREFIX_SEQUENCES = "/sequences";
    public static final String PREFIX_LOGGING = "/logging";
    public static final String PREFIX_USERS = "/users";
    public static final String PATH_PARAM_USER = "/" + "{userId}";
    public static final String PREFIX_DATA_SERVICES = "/data-services";
    public static final String PREFIX_TEMPLATES = "/templates";
    public static final String PREFIX_MESSAGE_STORE = "/message-stores";
    public static final String PREFIX_MESSAGE_PROCESSORS = "/message-processors";
    public static final String PREFIX_EXTERNAL_VAULTS = "/external-vaults";
    public static final String PATH_PARAM_EXTERNAL_VAULT_NAME = "/" + "{vault}";
    public static final String PREFIX_LOCAL_ENTRIES = "/local-entries";
    public static final String PREFIX_CONNECTORS = "/connectors";
    public static final String PREFIX_LOGIN = "/login";
    public static final String PREFIX_LOGOUT = "/logout";
    public static final String PREFIX_SERVER_DATA = "/server";
    public static final String PREFIX_LOG_FILES = "/logs";
    public static final String PREFIX_TRANSACTION = "/transactions";
    public static final String PREFIX_DATA_SOURCES = "/data-sources";
    public static final String PREFIX_ROLES = "/roles";
    public static final String PATH_PARAM_ROLE = "/" + "{role}";
    public static final String PATH_PARAM_TRANSACTION = "/" + "{param}";
    public static final String ROOT_CONTEXT = "/";
    public static final String PREFIX_CONFIGS = "/configs";
    public static final String PREFIX_ICP = "/icp";
    public static final String PREFIX_ARTIFACTS = "/artifacts";

    public static final String COUNT = "count";
    public static final String TOTAL_COUNT = "totalCount";
    public static final String LIST = "list";
    public static final String ACTIVE_COUNT = "activeCount";
    public static final String ACTIVE_LIST = "activeList";
    public static final String FAULTY_COUNT = "faultyCount";
    public static final String FAULTY_LIST = "faultyList";
    public static final String NAME = "name";
    public static final String STATUS = "status";
    public static final String URL = "url";
    public static final String URL_LIST= "urlList";
    public static final String VERSION = "version";
    public static final String CONTAINER = "container";
    public static final String TYPE = "type";
    public static final String METHOD = "method";
    public static final String STATS = "stats";
    public static final String TRACING = "tracing";
    public static final String ENABLED = "enabled";
    public static final String DISABLED = "disabled";
    public static final String MESSAGE = "message";
    public static final String LEVEL = "level";
    public static final String PARENT = "parent";
    public static final String LOGGER_NAME = "loggerName";
    public static final String COMPONENT_NAME = "componentName";
    public static final String LOGGING_LEVEL = "loggingLevel";
    public static final String ROOT_LOGGER = "rootLogger";
    public static final String ANONYMOUS_USER = "anonymous";

    public static final String NO_ENTITY_BODY = "NO_ENTITY_BODY";
    public static final String HTTP_STATUS_CODE = "HTTP_SC";
    public static final String HTTP_METHOD_PATCH = "PATCH";
    public static final String NOT_FOUND = "404";
    public static final String FORBIDDEN = "403";
    public static final String INTERNAL_SERVER_ERROR = "500";
    public static final String BAD_REQUEST = "400";
    public static final String MESSAGE_TYPE = "messageType";
    public static final String CONTENT_TYPE = "ContentType";

    public static final String MEDIA_TYPE_APPLICATION_OCTET_STREAM = "application/octet-stream";

    public static final String HTTP_METHOD_PROPERTY = "HTTP_METHOD";
    public static final String HTTP_POST = "POST";
    public static final String HTTP_GET = "GET";
    public static final String HTTP_PUT = "PUT";
    public static final String HTTP_DELETE = "DELETE";

    public static final String HEADER_VALUE_APPLICATION_JSON = "application/json";
    public static final String MANAGEMENT_APPLICATION_JSON = "application/json+management";
    public static final String MESSAGE_JSON_ATTRIBUTE = "Message";

    // Json attribute in response for synapse configuration
    public static final String SYNAPSE_CONFIGURATION = "configuration";

    // Types of functional components
    public static final int ITEM_TYPE_IMPORT = 14;

    // Synapse service statuses
    public static final String ACTIVE_STATUS = "active";
    public static final String INACTIVE_STATUS = "inactive";
    public static final String TRIGGER_STATUS = "trigger";

    // Constant on pax logging
    public static final String PAX_LOGGING_CONFIGURATION_PID = "org.ops4j.pax.logging";

    // Constants used for the users resource
    public static final String USER_ID = "userId";
    public static final String USERS = "users";
    public static final String DOMAIN = "domain";
    public static final String ROLES = "roles";
    public static final String ROLE = "role";
    public static final String PASSWORD = "password";
    public static final String NEW_PASSWORD = "newPassword";
    public static final String CONFIRM_PASSWORD = "confirmPassword";
    public static final String OLD_PASSWORD = "oldPassword";
    public static final String IS_ADMIN = "isAdmin";
    public static final String ADMIN = "admin";
    public static final String PATTERN = "pattern";
    public static final String PASSWORD_MASKED_VALUE = "*****";

    public static final String USERNAME_PROPERTY = "USERNAME";
    public static final String IS_ADMIN_USER_PROPERTY = "IS_ADMIN_USER";
    public static final String ICP_AUTHENTICATED_PROPERTY = "ICP_AUTHENTICATED";

    // Constant QNames used in the internal-apis.xml
    public static final QName NAME_ATTR = new QName("name");
    public static final QName USER_STORE_Q = new QName("UserStore");
    public static final QName USERS_Q = new QName("users");
    public static final QName APIS_Q = new QName("apis");
    public static final QName API_Q = new QName("api");

    public static final String MGT_API_NAME = "ManagementApi";

    // tracing constants
    static final String TRACE = "trace";
    static final String STATISTICS = "statistics";
    static final String ENABLE = "enable";
    static final String DISABLE = "disable";

    // toml properties
    public static String FILE_BASED_USER_STORE_ENABLE = "internal_apis.file_user_store.enable";
    public static String MAKE_NON_ADMIN_USERS_READ_ONLY = "user_access.make_non_admin_users_read_only";

    public static final String AUDIT_LOG_TYPE_ENDPOINT = "endpoint";
    public static final String AUDIT_LOG_TYPE_USER = "user";
    public static final String AUDIT_LOG_TYPE_PROXY_SERVICE = "proxy_service";
    public static final String AUDIT_LOG_TYPE_LOG_LEVEL = "log_level";
    public static final String AUDIT_LOG_TYPE_ROOT_LOG_LEVEL = "root_log_level";
    public static final String AUDIT_LOG_TYPE_MESSAGE_PROCESSOR = "message_processor";
    public static final String AUDIT_LOG_TYPE_INBOUND_ENDPOINT = "inbound_endpoint";
    public static final String AUDIT_LOG_TYPE_TASK = "task";
    public static final String AUDIT_LOG_TYPE_CARBON_APPLICATION = "carbon_application";
    public static final String AUDIT_LOG_TYPE_CONNECTOR = "connector";

    public static final String AUDIT_LOG_TYPE_API_STATE_CHANGE = "api_";
    public static final String AUDIT_LOG_TYPE_PROXY_SERVICE_STATE_CHANGE = "proxy_service_";
    public static final String AUDIT_LOG_TYPE_INBOUND_ENDPOINT_STATE_CHANGE = "inbound_endpoint_";
    public static final String AUDIT_LOG_TYPE_SEQUENCE_TEMPLATE_STATE_CHANGE = "sequence_template_";
    public static final String AUDIT_LOG_TYPE_SEQUENCE_STATE_CHANGE = "sequence_";
    public static final String AUDIT_LOG_TYPE_ENDPOINT_STATE_CHANGE = "endpoint_";
    public static final String AUDIT_LOG_TYPE_REGISTRY_RESOURCE = "registry_resource";
    public static final String AUDIT_LOG_TYPE_REGISTRY_RESOURCE_PROPERTIES = "registry_resource_properties";

    // Audit log types for tracing
    public static final String AUDIT_LOG_TYPE_PROXY_SERVICE_TRACE = "proxy_service_trace";
    public static final String AUDIT_LOG_TYPE_ENDPOINT_TRACE = "endpoint_trace";
    public static final String AUDIT_LOG_TYPE_INBOUND_ENDPOINT_TRACE = "inbound_endpoint_trace";
    public static final String AUDIT_LOG_TYPE_API_TRACE = "api_trace";
    public static final String AUDIT_LOG_TYPE_SEQUENCE_TRACE = "sequence_trace";
    public static final String AUDIT_LOG_TYPE_SEQUENCE_TEMPLATE_TRACE = "sequence_template_trace";

    // Audit log types for statistics
    public static final String AUDIT_LOG_TYPE_PROXY_SERVICE_STATISTICS = "proxy_service_statistics";
    public static final String AUDIT_LOG_TYPE_ENDPOINT_STATISTICS = "endpoint_statistics";
    public static final String AUDIT_LOG_TYPE_INBOUND_ENDPOINT_STATISTICS = "inbound_endpoint_statistics";
    public static final String AUDIT_LOG_TYPE_API_STATISTICS = "api_statistics";
    public static final String AUDIT_LOG_TYPE_SEQUENCE_STATISTICS = "sequence_statistics";
    public static final String AUDIT_LOG_TYPE_SEQUENCE_TEMPLATE_STATISTICS = "sequence_template_statistics";

    public static final String PROXY_SERVICES = "proxy-services";
    public static final String NODES = "nodes";
    public static final String APIS = "apis";
    public static final String INBOUND_ENDPOINTS = "inbound-endpoints";
    public static final String SEQUENCE_TEMPLATE = "templates_sequence";
    public static final String SEQUENCES = "sequences";
    public static final String ENDPOINTS = "endpoints";
    public static final String CARBON_APPLICATIONS = "carbonapps";
    public static final String DATA_SERVICES = "data-services";
    public static final String DATA_SOURCES = "data-sources";
    public static final String LOCAL_ENTRIES = "local-entries";
    public static final String LOG_FILES = "logs";
    public static final String LOGGING_RESOURCES = "log-configs";
    public static final String CONNECTORS = "connectors";
    public static final String MESSAGE_PROCESSORS = "message-processors";
    public static final String MESSAGE_STORES = "message-stores";
    public static final String TEMPLATES = "templates";
    public static final String TASKS = "tasks";
    public static final String REGISTRY_RESOURCES = "registry-resources";
    public static final String RESOURCE_TYPE = "resourceType";
    public static final String AUDIT_LOG_ACTION_ENABLE = "enabled";
    public static final String AUDIT_LOG_ACTION_DISABLED = "disabled";
    public static final String AUDIT_LOG_ACTION_CREATED = "created";
    public static final String AUDIT_LOG_ACTION_DELETED = "deleted";
    public static final String AUDIT_LOG_ACTION_UPDATED = "updated";
    public static final String AUDIT_LOG_ACTION_TRIGGERED = "triggered";

    public static final String DOMAIN_SEPARATOR;

    static {
        String userDomainSeparator = CarbonServerConfigurationService.getInstance()
                .getFirstProperty("UserDomainSeparator");
        if (userDomainSeparator != null && !userDomainSeparator.trim().isEmpty()) {
            DOMAIN_SEPARATOR = userDomainSeparator.trim();
        } else {
            DOMAIN_SEPARATOR = "/";
        }
    }

    static final String SERVICE_PID = "service.pid";
    public static final String REGISTRY_PATH = "path";
    public static final String REGISTRY_PATH_FOR_PROPERTY = "pathForProp";
    public static final String EXPAND_PARAM = "expand";
    public static final String VALUE_TRUE = "true";
    public static final String CONTENT_TYPE_MULTIPART_FORM_DATA = "multipart/form-data";
    public static final String REGISTRY_ROOT_PATH = "registry";
    public static final String ERROR_KEY = "error";
    public static final String CONFIGURATION_REGISTRY_PATH = "registry/config";
    public static final String CONFIGURATION_REGISTRY_PREFIX = "conf:";
    public static final String GOVERNANCE_REGISTRY_PATH = "registry/governance";
    public static final String GOVERNANCE_REGISTRY_PREFIX = "gov:";
    public static final String LOCAL_REGISTRY_PATH = "registry/local";
    public static final String LOCAL_REGISTRY_PREFIX = "local:";
    public static final String DEFAULT_MEDIA_TYPE = "text/plain";
    public static final String MEDIA_TYPE_KEY = "mediaType";
    public static final String PROPERTY_EXTENSION = ".properties";
    public static final String COLLECTIONS_PROPERTY_EXTENSIONS = ".collections.properties";
    public static final String VALUE_KEY = "value";
    public static final String REGISTRY_RESOURCE_NAME = "registryResourceName";
    public static final String REGISTRY_PROPERTY_NAME = "propertyName";
    public static final String FILE = "file";
    public static final int MAXIMUM_RETRY_COUNT = 5;

    // Searching constants
    public static final String SEARCH_KEY = "searchKey";
    public static final Character BASIC_AUTH_SEPARATOR_CHAR = ':';

    public static final String ARTIFACT_TYPE = "type";
    public static final String PROXY_SERVICE = "proxy-service";
    public static final String ENDPOINT = "endpoint";
    public static final String INBOUND_ENDPOINT = "inbound-endpoint";
    public static final String MESSAGE_PROCESSOR = "message-processor";
    public static final String API = "api";
    public static final String SEQUENCE = "sequence";
    public static final String TASK = "task";
    public static final String MESSAGE_STORE = "message-store";
    public static final String CARBON_APPLICATION = "carbonapp";
    public static final String DATA_SOURCE = "data-source";
    public static final String DATA_SERVICE = "data-service";
    public static final String TEMPLATE = "template";

}

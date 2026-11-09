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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.axiom.om.OMAttribute;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMNode;
import org.apache.axiom.om.impl.llom.OMTextImpl;
import org.apache.axis2.AxisFault;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.synapse.MessageContext;
import org.apache.synapse.aspects.AspectConfiguration;
import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.rest.RESTConstants;
import org.json.JSONArray;
import org.json.JSONObject;
import org.ops4j.pax.logging.PaxLoggingConstants;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;
import org.wso2.config.mapper.ConfigParser;
import org.wso2.micro.core.util.AuditLogger;
import org.wso2.micro.integrator.initializer.utils.ConfigurationHolder;
import org.wso2.micro.integrator.registry.MicroIntegratorRegistry;
import org.wso2.micro.integrator.security.MicroIntegratorSecurityUtils;
import org.wso2.micro.integrator.security.user.api.RealmConfiguration;
import org.wso2.micro.integrator.security.user.api.UserStoreException;
import org.wso2.micro.integrator.security.user.api.UserStoreManager;
import org.wso2.micro.integrator.security.user.core.UserCoreConstants;
import org.wso2.micro.integrator.security.user.core.common.AbstractUserStoreManager;
import org.wso2.micro.service.mgt.ServiceAdmin;

import javax.xml.namespace.QName;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.wso2.micro.integrator.management.apis.Constants.BAD_REQUEST;
import static org.wso2.micro.integrator.management.apis.Constants.COLLECTIONS_PROPERTY_EXTENSIONS;
import static org.wso2.micro.integrator.management.apis.Constants.CONFIGURATION_REGISTRY_PATH;
import static org.wso2.micro.integrator.management.apis.Constants.CONFIGURATION_REGISTRY_PREFIX;
import static org.wso2.micro.integrator.management.apis.Constants.FILE;
import static org.wso2.micro.integrator.management.apis.Constants.FORBIDDEN;
import static org.wso2.micro.integrator.management.apis.Constants.GOVERNANCE_REGISTRY_PATH;
import static org.wso2.micro.integrator.management.apis.Constants.GOVERNANCE_REGISTRY_PREFIX;
import static org.wso2.micro.integrator.management.apis.Constants.INTERNAL_SERVER_ERROR;
import static org.wso2.micro.integrator.management.apis.Constants.LOCAL_REGISTRY_PATH;
import static org.wso2.micro.integrator.management.apis.Constants.LOCAL_REGISTRY_PREFIX;
import static org.wso2.micro.integrator.management.apis.Constants.PROPERTY_EXTENSION;
import static org.wso2.micro.integrator.management.apis.Constants.REGISTRY_ROOT_PATH;
import static org.wso2.micro.integrator.management.apis.Constants.USERNAME_PROPERTY;
import static org.wso2.micro.integrator.registry.MicroIntegratorRegistryConstants.URL_SEPARATOR;

public class Utils {

    private static final Log LOG = LogFactory.getLog(Utils.class);

    /**
     * Fragments of artifact parameter names which indicate that the value of the parameter is a secret. A fragment
     * which spans two words lists the separators a name may be spelled with, since {@code apiKey}, {@code api.key}
     * and {@code api_key} all name the same parameter.
     */
    private static final String[] SECRET_PARAMETER_NAME_FRAGMENTS =
            {Constants.PASSWORD, "secret", "credential", "token", "passphrase", "signature",
             "apikey", "api.key", "api_key", "api-key",
             "privatekey", "private.key", "private_key", "private-key"};

    /**
     * Fragments which carry one of the secret fragments but hold no secret, such as the {@code tokenUrl} of an OAuth
     * configuration or the {@code allowCredentials} flag of a CORS handler. Each is dropped from the name before the
     * secret fragments are looked for, so that such a parameter stays visible through the management API. They are
     * compound because a short fragment here would hide real secrets.
     */
    private static final String[] NON_SECRET_PARAMETER_NAME_FRAGMENTS =
            {"allowcredentials",
             "tokenurl", "token.url", "token_url", "token-url",
             "tokenendpoint", "token.endpoint", "token_endpoint", "token-endpoint",
             "tokentype", "token.type", "token_type", "token-type",
             "signaturelength", "signaturealgorithm",
             "passwordcallbackclass", "passwordtype",
             "secretalias", "privatekeyalias"};

    private static final QName NAME_ATTRIBUTE = new QName("name");
    private static final QName KEY_ATTRIBUTE = new QName("key");
    private static final QName VALUE_ATTRIBUTE = new QName("value");
    private static final QName DEFAULT_VALUE_ATTRIBUTE = new QName(Constants.DEFAULT_VALUE);
    private static final QName EXPRESSION_ATTRIBUTE = new QName("expression");

    /**
     * Attributes which hold the value of an element. The {@code defaultValue} of a sequence template parameter is the
     * value the template uses when a caller does not pass the parameter, therefore it is as much a secret as a value.
     */
    private static final QName[] VALUE_ATTRIBUTES = {VALUE_ATTRIBUTE, DEFAULT_VALUE_ATTRIBUTE};

    /**
     * Elements which may carry their value as a child element, which is how a parameter or a property holding well
     * formed XML is serialized.
     */
    private static final String[] XML_VALUE_ELEMENTS = {"parameter", "property"};

    /**
     * Matches the credentials of a URI such as {@code vfs:sftp://user:password@host/path}, which is how the VFS
     * transport parameters of a proxy service carry the credentials of the remote file system. The password group
     * accepts {@code @} and is matched greedily, so that the credentials of a URI which carries an unencoded
     * {@code @} within the password are anchored on the last {@code @} and are therefore masked as a whole.
     */
    private static final Pattern URI_CREDENTIALS_PATTERN =
            Pattern.compile("([a-zA-Z][a-zA-Z0-9+.\\-]*://[^/?#\\s:@]+):([^/?#\\s]+)@");

    /**
     * Expressions which resolve a secret at runtime. A value which holds one of these carries no secret, therefore it
     * is left as it is so that the configured alias remains visible.
     */
    private static final String[] SECURE_VAULT_EXPRESSIONS = {"vault-lookup", "vault-decrypt", "$secret{"};

    public static String getQueryParameter(MessageContext messageContext, String key) {

        if (Objects.nonNull(messageContext.getProperty(RESTConstants.REST_QUERY_PARAM_PREFIX + key))) {
            return messageContext.getProperty(RESTConstants.REST_QUERY_PARAM_PREFIX + key).toString();
        }
        return null;
    }

    /**
     * Extracts the value set for the patch parameter with the given key.
     *
     * @param messageContext message context to extract the parameter from
     * @param key            the key defined in the uri template
     * @return the resolved value from the url. Returns null if not present.
     */
    public static String getPathParameter(MessageContext messageContext, String key){
        if (Objects.nonNull(messageContext.getProperty(RESTConstants.REST_URI_VARIABLE_PREFIX + key))) {
            return messageContext.getProperty(RESTConstants.REST_URI_VARIABLE_PREFIX + key).toString();
        }
        return null;
    }

    /**
     * Returns the string representation of a property set in the message context
     *
     * @param messageContext the message context to extract the property from
     * @param key            the key of the property
     * @return the string if a non empty value has been set. Returns null, if the property is not present or if the
     * value is empty.
     */
    public static String getStringPropertyFromMessageContext(MessageContext messageContext, String key) {
        Object propertyObject = messageContext.getProperty(key);
        if (Objects.nonNull(propertyObject)) {
            String propertyString = propertyObject.toString();
            if (!StringUtils.isEmpty(propertyString)) {
                return propertyString;
            }
        }
        return null;
    }

    /**
     * Validates if the request is authenticated.
     *
     * @param messageContext the message context to extract the property from
     * @return true if the user is authenticated.
     */
    public static boolean isUserAuthenticated(MessageContext messageContext) {
        return !Objects.isNull(getStringPropertyFromMessageContext(messageContext, USERNAME_PROPERTY));
    }

    public static void setJsonPayLoad(org.apache.axis2.context.MessageContext axis2MessageContext, Object jsonObject) {

        try {
            JsonUtil.getNewJsonPayload(axis2MessageContext, jsonObject.toString(), true, true);
        } catch (AxisFault axisFault) {
            axis2MessageContext.setProperty(Constants.HTTP_STATUS_CODE, Constants.INTERNAL_SERVER_ERROR);
            LOG.error("Error occurred while setting json payload", axisFault);
        }

        if (axis2MessageContext.getConfigurationContext().getAxisConfiguration().
                getMessageFormatter(Constants.MANAGEMENT_APPLICATION_JSON) != null) {
            axis2MessageContext.setProperty("messageType", Constants.MANAGEMENT_APPLICATION_JSON);
        } else {
            axis2MessageContext.setProperty("messageType", Constants.HEADER_VALUE_APPLICATION_JSON);
        }

        axis2MessageContext.setProperty("ContentType", Constants.HEADER_VALUE_APPLICATION_JSON);
        axis2MessageContext.removeProperty(Constants.NO_ENTITY_BODY);
    }

    static JSONObject handleResourceStateChange(String performedBy, String type, JSONObject info,
                                    AspectConfiguration config, String artifactName,
                                    org.apache.axis2.context.MessageContext axisMsgCtx, JsonObject payload) {

        JSONObject response;
        String msg;
        if (payload.has(Constants.TRACE)) {
            String traceType = type + Constants.TRACE;
            response = handleTracing(performedBy, traceType, info, config, artifactName, axisMsgCtx, payload);
        } else if (payload.has(Constants.STATISTICS)) {
            String statisticsType = type + Constants.STATISTICS;
            response =  handleStatistics(performedBy, statisticsType, info, config, artifactName, axisMsgCtx, payload);
        } else {
            msg = "Invalid payload. Required parameter is missing.";
            response = createJsonError(msg, axisMsgCtx, Constants.BAD_REQUEST);
        }
        return response;
    }

    private static JSONObject handleTracing(String performedBy, String type, JSONObject info,
                                    AspectConfiguration config, String artifactName,
                                    org.apache.axis2.context.MessageContext axisMsgCtx, JsonObject payload) {

        JSONObject response = new JSONObject();
        String msg;
        String traceState = payload.get(Constants.TRACE).getAsString();
        if (Constants.ENABLE.equalsIgnoreCase(traceState)) {
            config.enableTracing();
            msg = "Enabled tracing for ('" + artifactName + "')";
            response.put(Constants.MESSAGE, msg);
            AuditLogger.logAuditMessage(performedBy, type, Constants.AUDIT_LOG_ACTION_ENABLE, info);
        } else if (Constants.DISABLE.equalsIgnoreCase(traceState)) {
            config.disableTracing();
            msg = "Disabled tracing for ('" + artifactName + "')";
            response.put(Constants.MESSAGE, msg);
            AuditLogger.logAuditMessage(performedBy, type, Constants.AUDIT_LOG_ACTION_DISABLED, info);
        } else {
            msg = "Invalid value for state " + Constants.TRACE;
            return createJsonError(msg, axisMsgCtx, Constants.BAD_REQUEST);
        }
        LOG.info(msg);
        return response;
    }

    public static JSONObject handleStatistics(String performedBy, String type, JSONObject info,
                                    AspectConfiguration config, String artifactName,
                                    org.apache.axis2.context.MessageContext axisMsgCtx, JsonObject payload) {

        JSONObject response = new JSONObject();
        String msg;
        String statisticsState = payload.get(Constants.STATISTICS).getAsString();
        if (Constants.ENABLE.equalsIgnoreCase(statisticsState)) {
            config.enableStatistics();
            msg = "Enabled statistics for ('" + artifactName + "')";
            response.put(Constants.MESSAGE, msg);
            AuditLogger.logAuditMessage(performedBy, type, Constants.AUDIT_LOG_ACTION_ENABLE, info);
        } else if (Constants.DISABLE.equalsIgnoreCase(statisticsState)) {
            config.disableStatistics();
            msg = "Disabled statistics for ('" + artifactName + "')";
            response.put(Constants.MESSAGE, msg);
            AuditLogger.logAuditMessage(performedBy, type, Constants.AUDIT_LOG_ACTION_DISABLED, info);
        } else {
            msg = "Invalid value for state " + Constants.STATISTICS;
            return createJsonError(msg, axisMsgCtx, Constants.BAD_REQUEST);
        }
        LOG.info(msg);
        return response;
    }

    /**
     * Public wrapper method for handling tracing operations.
     */
    public static JSONObject handleTracing(String performedBy, String type, String resourceType, JSONObject info,
                                           AspectConfiguration config, String artifactName,
                                           org.apache.axis2.context.MessageContext axisMsgCtx) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Handling tracing operation for artifact: " + artifactName + ", type: " + type);
        }
        JsonObject payload = getJsonPayload(axisMsgCtx);
        return handleTracing(performedBy, type, info, config, artifactName, axisMsgCtx, payload);
    }

    /**
     * Public wrapper method for handling statistics operations.
     */
    public static JSONObject handleStatistics(String performedBy, String type, String resourceType, JSONObject info,
                                              AspectConfiguration config, String artifactName,
                                              org.apache.axis2.context.MessageContext axisMsgCtx) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Handling statistics operation for artifact: " + artifactName + ", type: " + type);
        }
        JsonObject payload = getJsonPayload(axisMsgCtx);
        return handleStatistics(performedBy, type, info, config, artifactName, axisMsgCtx, payload);
    }

    public static JSONObject createJSONList(int count) {

        JSONObject jsonBody = new JSONObject();
        JSONArray list = new JSONArray();
        jsonBody.put(Constants.COUNT, count);
        jsonBody.put(Constants.LIST, list);
        return jsonBody;
    }

    /**
     * Create a json list to list down carbon applications
     * @param activeCount active cApp count
     * @param faultyCount faulty cApp count
     * @return json object
     */
    public static JSONObject createCAppJSONList(int activeCount, int faultyCount) {

        JSONObject jsonBody = new JSONObject();
        jsonBody.put(Constants.TOTAL_COUNT, activeCount + faultyCount);
        jsonBody.put(Constants.ACTIVE_COUNT, activeCount);
        jsonBody.put(Constants.ACTIVE_LIST, new JSONArray());
        jsonBody.put(Constants.FAULTY_COUNT, faultyCount);
        jsonBody.put(Constants.FAULTY_LIST, new JSONArray());
        return jsonBody;
    }

    /**
     * Creates a json response according to the message provided and sets the provided HTTP code.
     *
     * @param message             the error response to be sent to the client
     * @param exception           the exception to be logged on the server side. The error response will be extracted
     *                            from the
     *                            exception.
     * @param axis2MessageContext message context to set the json payload to
     * @param statusCode          the HTTP status code to be returned
     * @return error response
     */
    public static JSONObject createJsonError(String message, Throwable exception,
                                             org.apache.axis2.context.MessageContext axis2MessageContext, String statusCode) {
        LOG.error(message, exception);
        return createResponse(message + exception.getMessage(), axis2MessageContext, statusCode);
    }

    /**
     * Creates a json response according to the message provided and sets the provided HTTP code.
     *
     * @param message             the error response to be sent to the client
     * @param axis2MessageContext message context to set the json payload to
     * @param statusCode          the HTTP status code to be returned
     * @return error response
     */
    public static JSONObject createJsonError(String message, org.apache.axis2.context.MessageContext axis2MessageContext,
                                             String statusCode) {
        LOG.error(message);
        return createResponse(message, axis2MessageContext, statusCode);
    }


    private static JSONObject createResponse(String message, org.apache.axis2.context.MessageContext axis2MessageContext,
                                             String statusCode) {
        JSONObject jsonBody = Utils.createJsonErrorObject(message);
        axis2MessageContext.setProperty(Constants.HTTP_STATUS_CODE, statusCode);
        return jsonBody;
    }

    public static JSONObject createJsonErrorObject(String error) {

        JSONObject errorObject = new JSONObject();
        errorObject.put("Error", error);
        return errorObject;
    }

    public static boolean isDoingPOST(org.apache.axis2.context.MessageContext axis2MessageContext) {

        return Constants.HTTP_POST.equals(axis2MessageContext.getProperty(Constants.HTTP_METHOD_PROPERTY));
    }

    /**
     * Returns the JSON payload of a given message
     *
     * @param axis2MessageContext axis2MessageContext
     * @return JsonObject payload
     */
    public static JsonObject getJsonPayload(org.apache.axis2.context.MessageContext axis2MessageContext)
            throws IOException {

        InputStream jsonStream = JsonUtil.getJsonPayload(axis2MessageContext);
        String jsonString = IOUtils.toString(jsonStream);
        return new JsonParser().parse(jsonString).getAsJsonObject();
    }

    /**
     * Returns the ServiceAdmin based on a given message context
     *
     * @param messageContext Synapse message context
     */
    public static ServiceAdmin getServiceAdmin(MessageContext messageContext) {

        ServiceAdmin serviceAdmin = ServiceAdmin.getInstance();
        if (!serviceAdmin.isInitailized()) {
            serviceAdmin.init(messageContext.getConfiguration().getAxisConfiguration());
        }
        return serviceAdmin;
    }

    /**
     * Method to update pax-logging configuration.
     */
    public static void updateLoggingConfiguration() throws IOException {

        ConfigurationAdmin configurationAdmin = ConfigurationHolder.getInstance().getConfigAdminService();
        Configuration configuration =
                configurationAdmin.getConfiguration(Constants.PAX_LOGGING_CONFIGURATION_PID, "?");
        Dictionary properties = new Hashtable<>();
        properties.put(Constants.SERVICE_PID, PaxLoggingConstants.LOGGING_CONFIGURATION_PID);
        Hashtable paxLoggingProperties = getPaxLoggingProperties();
        paxLoggingProperties.forEach(properties::put);
        configuration.update(properties);
    }

    private static Hashtable getPaxLoggingProperties() throws IOException {
        String paxPropertiesFileLocation = System.getProperty("org.ops4j.pax.logging.property.file");
        if (StringUtils.isNotEmpty(paxPropertiesFileLocation)) {
            File file = new File(paxPropertiesFileLocation);
            if (file.exists()) {
                Properties properties = new Properties();
                properties.load(new FileInputStream(file));
                return properties;
            }
        }
        return new Hashtable();
    }

    /**
     * Util method to return the specified  property from a properties file.
     *
     * @param srcFile - The source file which needs to be looked up.
     * @param key     - Key of the property.
     * @return - Value of the property.
     */
    public static String getProperty(File srcFile, String key) throws IOException {

        String value;
        try (FileInputStream fis = new FileInputStream(srcFile)) {
            Properties properties = new Properties();
            properties.load(fis);
            value = properties.getProperty(key);
        } catch (IOException e) {
            throw new IOException("Error occurred while reading the input stream");
        }
        return value;
    }

    /**
     * This method will return the path to logs directory.
     *
     * @return path as string.
     */
    public static String getCarbonLogsPath() {

        String carbonLogsPath = System.getProperty("carbon.logs.path");
        if (carbonLogsPath == null) {
            carbonLogsPath = System.getenv("CARBON_LOGS");
            if (carbonLogsPath == null) {
                return getCarbonHome() + File.separator + "repository" + File.separator + "logs";
            }
        }
        return carbonLogsPath;
    }

    /**
     * This method will return the path to cApps directory.
     *
     * @return path as string.
     */
    public static String getCAppPath() {
        return Paths.get(getCarbonHome(), "repository", "deployment", "server", "carbonapps").toString();
    }

    /**
     * This method will return the path to CARBON_HOME.
     *
     * @return path as String.
     */
    public static String getCarbonHome() {

        String carbonHome = System.getProperty("carbon.home");
        if (carbonHome == null) {
            carbonHome = System.getenv("CARBON_HOME");
            System.setProperty("carbon.home", carbonHome);
        }
        return carbonHome;
    }

    /**
     * This method will provide information on all the files in the logs directory.
     *
     * @return list of file info objects.
     */
    public static List<LogFileInfo> getLogFileInfoList() {

        String folderPath = Utils.getCarbonLogsPath();
        List<LogFileInfo> logFilesList = new ArrayList<>();
        LogFileInfo logFileInfo;

        File folder = new File(folderPath);
        File[] listOfFiles = folder.listFiles();

        if (listOfFiles == null || listOfFiles.length == 0) {
            // folder.listFiles can return a null, in that case return a default log info
            if (LOG.isDebugEnabled()) {
                LOG.debug("Could not find any log file in " + folderPath);
            }
            return getDefaultLogInfoList();
        }
        for (File file : listOfFiles) {
            String filename = file.getName();
            if (!filename.endsWith(".lck")) {
                String filePath = Utils.getCarbonLogsPath() + File.separator + filename;
                File logfile = new File(filePath);
                logFileInfo = new LogFileInfo(filename, getFileSize(logfile));
                logFilesList.add(logFileInfo);
            }
        }
        return logFilesList;
    }

    private static String getFileSize(File file) {

        long bytes = file.length();
        int unit = 1024;
        if (bytes < unit) {
            return bytes + " B";
        }
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        char pre = "KMGTPE".charAt(exp - 1);
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    private static List<LogFileInfo> getDefaultLogInfoList() {

        List<LogFileInfo> defaultLogFileInfoList = new ArrayList<>();
        defaultLogFileInfoList.add(new LogFileInfo("NO_LOG_FILES", "---"));
        return defaultLogFileInfoList;
    }
    protected static void setInvalidUserStoreResponse(org.apache.axis2.context.MessageContext axis2MessageContext) {
        JSONObject response =
                Utils.createJsonError("User management is not supported with the file-based user store. " +
                                "Please plug in a user store for the correct functionality",
                        axis2MessageContext, FORBIDDEN);
        Utils.setJsonPayLoad(axis2MessageContext, response);
    }

    protected static UserStoreManager getUserStore(String domain) throws UserStoreException {
        UserStoreManager userStoreManager = MicroIntegratorSecurityUtils.getUserStoreManager();
        if (!StringUtils.isEmpty(domain) && userStoreManager instanceof AbstractUserStoreManager &&
                !UserCoreConstants.PRIMARY_DEFAULT_DOMAIN_NAME.equalsIgnoreCase(domain)) {
            userStoreManager = ((AbstractUserStoreManager) userStoreManager).getSecondaryUserStoreManager(domain);
            if (userStoreManager == null) {
                throw new UserStoreException("Could not find a user-store for the given domain " + domain);
            }
        }
        return userStoreManager;
    }

    protected static RealmConfiguration getRealmConfiguration() throws UserStoreException {
        return MicroIntegratorSecurityUtils.getRealmConfiguration();
    }
    public static String addDomainToName(String name, String domainName) {
        if (domainName != null && name != null && !name.contains(Constants.DOMAIN_SEPARATOR) &&
                !"PRIMARY".equalsIgnoreCase(domainName)) {
            if (!"Internal".equalsIgnoreCase(domainName) && !"Workflow".equalsIgnoreCase(domainName) &&
                    !"Application".equalsIgnoreCase(domainName)) {
                name = domainName.toUpperCase() + Constants.DOMAIN_SEPARATOR + name;
            } else {
                name = domainName.substring(0, 1).toUpperCase() + domainName.substring(1).toLowerCase() +
                        Constants.DOMAIN_SEPARATOR + name;
            }
        }
        return name;
    }

    /**
     * This method is used to check the validity of the input path.
     *
     * @param registryPath        File path
     * @param axis2MessageContext AXIS2 message context
     * @return Validated path
     */
    public static String validatePath(String registryPath,
            org.apache.axis2.context.MessageContext axis2MessageContext, MessageContext messageContext) {

        if (StringUtils.isEmpty(registryPath)) {
            JSONObject jsonBody = Utils.createJsonError("Registry path not found in the request", axis2MessageContext,
                    BAD_REQUEST);
            Utils.setJsonPayLoad(axis2MessageContext, jsonBody);
            return null;
        }
        MicroIntegratorRegistry microIntegratorRegistry =
                (MicroIntegratorRegistry) messageContext.getConfiguration().getRegistry();
        String registryParentPath = formatPath(microIntegratorRegistry.getRegRoot());
        String registryRoot = formatPath(registryParentPath + REGISTRY_ROOT_PATH);
        String validatedPath;

        try {
            File validatedPathFile = new File(formatPath(registryParentPath + File.separator + registryPath));
            File registryRootFile = new File(registryRoot);
            if (!validatedPathFile.getCanonicalPath().startsWith(registryRootFile.getCanonicalPath())) {
                JSONObject jsonBody = Utils.createJsonError("The registry path  '" + registryPath
                                + "' is illegal", axis2MessageContext,
                        BAD_REQUEST);
                Utils.setJsonPayLoad(axis2MessageContext, jsonBody);
                return null;
            } else {
                String validatedCanonicalPath = formatPath(validatedPathFile.getCanonicalPath());
                String rootCanonicalPath = formatPath(registryRootFile.getCanonicalPath());
                validatedPath = formatPath(validatedCanonicalPath.replace(rootCanonicalPath, REGISTRY_ROOT_PATH));
            }
        } catch (IOException | IllegalArgumentException e) {
            JSONObject jsonBody = Utils.createJsonError(
                    "Error while resolving the canonical path of the registry path : " + registryPath,
                    axis2MessageContext, INTERNAL_SERVER_ERROR);
            Utils.setJsonPayLoad(axis2MessageContext, jsonBody);
            return null;
        }
        return validatedPath;
    }

    /**
     * Format the string paths to match any platform.. windows, linux etc..
     *
     * @param path  Input file path
     * @return      Formatted file path
     */
    public static String formatPath(String path) {
        // removing white spaces
        String pathFormatted = path.replaceAll("\\b\\s+\\b", "%20");
        try {
            pathFormatted = java.net.URLDecoder.decode(pathFormatted, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            LOG.error("Unsupported Encoding in the path :" + pathFormatted);
        }
        // replacing all "\" with "/"
        return pathFormatted.replace('\\', '/');
    }

    /**
     * This method checks whether a registry file exists or not.
     *
     * @param registryPath  Registry path
     * @return              Boolean output indicating the existence of the registry
     */
    public static boolean isRegistryExist(String registryPath,
                                          MessageContext messageContext) {
        MicroIntegratorRegistry microIntegratorRegistry =
                (MicroIntegratorRegistry) messageContext.getConfiguration().getRegistry();
        String regRoot = microIntegratorRegistry.getRegRoot();
        String resolvedPath = formatPath(regRoot + File.separator + registryPath + File.separator);
        try {
            File file = new File(resolvedPath);
            return file.exists();
        } catch (Exception e) {
            LOG.error("Error occurred while checking the existence of the registry", e);
            return false;
        }
    }

    /**
     * This method checks whether a registry resource property exists or not.
     *
     * @param registryPath    Registry path
     * @param messageContext  Message context
     * @return                Boolean output indicating the existence of the registry resource property
     */
    public static boolean isRegistryResourcePropertyExist(String registryPath,
                                                          MessageContext messageContext) {
        MicroIntegratorRegistry microIntegratorRegistry =
                (MicroIntegratorRegistry) messageContext.getConfiguration().getRegistry();
        String regRoot = microIntegratorRegistry.getRegRoot();
        String resolvedPath = formatPath(regRoot + File.separator + registryPath);
        try {
            File file = new File(resolvedPath);
            if (file.exists()) {
                if (file.isDirectory()) {
                    return isRegistryExist(registryPath + COLLECTIONS_PROPERTY_EXTENSIONS, messageContext);
                } else {
                    return isRegistryExist(registryPath + PROPERTY_EXTENSION, messageContext);
                }
            } else {
                return false;
            }
        } catch (Exception e) {
            LOG.error("Error occurred while checking the existence of the registry", e);
            return false;
        }
    }

    /**
     * Format the path by adding the relevant registry type prefix.
     * @param path  Registry path
     * @return      Formatted path with the prefix
     */
    public static String getRegistryPathPrefix(String path) {

        String pathWithPrefix;
        if (path.startsWith(CONFIGURATION_REGISTRY_PATH)) {
            pathWithPrefix = path.replace(CONFIGURATION_REGISTRY_PATH, CONFIGURATION_REGISTRY_PREFIX);
        } else if (path.startsWith(GOVERNANCE_REGISTRY_PATH)) {
            pathWithPrefix = path.replace(GOVERNANCE_REGISTRY_PATH, GOVERNANCE_REGISTRY_PREFIX);
        } else if (path.startsWith(LOCAL_REGISTRY_PATH)) {
            pathWithPrefix = path.replace(LOCAL_REGISTRY_PATH, LOCAL_REGISTRY_PREFIX);
        } else {
            return null;
        }
        return pathWithPrefix;
    }

    /**
     * This method returns a string containing file content.
     *
     * @param axis2MessageContext Axis2 message context
     * @return A string containing file content
     */
    public static String getPayload(org.apache.axis2.context.MessageContext axis2MessageContext) {

        try {
            InputStream inputStream = getInputStream(axis2MessageContext);
            return IOUtils.toString(inputStream);
        } catch (Exception e) {
            LOG.error("Error occurred while fetching the payload", e);
            return null;
        }
    }

    /**
     * Returns input stream from the payload.
     * @param messageContext    Axis2 message context
     * @return                  InputStream of payload
     */
    private static InputStream getInputStream(org.apache.axis2.context.MessageContext messageContext) {
        if (messageContext == null) {
            return null;
        } else {
            Object o = messageContext.getProperty("bufferedInputStream");
            if (o instanceof InputStream) {
                InputStream is = (InputStream) o;
                if (is.markSupported()) {
                    try {
                        is.reset();
                    } catch (IOException e) {
                        LOG.error("Error occurred while fetching the payload", e);
                        return null;
                    }
                }
                return is;
            } else {
                return null;
            }
        }
    }

    /**
     * This method returns a byte array containing file content.
     *
     * @param messageContext    Synapse message context
     * @return                  A byte array containing file content
     */
    public static byte[] getPayloadFromMultipart(MessageContext messageContext) {

        try {
            OMNode fileOmNode = messageContext.getEnvelope().getBody().getFirstElement()
                    .getFirstChildWithName(new QName(FILE)).getFirstOMChild();
            return Base64.getDecoder().decode(((OMTextImpl) fileOmNode).getText());
        } catch (Exception e) {
            LOG.error("Error occurred while fetching the payload", e);
            return null;
        }
    }

    /**
     * Sends unauthorized fault response.
     * @param axis2MessageContext   AXIS2 message context
     */
    public static void sendForbiddenFaultResponse(org.apache.axis2.context.MessageContext axis2MessageContext) {
        axis2MessageContext.setProperty(Constants.NO_ENTITY_BODY, true);
        axis2MessageContext.setProperty(Constants.HTTP_STATUS_CODE, 403);
    }

    /**
     * Fetches the registry name from the registy path.
     * @param path  Registry path
     * @return      Registry name
     */
    public static String getResourceName(String path) {
        if (path != null) {
            String correctedPath = path;
            if (path.endsWith(URL_SEPARATOR)) {
                correctedPath = path.substring(0, path.lastIndexOf(URL_SEPARATOR));
            }
            return correctedPath.substring(correctedPath.lastIndexOf(URL_SEPARATOR) + 1, correctedPath.length());
        }
        return "";
    }

    /**
     * Checks whether the secrets of an artifact configuration should be masked before it is returned. Masking is
     * enabled unless a deployment turns it off through {@code [management_api.secret_masking] enabled = false}, so
     * that masking applies by default and only a deployment which opts out is served the configuration as it is.
     * Masking is also kept on for a value which is not an explicit false, so that a mistyped value masks rather than
     * exposes the secrets.
     *
     * @return true if the secrets of a configuration have to be masked
     */
    public static boolean isSecretMaskingEnabled() {
        Map<String, Object> parsedConfigs = ConfigParser.getParsedConfigs();
        if (parsedConfigs == null) {
            return true;
        }
        Object maskingEnabled = parsedConfigs.get(Constants.SECRET_MASKING_ENABLED);
        if (maskingEnabled == null) {
            return true;
        }
        // The masking is turned off only by an explicit false, which a deployment.toml may quote, so that a value
        // which is mistyped keeps the secrets masked instead of exposing them.
        return !Boolean.FALSE.toString().equalsIgnoreCase(String.valueOf(maskingEnabled).trim());
    }

    /**
     * Checks whether the given artifact parameter holds a secret which should not be exposed via the management API.
     * The decision is based only on the name of the parameter, since the resolved value of a parameter carries no
     * indication of whether it is a secret.
     *
     * @param parameterName name of the artifact parameter
     * @return true if the value of the parameter has to be masked
     */
    public static boolean isSecretParameter(String parameterName) {
        if (parameterName == null) {
            return false;
        }
        String normalizedName = parameterName.toLowerCase(Locale.ENGLISH);
        // Only the matched fragment is dropped, so a name such as smtp.allowCredentials.password stays masked.
        for (String nonSecretNameFragment : NON_SECRET_PARAMETER_NAME_FRAGMENTS) {
            normalizedName = normalizedName.replace(nonSecretNameFragment, "");
        }
        for (String secretNameFragment : SECRET_PARAMETER_NAME_FRAGMENTS) {
            if (normalizedName.contains(secretNameFragment)) {
                return true;
            }
        }
        return false;
    }


    /**
     * Returns the value of the given artifact parameter with its secrets masked. The value is masked as a whole when
     * the name of the parameter denotes a secret. Otherwise only the credentials carried by a URI are masked, since a
     * parameter such as {@code transport.vfs.FileURI} holds the credentials of the remote file system within the URI
     * and therefore cannot be detected using the name of the parameter. A value which is not a string is returned as
     * it is, since only a string can carry a URI.
     * <p>
     * The value is returned as it is when a deployment turns the secret masking off.
     *
     * @param parameterName name of the artifact parameter
     * @param value         value of the artifact parameter
     * @return the masked value, or the given value when it carries no secret or the masking is turned off
     */
    public static Object maskSecretValue(String parameterName, Object value) {
        if (!isSecretMaskingEnabled()) {
            return value;
        }
        if (isSecretParameter(parameterName)) {
            return Constants.PASSWORD_MASKED_VALUE;
        }
        if (!(value instanceof String)) {
            return value;
        }
        String maskedValue = maskCredentialsInUri((String) value);
        return maskedValue != null ? maskedValue : value;
    }

    /**
     * Returns a copy of the given parameter map where the secrets held by the parameters are masked, unless a
     * deployment turns the secret masking off. The given map is never modified since artifacts return the live
     * parameter map used at runtime, and a copy is returned whether or not the masking is turned off.
     *
     * @param parameters artifact parameters
     * @return a copy of the given parameters, masked unless the masking is turned off
     */
    public static Map<String, Object> maskSecretParameters(Map<String, ?> parameters) {
        if (parameters == null) {
            return null;
        }
        Map<String, Object> maskedParameters = new LinkedHashMap<>(parameters);
        maskedParameters.replaceAll(Utils::maskSecretValue);
        return maskedParameters;
    }

    /**
     * Returns the json representation of the parameters of a sequence template where the default value of a secret
     * parameter is masked, unless a deployment turns the secret masking off. A default value is the value a template
     * uses when a caller does not pass the parameter, so the default value of a parameter such as {@code password} is
     * the secret itself. The json representation is built as a copy, hence the parameters used at runtime are never
     * modified.
     *
     * @param parameters parameters of a sequence template
     * @return the json representation of the given parameters, masked unless the masking is turned off
     */
    public static JSONArray maskSecretTemplateParameters(Collection<?> parameters) {
        if (parameters == null) {
            return null;
        }
        JSONArray parametersJson = new JSONArray(parameters);
        if (!isSecretMaskingEnabled()) {
            return parametersJson;
        }
        for (int i = 0; i < parametersJson.length(); i++) {
            JSONObject parameter = parametersJson.optJSONObject(i);
            if (parameter != null && parameter.has(Constants.DEFAULT_VALUE)
                    && isSecretParameter(parameter.optString(Constants.NAME, null))) {
                parameter.put(Constants.DEFAULT_VALUE, Constants.PASSWORD_MASKED_VALUE);
            }
        }
        return parametersJson;
    }

    /**
     * Returns a copy of the given json representation of an artifact where the secret values are masked, unless a
     * deployment turns the secret masking off. A secret is either the value of a field whose name denotes a secret,
     * or the credentials carried by a URI such as the address of an endpoint. The given object is never modified,
     * since artifacts cache and reuse it, and a copy is returned whether or not the masking is turned off.
     *
     * @param artifactJson json representation of an artifact
     * @return a copy of the given json representation, masked unless the masking is turned off
     */
    public static JSONObject maskSecretsInJson(JSONObject artifactJson) {
        if (artifactJson == null) {
            return null;
        }
        JSONObject maskedJson = new JSONObject(artifactJson.toString());
        maskSecretsInJsonObject(maskedJson);
        return maskedJson;
    }

    /**
     * Masks the secret values held by the given json object and by every object nested under it.
     *
     * @param jsonObject json object to be masked
     */
    private static void maskSecretsInJsonObject(JSONObject jsonObject) {
        for (String key : new ArrayList<>(jsonObject.keySet())) {
            Object value = jsonObject.get(key);
            if (value instanceof JSONObject) {
                maskSecretsInJsonObject((JSONObject) value);
            } else if (value instanceof JSONArray) {
                maskSecretsInJsonArray((JSONArray) value);
            } else if (value instanceof String) {
                jsonObject.put(key, maskSecretValue(key, value));
            }
        }
    }

    /**
     * Masks the secret values held by the objects of the given json array.
     *
     * @param jsonArray json array to be masked
     */
    private static void maskSecretsInJsonArray(JSONArray jsonArray) {
        for (int i = 0; i < jsonArray.length(); i++) {
            Object value = jsonArray.get(i);
            if (value instanceof JSONObject) {
                maskSecretsInJsonObject((JSONObject) value);
            } else if (value instanceof JSONArray) {
                maskSecretsInJsonArray((JSONArray) value);
            }
        }
    }

    /**
     * Masks the values of all the secret parameters of an artifact configuration. Parameters which are resolved via
     * a registry key are left untouched since their values are not serialized into the configuration.
     * <p>
     * The given element is copied before it is masked, so an element owned by the synapse configuration can be
     * passed. Dropping that copy would make the masked value the one used at runtime.
     * <p>
     * The element is returned as it is when a deployment turns the secret masking off, since the configuration of an
     * artifact is the source a management API client was served unmasked before the secrets were masked.
     *
     * @param artifactElement artifact configuration
     * @return a masked copy of the given element, or the given element when the masking is turned off
     */
    public static OMElement maskSecretsInConfiguration(OMElement artifactElement) {
        if (artifactElement == null) {
            return null;
        }
        if (!isSecretMaskingEnabled()) {
            return artifactElement;
        }
        // The copy keeps the masking away from an element which is owned by the synapse configuration.
        OMElement maskedElement = artifactElement.cloneOMElement();
        maskSecretElements(maskedElement);
        return maskedElement;
    }

    /**
     * Walks the given element and masks the value of every secret parameter element found underneath it.
     *
     * @param element element to be inspected
     */
    private static void maskSecretElements(OMElement element) {
        for (OMNode child = element.getFirstOMChild(); child != null; child = child.getNextOMSibling()) {
            if (!(child instanceof OMElement)) {
                continue;
            }
            OMElement childElement = (OMElement) child;
            if (isSecretElement(childElement)) {
                maskElementValue(childElement);
            } else {
                maskCredentialsInUris(childElement);
                maskSecretElements(childElement);
            }
        }
    }

    /**
     * Checks whether the given element holds a secret. This covers the shapes used across the synapse artifacts, which
     * are a named parameter such as {@code <parameter name="solace.password">}, a named property such as
     * {@code <property name="pwd" value="..."/>} and a dedicated element such as the {@code <password>} of a database
     * mediator or the {@code <clientSecret>} of an OAuth endpoint.
     *
     * @param element element to be inspected
     * @return true if the value of the element has to be masked
     */
    private static boolean isSecretElement(OMElement element) {
        if (element.getAttributeValue(KEY_ATTRIBUTE) != null
                || element.getAttributeValue(EXPRESSION_ATTRIBUTE) != null) {
            // The value is either resolved through the registry or evaluated at runtime, hence the configuration
            // carries no secret.
            return false;
        }
        if (isSecretParameter(element.getAttributeValue(NAME_ATTRIBUTE)) && holdsValue(element)) {
            return true;
        }
        // A dedicated element such as <password> or <clientSecret> holds the secret by itself, whereas an element
        // such as <clientCredentials> only groups other elements and therefore has to be inspected further.
        return element.getFirstElement() == null && isSecretParameter(element.getLocalName());
    }

    /**
     * Checks whether the given element carries a value of its own. An element which is named after a secret is masked
     * only when it does, since a mediator or an inline endpoint such as {@code <class name="...TokenValidator">} or
     * {@code <endpoint name="TokenServiceEP">} is also named, but holds a configuration which has to be inspected
     * further instead of being replaced as a whole. Likewise a template parameter which only declares a name has no
     * value to be masked.
     *
     * @param element element to be inspected
     * @return true if the element carries a value
     */
    private static boolean holdsValue(OMElement element) {
        for (QName valueAttribute : VALUE_ATTRIBUTES) {
            if (element.getAttribute(valueAttribute) != null) {
                return true;
            }
        }
        if (element.getFirstElement() == null) {
            return StringUtils.isNotBlank(element.getText());
        }
        for (String xmlValueElement : XML_VALUE_ELEMENTS) {
            if (xmlValueElement.equals(element.getLocalName())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Replaces the value of the given element with {@link Constants#PASSWORD_MASKED_VALUE}. A value held by an
     * attribute is masked in place. Otherwise the existing value is dropped first, since a parameter value can also be
     * serialized as a child element when it is well formed XML.
     *
     * @param element element whose value should be masked
     */
    private static void maskElementValue(OMElement element) {
        boolean maskedAnAttribute = false;
        for (QName valueAttributeName : VALUE_ATTRIBUTES) {
            OMAttribute valueAttribute = element.getAttribute(valueAttributeName);
            if (valueAttribute != null) {
                valueAttribute.setAttributeValue(Constants.PASSWORD_MASKED_VALUE);
                maskedAnAttribute = true;
            }
        }
        if (maskedAnAttribute) {
            return;
        }
        OMNode valueNode = element.getFirstOMChild();
        while (valueNode != null) {
            OMNode nextValueNode = valueNode.getNextOMSibling();
            valueNode.detach();
            valueNode = nextValueNode;
        }
        element.setText(Constants.PASSWORD_MASKED_VALUE);
    }

    /**
     * Masks the credentials carried by any URI of the given element. The credentials of a remote file system are
     * configured as a part of the URI, therefore they cannot be detected using the name of the parameter.
     *
     * @param element element to be inspected
     */
    private static void maskCredentialsInUris(OMElement element) {
        Iterator<?> attributes = element.getAllAttributes();
        while (attributes.hasNext()) {
            OMAttribute attribute = (OMAttribute) attributes.next();
            String maskedValue = maskCredentialsInUri(attribute.getAttributeValue());
            if (maskedValue != null) {
                attribute.setAttributeValue(maskedValue);
            }
        }
        if (element.getFirstElement() == null) {
            String maskedText = maskCredentialsInUri(element.getText());
            if (maskedText != null) {
                element.setText(maskedText);
            }
        }
    }

    /**
     * Masks the credentials of the given URI. Credentials which resolve their password through a secure vault
     * expression are left as they are. The expression is looked for only within the password of each URI, since a
     * value which carries an expression elsewhere, such as in its query, can still carry a password in plain text.
     *
     * @param value value which may be a URI
     * @return the masked URI, or null if the given value carries no credentials to be masked
     */
    private static String maskCredentialsInUri(String value) {
        if (StringUtils.isEmpty(value)) {
            return null;
        }
        Matcher matcher = URI_CREDENTIALS_PATTERN.matcher(value);
        StringBuffer maskedValue = new StringBuffer();
        boolean masked = false;
        while (matcher.find()) {
            String credentials = matcher.group();
            if (!containsSecureVaultExpression(matcher.group(2))) {
                credentials = matcher.group(1) + ":" + Constants.PASSWORD_MASKED_VALUE + "@";
                masked = true;
            }
            matcher.appendReplacement(maskedValue, Matcher.quoteReplacement(credentials));
        }
        if (!masked) {
            return null;
        }
        matcher.appendTail(maskedValue);
        return maskedValue.toString();
    }

    /**
     * Checks whether the given value resolves its secret at runtime, in which case the configured value is not a
     * secret by itself.
     *
     * @param value value to be inspected
     * @return true if the value holds a secure vault expression
     */
    private static boolean containsSecureVaultExpression(String value) {
        for (String expression : SECURE_VAULT_EXPRESSIONS) {
            if (value.contains(expression)) {
                return true;
            }
        }
        return false;
    }
}

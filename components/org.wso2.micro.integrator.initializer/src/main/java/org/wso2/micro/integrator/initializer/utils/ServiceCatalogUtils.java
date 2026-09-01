/*
 *  Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.micro.integrator.initializer.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.MalformedJsonException;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.media.XML;
import io.swagger.v3.oas.models.parameters.HeaderParameter;
import io.swagger.v3.oas.models.parameters.RequestBody;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;
import io.swagger.v3.parser.OpenAPIV3Parser;
import io.swagger.v3.parser.core.models.SwaggerParseResult;
import org.apache.axiom.om.OMElement;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.description.AxisOperation;
import org.apache.axis2.description.AxisResource;
import org.apache.axis2.description.AxisResources;
import org.apache.axis2.description.AxisService;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.commons.resolvers.ResolverException;
import org.apache.synapse.commons.resolvers.SystemResolver;
import org.apache.synapse.config.SynapseConfigUtils;
import org.apache.synapse.core.axis2.ProxyService;
import org.wso2.carbon.securevault.SecretCallbackHandlerService;
import org.wso2.config.mapper.ConfigParser;
import org.wso2.micro.application.deployer.AppDeployerUtils;
import org.wso2.micro.application.deployer.CarbonApplication;
import org.wso2.micro.core.util.CarbonException;
import org.wso2.micro.core.util.StringUtils;
import org.wso2.micro.integrator.dataservices.core.DataHolder;
import org.wso2.micro.integrator.initializer.deployment.application.deployer.CappDeployer;
import org.wso2.securevault.SecretResolver;
import org.wso2.securevault.SecretResolverFactory;
import org.wso2.securevault.commons.MiscellaneousUtil;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.wso2.micro.integrator.initializer.utils.Constants.*;

/**
 * Util class for service catalog feature.
 */
public class ServiceCatalogUtils {

    private static final Log log = LogFactory.getLog(ServiceCatalogUtils.class);
    private static SecretResolver secretResolver;
    private static List<ServiceMetaDataHolder> md5List = new CopyOnWriteArrayList<>();
    private static Boolean alreadyUploaded = false;
    private static String resolvedHostName;
    private static String resolvedGroupId;
    private static String httpListenerPort;
    private static String httpsListenerPort;
    private static String resolvedUrl;
    private static String lineSeparator;
    private static Map<String, Object> parsedConfigs;
    private static final String API_VERSION;
    private static final FilenameFilter CAPP_FILTER = (f, name) -> name.endsWith(".car");

    static {
        String apiVersion = System.getProperty(SERVICE_CATALOG_API_VERSION_PROPERTY);
        if (apiVersion == null) {
            apiVersion = SERVICE_CATALOG_DEFAULT_API_VERSION;
        }
        API_VERSION = apiVersion;
    }

    /**
     * Update the service url by injecting env variables.
     *
     * @param currentUrl current url.
     * @return updated url.
     * @throws ResolverException environment variables are not set correctly.
     */
    private static String updateServiceUrl(String currentUrl) throws ResolverException {
        /*
            Supported formats
            https://{MI_HOST}:{MI_PORT}/api1
            https://{MI_URL}/api1
        */
        SystemResolver resolver = new SystemResolver();
        if (parsedConfigs == null) {
            parsedConfigs = ConfigParser.getParsedConfigs();
        }
        if (currentUrl.contains(HOST) || currentUrl.contains(PORT)) {
            // update the {MI_HOST} with server.hostname value
            if (currentUrl.contains(HOST)) {
                if (resolvedHostName == null) {
                    try {
                        resolver.setVariable(MI_HOST);
                        resolvedHostName = resolver.resolve();
                    } catch (ResolverException e) {
                        resolvedHostName = (String) parsedConfigs.get(SERVER_HOSTNAME);
                    }
                }
                currentUrl = currentUrl.replace(HOST, resolvedHostName);
            }
            // update the {MI_PORT} with server listener ports
            if (currentUrl.contains(PORT)) {
                if (currentUrl.startsWith("https")) {
                    if (httpsListenerPort == null) {
                        try {
                            resolver.setVariable(MI_PORT);
                            httpsListenerPort = resolver.resolve();
                        } catch (ResolverException e) {
                            int portOffset = Integer.parseInt(System.getProperty(SERVER_PORT_OFFSET));
                            int httpsPort = Integer.parseInt(String.valueOf(parsedConfigs.get(HTTPS_LISTENER_PORT)));
                            httpsListenerPort = String.valueOf(httpsPort + portOffset);
                        }
                    }
                    currentUrl = currentUrl.replace(PORT, httpsListenerPort);
                } else if (currentUrl.startsWith("http")) {
                    if(httpListenerPort == null) {
                        try {
                            resolver.setVariable(MI_PORT);
                            httpListenerPort = resolver.resolve();
                        } catch (ResolverException e) {
                            int portOffset = Integer.parseInt(System.getProperty(SERVER_PORT_OFFSET));
                            int httpsPort = Integer.parseInt(String.valueOf(parsedConfigs.get(HTTP_LISTENER_PORT)));
                            httpListenerPort = String.valueOf(httpsPort + portOffset);
                        }
                    }
                    currentUrl = currentUrl.replace(PORT, httpListenerPort);
                }
            }
        } else if (currentUrl.contains(URL)) {
            if (resolvedUrl == null) {
                resolver.setVariable(MI_URL);
                resolvedUrl = resolver.resolve();
            }
            currentUrl = currentUrl.replace(MI_URL, resolvedUrl);
        }
        return currentUrl;
    }

    /**
     * Dynamically resolve the {MI_GROUP_ID} placeholder in the metadata file using env variables.
     * @param yaml metadata file.
     */
    private static void replaceGroupIdPlaceholder(Map<String, Object> yaml) {
        SystemResolver resolver = new SystemResolver();
        if (resolvedGroupId == null) {
            try {
                resolver.setVariable(MI_GROUP_ID);
                resolvedGroupId = resolver.resolve();
            } catch (ResolverException e) {
                // if the env variable is not set, use the existing value
                return;
            }
        }
        String key = (String) yaml.get(METADATA_KEY);
        if (key.contains(GROUP_ID)) {
            yaml.put(METADATA_KEY, key.replace(GROUP_ID, resolvedGroupId));
        }
        String name = (String) yaml.get(METADATA_NAME);
        if (name.contains(GROUP_ID)) {
            yaml.put(METADATA_NAME, name.replace(GROUP_ID, resolvedGroupId));
        }
        String displayName = (String) yaml.get(METADATA_DISPLAY_NAME);
        if (displayName.contains(GROUP_ID)) {
            yaml.put(METADATA_DISPLAY_NAME, displayName.replace(GROUP_ID, resolvedGroupId));
        }
        String description = (String) yaml.get(METADATA_DESCRIPTION);
        if (description.contains(GROUP_ID)) {
            yaml.put(METADATA_DESCRIPTION, description.replaceAll(GROUP_ID_REGEX, resolvedGroupId));
        }
    }

    /**
     * Update the serviceUrl of the given metadata file (if required) and return its key value.
     *
     * @param yamlFile metadata yaml file.
     * @return key value of the metadata file.
     * @throws IOException       Error occurred while updating the metadata file.
     * @throws ResolverException Error occurred while reading env variables.
     */
    public static String updateMetadataWithServiceUrl(File yamlFile) throws IOException, ResolverException {
        Yaml yaml = new Yaml();
        InputStream yamlStream = new FileInputStream(yamlFile);
        Map<String, Object> obj = (Map<String, Object>) yaml.load(yamlStream);
        String currentServiceUrl = (String) obj.get(SERVICE_URL);
        obj.put(SERVICE_URL, updateServiceUrl(currentServiceUrl));
        replaceGroupIdPlaceholder(obj);

        // Additional configurations
        DumperOptions options = new DumperOptions();
        options.setIndent(2);
        options.setPrettyFlow(true);
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);

        Yaml output = new Yaml(options);
        String updatedYaml = output.dump(obj);

        DataOutputStream outputStream = new DataOutputStream(new FileOutputStream(yamlFile, false));
        outputStream.write(updatedYaml.getBytes());
        outputStream.close();
        yamlStream.close();
        return (String) obj.get(METADATA_KEY);
    }

    /**
     * Publish a given ZIP file to APIM service catalog endpoint. Retry if failed.
     *
     * @param apimConfigs        Map containing deployment.toml configuration
     * @param attachmentFilePath path of the ZIP file to be updated.
     */
    public static void publishToAPIM(Map<String, String> apimConfigs, String attachmentFilePath) {
        int responseCode = uploadZip(apimConfigs, attachmentFilePath);
        if (responseCode == -1) return; // error occurred while uploading to service catalog
        switch (responseCode) {
            case HttpURLConnection.HTTP_OK:
                log.info("Successfully updated the service catalog");
                break;
            case HttpURLConnection.HTTP_GATEWAY_TIMEOUT:
            case HttpURLConnection.HTTP_UNAVAILABLE:
                if (log.isDebugEnabled()) {
                    log.debug("APIM responds with the status code : " + responseCode + " start " +
                            "retrying");
                }
                int retryCount = RETRY_COUNT;
                for (int i = 0; i < retryCount; i++) {
                    try {
                        log.info("Retrying to connect with APIM. Remaining retry count : " + (retryCount - i));
                        Thread.sleep(INTERVAL_BETWEEN_RETRIES);
                        responseCode = uploadZip(apimConfigs, attachmentFilePath);
                        if (responseCode == HttpURLConnection.HTTP_OK) {
                            break;
                        }
                    } catch (InterruptedException e) {
                        log.error("Service catalog thread interrupted", e);
                    }
                }
                log.error("Could not connect with APIM after " + retryCount + " retries.");
                break;
            case UNAUTHENTICATED:
                log.error("Unauthenticated, please verify the username and password provided for service catalog");
                break;
            default:
                log.error("Unknown response code received from the service catalog endpoint: " + responseCode);
                break;
        }
    }

    /**
     * Call service catalog and fetch details of all the services.
     *
     * @param apimConfigs Map containing APIM configuration.
     * @return Map of APIs and their current MD5 sum values, null if error occurred.
     */
    public static Map<String, String> getAllServices(Map<String, String> apimConfigs) {

        Map<String, String> md5Map = new HashMap<>();
        String APIMHost = apimConfigs.get(APIM_HOST);
        String credentials =
                Base64.getEncoder().encodeToString(
                        (apimConfigs.get(USER_NAME) + ":" + apimConfigs.get(PASSWORD)).getBytes());

        // create get all services url
        if (!APIMHost.endsWith("/")) {
            APIMHost = APIMHost + "/";
        }
        APIMHost = APIMHost + SERVICE_CATALOG_ENDPOINT_PREFIX + API_VERSION + SERVICE_CATALOG_GET_SERVICES_ENDPOINT;

        try {
            HttpsURLConnection connection = (HttpsURLConnection) new URL(APIMHost).openConnection();
            connection.setDoOutput(true);
            connection.setRequestMethod("GET");
            connection.addRequestProperty("Accept", "application/json");
            connection.addRequestProperty("Authorization", "Basic " + credentials);
            connection.setHostnameVerifier(getHostnameVerifier());

            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                try (BufferedReader in = new BufferedReader(new InputStreamReader(
                        connection.getInputStream()))) {
                    String inputLine;
                    StringBuilder response = new StringBuilder();

                    while ((inputLine = in.readLine()) != null) {
                        response.append(inputLine);
                    }

                    JsonParser parser = new JsonParser();
                    JsonObject rootObject = parser.parse(response.toString()).getAsJsonObject();
                    JsonArray serviceList = rootObject.getAsJsonArray(LIST_STRING);
                    for (JsonElement service : serviceList) {
                        String serviceKey = ((JsonObject) service).get(SERVICE_KEY).getAsString();
                        String md5 = ((JsonObject) service).get(MD5).getAsString();
                        md5Map.put(serviceKey, md5);
                    }
                }
                return md5Map;
            } else {
                log.error("Error occurred while fetching services from the service catalog");
            }
        } catch (MalformedURLException e) {
            log.error("Service catalog url " + APIMHost + " is malformed. Please check the configuration", e);
        } catch (MalformedJsonException e) {
            log.error("Invalid JSON response received from service catalog", e);
        } catch (ProtocolException e) {
            log.error("Error occurred while creating the connection with APIM", e);
        } catch (IOException e) {
            log.error("Error occurred while reading the response from service catalog", e);
        }
        return null;
    }

    /**
     * Upload the given ZIP file to the APIM service catalog endpoint.
     *
     * @param apimConfigs        map containing APIM configurations.
     * @param attachmentFilePath location of the zip file that needs to be uploaded.
     * @return status code returned from APIM, -1 if error occurred.
     */
    private static int uploadZip(Map<String, String> apimConfigs, String attachmentFilePath) {
        try {
            String APIMHost = apimConfigs.get(APIM_HOST);

            // create POST URL
            if (!APIMHost.endsWith("/")) {
                APIMHost = APIMHost + "/";
            }
            APIMHost = APIMHost + SERVICE_CATALOG_ENDPOINT_PREFIX + API_VERSION + SERVICE_CATALOG_PUBLISH_ENDPOINT;

            String encodeBytes =
                    Base64.getEncoder().encodeToString(
                            (apimConfigs.get(USER_NAME) + ":" + apimConfigs.get(PASSWORD)).getBytes());

            File binaryFile = new File(attachmentFilePath);
            String boundary = "------------------------" +
                    Long.toHexString(System.currentTimeMillis()); // Generate some unique random value.
            String CRLF = "\r\n"; // Line separator required by multipart/form-data.
            HttpsURLConnection connection = (HttpsURLConnection) new URL(APIMHost).openConnection();
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);
            connection.addRequestProperty("Accept", "application/json");
            connection.addRequestProperty("Authorization", "Basic " + encodeBytes);
            connection.setHostnameVerifier(getHostnameVerifier());

            OutputStream output = connection.getOutputStream();

            // Send binary file - part
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(output, StandardCharsets.UTF_8), true);
            writer.append("--").append(boundary).append(CRLF);
            writer.append("Content-Disposition: form-data; name=\"file\"; filename=\"").append(
                    binaryFile.getName()).append("\"").append(CRLF);
            writer.append("Content-Type: application/octet-stream").append(CRLF);
            writer.append(CRLF).flush();

            // File data
            Files.copy(binaryFile.toPath(), output);
            output.flush();

            // Add verifier
            String verifier = new Gson().toJson(md5List);
            writer.append("--").append(boundary).append(CRLF);
            writer.append("Content-Disposition: form-data; name=\"" + VERIFIER + "\"").append(CRLF);
            writer.append(CRLF);
            writer.append(verifier).append(CRLF);

            // End of multipart/form-data.
            writer.append(CRLF).append("--").append(boundary).append("--").flush();

            // Read the response if debug enabled.
            if (log.isDebugEnabled()) {
                try (BufferedReader br = new BufferedReader(new BufferedReader(new InputStreamReader(
                        connection.getInputStream())))) {
                    StringBuilder response = new StringBuilder();
                    String strCurrentLine;
                    while ((strCurrentLine = br.readLine()) != null) {
                        response.append(strCurrentLine);
                    }
                    log.debug("Response from APIM : " + response);
                }
            }
            return connection.getResponseCode();
        } catch (MalformedURLException e) {
            log.error("Service catalog url is malformed, please check the configured APIM host", e);
        } catch (IOException e) {
            log.error("Error occurred while uploading metadata to service catalog endpoint", e);
        }
        return -1;
    }

    /**
     * Read APIM host configurations from deployment.toml file.
     *
     * @param secretCallbackHandlerService secret callback handler reference.
     * @return map of resolved values.
     */
    public static Map<String, String> readConfiguration(SecretCallbackHandlerService secretCallbackHandlerService) {
        Map<String, String> configMap = new HashMap<>();
        Map<String, String> catalogProperties =
                (Map<String, String>) ((ArrayList) ConfigParser.getParsedConfigs().get(
                        SERVICE_CATALOG_CONFIG)).get(0);

        String apimHost = catalogProperties.get(APIM_HOST);

        String userName = catalogProperties.get(USER_NAME);
        String password = catalogProperties.get(PASSWORD);
        String executorThreads = catalogProperties.get(SERVICE_CATALOG_EXECUTOR_THREADS);
        if (secretResolver == null) {
            secretResolver = SecretResolverFactory.create((OMElement) null, false);
        }
        if (!secretResolver.isInitialized()) {
            secretResolver.init(secretCallbackHandlerService.getSecretCallbackHandler());
        }
        String alias = MiscellaneousUtil.getProtectedToken(userName);
        if (!StringUtils.isEmpty(alias)) {
            userName = secretResolver.resolve(alias);
        }
        alias = MiscellaneousUtil.getProtectedToken(password);
        if (!StringUtils.isEmpty(alias)) {
            password = secretResolver.resolve(alias);
        }

        configMap.put(APIM_HOST, apimHost);
        configMap.put(USER_NAME, userName);
        configMap.put(PASSWORD, password);
        configMap.put(SERVICE_CATALOG_EXECUTOR_THREADS, executorThreads);
        return configMap;
    }

    /**
     * Process metadata folder and move to temporary location.
     *
     * @param tempDir            temporary directory to put metadata.
     * @param metadataFolder     metadata folder inside CAPP.
     * @param metadataYamlFolder YAML folder inside CAPP.
     * @param md5MapOfAllService map containing md5 values of all services.
     * @return metadata processed successfully
     * @throws IOException              error occurred while moving files.
     * @throws ResolverException        error occurred while updating the metadata file.
     * @throws NoSuchAlgorithmException could not find the MD% algorithm.
     */
    private static boolean processMetadata(File tempDir, File metadataFolder, File metadataYamlFolder,
                                           Map<String, String> md5MapOfAllService) throws IOException
            , ResolverException, NoSuchAlgorithmException {
        String metaFileName = metadataYamlFolder.getName();
        if  (metaFileName.contains(PROXY_SERVICE_SUFFIX) && !(new File(metadataFolder,
                metaFileName.replaceAll(METADATA_FOLDER_STRING, SWAGGER_FOLDER_STRING))).exists()) {
            return processProxyServiceMetadata(tempDir, metadataYamlFolder, md5MapOfAllService);
        } else if (metaFileName.contains(DATA_SERVICE_SUFFIX)) {
            return processServiceMetadata(metadataFolder, metaFileName, tempDir, metadataYamlFolder,
                    md5MapOfAllService, true);
        } else {
            return processServiceMetadata(metadataFolder, metaFileName, tempDir, metadataYamlFolder,
                    md5MapOfAllService, false);
        }
    }

    /**
     * Process metadata folder of APIs and move to temporary location.
     *
     * @param metadataFolder     metadata folder inside CAPP.
     * @param metaFileName       metadata file name.
     * @param tempDir            temporary directory to put metadata.
     * @param metadataYamlFolder YAML folder inside CAPP.
     * @param md5MapOfAllService map containing md5 values of all services.
     * @return metadata processed successfully
     * @throws IOException              error occurred while moving files.
     * @throws ResolverException        error occurred while updating the metadata file.
     * @throws NoSuchAlgorithmException could not find the MD% algorithm.
     */
    private static boolean processServiceMetadata(File metadataFolder, String metaFileName, File tempDir,
                                                  File metadataYamlFolder, Map<String, String> md5MapOfAllService,
                                                  boolean isDataService)
            throws IOException, ResolverException, NoSuchAlgorithmException {
        String APIName = metaFileName.substring(0, metaFileName.indexOf(METADATA_FOLDER_STRING));
        String APIVersion =
                metaFileName.substring(metaFileName.lastIndexOf(METADATA_FOLDER_STRING) +
                        METADATA_FOLDER_STRING.length());
        // Create new folder in temp directory for this API
        File newMetaFile = new File(tempDir, APIName + "_v" + APIVersion);

        File newYamlFile = new File(newMetaFile, METADATA_FILE_NAME);
        File metadataYamlFile =
                new File(metadataYamlFolder, APIName + METADATA_FILE_STRING + APIVersion + YAML_FILE_EXTENSION);
        File swaggerFile;
        File newSwaggerFile = new File(newMetaFile, SWAGGER_FILE_NAME);;
        if (isDataService) {
            swaggerFile = new File(metadataYamlFolder, SWAGGER_FILE_NAME);
        } else {
            File swaggerFolder = new File(metadataFolder, APIName + SWAGGER_FOLDER_STRING + APIVersion);
            swaggerFile =
                    new File(swaggerFolder, APIName + SWAGGER_FILE_STRING + APIVersion + YAML_FILE_EXTENSION);
        }

        // Edit metadata yaml and add host details
        String key = updateMetadataWithServiceUrl(metadataYamlFile);
        String md5FromServer = md5MapOfAllService.get(key);
        if (isDataService) {
            if (!readServiceWsdlOrYaml(metadataYamlFile, metadataYamlFolder, false)) {
                log.error("Could not find WSDL definition of data service: " + metaFileName.substring(0,
                        metaFileName.indexOf(DATA_SERVICE_SUFFIX + METADATA_FOLDER_STRING)));
                return false;
            }
        }

        // generate md5 values for verifier
        String md5SumOfMetadata = getFileChecksum(metadataYamlFile);
        String md5SumOfSwagger = getFileChecksum(swaggerFile);
        String newMD5String = md5SumOfSwagger + md5SumOfMetadata;

        // if API is not registered yet or, API is modified (md5 changed), add metadata files to temp folder
        if (StringUtils.isEmpty(md5FromServer) || !newMD5String.equals(md5FromServer)) {
            if (!newMetaFile.mkdir()) {
                log.error("Could not create temporary files");
                return false;
            }
            // Copy metadata yaml file to the temp location.
            FileUtils.copyFile(metadataYamlFile, newYamlFile);
            // Copy swagger yaml file to the temp location.
            FileUtils.copyFile(swaggerFile, newSwaggerFile);

            // Add to map to be included in filter formdata field.
            // if API is changed add the previous MD5 sum, if new API add the new MD5 sum.
            md5List.add(new ServiceMetaDataHolder(key, StringUtils.isEmpty(md5FromServer) ? newMD5String : md5FromServer));
        } else {
            alreadyUploaded = true;
            if (log.isDebugEnabled()) {
                log.debug(APIName + " is already updated in the service catalog");
            }
        }
        return true;
    }

    /**
     * Process metadata folder of proxy service and move to temporary location.
     *
     * @param tempDir            temporary directory to put metadata.
     * @param metadataYamlFolder YAML folder inside CAPP.
     * @param md5MapOfAllService map containing md5 values of all services.
     * @return metadata processed successfully
     * @throws IOException              error occurred while moving files.
     * @throws ResolverException        error occurred while updating the metadata file.
     * @throws NoSuchAlgorithmException could not find the MD% algorithm.
     */
    private static boolean processProxyServiceMetadata(File tempDir, File metadataYamlFolder,
                                           Map<String, String> md5MapOfAllService) throws IOException
            , ResolverException, NoSuchAlgorithmException {

        String metaFileName = metadataYamlFolder.getName();
        String proxyServiceName = metaFileName.substring(0,
                metaFileName.indexOf(PROXY_SERVICE_SUFFIX + METADATA_FOLDER_STRING));
        String proxyServiceVersion =
                metaFileName.substring(metaFileName.lastIndexOf(METADATA_FOLDER_STRING) +
                        METADATA_FOLDER_STRING.length());

        // Create new folder in temp directory for this proxy service
        File newMetaFile = new File(tempDir,  proxyServiceName + PROXY_SERVICE_SUFFIX
                + "_v" + proxyServiceVersion);
        File newYamlFile = new File(newMetaFile, METADATA_FILE_NAME);
        File metadataYamlFile = new File(metadataYamlFolder, proxyServiceName + PROXY_SERVICE_SUFFIX
                + METADATA_FILE_STRING + proxyServiceVersion + YAML_FILE_EXTENSION);

        // Edit metadata yaml and add host details
        String key = updateMetadataWithServiceUrl(metadataYamlFile);
        String md5FromServer = md5MapOfAllService.get(key);

        // Check WSDL file is fetched
        if (!readServiceWsdlOrYaml(metadataYamlFile, metadataYamlFolder, true)) {
            log.error("Could not find WSDL definition of proxy service: " + proxyServiceName);
            return false;
        }
        File wsdlFile = new File(metadataYamlFolder, WSDL_FILE_NAME);

        // Generate md5 values for verifier
        String md5SumOfMetadata = getFileChecksum(metadataYamlFile);
        String md5SumOfWSDL = getFileChecksum(wsdlFile);
        String newMD5String = md5SumOfWSDL + md5SumOfMetadata;

        // if proxy-service is not registered yet or, proxy-service is modified (md5 changed),
        // add metadata files to temp folder
        if (StringUtils.isEmpty(md5FromServer) || !newMD5String.equals(md5FromServer)) {
            if (!newMetaFile.mkdir()) {
                log.error("Could not create temporary files");
                return false;
            }
            // Copy metadata yaml file to the temp location.
            FileUtils.copyFile(metadataYamlFile, newYamlFile);
            // Copy wsdl file to the temp location.
            FileUtils.copyFile(wsdlFile, new File(newMetaFile, wsdlFile.getName()));

            // Add to map to be included in filter formdata field.
            // if API is changed add the previous MD5 sum, if new API add the new MD5 sum.
            md5List.add(new ServiceMetaDataHolder(key, StringUtils.isEmpty(md5FromServer) ? newMD5String : md5FromServer));
        } else {
            alreadyUploaded = true;
            if (log.isDebugEnabled()) {
                log.debug(proxyServiceName + " is already updated in the service catalog");
            }
        }
        return true;
    }

    /**
     * Create the final ZIP file wrapping all the metadata files.
     *
     * @param destArchiveName location of the ZIP file.
     * @param sourceDir       location of the metadata folder.
     * @return result of zip creation process. ( successful / failed ).
     */
    public static boolean archiveDir(String destArchiveName, String sourceDir) {
        File zipDir = new File(sourceDir);
        if (zipDir.exists() && zipDir.list().length == 0) {
            if (alreadyUploaded) {
                log.info("Service catalog already contains the latest configs, aborting the service-catalog uploader");
            } else {
                log.info("Metadata not included, hence not publishing to Service Catalog");
            }
            return false;
        }

        try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(destArchiveName))) {
            zipDir(zipDir, zos, sourceDir);

        } catch (Exception ex) {
            log.error("Error occurred while creating the archive", ex);
            return false;
        }
        return true;
    }

    private static void zipDir(File zipDir, ZipOutputStream zos, String archiveSourceDir) throws IOException {
        //get a listing of the directory content
        String[] dirList = zipDir.list();
        byte[] readBuffer = new byte[40960];
        int bytesIn;
        //loop through dirList, and zip the files
        for (String s : dirList) {
            File f = new File(zipDir, s);
            //place the zip entry in the ZipOutputStream object
            zos.putNextEntry(new ZipEntry(getZipEntryPath(f, archiveSourceDir)));
            if (f.isDirectory()) {
                //if the File object is a directory, call this
                //function again to add its content recursively
                zipDir(f, zos, archiveSourceDir);
                //loop again
                continue;
            }
            //if we reached here, the File object f was not a directory
            //create a FileInputStream on top of f
            FileInputStream fis = new FileInputStream(f);

            //now write the content of the file to the ZipOutputStream
            while ((bytesIn = fis.read(readBuffer)) != -1) {
                zos.write(readBuffer, 0, bytesIn);
            }
            //close the Stream
            fis.close();
        }
    }

    private static String getZipEntryPath(File f, String archiveSourceDir) {
        String entryPath = f.getPath();
        entryPath = entryPath.substring(archiveSourceDir.length() + 1);
        if (File.separatorChar == '\\') {
            entryPath = entryPath.replace(File.separatorChar, '/');
        }
        if (f.isDirectory()) {
            entryPath += "/";
        }
        return entryPath;
    }

    /**
     * Disabling the hostname verification.
     *
     * @return true for all the host names.
     */
    private static HostnameVerifier getHostnameVerifier() {
        return (s, sslSession) -> true;
    }

    /**
     * Check pre-conditions before stating the service-catalog uploading process.
     *
     * @return pre-condition are matched / not matched.
     */
    public static boolean checkPreConditions() {
        if (log.isDebugEnabled()) {
            log.debug("Start service-catalog uploading process");
        }
        // Check for faulty CAPPs. If atleast one CAPP is fault MI is not ready - readiness probe.
        ArrayList<String> faultyCapps = new ArrayList<>(CappDeployer.getFaultyCapps());
        if (!faultyCapps.isEmpty()) {
            log.info("Faulty CAPPs detected - aborting the service-catalog uploader");
            return false;
        }
        // Skip if no CAPPs are deployed
        ArrayList<CarbonApplication> deployedCapps = new ArrayList<>(CappDeployer.getCarbonApps());
        if (deployedCapps.isEmpty()) {
            log.info("Cannot find carbon applications - aborting the service-catalog uploader");
            return false;
        }
        // Skip if no APIs or Proxy Services are deployed
        Collection APITable =
                SynapseConfigUtils.getSynapseConfiguration(
                        org.wso2.micro.core.Constants.SUPER_TENANT_DOMAIN_NAME).getAPIs();
        Collection proxyTable =
                SynapseConfigUtils.getSynapseConfiguration(
                        org.wso2.micro.core.Constants.SUPER_TENANT_DOMAIN_NAME).getProxyServices();
        ConfigurationContext configurationContext = DataHolder.getInstance().getConfigurationContext();
        HashMap<String, AxisService> services = configurationContext.getAxisConfiguration().getServices();
        if (APITable.isEmpty() && proxyTable.isEmpty() && services.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("Cannot find APIs, Proxy Services, or Data Services - aborting the service-catalog uploader");
            }
            return false;
        }
        return true;
    }

    /**
     * Extract CAPPs and put metadata files in the temporary folder.
     *
     * @param targetDir          temporary folder location.
     * @param repoLocation       location of the deployment folder of MI.
     * @param md5MapOfAllService map containing md5 values of all services.
     * @return true if extraction was successful, false otherwise.
     */
    public static boolean extractMetadataFromCAPPs(File targetDir, String repoLocation,
                                                   Map<String, String> md5MapOfAllService) {
        File cappFolder = new File(repoLocation, CAPP_FOLDER_NAME);
        File[] files = cappFolder.listFiles(CAPP_FILTER);
        if (files == null) return false; // should not reach here. Checked in checkPreConditions() method

        for (File file : files) {
            if (!extractCApp(file, targetDir, md5MapOfAllService)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Extract given CAPP and put metadata files in the temporary folder.
     *
     * @param cAppName           CAPP name to extract
     * @param targetDir          temporary folder location.
     * @param repoLocation       location of the deployment folder of MI.
     * @param md5MapOfAllService map containing md5 values of all services.
     * @return true if extraction was successful, false otherwise.
     */
    public static boolean extractMetadataFromCAPP(String cAppName, File targetDir, String repoLocation,
                                                   Map<String, String> md5MapOfAllService) {
        File cappFolder = new File(repoLocation, CAPP_FOLDER_NAME);
        File[] files = cappFolder.listFiles(CAPP_FILTER);
        if (files == null) return false; // should not reach here. Checked in checkPreConditions() method

        if (cAppName != null && !cAppName.isEmpty()) {
            File file = new File(cappFolder, cAppName);
            if (file.exists()) {
                return extractCApp(file, targetDir, md5MapOfAllService);
            } else {
                log.error("CAPP file not found: " + cAppName);
                return false;
            }
        } else {
            log.error("CAPP name should be defined");
            return false;
        }
    }

    /**
     * Extracts the contents of a Carbon Application (CApp) archive, processes its metadata, and generates
     * the corresponding MD5 checksums for all services defined within the metadata.
     *
     * This method first extracts the CApp archive to a temporary directory and checks if a metadata folder
     * exists. If the metadata folder is found, it iterates through the metadata files and processes them
     * to update the target directory with necessary files. The method handles different exceptions, including
     * I/O issues, errors in extracting the CApp, misconfigured environment variables, and MD5 generation failures.
     *
     * @param cApp The Carbon Application (CApp) archive to be extracted.
     * @param targetDir The target directory where extracted files should be placed.
     * @param md5MapOfAllService A map containing MD5 checksums of all services, used for verification.
     * @return true if the extraction and processing were successful, false otherwise.
     */
    private static boolean extractCApp(File cApp, File targetDir, Map<String, String> md5MapOfAllService) {
        FilenameFilter metaFilter = (f, name) -> name.contains(METADATA_FOLDER_STRING);

        /*
         * Sample folder structure of metadata inside the new Carbon Application
         * metadata
         *  - testApi_metadata_1.0.0
         *    - testApi_metadata-1.0.0.yaml
         *    - artifact.xml
         *  - testApi_swagger_1.0.0
         *    - testApi_swagger-1.0.0.yaml
         *    - artifact.xml
         *  - testProxyService_proxy_metadata_1.0.0
         *    - testProxyService_proxy_metadata-1.0.0.yaml
         *    - artifact.xml
         */

        try {
            // Extract the CAPP and get extracted location.
            String tempExtractedDirPath = AppDeployerUtils.extractCarbonApp(cApp.getPath());
            File metadataFolder = new File(tempExtractedDirPath, METADATA_FOLDER_NAME);
            // does not have a metadata folder -> old CAPP format.
            if (metadataFolder.exists()) {
                File[] metadataFolders = metadataFolder.listFiles(metaFilter);
                if (metadataFolders != null) {
                    for (File metadataYamlFolder : metadataFolders) {
                        if (!processMetadata(targetDir, metadataFolder, metadataYamlFolder, md5MapOfAllService)) {
                            return false;
                        }
                    }
                }
            }
        } catch (IOException e) {
            log.error("Error occurred while processing the metadata files", e);
            return false;
        } catch (CarbonException e) {
            log.error("Error occurred when extracting the carbon application", e);
            return false;
        } catch (ResolverException e) {
            log.error("Environment variables are not configured correctly", e);
            return false;
        } catch (NoSuchAlgorithmException e) {
            log.error("Could not generate the MD5 sum", e);
            return false;
        }
        return true;
    }

    /**
     * Create temporary folder structure.
     *
     * @param folderPath path to the temp directory.
     * @return folder creation result.
     */
    public static boolean createTemporaryFolders(String folderPath) {
        File serviceCatalogFolder = new File(folderPath);
        if (serviceCatalogFolder.exists()) {
            try {
                FileUtils.forceDelete(serviceCatalogFolder);
            } catch (IOException e) {
                log.error("Error occurred while removing temporary directories", e);
            }
        }
        boolean created = serviceCatalogFolder.mkdir();
        if (!created) {
            log.error("Could not create temporary directories required for service catalog");
            return false;
        }

        File tempDir = new File(folderPath, TEMP_FOLDER_NAME);
        if (tempDir.exists()) {
            tempDir.delete();
        }
        created = tempDir.mkdir();
        if (!created) {
            log.error("Could not create temporary directories required for service catalog");
        }
        return created;
    }

    /**
     * Generate MD5 sum of a given file.
     *
     * @param file input file.
     * @return MD5 sum of the given file.
     * @throws IOException              error occurred while processing the file.
     * @throws NoSuchAlgorithmException Could not find the MD5 sum algo.
     */
    private static String getFileChecksum(File file) throws NoSuchAlgorithmException, IOException {
        MessageDigest md5Digest = MessageDigest.getInstance("MD5");
        FileInputStream fis = new FileInputStream(file);

        byte[] byteArray = new byte[1024];
        int bytesCount;

        while ((bytesCount = fis.read(byteArray)) != -1) {
            md5Digest.update(byteArray, 0, bytesCount);
        }
        fis.close();

        byte[] bytes = md5Digest.digest();

        StringBuilder sb = new StringBuilder();
        for (byte aByte : bytes) {
            sb.append(Integer.toString((aByte & 0xff) + 0x100, 16).substring(1));
        }
        return sb.toString();
    }

    /**
     * Reads WSDL of the proxy service and make a WSDL file in the given location.
     *
     * @param metadataYamlFile service metadata yaml file.
     * @param storeLocation read wsdl file store location.
     * @return WSDL file creation result.
     */
    private static boolean readServiceWsdlOrYaml(File metadataYamlFile, File storeLocation, boolean readWsdl) {
        String queryParam;
        String fileName;
        if (readWsdl) {
            queryParam = WSDL_URL_PATH;
            fileName = WSDL_FILE_NAME;
        } else {
            queryParam = SWAGGER_URL_PATH;
            fileName = SWAGGER_FILE_NAME;
        }
        BufferedReader bufferedReader = null;
        try {
            String serviceUrl = getServiceUrlFromMetadata(metadataYamlFile);
            if (serviceUrl == null) {
                return false;
            }
            if (serviceUrl.endsWith(PATH_SEPARATOR)) {
                serviceUrl = serviceUrl.substring(serviceUrl.length() - 1);
            }
            serviceUrl = serviceUrl + queryParam;
            URL website = new URL(serviceUrl);
            URLConnection connection = website.openConnection();
            bufferedReader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            StringBuilder responseWsdlOrYaml = new StringBuilder();
            if (lineSeparator == null) {
                lineSeparator = System.getProperty("line.separator");
            }
            String inputLine;
            while ((inputLine = bufferedReader.readLine()) != null) {
                responseWsdlOrYaml.append(inputLine).append(lineSeparator);
            }
            if (responseWsdlOrYaml.length() == 0) {
                return false;
            }
            if (!readWsdl) {
                responseWsdlOrYaml = updateYamlWithSoapResource(storeLocation, responseWsdlOrYaml);
            }
            if (storeLocation.exists()) {
                String wsdlString = responseWsdlOrYaml.toString();
                boolean shouldSchemaLocationBeChanged = false;
                if (readWsdl) {
                    Collection<ProxyService> proxyTable =
                            SynapseConfigUtils.getSynapseConfiguration(
                                    org.wso2.micro.core.Constants.SUPER_TENANT_DOMAIN_NAME).getProxyServices();
                    shouldSchemaLocationBeChanged = shouldSchemaLocationBeChanged(storeLocation, proxyTable);
                }
                if (shouldSchemaLocationBeChanged) {
                    // Replace schemaLocation values ending with .xsd and change everything up to ? to xyz using regex
                    String regexPattern = "(schemaLocation=\")[^\"?]*\\?(.*\\.xsd\")";
                    String baseUrl = serviceUrl.replace(queryParam, "?");
                    String replacement = "$1" + baseUrl + "$2";
                    wsdlString = wsdlString.replaceAll(regexPattern, replacement);
                }
                Files.write(Paths.get(storeLocation.getAbsolutePath(), fileName), wsdlString.getBytes());
                return true;
            }
        } catch (IOException e) {
            log.error("Error while getting WSDL definition of the given proxy service", e);
        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    log.error("Error while closing the buffer reader", e);
                }
            }
        }

        return false;
    }

    private static boolean shouldSchemaLocationBeChanged(File storeLocation, Collection<ProxyService> proxyTable) {
        String metaFileName = storeLocation.getName();
        String proxyServiceName = metaFileName.substring(0,
                metaFileName.indexOf(PROXY_SERVICE_SUFFIX + METADATA_FOLDER_STRING));
        if (proxyTable != null && !proxyTable.isEmpty()) {
            for (ProxyService proxyService : proxyTable) {
                if (proxyService.getName().equals(proxyServiceName) && proxyService.getResourceMap() != null) {
                    return true;
                }
            }
        }
        return false;
    }

    private static StringBuilder updateYamlWithSoapResource(File storeLocation, StringBuilder responseWsdlOrYaml)
            throws JsonProcessingException {
        String metaFileName = storeLocation.getName();
        List<String> restResource = new ArrayList<>();
        List<String> soapResource = new ArrayList<>();
        String dataServiceName = metaFileName.substring(0,
                metaFileName.indexOf(DATA_SERVICE_SUFFIX + METADATA_FOLDER_STRING));
        ConfigurationContext configurationContext = DataHolder.getInstance().getConfigurationContext();
        HashMap<String, AxisService> dataServiceTable = configurationContext.
                getAxisConfiguration().getServices();
        if (dataServiceTable == null || dataServiceTable.isEmpty()) {
            return responseWsdlOrYaml;
        }
        AxisService dataService = dataServiceTable.get(dataServiceName);
        if (dataService == null) {
            return responseWsdlOrYaml;
        }
        if (hasSoapRequest(dataService, getTotalNoOfResources(dataService, restResource),
                soapResource, restResource)) {
            SwaggerParseResult result = new OpenAPIV3Parser().readContents(responseWsdlOrYaml.toString());
            OpenAPI openAPI = result.getOpenAPI();
            if (openAPI == null) {
                return responseWsdlOrYaml;
            }
            io.swagger.v3.oas.models.Paths paths = openAPI.getPaths();
            if (soapResource.size() > 0) {
                PathItem pathItem = updateYamlContentWithSoapRequestResource(soapResource);
                openAPI.path("/*", pathItem);
            }
            openAPI.setPaths(paths);
            responseWsdlOrYaml = new StringBuilder(io.swagger.v3.core.util.Yaml.mapper().writeValueAsString(openAPI));
        }
        return responseWsdlOrYaml;
    }

    private static PathItem updateYamlContentWithSoapRequestResource(List<String> soapOperation) {
        PathItem pathItem = new PathItem();
        // Create a new POST operation
        Operation postOperation = new Operation();
        postOperation.setSummary("SOAP Request");
        postOperation.setOperationId("createSoapRequest");

        // Add a SOAP action header
        HeaderParameter headerParameter = new HeaderParameter();
        headerParameter.setName("SOAPAction");
        headerParameter.setRequired(true);
        RequestBody requestBody = new RequestBody();
        if (soapOperation.contains(URN_REQUEST_BOX) && soapOperation.size() > 1) {
            requestBody.description("This example accommodates both single and request box operations. " +
                    "Populate `dat:operation_name` for single requests or `dat:request_box` " +
                    "for invoking multiple operations. Include only the relevant sections for your operation.");
        }
        Content content = new Content();
        headerParameter.setSchema(new io.swagger.v3.oas.models.media.StringSchema()._enum(soapOperation));
        addSoapRequestSamplePayload(content, Constants.SOAP_11_NAME_SPACE, Constants.TEXT_XML, soapOperation);
        addSoapRequestSamplePayload(content, Constants.SOAP_12_NAME_SPACE, Constants.APPLICATION_SOAP, soapOperation);
        requestBody.content(content).required(true);
        postOperation.addParametersItem(headerParameter);
        postOperation.setRequestBody(requestBody);

        // Set the response for the operation
        ApiResponses responses = new ApiResponses();
        ApiResponse response = new ApiResponse();
        response.setDescription("SOAP Response");
        Content responseContent = new Content();
        MediaType responseMediaType = new MediaType();
        responseContent.addMediaType(Constants.APPLICATION_XML, responseMediaType);
        response.setContent(responseContent);
        responses.addApiResponse("200", response);
        postOperation.setResponses(responses);

        pathItem.setPost(postOperation);
        return pathItem;
    }

    private static void addSoapRequestSamplePayload(Content content, String namespace,
                                                    String requestType, List<String> soapOperation) {
        Schema<?> bodySchema = new Schema().type(OBJECT).xml(new XML().name(SOAP_ENV_BODY));
        HashMap<String, Schema> bodyProperties = new HashMap<>();
        if ((soapOperation.size() > 0 && !soapOperation.contains(URN_REQUEST_BOX)) ||
                (soapOperation.contains(URN_REQUEST_BOX) && soapOperation.size() > 1)) {
            bodyProperties.put("soap", createSoapOperation());
        }
        if (soapOperation.contains(URN_REQUEST_BOX)) {
            bodyProperties.put(REQUEST_BOX, createRequestBoxOperation());
        }
        bodySchema.properties(bodyProperties);
        Schema<?> headerSchema = new Schema<>()
                .type(OBJECT)
                .xml(new XML().name(SOAP_ENV_HEADER));
        Schema<?> envelopeSchema = new Schema<Object>()
                .type(OBJECT)
                .properties(new HashMap<String, Schema<?>>() {{
                    put("Header", headerSchema);
                    put("Body", bodySchema);
                    put("xmlns:dat", new Schema<String>().type(STRING).
                            example("http://ws.wso2.org/dataservice").xml(new XML().attribute(true)));
                }}).xml(new XML()
                        .name(ENVELOPE)
                        .namespace(namespace)
                        .prefix(SOAP_ENV));
        MediaType mediaType = new MediaType().schema(envelopeSchema);
        content.addMediaType(requestType, mediaType);
    }

    private static Schema createSoapOperation() {
        return new Schema<>().type(OBJECT).properties(
                new HashMap<String, Schema>() {{
                    put(PARAM_NAME_2, new Schema<String>().type(STRING).example(QUESTION_MARK));
                    put(PARAM_NAME_1, new Schema<String>().type(STRING).example(QUESTION_MARK));
                    ;
                }}).xml(new XML().name(OPERATION_NAME));
    }

    private static Schema createRequestBoxOperation() {
        Schema operation1 = new Schema<>().type(OBJECT).properties(
                        new HashMap<String, Schema>() {{
                            put(PARAM_NAME_2, new Schema<String>().type(STRING).example(QUESTION_MARK));
                            put(PARAM_NAME_1, new Schema<String>().type(STRING).example(QUESTION_MARK));
                            ;
                        }})
                .xml(new XML().name(OPERATION_NAME_1));
        Schema<?> operation2 = new Schema().type(OBJECT).properties(
                        new HashMap<String, Schema>() {{
                            put(PARAM_NAME_1, new Schema().type(STRING).example(QUESTION_MARK));
                        }})
                .xml(new XML().name(OPERATION_NAME_2));
        return new Schema()
                .type(OBJECT)
                .properties(new HashMap<String, Schema>() {{
                    put("operation_1", operation2);
                    put("operation_2", operation1);
                }})
                .xml(new XML().name(DAT_REQUEST_BOX));
    }

    private static int getTotalNoOfResources(AxisService dataService, List<String> restResource) {
        Object dataServiceObject = dataService.getParameter(SWAGGER_RESOURCE_OBJECT).getValue();
        int noOfRestResource = 0;
        for (Map.Entry<String, AxisResource> entry : ((AxisResources) dataServiceObject).
                getAxisResourceMap().getResources().entrySet()) {
            noOfRestResource = noOfRestResource + entry.getValue().getMethods().size();
            for (String method : entry.getValue().getMethods()) {
                String resourceName = UNDERSCORE.concat(method.toLowerCase()).concat(entry.getKey().
                        replaceAll(CURLY_OPEN_BRACKET, EMPTY_STRING).replaceAll(CURLY_CLOSE_BRACKET, EMPTY_STRING).
                        replaceAll(SLASH, UNDERSCORE).toLowerCase());
                restResource.add(resourceName);
            }
        }
        return noOfRestResource;
    }

    private static boolean hasSoapRequest(AxisService dataService, int noOfRestResource, List<String> soapResource,
                                          List<String> restResource) {
        if ((noOfRestResource > 0 && dataService.getPublishedOperations().size() > noOfRestResource) ||
                (noOfRestResource == 0 && dataService.getPublishedOperations().size() > 0)) {
            for (AxisOperation operation : dataService.getPublishedOperations()) {
                String operationName = operation.getName().toString();
                if (!restResource.contains(operationName)) {
                    soapResource.add(URN + operationName);
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Reads service URL from metadata file.
     *
     * @param yamlFile location of the yamlFile.
     * @return URL of the metadata service.
     */
    private static String getServiceUrlFromMetadata(File yamlFile) {
        Yaml yaml = new Yaml();
        String currentServiceUrl = null;
        try (InputStream yamlStream = new FileInputStream(yamlFile)) {
            Map<String, Object> obj = (Map<String, Object>) yaml.load(yamlStream);
            currentServiceUrl = (String) obj.get(SERVICE_URL);
        } catch (IOException e) {
            log.error("Failed to fetch serviceUrl from the metadata YAML file or file does not exist");
        }
        return currentServiceUrl;
    }

    /**
     * Checks whether Service Catalog configuration is Enabled.
     *
     * @return whether Service Catalog is enabled.
     */
    public static boolean isServiceCatalogEnabled() {
        Map<String, Object> catalogProperties;
        if (ConfigParser.getParsedConfigs().get(SERVICE_CATALOG_CONFIG) != null) {
            catalogProperties =
                    (Map<String, Object>) ((ArrayList) ConfigParser.getParsedConfigs().get(
                            SERVICE_CATALOG_CONFIG)).get(0);
            if ((boolean) catalogProperties.get(ENABLE)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Retrieve Executor Thread configuration for Service Catalog.
     *
     * @param serviceCatalogConfig  Map containing deployment.toml configuration
     * @param def                   Default value
     *
     * @return Executor Thread count.
     */
    public static int getExecutorThreadCount(Map<String, String> serviceCatalogConfig, int def) {
        String executorThreads = serviceCatalogConfig.get(SERVICE_CATALOG_EXECUTOR_THREADS);
        int threads;
        if (executorThreads != null) {
            try {
                threads = Integer.parseInt(executorThreads);
                if (log.isDebugEnabled()) {
                    log.debug("Service Catalog Executor Thread count is set to " + threads);
                }
            } catch (NumberFormatException e) {
                log.warn("Invalid Service Catalog Executor Thread count. Setting to default " + def);
                return def;
            }
            return threads ;
        }
        if (log.isDebugEnabled()) {
            log.debug("Service Catalog Executor Thread count is not defined. Setting to default " + def);
        }
        return def;
    }

    /**
     * Determines if the server is currently in startup mode.
     *
     * This method checks for the presence of the "setup" system property
     * which is set during server initialization. The property is used to
     * indicate that the server is still in the process of starting up.
     *
     * @return true if the server is in startup mode, false otherwise
     */
    public static boolean isServerInStartupMode() {
        String isStartUpMode = System.getProperty("org.wso2.mi.server.startup.mode");
        return isStartUpMode != null && Boolean.parseBoolean(isStartUpMode);
    }
}

/**
* Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
* WSO2 Inc. licenses this file to you under the Apache License,
* Version 2.0 (the "License"); you may not use this file except
* in compliance with the License.
* you may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
*
*/

package org.wso2.micro.integrator.registry;

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMException;
import org.apache.axiom.om.OMNode;
import org.apache.axiom.om.impl.builder.StAXOMBuilder;
import org.apache.axiom.om.impl.llom.OMDocumentImpl;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.SynapseException;
import org.apache.synapse.registry.AbstractRegistry;
import org.apache.synapse.registry.RegistryEntry;
import org.apache.synapse.util.SynapseBinaryDataSource;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.activation.DataHandler;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import static org.wso2.micro.integrator.registry.MicroIntegratorRegistryConstants.CHILD_FILES_LIST_KEY;
import static org.wso2.micro.integrator.registry.MicroIntegratorRegistryConstants.COLLECTION_PROPERTY_EXTENTION;
import static org.wso2.micro.integrator.registry.MicroIntegratorRegistryConstants.CONFIGURATION_REGISTRY_PATH;
import static org.wso2.micro.integrator.registry.MicroIntegratorRegistryConstants.CONFIG_DIRECTORY_NAME;
import static org.wso2.micro.integrator.registry.MicroIntegratorRegistryConstants.CONFIG_REGISTRY_PREFIX;
import static org.wso2.micro.integrator.registry.MicroIntegratorRegistryConstants.DEFAULT_MEDIA_TYPE;
import static org.wso2.micro.integrator.registry.MicroIntegratorRegistryConstants.ERROR_KEY;
import static org.wso2.micro.integrator.registry.MicroIntegratorRegistryConstants.FILE_PROTOCOL_PREFIX;
import static org.wso2.micro.integrator.registry.MicroIntegratorRegistryConstants.FILE_TYPE_DIRECTORY;
import static org.wso2.micro.integrator.registry.MicroIntegratorRegistryConstants.GOVERNANCE_DIRECTORY_NAME;
import static org.wso2.micro.integrator.registry.MicroIntegratorRegistryConstants.GOVERNANCE_REGISTRY_PATH;
import static org.wso2.micro.integrator.registry.MicroIntegratorRegistryConstants.GOVERNANCE_REGISTRY_PREFIX;
import static org.wso2.micro.integrator.registry.MicroIntegratorRegistryConstants.HIDDEN_FILE_PREFIX;
import static org.wso2.micro.integrator.registry.MicroIntegratorRegistryConstants.LOCAL_DIRECTORY_NAME;
import static org.wso2.micro.integrator.registry.MicroIntegratorRegistryConstants.LOCAL_REGISTRY_PATH;
import static org.wso2.micro.integrator.registry.MicroIntegratorRegistryConstants.LOCAL_REGISTRY_PREFIX;
import static org.wso2.micro.integrator.registry.MicroIntegratorRegistryConstants.LIST;
import static org.wso2.micro.integrator.registry.MicroIntegratorRegistryConstants.NAME_KEY;
import static org.wso2.micro.integrator.registry.MicroIntegratorRegistryConstants.PROPERTIES_KEY;
import static org.wso2.micro.integrator.registry.MicroIntegratorRegistryConstants.PROPERTY_EXTENTION;
import static org.wso2.micro.integrator.registry.MicroIntegratorRegistryConstants.PROPERTY_FILE_VALUE;
import static org.wso2.micro.integrator.registry.MicroIntegratorRegistryConstants.TYPE_KEY;
import static org.wso2.micro.integrator.registry.MicroIntegratorRegistryConstants.URL_SEPARATOR;
import static org.wso2.micro.integrator.registry.MicroIntegratorRegistryConstants.VALUE_KEY;
import static org.wso2.micro.integrator.registry.MicroIntegratorRegistryConstants.REG_DEP_FAILURE_IDENTIFIER;

public class MicroIntegratorRegistry extends AbstractRegistry {

    private static final Log log = LogFactory.getLog(MicroIntegratorRegistry.class);

    private static final int DELETE_RETRY_SLEEP_TIME = 10;
    private static final long DEFAULT_CACHABLE_DURATION = 0;
    private static final int MAX_KEYS = 200;

    private static final String METADATA_DIR_NAME = ".metadata";
    private static final String METADATA_FILE_SUFFIX = ".meta";
    private static final String METADATA_KEY_MEDIA_TYPE = "mediaType";

    private static final String NEW_LINE_CHAR = System.getProperty("line.separator");

    private static final int FILE = 1;
    private static final int HTTP = 2;
    private static final int HTTPS = 3;

    /**
     * File system path corresponding to the FILE url path. This is a system depending path
     * used for accessing resources as files.
     */
    private String localRegistry;
    private String configRegistry;
    private String govRegistry;
    private String regRoot;
    /**
     * Specifies whether the registry is in the local host or a remote registry.
     * Local host means the same computer as ESB is running.
     */
    private int registryType = MicroIntegratorRegistryConstants.LOCAL_HOST_REGISTRY;

    /**
     * Contains the protocol for the registry. Allowd values are FILE, HTTP and HTTPS.
     */
    private int registryProtocol = FILE;

    private static Map<String, Long> resourceLastModifiedMap = new HashMap<String, Long>();

    public MicroIntegratorRegistry() {
        //default registry is file system based resided in carbon home
        String defaultFSRegRoot = RegistryHelper.getHome().replace(File.separator, URL_SEPARATOR);
        if (!defaultFSRegRoot.endsWith(URL_SEPARATOR)) {
            defaultFSRegRoot = defaultFSRegRoot + URL_SEPARATOR;
        }
        //Default registry root : <CARBON_HOME>/registry/
        defaultFSRegRoot += "registry" + URL_SEPARATOR;

        //create default file system paths for registry
        //Default registry local registry location : <CARBON_HOME>/registry/local
        this.localRegistry = getUri(defaultFSRegRoot, "local");
        //Default registry config registry location : <CARBON_HOME>/registry/config
        this.configRegistry = getUri(defaultFSRegRoot, "config");
        //Default registry governance registry location : <CARBON_HOME>/registry/governance
        this.govRegistry = getUri(defaultFSRegRoot, "governance");

        this.regRoot = defaultFSRegRoot;

        initRegistryListener(defaultFSRegRoot);
    }

    private void initRegistryListener(String regRoot) {
        try {
            MicroIntegratorRegistryListener watcher = new MicroIntegratorRegistryListener(Paths.get(regRoot));
            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.submit(watcher);
            executor.shutdown();
        } catch (IOException e) {
            String msg = "Error while initiating registry resource listener task";
            log.error(msg, e);
            throw new SynapseException(msg, e);
        }
    }

    public static Long getResourceLastModifiedEntry(String path) {
        return resourceLastModifiedMap.get(path);
    }

    public static void addResourceLastModifiedEntry(String path, Long version) {
        synchronized (resourceLastModifiedMap) {
            resourceLastModifiedMap.put(path, version);
        }
    }

    public static void deleteResourceLastModifiedEntry(String path) {
        synchronized (resourceLastModifiedMap) {
            resourceLastModifiedMap.remove(path);
        }
    }

    /**
     * Initializing the repository which requires to store the secure vault
     * cipher text
     */
    private void initSecurityRepo() {
		 //	Here, the secure vault resource is created, if it does not exist in the registry.
		if (!isResourceExists(MicroIntegratorRegistryConstants.CONNECTOR_SECURE_VAULT_CONFIG_REPOSITORY)) {
			newResource(MicroIntegratorRegistryConstants.CONNECTOR_SECURE_VAULT_CONFIG_REPOSITORY, true);
		}
    }

    /**
     * Returns true if the given registry path is write-protected against CAPP deployment.
     * Protection is only enforced when {@code vault.registry.lookup.enabled=true}
     * in synapse.properties, so that the constraint is inactive in the default deployment.
     */
    public static boolean isWriteProtected(String path) {
        if (!org.apache.synapse.config.SynapsePropertiesLoader.getBooleanProperty(
                MicroIntegratorRegistryConstants.PROP_VAULT_REGISTRY_LOOKUP_ENABLED, false)) {
            return false;
        }
        return MicroIntegratorRegistryConstants.CONNECTOR_SECURE_VAULT_CONFIG_REPOSITORY.equals(path);
    }

    private String getUri(String defaultFSRegRoot, String subDirectory) {
        return Paths.get(defaultFSRegRoot + subDirectory).toUri().normalize().toString()
                + MicroIntegratorRegistryConstants.URL_SEPARATOR;
    }

    @Override
    public void init(Properties properties) {

        super.init(properties);
        for (Object o : properties.keySet()) {
            if (o != null) {
                String name = (String) o;
                String value = (String) properties.get(name);
                addConfigProperty(name, value);
            }
        }
        System.setProperty("mi.registry.gov", govRegistry);
        log.debug("MI lightweight registry is initialized.");

        initSecurityRepo();
    }

    @Override
    public OMNode lookup(String key) {

        if (log.isDebugEnabled()) {
            log.debug("==> Repository fetch of resource with key : " + key);
        }

        String resolvedRegKeyPath = resolveRegistryURI(key);
        URLConnection urlConnection;
        URL url = null;
        try {
            url = new URL(resolvedRegKeyPath);
        } catch (MalformedURLException e) {
            handleException("Invalid path '" + resolvedRegKeyPath + "' for URL", e);
        }

        if (lookupUtil(key, url)) {
            return null;
        }

        try {
            urlConnection = url.openConnection();
            urlConnection.connect();
        } catch (IOException e) {
            return null;
        }

        InputStream input = null;
        try {
            input = urlConnection.getInputStream();
        } catch (IOException e) {
            handleException("Error when getting a stream from the URL", e);
        }

        if (input == null) {
            return null;
        }

        BufferedInputStream inputStream = new BufferedInputStream(input);
        OMNode result = null;
        try {
            XMLStreamReader parser = XMLInputFactory.newInstance().
                    createXMLStreamReader(inputStream);
            StAXOMBuilder builder = new StAXOMBuilder(parser);
            result = builder.getDocumentElement();

            if (result != null && result.getParent() != null) {
                result.detach();
                OMDocumentImpl parent = new OMDocumentImpl(OMAbstractFactory.getOMFactory());
                parent.addChild(result);
            }

        } catch (OMException | XMLStreamException ignored) {
            result = treatContentAsNonXmlOnXmlError(url);
        } catch (RuntimeException runtimeException) {
            // Woodstox Exception Handling:
            // The WstxLazyException is often wrapped by internal layers and its ClassLoader
            // is isolated from this component's ClassLoader. Here, we check the message
            // to safely identify this specific, anticipated XML parsing failure.
            if (runtimeException.getMessage() != null
                    && runtimeException.getMessage().contains("com.ctc.wstx.exc.WstxLazyException")) {
                result = treatContentAsNonXmlOnXmlError(url);
            } else {
                throw runtimeException;
            }
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                log.error("Error in closing the input stream.", e);
            }

        }
        return result;
    }

    private OMNode treatContentAsNonXmlOnXmlError(URL url) {
        if (log.isDebugEnabled()) {
            log.debug("The resource at the provided URL isn't well-formed XML. So, takes it as a text");
        }

        OMNode result;
        try {
            result = readNonXML(url);
        } catch (IOException e) {
            log.error("Error occurred while retrieving text content from registry artifact", e);
            result = null;
        }

        if (result != null && result.getParent() != null) {
            result.detach();
            OMDocumentImpl parent = new OMDocumentImpl(OMAbstractFactory.getOMFactory());
            parent.addChild(result);
        }

        return result;
    }

    private boolean lookupUtil(String key, URL url) {
        if (url == null) {
            handleException("Unable to create URL for target resource : " + key);
        }

        if ("file".equals(url.getProtocol())) {
            try {
                if (new File(url.toURI()).exists()) {
                    try (InputStream inputStream = url.openStream()) {
                        // This try-with-resources block is used to check whether the resource is accessible
                    } catch (IOException e) {
                        log.error("Error occurred while accessing registry resource: " + key, e);
                        return true;
                    }
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Requested registry resource does not exist : " + key);
                    }
                    return true;
                }
            } catch (URISyntaxException e) {
                if (log.isDebugEnabled()) {
                    log.debug("Error occurred while accessing registry resource: " + key, e);
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isResourceExists(String key) {
        String resolvedRegKeyPath = resolveRegistryURI(key);
        try {
            // here, a URL object is created in order to remove the protocol from the file path
            File file = new File(new URL(resolvedRegKeyPath).getFile());
            return file.exists();
        } catch (MalformedURLException e) {
            log.error("Error in fetching resource: " + key, e);
            return false;
        }
    }

    /**
     * The micro integrator expects the properties of a directory to be available inside the given directory as a
     * property file. For an example, if a directory key, conf:/foo/bar is passed as the key, the micro integrator
     * registry expects the properties to be available in the file, conf:/foo/bar/bar.properties. For a file,
     * conf:/foo/bar/example.xml, the properties need to be given in the file, conf:/foo/bar/example.xml.properties
     *
     * @param key the path to the directory
     * @return the properties defined
     */
    private Properties lookupProperties(String key) {
        if (log.isDebugEnabled()) {
            log.debug("==> Repository fetch of resource with key : " + key);
        }
        String resolvedRegKeyPath = resolveRegistryURI(key);
        Properties result = new Properties();

        URL url = null;
        // get the path to the relevant property file
        try {
            resolvedRegKeyPath = getPropertyFileURI(resolvedRegKeyPath);
            url = new URL(resolvedRegKeyPath);
        } catch (MalformedURLException e) {
            handleException("Invalid path '" + resolvedRegKeyPath + "' for URL", e);
        }

        if (lookupUtil(key, url)) {
            return null;
        }

        try {
            URLConnection urlConnection = url.openConnection();
            urlConnection.connect();
            try (InputStream input = urlConnection.getInputStream()) {
                if (input == null) {
                    return null;
                }
                result.load(input);
            }
        } catch (IOException e) {
            log.error("Error in loading properties", e);
            return null;
        }
        return result;
    }

    /**
     * This methods append the properties file to the resource URL
     *
     * @param originalURL the path to the resource
     * @return URL of the relevant property file
     */
    private String getPropertyFileURI(String originalURL) throws MalformedURLException {

        originalURL = originalURL.trim();
        // here, a URL object is created in order to remove the protocol from the file path
        boolean isDirectory = new File(new URL(originalURL).getFile()).isDirectory();
        if (originalURL.endsWith(URL_SEPARATOR)) {
            originalURL = originalURL.substring(0, originalURL.length() - 1);
        }
        if (isDirectory) {
            return originalURL + COLLECTION_PROPERTY_EXTENTION;
        }
        return originalURL + PROPERTY_EXTENTION;
    }

    @Override
    public RegistryEntry getRegistryEntry(String key) {

        // get information from the actual resource
        MediationRegistryEntryImpl entryEmbedded = new MediationRegistryEntryImpl();

        try {
            URL url = new URL(resolveRegistryURI(key));
            if ("file".equals(url.getProtocol())) {
                try (InputStream inputStream = url.openStream()) {
                    // This try-with-resources block is used to check whether the resource is accessible
                } catch (IOException e) {
                    log.error("Error occurred while accessing registry resource: " + key, e);
                    return null;
                }
            }
            URLConnection urlc = url.openConnection();

            entryEmbedded.setKey(key);
            entryEmbedded.setName(url.getFile());
            entryEmbedded.setType(MicroIntegratorRegistryConstants.FILE);
            entryEmbedded.setDescription("Resource at : " + url.toString());
            entryEmbedded.setLastModified(urlc.getLastModified());
            entryEmbedded.setVersion(getLastModifiedTimestamp(resolveRegistryURI(key), urlc));

            if (urlc.getExpiration() > 0) {
                entryEmbedded.setCachableDuration(
                        urlc.getExpiration() - System.currentTimeMillis());
            } else {
                entryEmbedded.setCachableDuration(getCachableDuration());
            }

        } catch (MalformedURLException e) {
            handleException("Invalid URL reference " + resolveRegistryURI(key), e);
        } catch (IOException e) {
            handleException("IO Error reading from URL " + resolveRegistryURI(key), e);
        }

        return entryEmbedded;
    }

    private long getLastModifiedTimestamp(String filePath, URLConnection urlc) {
        Long lastModifiedFromMap = getResourceLastModifiedEntry(filePath);
        Long urlLastModified = urlc.getLastModified(); // Get the last modified timestamp from urlc
        Long timestamp;
        if (lastModifiedFromMap != null && lastModifiedFromMap > urlLastModified) {
            timestamp = lastModifiedFromMap;
        } else {
            timestamp = urlLastModified;
        }

        Long lastModifiedPropertiesFromMap = getResourceLastModifiedEntry(filePath + ".properties");
        if (lastModifiedPropertiesFromMap != null && lastModifiedPropertiesFromMap > timestamp) {
            timestamp = lastModifiedPropertiesFromMap;
        }
        return timestamp;
    }

    @Override
    public OMNode lookupFormat(String key) {
        //TODO verify this
        return lookup(key);
    }

    @Override
    public RegistryEntry[] getChildren(RegistryEntry entry) {

        if (entry == null) {
            // give the children of the root
            // null or key = "" stands for root of local registry

            MediationRegistryEntryImpl registryEntry = new MediationRegistryEntryImpl();
            registryEntry.setKey(MicroIntegratorRegistryConstants.LOCAL_REGISTRY_PREFIX + "/");
            entry = registryEntry;
        }

        String resourcePath = resolveRegistryURI(entry.getKey());
        String resourceRootEntry = entry.getKey();

        if (registryType == MicroIntegratorRegistryConstants.LOCAL_HOST_REGISTRY) {

            // registry is in the local FILE system. access it directly.
            File file = null;
            try {
                file = new File(new URI(resourcePath));
            } catch (URISyntaxException e) {
                handleException(e.getMessage(), e);
            }

            if (file == null || !file.isDirectory()) {
                return null;
            }

            if (!resourceRootEntry.endsWith(URL_SEPARATOR)) {
                resourceRootEntry += URL_SEPARATOR;
            }

            String[] children = file.list();
            if (children == null) {
                // The directory was removed or became unreadable after the isDirectory() check above.
                if (log.isDebugEnabled()) {
                    log.debug("Unable to list the children of " + file.getPath()
                        + ". It may have been removed concurrently.");
                }
                return null;
            }
            RegistryEntry[] entries = new RegistryEntry[children.length];

            for (int i = 0; i < children.length; i++) {
                MediationRegistryEntryImpl registryEntry = new MediationRegistryEntryImpl();

                //Set registry entry key
                registryEntry.setKey(resourceRootEntry + children[i]);

                // set if the registry entry is a FILE or a FOLDER
                try {
                    File entryFile = new File(new URI(resourcePath + URL_SEPARATOR + children[i]));
                    if (entryFile.isDirectory()) {
                        registryEntry.setType(MicroIntegratorRegistryConstants.FOLDER);
                    } else {
                        registryEntry.setType(MicroIntegratorRegistryConstants.FILE);
                    }
                    entries[i] = registryEntry;
                } catch (URISyntaxException e) {
                    handleException("Error occurred while checking file type due to :" + e.getMessage(), e);
                }

            }
            return entries;

        } else if (registryType == MicroIntegratorRegistryConstants.REMOTE_HOST_REGISTRY) {
            // TODO : implement for remote registries.
            log.warn("Remote registry functionality not implemented yet");
        }

        return null;
    }

    @Override
    public RegistryEntry[] getDescendants(RegistryEntry entry) {

        ArrayList<RegistryEntry> list = new ArrayList<RegistryEntry>();

        fillDescendants(entry, list);

        RegistryEntry[] descendants = new RegistryEntry[list.size()];
        for (int i = 0; i < list.size(); i++) {
            descendants[i] = list.get(i);
        }

        return descendants;
    }

    @Override
    public void delete(String path) {
        if (registryType == MicroIntegratorRegistryConstants.LOCAL_HOST_REGISTRY) {
            try {
                String targetPath = resolveRegistryURI(path);
                File parentFile = new File(new URI(getParentPath(targetPath, false)));
                String fileName = getResourceName(targetPath);
                removeResource(path);
                deleteMetadata(parentFile, fileName);
                if (RegistryHelper.isDirectoryEmpty(parentFile.getPath()) && !CONFIG_DIRECTORY_NAME.equals
                        (parentFile.getName()) && !GOVERNANCE_DIRECTORY_NAME.equals(parentFile.getName())) {
                    deleteDirectory(parentFile);
                }
            } catch (URISyntaxException e) {
                handleException("Error while deleting the registry path " + path, e);
            }
        } else {
            // Warn the user that unable to delete remote registry resources
            log.warn("Deleting remote resources NOT SUPPORTED. Unable to delete: " + path);
        }
    }

    @Override
    public void newResource(String path, boolean isDirectory) {
        if (registryType == MicroIntegratorRegistryConstants.LOCAL_HOST_REGISTRY) {
            String resolvedPath = resolveRegistryURI(path);

            if (isDirectory && !resolvedPath.endsWith(URL_SEPARATOR)) {
                resolvedPath += URL_SEPARATOR;
            }

            String parent = getParentPath(resolvedPath, isDirectory);
            String fileName = getResourceName(resolvedPath);
            try {
                addResource(parent, fileName, !isDirectory);
            } catch (Exception e) {
                handleException(REG_DEP_FAILURE_IDENTIFIER + "Error when adding a new resource", e);
            }
        } else {
            // Warn the user that unable to create resources in remote registry resources
            log.warn("Creating new resources in remote registry is NOT SUPPORTED. Unable to create: " + path);
        }
    }

    @Override
    public void newNonEmptyResource(String path, boolean isDirectory, String mediaType, String content,
                                    String propertyName) {

        Properties properties = null;
        if (StringUtils.isNotEmpty(propertyName)) {
            properties = new Properties();
            properties.setProperty(propertyName, content);
            content = "";
        }
        addNewNonEmptyResource(path, isDirectory, mediaType, content, properties);
    }

    /**
     * Add new resource element to the registry.
     *
     * @param path             registry resource path
     * @param isDirectory      whether the resource is directory or not
     * @param mediaType        media type of the registry resource
     * @param content          content of the registry resource
     * @param properties       properties defined in the registry resource
     */
    public void addNewNonEmptyResource(String path, boolean isDirectory, String mediaType, String content,
                                        Properties properties) {
        if (registryType == MicroIntegratorRegistryConstants.LOCAL_HOST_REGISTRY) {
            String targetPath = resolveRegistryURI(path);

            if (isDirectory && !targetPath.endsWith(URL_SEPARATOR)) {
                targetPath += URL_SEPARATOR;
            }
            String parent = getParentPath(targetPath, isDirectory);
            try {
                File parentFile = new File(new URI(parent));
                if (isDirectory) {
                    File collection = new File(new URI(targetPath));
                    if (!collection.exists() && !collection.mkdirs()) {
                        handleException("Unable to create collection: " + collection.getPath());
                    }
                    if (properties != null && !properties.isEmpty()) {
                        writeProperties(parentFile, getResourceName(targetPath), properties, true);
                    }
                } else {
                    String fileName = getResourceName(targetPath);
                    Properties metadata = null;
                    if (mediaType != null) {
                        metadata = new Properties();
                        metadata.setProperty(METADATA_KEY_MEDIA_TYPE, mediaType);
                    }
                    writeToFile(parentFile, fileName, content, metadata, properties);
                }
            } catch (Exception e) {
                handleException(REG_DEP_FAILURE_IDENTIFIER + "Error when adding a new resource", e);
            }
        } else {
            log.warn("Creating new resource in remote registry is NOT SUPPORTED. Unable to create: " + path);
        }
    }

    /**
     * Updates the registry resource pointed by the given key.
     *
     * @param path   Key of the resource to be updated
     * @param value New value of the resource
     */
    @Override
    public void updateResource(String path, Object value) {
        if (registryType == MicroIntegratorRegistryConstants.LOCAL_HOST_REGISTRY) {
            try {
                File file = new File(new URI(resolveRegistryURI(path)));
                if (file.exists()) {
                    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
                        if (value != null) {
                            writer.write(value.toString());
                        } else {
                            log.warn("Updating the registry location : " + path + " with empty content");
                            writer.write("");
                        }
                        writer.flush();
                    } catch (IOException e) {
                        handleException("Couldn't write to registry entry: " + path, e);
                    }
                }
            } catch (URISyntaxException e) {
                handleException("Error occurred while updating resource: " + path, e);
            }
        } else {
            log.warn("Updating remote registry is NOT SUPPORTED. Unable to update: " + path);
        }
    }


    @Override
    public void updateRegistryEntry(RegistryEntry entry) {
        //Nothing to do here
    }


    private void handleException(String msg, Exception e) {
        log.error(msg, e);
        throw new SynapseException(msg, e);
    }

    private void handleException(String msg) {
        log.error(msg);
        throw new SynapseException(msg);
    }

    private long getCachableDuration() {
        String cachableDuration = (String) properties.get("cachableDuration");
        return cachableDuration == null ? DEFAULT_CACHABLE_DURATION : Long.parseLong(cachableDuration);
    }

    private void fillDescendants(RegistryEntry parent, ArrayList<RegistryEntry> list) {

        RegistryEntry[] entries = getChildren(parent);
        if (entries != null) {
            for (RegistryEntry entry : entries) {

                if (list.size() > MAX_KEYS) {
                    break;
                }

                fillDescendants(entry, list);
            }
        } else {
            list.add(parent);
        }
    }

    /**
     * Removes the local file system based registry resource identified by the given key.
     * If the key points to a directory, all its subdirectories and files in those directories will be deleted.
     *
     * @param key resource key
     */
    private void removeResource(String key) {
        try {
            String resourcePath = resolveRegistryURI(key);
            File resource = new File(new URI(resourcePath));
            if (resource.exists()) {
                if (resource.isFile()) {
                    deleteFile(resource);
                    // the properties also need to be removed when removing the resource
                    File resourceProperties = new File(new URI(resourcePath + PROPERTY_EXTENTION));
                    if (resourceProperties.exists()) {
                        deleteFile(resourceProperties);
                    }
                } else if (resource.isDirectory()) {
                    deleteDirectory(resource);
                    // the properties also need to be removed when removing the resource
                    File resourceProperties = new File(new URI(resourcePath + COLLECTION_PROPERTY_EXTENTION));
                    if (resourceProperties.exists()) {
                        deleteFile(resourceProperties);
                    }
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Unable to remove registry resource as " + key + " does not exist.");
                }
            }
        } catch (URISyntaxException e) {
            handleException("Error occurred due to invalid URI while removing resource: " + key, e);
        }

    }

    private void deleteFile(File file) {

        boolean success = file.delete();
        if (!success) {
            // try with this work around to overcome a known bug in windows
            // work around:
            // run garbage collector and sleep for some time and delete
            // if still didn't delete,
            // rename FILE to a temp FILE in "temp" dir and mark it to delete on exist

            System.gc();
            try {
                Thread.sleep(DELETE_RETRY_SLEEP_TIME);
            } catch (InterruptedException e) {
                // ignore the exception
                log.error("Sleep wait interrupted while waiting for second retry to delete registry resource", e);
            }

            success = file.delete();
            if (!success) {
                int suffix = 1;
                File renamedFile;

                File tempDir = new File("temp");
                if (!tempDir.exists()) {
                    tempDir.mkdir();
                }

                do {
                    String changedName = "d" + suffix + file.getName();
                    renamedFile = new File(tempDir, changedName);
                    suffix++;
                } while (renamedFile.exists());

                if (file.renameTo(renamedFile)) {
                    renamedFile.deleteOnExit();
                } else {
                    if (file.exists()) {
                        log.warn("Seems that the file still exists but unable to delete the resource: "
                                + file.getName() + "could be due to another process is trying to delete the file");
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("Unable to delete the resource as " +
                                    file.getName() + " does not exist.");
                        }
                    }
                }
            }
        }
    }

    /**
     * Recursively deletes the given directory.
     * <p>
     * Deletion is treated as a convergent operation: the desired end state is that the directory is
     * absent. The registry is not guarded against concurrent modification, so the same directory
     * can be removed underneath us while we are deleting it. In a cluster this happens when the
     * same CApp is hot-undeployed on several nodes that share the registry location, but it can
     * equally be another thread of this node or an external process. In all of those cases the
     * directory being already gone is a success, not a failure, so this method does not throw.
     * Genuine failures (permission issues, IO errors, content being re-created underneath us)
     * are logged so that they remain visible.
     *
     * @param dir directory to delete
     */
    private void deleteDirectory(File dir) {

        File[] children = dir.listFiles();
        if (children == null) {
            // listFiles() returns null when the directory no longer exists, is not a directory,
            // or could not be read.
            if (!dir.exists()) {
                if (log.isDebugEnabled()) {
                    log.debug("Unable to delete the resource as " + dir.getPath()
                        + " does not exist. It may have already been deleted concurrently.");
                }
                return;
            }
            log.warn("Unable to list the contents of the registry directory: " + dir.getPath()
                + ". Skipping the deletion of the directory.");
            return;
        }

        for (File child : children) {
            if (child != null) {
                if (child.isFile()) {
                    deleteFile(child);
                } else if (child.isDirectory()) {
                    deleteDirectory(child);
                }
            }
        }

        boolean success = dir.delete();
        if (!success) {
            if (!dir.exists()) {
                // The directory was removed in the meantime. The desired end state is already reached.
                if (log.isDebugEnabled()) {
                    log.debug("The registry directory " + dir.getPath()
                        + " has already been deleted concurrently while it was being removed.");
                }
            } else {
                log.error("Unable to delete the resource: " + dir.getPath()
                    + ". The directory still exists, it could be due to another process "
                    + "holding or re-creating its content.");
            }
        }
    }

    /**
     * @param resourcePath If the resource is a directory it must end with URL_SEPARATOR
     * @param isDirectory
     * @return
     */
    private String getParentPath(String resourcePath, boolean isDirectory) {
        if (resourcePath != null) {
            String tempPath = resourcePath;
            if (isDirectory) {
                tempPath = resourcePath.substring(0, resourcePath.lastIndexOf(URL_SEPARATOR));
            }

            return tempPath.substring(0, tempPath.lastIndexOf(URL_SEPARATOR));
        }
        return "";
    }

    private String getResourceName(String path) {
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
     * Adds a new resource to the registry.
     *
     * @param parentName   Key of the parent of the new resource
     * @param resourceName Name of the new resource
     * @param isLeaf       Specifies whether the new resource is a leaf or not. In a FILE system based
     *                     registry, leaf is a FILE and non-leaf is a FOLDER.
     * @throws Exception if an error occurs while creating the resources
     */
    private void addResource(String parentName, String resourceName, boolean isLeaf) throws Exception {

        if (isLeaf) {
            createFile(new URI(parentName), resourceName);
        } else {
            createFolder(new URI(parentName), resourceName);
        }

    }

    private void createFile(URI parentName, String newFileName) throws Exception {
        /*
            search for parent. if found, create the new FILE in it
        */
        File parent = new File(parentName);
        if (!parent.exists() && !parent.mkdirs()) {
            handleException("Unable to create parent directory: " + parentName);
        }
        File newFile = new File(parent, newFileName);
        if (!newFile.exists() && !newFile.createNewFile()) {
            handleException("Couldn't create resource: " + newFileName);
        }
    }

    /**
     * Function to write to file, create if not exists including directory structure.
     *
     * @param parent             parent file
     * @param newFileName        new file name to be created
     * @param content            content to be included in the new file
     * @param metadata           meta data of the new file
     * @param resourceProperties resource properties to be added to the new resource
     */
    private void writeToFile(File parent, String newFileName, String content, Properties metadata, Properties resourceProperties) {
        /*
            search for parent. if found, create the new FILE in it
        */
        if (!parent.exists() && !parent.mkdirs()) {
            handleException("Unable to create parent directory: " + parent.getPath());
        }
        File newFile = new File(parent, newFileName);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(newFile))) {
            writer.write(content);
            writer.flush();
            if (metadata != null) {
                writeMetadata(parent, newFileName, metadata);
            }
            if (resourceProperties != null) {
                writeProperties(parent, newFileName, resourceProperties, false);
            }
            if (log.isDebugEnabled()) {
                log.debug("Successfully content written to file : " + parent.getPath() + URL_SEPARATOR + newFileName);
            }
        } catch (IOException e) {
            handleException(REG_DEP_FAILURE_IDENTIFIER + "Couldn't write to registry resource: " +
                    parent.getPath() + URL_SEPARATOR + newFileName, e);
        }
    }

    /**
     * Writes metadata related to the given resource.
     *
     * @param parent           parent dir of the resource
     * @param resourceFileName filename of the resource
     * @param metadata         metadata properties object
     */
    private void writeMetadata(File parent, String resourceFileName, Properties metadata) {

        File metadataDir = new File(parent, METADATA_DIR_NAME);
        if (!metadataDir.exists() && !metadataDir.mkdirs()) {
            handleException("Unable to create metadata directory: " + metadataDir.getPath());
        }
        File newMetadataFile = new File(metadataDir, resourceFileName + METADATA_FILE_SUFFIX);
        try (BufferedWriter metadataWriter = new BufferedWriter(new FileWriter(newMetadataFile))) {
            metadata.store(metadataWriter, null);
            if (log.isDebugEnabled()) {
                log.debug("Successfully written metadata to file: " + newMetadataFile.getPath());
            }
        } catch (IOException e) {
            handleException("Couldn't write to metadata file: " + newMetadataFile.getPath(), e);
        }
    }

    /**
     * Create a new properties file for the registry resource.
     *
     * @param parent            destination location of the properties file
     * @param resourceFileName  name of the registry resource
     * @param properties        list of properties
     * @param isCollection      whether the resource is a collection or not
     */
    private void writeProperties(File parent, String resourceFileName, Properties properties, boolean isCollection) {

        File resourcePropertiesFile;
        if (isCollection) {
            resourcePropertiesFile = new File(parent, resourceFileName + COLLECTION_PROPERTY_EXTENTION);
        } else {
            resourcePropertiesFile = new File(parent, resourceFileName + PROPERTY_EXTENTION);
        }

        try (BufferedWriter propertiesWriter = new BufferedWriter(new FileWriter(resourcePropertiesFile))) {
            properties.store(propertiesWriter, null);
            if (log.isDebugEnabled()) {
                log.debug("Successfully written resource properties to file: " + resourcePropertiesFile.getPath());
            }
        } catch (IOException e) {
            handleException("Couldn't write to resource properties file: " + resourcePropertiesFile.getPath(), e);
        }
    }

    /**
     * Delete metadata related to the given resource.
     *
     * @param parent           parent dir of the resource
     * @param resourceFileName filename of the resource
     */
    private void deleteMetadata(File parent, String resourceFileName) {

        String metadataDirPath = parent.getPath() + File.separator + METADATA_DIR_NAME;
        File metadataDir = new File(metadataDirPath);
        if (!metadataDir.exists()) {
            return;
        }
        File metadataFile = new File(metadataDir, resourceFileName + METADATA_FILE_SUFFIX);
        deleteFile(metadataFile);
        if (RegistryHelper.isDirectoryEmpty(metadataDirPath)) {
            deleteDirectory(metadataDir);
        }
        if (log.isDebugEnabled()) {
            log.debug("Successfully deleted metadata file: " + metadataFile.getPath());
        }
    }

    private void createFolder(URI parentName, String newFolderName) throws Exception {
        /*
            search for parent. if found, create the new FOLDER in it.
        */
        File parent = new File(parentName);
        if (parent.exists() || parent.mkdirs()) {
            File newEntry = new File(parent, newFolderName);
            if (!newEntry.exists()) { // create folder if it doesn't exists only.
                if (!newEntry.mkdir()) {
                    handleException("Couldn't create folder: " + newFolderName);
                }
            }
        } else {
            handleException("Parent folder: " + parentName + " cannot be created.");
        }
    }

    /**
     * Function to resolve the registry key and generate absolute URI of the registry resource
     *
     * @param key
     * @return
     */
    private String resolveRegistryURI(String key) {
        String resolvedPath = null;

        if (key != null || !key.isEmpty()) {
            String registryRoot = "";
            String resourcePath = "";
            if (key.startsWith(CONFIG_REGISTRY_PREFIX)) {
                registryRoot = configRegistry;
                resourcePath = key.substring(CONFIG_REGISTRY_PREFIX.length());

            } else if (key.startsWith(GOVERNANCE_REGISTRY_PREFIX)) {
                registryRoot = govRegistry;
                resourcePath = key.substring(GOVERNANCE_REGISTRY_PREFIX.length());

            } else if (key.startsWith(MicroIntegratorRegistryConstants.LOCAL_REGISTRY_PREFIX)) {
                registryRoot = localRegistry;
                resourcePath = key.substring(MicroIntegratorRegistryConstants.LOCAL_REGISTRY_PREFIX.length());

            } else if (key.startsWith(MicroIntegratorRegistryConstants.RESOURCES_PREFIX)) {
                registryRoot = govRegistry;
                resourcePath = MicroIntegratorRegistryConstants.MI_RESOURCES_DIRECTORY_NAME + URL_SEPARATOR +
                        key.substring(MicroIntegratorRegistryConstants.RESOURCES_PREFIX.length());
            } else {
                registryRoot = govRegistry;
                resourcePath = key;
            }

            if (resourcePath.startsWith(URL_SEPARATOR)) {
                resourcePath = resourcePath.substring(1);
            }
            resolvedPath = registryRoot + resourcePath;

            //Test whether registry key has any illegel access
            File resolvedPathFile = null;
            File registryRootFile = null;
            try {
                resolvedPathFile = new File(new URI(resolvedPath));
                registryRootFile = new File(new URI(registryRoot));
                if (!resolvedPathFile.getCanonicalPath().startsWith(registryRootFile.getCanonicalPath())) {
                    handleException("The registry key  '" + key +
                            "' is illegal which points to a location outside the registry");
                }
            } catch (URISyntaxException | IOException e) {
                handleException("Error while resolving the canonical path of the registry key : " + key, e);
            }
        }
        return resolvedPath;
    }

    /**
     * Function to retrieve resource content as text
     *
     * @param url
     * @return
     * @throws IOException
     */
    private OMNode readNonXML (URL url) throws IOException {

        URLConnection urlConnection = url.openConnection();
        urlConnection.connect();

        try (InputStream inputStream = urlConnection.getInputStream()) {

            if (inputStream == null) {
                return null;
            }

            String mediaType = DEFAULT_MEDIA_TYPE;
            Properties metadata = getMetadata(url.getPath());
            if (metadata != null) {
                String mediaTypeValue = metadata.getProperty(METADATA_KEY_MEDIA_TYPE);
                if (StringUtils.isNotEmpty(mediaTypeValue)) {
                    mediaType = mediaTypeValue;
                }
            }

            if (DEFAULT_MEDIA_TYPE.equals(mediaType)) {
                StringBuilder strBuilder = new StringBuilder();
                try (BufferedReader bReader = new BufferedReader(new InputStreamReader(inputStream))) {
                    String line;
                    while ((line = bReader.readLine()) != null) {
                        strBuilder.append(line);
                        strBuilder.append(NEW_LINE_CHAR);
                    }
                    // need to remove new_line_charater from the last line, for single line texts
                    int length = strBuilder.length();
                    if (length != 0) {
                        strBuilder.setLength(length - NEW_LINE_CHAR.length());
                    }
                }
                return OMAbstractFactory.getOMFactory().createOMText(strBuilder.toString());
            } else {
                return OMAbstractFactory.getOMFactory()
                        .createOMText(new DataHandler(new SynapseBinaryDataSource(inputStream, mediaType)), true);
            }
        }
    }

    /**
     * Configure the ESB registry using registry parameters.
     * <p/>
     * root: FILE:directory -   registry is on local host
     * directory is used to access metadata
     * <p/>
     * root: http/https:location -  has to specify one of the following settings
     * localRegistry - location of the local registry
     * metadataService - url of the service to access metadata
     * If none of above parameters are given "registry" FOLDER is taken as the local registry.
     *
     * @param name name of the config
     * @param value value of the config
     */
    private void addConfigProperty(String name, String value) {

        if (name != null && value != null) {
            if (log.isDebugEnabled()) {
                log.debug("Processing registry configuration property : [Name: " + name + " Value: "+ value + "]");
            }

            if (name.equals(MicroIntegratorRegistryConstants.REG_ROOT)) {
                try {
                    URL rootPathUrl = new URL(value);
                    if (MicroIntegratorRegistryConstants.PROTOCOL_FILE.equals(rootPathUrl.getProtocol())) {
                        registryProtocol = FILE;
                        registryType = MicroIntegratorRegistryConstants.LOCAL_HOST_REGISTRY;

                        //Check existence of the target location
                        try (InputStream inputStream = rootPathUrl.openStream()) {
                            // This try-with-resources block is used to check whether the resource is accessible
                        } catch (IOException e) {
                            // If the registry is filesystem based, user may have provided the URI relative to the CARBON_HOME
                            if (log.isDebugEnabled()) {
                                log.debug("Configured registry path does not exists. Hence check existence " +
                                        "relative to CARBON_HOME");
                            }
                            String pathFromCarbonHome = RegistryHelper.getHome();
                            if (!pathFromCarbonHome.endsWith(URL_SEPARATOR)) {
                                pathFromCarbonHome += URL_SEPARATOR;
                            }
                            pathFromCarbonHome = rootPathUrl.getProtocol() + ":" + pathFromCarbonHome + value;
                            rootPathUrl = new URL(pathFromCarbonHome);
                            try (InputStream inputStream = rootPathUrl.openStream()) {
                                value = pathFromCarbonHome;
                            } catch (IOException e1) {
                                //Unable to open input stream to target location
                                handleException("Unable to open a connection to url : " + rootPathUrl, e1);
                            }
                        }

                        if (!value.endsWith(URL_SEPARATOR)) {
                            value += URL_SEPARATOR;
                        }

                    } else if (MicroIntegratorRegistryConstants.PROTOCOL_HTTP.equals(rootPathUrl.getProtocol())) {
                        registryProtocol = HTTP;
                        registryType = MicroIntegratorRegistryConstants.REMOTE_HOST_REGISTRY;
                        if (!value.endsWith(URL_SEPARATOR)) {
                            value += URL_SEPARATOR;
                        }

                    } else if (MicroIntegratorRegistryConstants.PROTOCOL_HTTPS.equals(rootPathUrl.getProtocol())) {
                        registryProtocol = HTTPS;
                        if (!value.endsWith(URL_SEPARATOR)) {
                            value += URL_SEPARATOR;
                        }

                    }

                    // Set config/gov/local registry properties
                    configRegistry = value + CONFIG_DIRECTORY_NAME + URL_SEPARATOR;
                    if (log.isDebugEnabled()) {
                        log.debug("Configuration Registry Location : " + configRegistry);
                    }
                    govRegistry = value + GOVERNANCE_DIRECTORY_NAME + URL_SEPARATOR;
                    if (log.isDebugEnabled()) {
                        log.debug("Governance Registry Location : " + govRegistry);
                    }
                    localRegistry = value + LOCAL_DIRECTORY_NAME + URL_SEPARATOR;
                    if (log.isDebugEnabled()) {
                        log.debug("Local Registry Location : " + localRegistry);
                    }
                    regRoot = value;

                } catch (MalformedURLException e) {
                    // don't set the root if this is not a valid URL
                    handleException("Registry root should be a valid URL.", e);
                }
            }
        } else {
            log.debug("Name and Value must need");
        }
    }

    public String getRegRoot() {
        if (regRoot.toLowerCase().startsWith(FILE_PROTOCOL_PREFIX)) {
            return regRoot.substring(FILE_PROTOCOL_PREFIX.length()) + ".." + URL_SEPARATOR;
        }
        return regRoot + ".." + URL_SEPARATOR;
    }

    @Override
    public Properties getResourceProperties(String entryKey) {

        Properties properties = new Properties();
        Properties resourceProperties = lookupProperties(entryKey);
        if (resourceProperties != null) {
            for (Object key : resourceProperties.keySet()) {
                Object value = resourceProperties.get(key);
                if (value instanceof List) {
                    if (((List) value).size() > 0) {
                        Object propertyValue = ((List) value).get(0);
                        if (propertyValue != null) {
                            properties.put(key, propertyValue);
                        }
                    }
                } else {
                    properties.put(key, value);
                }
            }
            return properties;
        }
        return null;
    }

    private Properties getMetadata(String fileUrl) {

        Properties metadata = new Properties();
        File file = new File(fileUrl);
        String metadataFilePath =
                file.getParent() + File.separator + METADATA_DIR_NAME + File.separator + file.getName()
                        + METADATA_FILE_SUFFIX;
        File metadataFile = new File(metadataFilePath);

        if (!metadataFile.exists()) {
            if (log.isDebugEnabled()) {
                log.debug("Metadata file does not exist in" + metadataFile.getPath());
            }
            return null;
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(metadataFile))) {
            metadata.load(reader);
        } catch (FileNotFoundException e) {
            log.error("Metadata file cannot be found at " + metadataFile.getPath(), e);
        } catch (IOException e) {
            log.error("Error while reading file " + metadataFile.getPath(), e);
        }

        return metadata;
    }

    /**
     * Returns the registry resource object
     *
     * @param key - registry key.
     * @return
     */
    public Resource getResource(String key) {

        String resolvedRegKeyPath = resolveRegistryURI(key);
        try {
            // here, a URL object is created in order to remove the protocol from the file path
            return new Resource(new File(new URL(resolvedRegKeyPath).getFile()));
        } catch (MalformedURLException e) {
            if (log.isDebugEnabled()) {
                log.debug("Requested registry resource does not exist " + key, e);
            }
            return null;
        }
    }

    /**
     * Returns a JSON object with the folder structure of the <MI-HOME>/registry directory,
     * which contains the files/ leaf level directories matching with the given search key.
     *
     * @param searchKey String
     * @param folderPath Path of the registry
     * @return JSON object containing the folder structure
     */
    public JSONObject getRegistryResourceJSON(String searchKey, String folderPath) {

        File node = new File(folderPath);
        JSONObject jsonObject = new JSONObject();
        addNodesToJSON(searchKey, node, jsonObject);

        JSONObject outputObject = new JSONObject();
        outputObject.put(LIST, jsonObject);
        return outputObject;
    }

    /**
     * Updates the JSON object with existing files and folder which
     * match with the searchKey, in the given registry directory.
     *
     * @param searchKey String
     * @param node       File
     * @param jsonObject JSON object with results
     */
    private void addNodesToJSON(String searchKey, File node, JSONObject jsonObject) {

        String nodeName = node.getName();

        if (node.isDirectory()) {
            JSONArray childArray = new JSONArray();
            String[] childNodes = node.list();
            if (childNodes != null) {
                for (String childNode : childNodes) {
                    if (isNodeNotRequiredToBeFetched(childNode)) {
                        JSONObject nodeJSONObject = new JSONObject();
                        File childFile = new File(node, childNode);
                        addNodesToJSON(searchKey, childFile, nodeJSONObject);
                        if (nodeJSONObject.has(NAME_KEY)) {
                            childArray.put(nodeJSONObject);
                        }
                    } else if (childNode.endsWith(PROPERTY_EXTENTION)) {
                        String propertyOwner = findResourceOfProperty(childNode);
                        if (propertyOwner.toLowerCase().contains(searchKey)) {
                            if (!Arrays.asList(childNodes).contains(propertyOwner)) {
                                JSONObject nodeJSONObject = new JSONObject();
                                nodeJSONObject.put(NAME_KEY, childNode);
                                nodeJSONObject.put(TYPE_KEY, PROPERTY_FILE_VALUE);
                                nodeJSONObject.put(CHILD_FILES_LIST_KEY, Collections.<String>emptyList());
                                childArray.put(nodeJSONObject);
                            }
                        }
                    }
                }
            }
            if (nodeName.toLowerCase().contains(searchKey) || childArray.length() != 0) {
                jsonObject.put(NAME_KEY, nodeName);
                jsonObject.put(TYPE_KEY, FILE_TYPE_DIRECTORY);
                jsonObject.put(CHILD_FILES_LIST_KEY, childArray);
            }

        } else if (nodeName.toLowerCase().contains(searchKey)) {
            String mediaType = DEFAULT_MEDIA_TYPE;
            Properties metadata = getMetadata(formatPath(node.getPath()));
            if (metadata != null) {
                String mediaTypeValue = metadata.getProperty(METADATA_KEY_MEDIA_TYPE);
                if (StringUtils.isNotEmpty(mediaTypeValue)) {
                    mediaType = mediaTypeValue;
                }
            }
            jsonObject.put(NAME_KEY, nodeName);
            jsonObject.put(TYPE_KEY, mediaType);
            jsonObject.put(CHILD_FILES_LIST_KEY, Collections.<String>emptyList());
        }
    }

    /**
     * Returns metadata (media type) of a specified registry.
     *
     * @param path Registry path
     * @return Result json object
     */
    public JSONObject getRegistryMediaType(String path) {

        File file = new File(path);
        String mediaType;
        JSONObject jsonBody = new JSONObject();
        if (file.isDirectory()) {
            jsonBody.put(ERROR_KEY, "Can not fetch metadata for a directory.");
        } else {
            Properties metadata = getMetadata(formatPath(file.getPath()));
            if (metadata != null) {
                String mediaTypeValue = metadata.getProperty(METADATA_KEY_MEDIA_TYPE);
                if (StringUtils.isNotEmpty(mediaTypeValue)) {
                    mediaType = mediaTypeValue;
                    jsonBody.put(NAME_KEY, file.getName());
                    jsonBody.put(METADATA_KEY_MEDIA_TYPE, mediaType);
                }
            } else {
                jsonBody.put(ERROR_KEY, "Error while fetching metadata");
            }
        }
        return jsonBody;
    }

    /**
     * Returns the converted file path with "conf:" and "gov:".
     *
     * @param registryPath   file path to be converted
     * @param carbonHomePath <MI-HOME> path
     * @return converted file path
     */
    private String getChildPath(String registryPath, String carbonHomePath) {
        String resolvedRegKeyPath;
        if (carbonHomePath.endsWith(URL_SEPARATOR)) {
            resolvedRegKeyPath = registryPath.replace(carbonHomePath, "");
        } else {
            resolvedRegKeyPath = registryPath.replace(carbonHomePath + URL_SEPARATOR, "");
        }
        if (resolvedRegKeyPath.startsWith(CONFIGURATION_REGISTRY_PATH)) {
            resolvedRegKeyPath = resolvedRegKeyPath.replace(CONFIGURATION_REGISTRY_PATH, CONFIG_REGISTRY_PREFIX);
        } else if (resolvedRegKeyPath.startsWith(GOVERNANCE_REGISTRY_PATH)) {
            resolvedRegKeyPath = resolvedRegKeyPath.replace(GOVERNANCE_REGISTRY_PATH, GOVERNANCE_REGISTRY_PREFIX);
        } else if (resolvedRegKeyPath.startsWith(LOCAL_REGISTRY_PATH)) {
            resolvedRegKeyPath = resolvedRegKeyPath.replace(LOCAL_REGISTRY_PATH, LOCAL_REGISTRY_PREFIX);
        }
        return formatPath(resolvedRegKeyPath);
    }

    /**
     * Format the string paths to match any platform.. windows, linux etc..
     *
     * @param path input file path
     * @return formatted file path
     */
    public static String formatPath(String path) {
        // removing white spaces
        String pathFormatted = path.replaceAll("\\b\\s+\\b", "%20");
        try {
            pathFormatted = java.net.URLDecoder.decode(pathFormatted, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            log.error("Unsupported Encoding in the path :" + pathFormatted);
        }
        // replacing all "\" with "/"
        return pathFormatted.replace('\\', '/');
    }

    /**
     * Returns JSON array with immediate children, metadata and properties of a given parent directory.
     *
     * @param folderPath Registry path
     * @return Resulting JSON array
     */
    public JSONArray getChildrenList(String folderPath, String carbonHomePath) {

        File node = new File(folderPath);
        JSONArray jsonArray = new JSONArray();
        addImmediateChildren(node, jsonArray, carbonHomePath);
        return jsonArray;
    }

    /**
     * Updates the JSON array with JSON objects for each child file/folder.
     *
     * @param node       Parent file
     * @param childArray JSON array to store child files
     */
    private void addImmediateChildren(File node, JSONArray childArray, String carbonHomePath) {

        if (node.isDirectory()) {
            String[] childNodes = node.list();
            if (childNodes != null) {
                for (String childNode : childNodes) {
                    if (isNodeNotRequiredToBeFetched(childNode)) {
                        JSONObject childJSONObject = new JSONObject();
                        File childFile = new File(node, childNode);
                        childJSONObject.put(NAME_KEY, childFile.getName());

                        String childPath = getChildPath(formatPath(childFile.getPath()), carbonHomePath);
                        Properties properties = getResourceProperties((childPath));
                        JSONArray propertiesJSONArray = new JSONArray();
                        if (properties != null) {
                            for (Object property : properties.keySet()) {
                                Object value = properties.get(property);
                                JSONObject propertyObject = new JSONObject();
                                propertyObject.put(NAME_KEY, property);
                                propertyObject.put(VALUE_KEY, value);
                                propertiesJSONArray.put(propertyObject);
                            }
                        }
                        childJSONObject.put(PROPERTIES_KEY, propertiesJSONArray);

                        String mediaType = DEFAULT_MEDIA_TYPE;
                        if (childFile.isDirectory()) {
                            mediaType = FILE_TYPE_DIRECTORY;
                        } else {
                            Properties metadata = getMetadata(formatPath(childFile.getPath()));
                            if (metadata != null) {
                                String mediaTypeValue = metadata.getProperty(METADATA_KEY_MEDIA_TYPE);
                                if (StringUtils.isNotEmpty(mediaTypeValue)) {
                                    mediaType = mediaTypeValue;
                                }
                            }
                        }
                        childJSONObject.put(METADATA_KEY_MEDIA_TYPE, mediaType);
                        childArray.put(childJSONObject);
                    } else if (childNode.endsWith(PROPERTY_EXTENTION)) {
                        String propertyOwner = findResourceOfProperty(childNode);
                        if (!Arrays.asList(childNodes).contains(propertyOwner)) {
                            JSONObject childJSONObject = new JSONObject();
                            File childFile = new File(node, propertyOwner);
                            childJSONObject.put(NAME_KEY, childNode);

                            String childPath = getChildPath(formatPath(childFile.getPath()), carbonHomePath);
                            Properties properties = getResourceProperties(childPath);
                            JSONArray propertiesJSONArray = new JSONArray();
                            if (properties != null) {
                                for (Object property : properties.keySet()) {
                                    Object value = properties.get(property);
                                    JSONObject propertyObject = new JSONObject();
                                    propertyObject.put(NAME_KEY, property);
                                    propertyObject.put(VALUE_KEY, value);
                                    propertiesJSONArray.put(propertyObject);
                                }
                            }
                            childJSONObject.put(PROPERTIES_KEY, propertiesJSONArray);
                            childJSONObject.put(METADATA_KEY_MEDIA_TYPE, PROPERTY_FILE_VALUE);
                            childArray.put(childJSONObject);
                        }
                    }
                }
            }
        }
    }

    /**
     * Checks the node name not to start with "." , node not to be a metadata file or metadata folder and
     * node not to be a property file.
     *
     * @param nodeName  Node name
     * @return          Boolean output of the checks
     */
    private boolean isNodeNotRequiredToBeFetched(String nodeName) {
        return !nodeName.startsWith(HIDDEN_FILE_PREFIX) && !nodeName.endsWith(METADATA_FILE_SUFFIX)
                && !nodeName.endsWith(METADATA_DIR_NAME) && !nodeName.endsWith(PROPERTY_EXTENTION);
    }

    /**
     * Updates existing property files.
     * @param path          Path of the registry resource
     * @param properties    New properties
     */
    public void updateProperties(String path, Properties properties) throws URISyntaxException {
        if (registryType == MicroIntegratorRegistryConstants.LOCAL_HOST_REGISTRY) {
            String targetPath = resolveRegistryURI(path);

            String parent = getParentPath(targetPath,  false);
            File parentFile = new File(new URI(parent));
            String fileName = getResourceName(targetPath);
            if (new File(new URI(targetPath)).isDirectory()) {
                writeProperties(parentFile, fileName, properties, true);
            } else {
                writeProperties(parentFile, fileName, properties, false);
            }

        } else {
            log.warn("Updating remote registry is NOT SUPPORTED. Unable to update: " + path);
        }
    }

    /**
     * Add content to a registry resource.
     *
     * @param path             registry resource path
     * @param mediaType        media type of the registry resource
     * @param content          content of the registry resource
     */
    public void addMultipartResource(String path, String mediaType, byte[] content) {
        if (registryType == MicroIntegratorRegistryConstants.LOCAL_HOST_REGISTRY) {
            String targetPath = resolveRegistryURI(path);

            String parent = getParentPath(targetPath, false);
            try {
                File parentFile = new File(new URI(parent));
                String fileName = getResourceName(targetPath);
                Properties metadata = null;
                if (mediaType != null) {
                    metadata = new Properties();
                    metadata.setProperty(METADATA_KEY_MEDIA_TYPE, mediaType);
                }
                writeToBinaryFile(parentFile, fileName, content, metadata);
            } catch (Exception e) {
                handleException("Error when adding a new resource", e);
            }
        } else {
            log.warn("Creating new resource in remote registry is NOT SUPPORTED. Unable to create: " + path);
        }
    }

    /**
     * Function to write to file, create if not exists including directory structure.
     *
     * @param parent             parent file
     * @param newFileName        new file name to be created
     * @param content            content to be included in the new file
     * @param metadata           metadata of the new file
     */
    private void writeToBinaryFile(File parent, String newFileName, byte[] content, Properties metadata) {
        if (!parent.exists() && !parent.mkdirs()) {
            handleException("Unable to create parent directory: " + parent.getPath());
        }
        File newFile = new File(parent, newFileName);
        try (FileOutputStream fos = new FileOutputStream(newFile)) {
            fos.write(content);
            if (metadata != null) {
                writeMetadata(parent, newFileName, metadata);
            }
            if (log.isDebugEnabled()) {
                log.debug("Successfully content written to file : " + parent.getPath() + URL_SEPARATOR + newFileName);
            }
        } catch (IOException e) {
            handleException("Couldn't write to registry resource: "
                    + parent.getPath() + URL_SEPARATOR + newFileName, e);
        }
    }

    /**
     * Returns the Registry resource path which is related to the properties path.
     *
     * @param propertyPath Property resource path
     * @return             Registry resource path
     */
    private String findResourceOfProperty(String propertyPath) {
        if (propertyPath.endsWith(COLLECTION_PROPERTY_EXTENTION)) {
            return propertyPath.substring(0, propertyPath.length() - COLLECTION_PROPERTY_EXTENTION.length());
        } else {
            return propertyPath.substring(0, propertyPath.length() - PROPERTY_EXTENTION.length());
        }
    }
}

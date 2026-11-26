/*
 * Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * you may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.micro.integrator.initializer.dashboard;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.NoConnectionReuseStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.wso2.carbon.inbound.endpoint.internal.http.api.ConfigurationLoader;
import org.wso2.config.mapper.ConfigParser;
import org.wso2.micro.core.util.StringUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.wso2.micro.integrator.initializer.dashboard.Constants.COLON;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.DASHBOARD_CONFIG_GROUP_ID;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.DASHBOARD_CONFIG_HEARTBEAT_BROADCAST;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.DASHBOARD_CONFIG_HEARTBEAT_INTERVAL;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.DASHBOARD_CONFIG_HEARTBEAT_TIMEOUT;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.DASHBOARD_CONFIG_NODE_ID;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.DASHBOARD_CONFIG_URL;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.DEFAULT_GROUP_ID;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.FORWARD_SLASH;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.HEADER_VALUE_APPLICATION_JSON;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.HTTPS_PREFIX;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.MANAGEMENT;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.NODE_ID_SYSTEM_PROPERTY;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.PRODUCT_MI;

/**
 * Manages heartbeats from micro integrator to dashboard.
 */
public class HeartBeatComponent {

    private HeartBeatComponent(){

    }

    private static final Log log = LogFactory.getLog(HeartBeatComponent.class);
    private static final Map<String, Object> configs = ConfigParser.getParsedConfigs();
    private static final int HEARTBEAT_WARN_THRESHOLD = 5;
    private static final Map<String, Integer> heartbeatFailures = new ConcurrentHashMap<>();
    private static final int MAX_HEARTBEAT_DELIVERY_THREADS = 10;
    private static CloseableHttpClient heartbeatClient;
    private static ScheduledExecutorService heartbeatScheduler;
    private static ExecutorService heartbeatDeliveryExecutor;

    public static synchronized void invokeHeartbeatExecutorService() {
        if (heartbeatScheduler != null && !heartbeatScheduler.isShutdown()) {
            log.debug("Heartbeat component is already active.");
            return;
        }
        // Check if new ICP is configured
        if (ICPHeartBeatComponent.isICPConfigured()) {
            log.info("New ICP configuration detected. Starting ICP heartbeat service.");
            ICPHeartBeatComponent.invokeICPHeartbeatExecutorService();
            return;
        }
        // Fall back to old dashboard heartbeat
        String heartbeatApiUrl = configs.get(DASHBOARD_CONFIG_URL)  + "/heartbeat";
        String groupId = getGroupId();
        String nodeId = getNodeId();
        long interval = getInterval();
        String mgtApiUrl = getMgtApiUrl();
        boolean broadcast = isBroadcastEnabled();
        List<String> configuredTarget = Collections.singletonList(heartbeatApiUrl);

        final CloseableHttpClient client;
        try {
            client = createHttpClient(getTimeoutMillis(broadcast), broadcast);
        } catch (GeneralSecurityException e) {
            log.error("Heartbeats are disabled: the TLS client could not be initialised.", e);
            return;
        }
        if (broadcast) {
            log.info("Heartbeat broadcast is enabled: every address " + heartbeatApiUrl + " resolves to will "
                    + "receive a copy of each heartbeat.");
        }

        JsonObject heartbeatPayload = new JsonObject();
        heartbeatPayload.addProperty("product", PRODUCT_MI);
        heartbeatPayload.addProperty("groupId", groupId);
        heartbeatPayload.addProperty("nodeId", nodeId);
        heartbeatPayload.addProperty("interval", interval);
        heartbeatPayload.addProperty("mgtApiUrl", mgtApiUrl);

        final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        final ExecutorService deliveryExecutor = Executors.newFixedThreadPool(MAX_HEARTBEAT_DELIVERY_THREADS);
        final Set<String> inFlight = ConcurrentHashMap.newKeySet();
        heartbeatClient = client;
        heartbeatScheduler = scheduler;
        heartbeatDeliveryExecutor = deliveryExecutor;
        Runnable runnableTask = () -> {
            try {
                String payload = heartbeatPayload.toString();
                List<String> targets = broadcast ? resolveReplicas(heartbeatApiUrl) : configuredTarget;
                for (String target : targets) {
                    if (!inFlight.add(target)) {
                        continue;
                    }
                    try {
                        deliveryExecutor.submit(() -> {
                            try {
                                sendHeartbeat(client, target, payload);
                            } finally {
                                inFlight.remove(target);
                            }
                        });
                    } catch (RejectedExecutionException e) {
                        inFlight.remove(target);
                        log.debug("Heartbeat delivery was rejected for " + target, e);
                    }
                }
            } catch (Exception e) {
                log.debug("Error occurred while processing the heartbeat.", e);
            }
        };
        scheduler.scheduleAtFixedRate(runnableTask, 1, interval, TimeUnit.SECONDS);
    }

    public static synchronized void shutdownHeartbeatExecutorService() {

        if (heartbeatScheduler != null) {
            heartbeatScheduler.shutdownNow();
            heartbeatScheduler = null;
        }
        if (heartbeatDeliveryExecutor != null) {
            heartbeatDeliveryExecutor.shutdownNow();
            heartbeatDeliveryExecutor = null;
        }
        heartbeatFailures.clear();
        if (heartbeatClient != null) {
            try {
                heartbeatClient.close();
            } catch (IOException e) {
                log.debug("Error occurred while closing the heartbeat HTTP client.", e);
            } finally {
                heartbeatClient = null;
            }
        }
    }

    /**
     * Every address the configured heartbeat host resolves to, as a ready-to-post URL. Pointed at a
     * Kubernetes headless service, that is one address per Ready ICP replica, so the broadcast set
     * follows scale up/down with no reconfiguration here.
     * <p>
     * Only used when broadcast is enabled, because rewriting the host to an address costs the hostname:
     * the Host header and TLS SNI carry the address instead, which breaks name-based virtual hosts and
     * SNI-selected certificates in front of ICP. It also duplicates delivery for any host with several
     * addresses - including a dual-stack localhost, which resolves to both 127.0.0.1 and ::1.
     * <p>
     * New replicas only become visible once the JVM's DNS cache expires, so deployments that scale ICP
     * must keep networkaddress.cache.ttl well below the heartbeat interval.
     */
    private static List<String> resolveReplicas(String heartbeatApiUrl) {
        try {
            URI configured = new URI(heartbeatApiUrl);
            InetAddress[] addresses = InetAddress.getAllByName(configured.getHost());
            List<String> targets = new ArrayList<>(addresses.length);
            for (InetAddress address : addresses) {
                targets.add(new URI(configured.getScheme(), null, normalizeHostAddressForUri(address),
                        configured.getPort(), configured.getPath(), configured.getQuery(),
                        configured.getFragment()).toString());
            }
            return targets;
        } catch (URISyntaxException | UnknownHostException e) {
            log.debug("Could not resolve ICP replicas from " + heartbeatApiUrl + "; posting to it as configured.",
                      e);
            return Collections.singletonList(heartbeatApiUrl);
        }
    }

    private static String normalizeHostAddressForUri(InetAddress address) {

        return address.getHostAddress().replace("%", "%25");
    }

    /**
     * Best-effort delivery to one replica: a failure is logged and dropped, because the next heartbeat
     * re-registers this node with whichever replicas missed it.
     */
    private static void sendHeartbeat(CloseableHttpClient client, String heartbeatApiUrl, String payload) {
        try {
            HttpPost httpPost = new HttpPost(heartbeatApiUrl);
            httpPost.setHeader("Accept", HEADER_VALUE_APPLICATION_JSON);
            httpPost.setHeader("Content-type", HEADER_VALUE_APPLICATION_JSON);
            httpPost.setEntity(new StringEntity(payload, StandardCharsets.UTF_8));
            try (CloseableHttpResponse response = client.execute(httpPost)) {
                JsonObject jsonResponse = getJsonResponse(response);
                if (jsonResponse != null && jsonResponse.has("status")
                        && "success".equals(jsonResponse.get("status").getAsString())) {
                    log.debug("Heartbeat sent successfully to " + heartbeatApiUrl);
                    Integer failures = heartbeatFailures.remove(heartbeatApiUrl);
                    if (failures != null && failures >= HEARTBEAT_WARN_THRESHOLD) {
                        log.info("Heartbeat reporting to dashboard recovered after " + failures
                                + " failed attempts for " + heartbeatApiUrl);
                    }
                } else {
                    log.debug("Error occurred while sending the heartbeat to " + heartbeatApiUrl);
                }
            }
        } catch (Exception e) {
            log.debug("Error occurred while processing the heartbeat to " + heartbeatApiUrl, e);
            int failures = heartbeatFailures.merge(heartbeatApiUrl, 1, Integer::sum);
            if (failures >= HEARTBEAT_WARN_THRESHOLD) {
                log.warn("Heartbeat reporting to dashboard has failed " + failures
                        + " consecutive times for " + heartbeatApiUrl
                        + ". Dashboard may be unreachable or misconfigured.", e);
            }
        }
    }

    /**
     * One client shared by every tick and every replica. Connections are reused only in broadcast mode;
     * disabling reuse otherwise preserves the previous one-client-per-heartbeat behaviour. ICP presents a
     * self-signed certificate and replicas are addressed by IP, so trust is anchored on the self-signed chain
     * with no host matching.
     */
    private static CloseableHttpClient createHttpClient(int timeoutMillis, boolean broadcast)
            throws GeneralSecurityException {
        RequestConfig timeouts = RequestConfig.custom()
                .setConnectTimeout(timeoutMillis)
                .setConnectionRequestTimeout(timeoutMillis)
                .setSocketTimeout(timeoutMillis)
                .build();
        HttpClientBuilder clientBuilder = HttpClients.custom()
                .setDefaultRequestConfig(timeouts)
                .setSSLSocketFactory(new SSLConnectionSocketFactory(
                        SSLContexts.custom().loadTrustMaterial(null, (TrustStrategy) new TrustSelfSignedStrategy())
                                   .build(), NoopHostnameVerifier.INSTANCE));
        if (!broadcast) {
            clientBuilder.setConnectionReuseStrategy(NoConnectionReuseStrategy.INSTANCE);
        }
        return clientBuilder.build();
    }

    private static String getMgtApiUrl() {
        String serviceIp = System.getProperty("carbon.local.ip");
        String httpApiPort = Integer.toString(ConfigurationLoader.getInternalInboundHttpsPort());
        String mgtApiUrl = HTTPS_PREFIX.concat(serviceIp).concat(COLON).concat(httpApiPort).concat(FORWARD_SLASH)
                                       .concat(MANAGEMENT).concat(FORWARD_SLASH);

        Object mgtApiServiceName = configs.get(Constants.DASHBOARD_CONFIG_MANAGEMENT_HOSTNAME);
        if (null != mgtApiServiceName) {
            serviceIp = mgtApiServiceName.toString();
            Object configuredMgtPort = configs.get(Constants.DASHBOARD_CONFIG_MANAGEMENT_PORT);
            if (null != configuredMgtPort) {
                String servicePort = configuredMgtPort.toString();
                mgtApiUrl = HTTPS_PREFIX.concat(serviceIp).concat(COLON).concat(servicePort).concat(FORWARD_SLASH)
                                        .concat(MANAGEMENT).concat(FORWARD_SLASH);
            } else {
                mgtApiUrl = HTTPS_PREFIX.concat(serviceIp).concat(FORWARD_SLASH).concat(MANAGEMENT)
                                        .concat(FORWARD_SLASH);
            }
        }
        return mgtApiUrl;
    }

    private static String getGroupId() {
        String groupId;
        Object id = configs.get(DASHBOARD_CONFIG_GROUP_ID);
        if (null != id) {
            groupId = id.toString();
        } else {
            groupId = DEFAULT_GROUP_ID;
        }
        return groupId;
    }

    private static String getNodeId() {
        String nodeId = System.getProperty(NODE_ID_SYSTEM_PROPERTY);
        if (StringUtils.isEmpty(nodeId)) {
            Object id = configs.get(DASHBOARD_CONFIG_NODE_ID);
            if (null != id) {
                nodeId = id.toString();
            } else {
                nodeId = generateRandomId();
            }
        }
        return nodeId;
    }

    private static long getInterval() {
        long interval = Constants.DEFAULT_HEARTBEAT_INTERVAL;
        Object configuredInterval = configs.get(DASHBOARD_CONFIG_HEARTBEAT_INTERVAL);
        if (null != configuredInterval) {
            interval = Integer.parseInt(configuredInterval.toString());
        }
        return interval;
    }

    /**
     * Whether one heartbeat is delivered to every address the dashboard host resolves to. Off by default:
     * it is only correct where those addresses are the ICP replicas themselves, as with a Kubernetes
     * headless service.
     */
    private static boolean isBroadcastEnabled() {
        Object configuredBroadcast = configs.get(DASHBOARD_CONFIG_HEARTBEAT_BROADCAST);
        return null != configuredBroadcast && Boolean.parseBoolean(configuredBroadcast.toString());
    }

    /**
     * How long one delivery attempt may take. Keep it below the heartbeat interval so a replica that
     * accepts connections but never answers cannot delay the remaining targets past the next tick. When
     * broadcast is disabled, an omitted timeout preserves the previous unbounded delivery behaviour.
     */
    private static int getTimeoutMillis(boolean broadcast) {
        Object configuredTimeout = configs.get(DASHBOARD_CONFIG_HEARTBEAT_TIMEOUT);
        if (null == configuredTimeout) {
            return broadcast ? (int) TimeUnit.SECONDS.toMillis(Constants.DEFAULT_HEARTBEAT_TIMEOUT) : 0;
        }
        try {
            long timeoutMillis = TimeUnit.SECONDS.toMillis(Long.parseLong(configuredTimeout.toString()));
            if (timeoutMillis > 0 && timeoutMillis <= Integer.MAX_VALUE) {
                return (int) timeoutMillis;
            }
        } catch (NumberFormatException e) {
            // Fall through and use the default timeout.
        }
        log.warn("Invalid heartbeat timeout '" + configuredTimeout + "'. Using the default value of "
                + Constants.DEFAULT_HEARTBEAT_TIMEOUT + " seconds.");
        return (int) TimeUnit.SECONDS.toMillis(Constants.DEFAULT_HEARTBEAT_TIMEOUT);
    }

    private static String generateRandomId() {
        return UUID.randomUUID().toString();
    }

    public static boolean isDashboardConfigured() {
        // Check for either old dashboard config or new ICP config
        return configs.get(DASHBOARD_CONFIG_URL) != null || ICPHeartBeatComponent.isICPConfigured();
    }

    public static JsonObject getJsonResponse(CloseableHttpResponse response) {
        String stringResponse = getStringResponse(response);
        JsonObject responseObject = null;
        try {
            Gson gson = new Gson();
            responseObject = gson.fromJson(stringResponse, JsonObject.class);
        } catch (JsonParseException e) {
            log.debug("Error occurred while parsing the heartbeat response.", e);
        }
        return responseObject;
    }

    public static String getStringResponse(CloseableHttpResponse response) {
        HttpEntity entity = response.getEntity();
        String stringResponse = "";
        try {
            stringResponse = EntityUtils.toString(entity, "UTF-8");
        } catch (IOException e) {
            log.debug("Error occurred while converting entity to string.", e);
        }
        return stringResponse;
    }
}

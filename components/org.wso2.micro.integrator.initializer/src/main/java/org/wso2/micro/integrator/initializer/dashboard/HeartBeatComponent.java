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
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.wso2.carbon.inbound.endpoint.internal.http.api.ConfigurationLoader;
import org.wso2.config.mapper.ConfigParser;
import org.wso2.micro.core.util.StringUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import static org.wso2.micro.integrator.initializer.dashboard.Constants.COLON;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.DASHBOARD_CONFIG_ADMIN_PASSWORD;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.DASHBOARD_CONFIG_GROUP_ID;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.DASHBOARD_CONFIG_HEARTBEAT_INTERVAL;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.DASHBOARD_CONFIG_NODE_ID;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.DASHBOARD_CONFIG_URL;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.DEFAULT_GROUP_ID;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.FORWARD_SLASH;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.HEADER_VALUE_APPLICATION_JSON;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.HMAC_ALGORITHM;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.HTTPS_PREFIX;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.MANAGEMENT;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.NODE_ID_SYSTEM_PROPERTY;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.PRODUCT_MI;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.REQUEST_FIELD_SIGNED_CHALLENGE;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.RESPONSE_FIELD_CHALLENGE;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.RESPONSE_FIELD_STATUS;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.STATUS_VALUE_CHALLENGE;
import static org.wso2.micro.integrator.initializer.dashboard.Constants.STATUS_VALUE_SUCCESS;

/**
 * Manages heartbeats from micro integrator to dashboard.
 */
public class HeartBeatComponent {

    private HeartBeatComponent(){

    }

    private static final Log log = LogFactory.getLog(HeartBeatComponent.class);
    private static final Map<String, Object> configs = ConfigParser.getParsedConfigs();

    private static final int HEARTBEAT_WARN_THRESHOLD = 5;
    // Confined to the single-threaded ScheduledExecutorService created in invokeHeartbeatExecutorService;
    // do not change the executor to a multi-threaded one without switching this to AtomicInteger.
    private static int consecutiveFailureCount = 0;

    public static void invokeHeartbeatExecutorService() {
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

        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        Runnable runnableTask = () -> {

            try {
                JsonObject response = sendHeartbeat(
                        heartbeatApiUrl, buildPayload(groupId, nodeId, interval, mgtApiUrl, null));
                String status = getStatus(response);
                if (STATUS_VALUE_SUCCESS.equals(status)) {
                    log.debug("Heartbeat sent successfully.");
                    onHeartbeatOutcomeSuccess();
                } else if (STATUS_VALUE_CHALLENGE.equals(status) && response.has(RESPONSE_FIELD_CHALLENGE)) {
                    // First contact: the dashboard won't register this node - and hand it the mgtApiUrl-scoped
                    // credential - until it's proven this node already holds the shared admin password.
                    completeRegistration(heartbeatApiUrl, groupId, nodeId, interval, mgtApiUrl,
                            response.get(RESPONSE_FIELD_CHALLENGE).getAsString());
                } else {
                    log.debug("Error occurred while sending the heartbeat.");
                }
            } catch (Exception e) {
                log.debug("Error occurred while processing the heartbeat.", e);
                onHeartbeatOutcomeFailure(e);
            }
        };
        scheduledExecutorService.scheduleAtFixedRate(runnableTask, 1, interval, TimeUnit.SECONDS);
    }

    private static void completeRegistration(String heartbeatApiUrl, String groupId, String nodeId, long interval,
                                              String mgtApiUrl, String challenge) throws Exception {
        String signedChallenge = signChallenge(challenge, groupId, nodeId, mgtApiUrl);
        if (signedChallenge == null) {
            log.warn("Cannot complete registration with the dashboard: " + DASHBOARD_CONFIG_ADMIN_PASSWORD
                    + " is not configured, so the node registration challenge cannot be signed.");
            return;
        }
        JsonObject response = sendHeartbeat(
                heartbeatApiUrl, buildPayload(groupId, nodeId, interval, mgtApiUrl, signedChallenge));
        if (STATUS_VALUE_SUCCESS.equals(getStatus(response))) {
            log.debug("Node registered with the dashboard successfully.");
            onHeartbeatOutcomeSuccess();
        } else {
            log.debug("Error occurred while completing node registration with the dashboard.");
        }
    }

    private static void onHeartbeatOutcomeSuccess() {
        if (consecutiveFailureCount >= HEARTBEAT_WARN_THRESHOLD) {
            log.info("Heartbeat reporting to dashboard recovered after " + consecutiveFailureCount
                    + " failed attempts.");
        }
        consecutiveFailureCount = 0;
    }

    private static void onHeartbeatOutcomeFailure(Exception e) {
        consecutiveFailureCount++;
        if (consecutiveFailureCount == HEARTBEAT_WARN_THRESHOLD) {
            log.warn("Heartbeat reporting to dashboard has failed " + consecutiveFailureCount
                    + " consecutive times. Dashboard may be unreachable or misconfigured. "
                    + "For further debugging, please refer debug logs.", e);
        } else if (consecutiveFailureCount > HEARTBEAT_WARN_THRESHOLD) {
            log.warn("Error occurred while processing the heartbeat for " + consecutiveFailureCount
                    + " consecutive times.", e);
        }
    }

    private static String signChallenge(String challenge, String groupId, String nodeId, String mgtApiUrl) {
        Object configuredPassword = configs.get(DASHBOARD_CONFIG_ADMIN_PASSWORD);
        if (configuredPassword == null || StringUtils.isEmpty(configuredPassword.toString())) {
            return null;
        }
        // Same canonicalization the dashboard verifies against: length-prefixed so that no rearrangement of
        // these fields can be crafted to collide with a different, legitimately-signed combination.
        String canonicalMessage = lengthPrefixed(challenge) + lengthPrefixed(groupId) + lengthPrefixed(nodeId)
                + lengthPrefixed(mgtApiUrl == null ? "" : mgtApiUrl);
        try {
            Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            mac.init(new SecretKeySpec(configuredPassword.toString().getBytes(StandardCharsets.UTF_8),
                    HMAC_ALGORITHM));
            byte[] signature = mac.doFinal(canonicalMessage.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(signature);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            log.debug("Error occurred while signing the node registration challenge.", e);
            return null;
        }
    }

    private static String lengthPrefixed(String value) {
        return value.length() + COLON + value;
    }

    private static JsonObject buildPayload(String groupId, String nodeId, long interval, String mgtApiUrl,
                                            String signedChallenge) {
        JsonObject payload = new JsonObject();
        payload.addProperty("product", PRODUCT_MI);
        payload.addProperty("groupId", groupId);
        payload.addProperty("nodeId", nodeId);
        payload.addProperty("interval", interval);
        payload.addProperty("mgtApiUrl", mgtApiUrl);
        if (signedChallenge != null) {
            payload.addProperty(REQUEST_FIELD_SIGNED_CHALLENGE, signedChallenge);
        }
        return payload;
    }

    private static String getStatus(JsonObject response) {
        return response != null && response.has(RESPONSE_FIELD_STATUS)
                ? response.get(RESPONSE_FIELD_STATUS).getAsString() : null;
    }

    /**
     * Does not catch its own exceptions - callers rely on them propagating so consecutive-failure tracking
     * (onHeartbeatOutcomeFailure) sees every network-level failure, matching this method's previous inline form.
     */
    private static JsonObject sendHeartbeat(String heartbeatApiUrl, JsonObject payload) throws Exception {
        final HttpPost httpPost = new HttpPost(heartbeatApiUrl);
        httpPost.setHeader("Accept", HEADER_VALUE_APPLICATION_JSON);
        httpPost.setHeader("Content-type", HEADER_VALUE_APPLICATION_JSON);
        try (CloseableHttpClient client = HttpClients.custom().setSSLSocketFactory(
                new SSLConnectionSocketFactory(
                        SSLContexts.custom().loadTrustMaterial(null,
                                (TrustStrategy) new TrustSelfSignedStrategy()).build(),
                        NoopHostnameVerifier.INSTANCE)).build()) {
            httpPost.setEntity(new StringEntity(payload.toString()));
            CloseableHttpResponse response = client.execute(httpPost);
            return getJsonResponse(response);
        }
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

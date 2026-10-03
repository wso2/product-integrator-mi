/*
 * Copyright (c) 2025, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
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
package org.wso2.micro.integrator.observability.metric.handler.prometheus.reporter;

import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.core.metrics.Counter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.SynapseConstants;
import org.wso2.config.mapper.ConfigParser;
import org.wso2.micro.integrator.observability.metric.handler.MetricReporter;
import org.wso2.micro.integrator.observability.util.MetricConstants;

/**
 * Prometheus Reporter implementation for API version 1.x
 */
public class PrometheusReporterV1 implements MetricReporter {

    private Counter TOTAL_REQUESTS_RECEIVED_PROXY_SERVICE;
    private Counter TOTAL_REQUESTS_RECEIVED_API;
    private Counter TOTAL_REQUESTS_RECEIVED_INBOUND_ENDPOINT;
    private Counter TOTAL_REQUESTS_RECEIVED_DATA_SERVICE;
    private Counter ERROR_REQUESTS_RECEIVED_PROXY_SERVICE;
    private Counter ERROR_REQUESTS_RECEIVED_API;
    private Counter ERROR_REQUESTS_RECEIVED_INBOUND_ENDPOINT;
    private Counter ERROR_REQUESTS_RECEIVED_DATA_SERVICE;

    private Histogram PROXY_LATENCY_HISTOGRAM;
    private Histogram API_LATENCY_HISTOGRAM;
    private Histogram INBOUND_ENDPOINT_LATENCY_HISTOGRAM;
    private Histogram DATA_SERVICE_LATENCY_HISTOGRAM;

    private Gauge SERVER_UP;
    private Gauge SERVICE_UP;
    private Gauge SERVER_VERSION;

    private static final Log log = LogFactory.getLog(PrometheusReporterV1.class);

    private double[] proxyLatencyBuckets;
    private double[] apiLatencyBuckets;
    private double[] inboundEndpointLatencyBuckets;
    private double[] dataServiceLatencyBuckets;

    private Map<String, Object> metricMap = new HashMap();
    private volatile boolean initialized = false;
    Map<String, Object> configs;

    private synchronized void initializeOnce() {
        if (initialized) {
            return;
        }
        initialized = true;
        configs = ConfigParser.getParsedConfigs();
        createBuckets(configs);
        JvmMetrics.builder().register();
    }

    @Override
    public void createMetrics(String serviceType, String type, String metricName, String metricHelp,
        String[] properties) {

        if (!initialized) {
            initializeOnce();
        }

        //Read the label names from the map
        String[] labels = properties;

        if (serviceType.equalsIgnoreCase(SERVICE.PROXY.name())) {
            if (type.equals(MetricConstants.COUNTER)) {
                TOTAL_REQUESTS_RECEIVED_PROXY_SERVICE = Counter.builder()
                    .name(MetricConstants.PROXY_REQUEST_COUNT_TOTAL)
                    .help(metricHelp).labelNames(labels).register();
                metricMap.put(metricName, TOTAL_REQUESTS_RECEIVED_PROXY_SERVICE);

            } else if (type.equals(MetricConstants.HISTOGRAM)) {
                PROXY_LATENCY_HISTOGRAM = Histogram.builder()
                    .name(MetricConstants.PROXY_LATENCY_SECONDS).help(metricHelp)
                    .labelNames(labels).classicUpperBounds(proxyLatencyBuckets).register();
                metricMap.put(metricName, PROXY_LATENCY_HISTOGRAM);
            }
        } else if (serviceType.equalsIgnoreCase(SERVICE.API.name())) {
            if (type.equals(MetricConstants.COUNTER)) {
                TOTAL_REQUESTS_RECEIVED_API = Counter.builder()
                    .name(MetricConstants.API_REQUEST_COUNT_TOTAL).help(metricHelp)
                    .labelNames(labels).register();
                metricMap.put(metricName, TOTAL_REQUESTS_RECEIVED_API);
            } else if (type.equals(MetricConstants.HISTOGRAM)) {
                API_LATENCY_HISTOGRAM = Histogram.builder()
                    .name(MetricConstants.API_LATENCY_SECONDS).help(metricHelp)
                    .labelNames(labels).classicUpperBounds(apiLatencyBuckets).register();
                metricMap.put(metricName, API_LATENCY_HISTOGRAM);
            }
        } else if (serviceType.equalsIgnoreCase(SERVICE.INBOUND_ENDPOINT.name())) {
            if (type.equals(MetricConstants.COUNTER)) {
                TOTAL_REQUESTS_RECEIVED_INBOUND_ENDPOINT = Counter.builder()
                    .name(MetricConstants.INBOUND_ENDPOINT_REQUEST_COUNT_TOTAL).help(metricHelp)
                    .labelNames(labels).register();
                metricMap.put(metricName, TOTAL_REQUESTS_RECEIVED_INBOUND_ENDPOINT);
            } else if (type.equals(MetricConstants.HISTOGRAM)) {
                INBOUND_ENDPOINT_LATENCY_HISTOGRAM = Histogram.builder()
                    .name(MetricConstants.INBOUND_ENDPOINT_LATENCY_SECONDS).help(metricHelp)
                    .labelNames(labels).classicUpperBounds(inboundEndpointLatencyBuckets)
                    .register();
                metricMap.put(metricName, INBOUND_ENDPOINT_LATENCY_HISTOGRAM);
            }
        } else if (serviceType.equalsIgnoreCase(SERVICE.DATA_SERVICE.name())) {
            if (type.equals(MetricConstants.COUNTER)) {
                TOTAL_REQUESTS_RECEIVED_DATA_SERVICE = Counter.builder()
                    .name(MetricConstants.DATA_SERVICE_REQUEST_COUNT_TOTAL).help(metricHelp)
                    .labelNames(labels).register();
                metricMap.put(metricName, TOTAL_REQUESTS_RECEIVED_DATA_SERVICE);
            } else if (type.equals(MetricConstants.HISTOGRAM)) {
                DATA_SERVICE_LATENCY_HISTOGRAM = Histogram.builder()
                    .name(MetricConstants.DATA_SERVICE_LATENCY_SECONDS).help(metricHelp)
                    .labelNames(labels).classicUpperBounds(dataServiceLatencyBuckets).register();
                metricMap.put(metricName, DATA_SERVICE_LATENCY_HISTOGRAM);
            }
        } else if (serviceType.equals(MetricConstants.SERVER)) {
            SERVER_UP = Gauge.builder()
                .name(MetricConstants.SERVER_UP).help("Server status").
                labelNames(labels).register();
            metricMap.put(MetricConstants.SERVER_UP, SERVER_UP);
        } else if (serviceType.equals(MetricConstants.VERSION)) {
            SERVER_VERSION = Gauge.builder().name(MetricConstants.SERVER_VERSION).help(metricHelp).
                labelNames(labels).register();
            metricMap.put(MetricConstants.SERVER_VERSION, SERVER_VERSION);
        } else {
            SERVICE_UP = Gauge.builder().name(MetricConstants.SERVICE_UP).help("Service status").
                labelNames(labels).register();
            metricMap.put(MetricConstants.SERVICE_UP, SERVICE_UP);
        }
    }

    @Override
    public void initMetrics() {
        this.initializeServeMetrics();
        this.initializeServerVersionMetrics();
        this.initializeArtifactDeploymentMetrics();

        this.initializeApiMetrics();
        this.initializeProxyMetrics();
        this.initializeInboundEndpointMetrics();
        this.initializeDataServiceMetrics();
    }

    @Override
    public void initErrorMetrics(String serviceType, String type, String metricName,
        String metricHelp, String[] properties) {
        if (serviceType.equals(SERVICE.API.name())) {
            ERROR_REQUESTS_RECEIVED_API = Counter.builder()
                .name(MetricConstants.API_REQUEST_COUNT_ERROR_TOTAL).help(metricHelp)
                .labelNames(properties).register();
            metricMap.put(metricName, ERROR_REQUESTS_RECEIVED_API);
        } else if (serviceType.equals(SERVICE.PROXY.name())) {
            ERROR_REQUESTS_RECEIVED_PROXY_SERVICE = Counter.builder()
                .name(MetricConstants.PROXY_REQUEST_COUNT_ERROR_TOTAL).help(metricHelp).
                labelNames(properties).register();
            metricMap.put(metricName, ERROR_REQUESTS_RECEIVED_PROXY_SERVICE);
        } else if (serviceType.equals(SERVICE.INBOUND_ENDPOINT.name())) {
            ERROR_REQUESTS_RECEIVED_INBOUND_ENDPOINT = Counter.builder()
                .name(MetricConstants.INBOUND_ENDPOINT_REQUEST_COUNT_ERROR_TOTAL).help(metricHelp)
                .labelNames(properties).register();
            metricMap.put(metricName, ERROR_REQUESTS_RECEIVED_INBOUND_ENDPOINT);
        } else if (serviceType.equals(SERVICE.DATA_SERVICE.name())) {
            ERROR_REQUESTS_RECEIVED_DATA_SERVICE = Counter.builder()
                .name(MetricConstants.DATA_SERVICE_REQUEST_COUNT_ERROR_TOTAL)
                .help(metricHelp).labelNames(properties).register();
            metricMap.put(metricName, ERROR_REQUESTS_RECEIVED_DATA_SERVICE);
        }
    }

    @Override
    public void incrementCount(String metricName, String[] properties) {
        Counter counter = (Counter) metricMap.get(metricName);
        if (counter == null) {
            log.error("Counter metric not found: " + metricName);
            return;
        }
        counter.labelValues(properties).inc();
    }

    @Override
    public void decrementCount(String metricName, String[] properties) {

    }

    @Override
    public Object getTimer(String metricName, String[] properties) {
        Histogram timer = (Histogram) metricMap.get(metricName);
        if (timer == null) {
            log.error("Timer metric not found: " + metricName);
            return null;
        }
        return timer.labelValues(properties).startTimer();
    }

    @Override
    public void observeTime(Object timer) {
        if (timer == null) {
            return;
        }
        try {
            // Call observeDuration() reflectively to avoid compile-time dependency on a nested Timer type
            java.lang.reflect.Method m = timer.getClass().getMethod("observeDuration");
            m.invoke(timer);
        } catch (NoSuchMethodException | IllegalAccessException |
                 java.lang.reflect.InvocationTargetException e) {
            log.error("Error invoking observeDuration on timer object", e);
        }
    }

    @Override
    public void serverUp(String host, String port, String javaHome, String javaVersion) {
        Gauge gauge = (Gauge) metricMap.get(MetricConstants.SERVER_UP);
        if (gauge == null) {
            return;
        }
        double epochSeconds = System.currentTimeMillis() / 1000.0;
        gauge.labelValues(host, port, javaHome, javaVersion).set(epochSeconds);
    }

    @Override
    public void serverVersion(String version, String updateLevel) {
        Gauge gauge = (Gauge) metricMap.get(MetricConstants.SERVER_VERSION);
        if (gauge == null) {
            return;
        }
        double epochSeconds = System.currentTimeMillis() / 1000.0;
        gauge.labelValues(version, updateLevel).set(epochSeconds);
    }

    @Override
    public void serverDown(String host, String port, String javaVersion, String javaHome) {
        Gauge gauge = (Gauge) metricMap.get(MetricConstants.SERVER_UP);
        if (gauge == null) {
            return;
        }
        gauge.labelValues(host, port, javaHome, javaVersion).set(0);
    }

    @Override
    public void serviceUp(String serviceName, String serviceType) {
        Gauge gauge = (Gauge) metricMap.get(MetricConstants.SERVICE_UP);
        if (gauge == null) {
            if (log.isDebugEnabled()) {
                log.debug("Metrics are not initialized. Skipping service_up for " + serviceType + " : "
                        + serviceName);
            }
            return;
        }
        double epochSeconds = System.currentTimeMillis() / 1000.0;
        gauge.labelValues(serviceName, serviceType).set(epochSeconds);

        if (serviceType.equals(SynapseConstants.PROXY_SERVICE_TYPE)) {
            setCounterValue(TOTAL_REQUESTS_RECEIVED_PROXY_SERVICE, serviceName, serviceType);
            setCounterValue(ERROR_REQUESTS_RECEIVED_PROXY_SERVICE, serviceName, serviceType);
        } else if (serviceType.equals(SynapseConstants.FAIL_SAFE_MODE_API)) {
            setCounterValue(TOTAL_REQUESTS_RECEIVED_API, serviceName, serviceType);
            setCounterValue(ERROR_REQUESTS_RECEIVED_API, serviceName, serviceType);
        } else {
            setCounterValue(TOTAL_REQUESTS_RECEIVED_INBOUND_ENDPOINT, serviceName, serviceType);
            setCounterValue(ERROR_REQUESTS_RECEIVED_INBOUND_ENDPOINT, serviceName, serviceType);
        }
    }

    @Override
    public void serviceDown(String serviceName, String serviceType) {
        Gauge gauge = (Gauge) metricMap.get(MetricConstants.SERVICE_UP);
        if (gauge == null) {
            return;
        }
        gauge.labelValues(serviceName, serviceType).set(0);
    }

    enum SERVICE {
        PROXY,
        API,
        INBOUND_ENDPOINT,
        DATA_SERVICE
    }

    /**
     * Create the API related metrics.
     */
    public void initializeApiMetrics() {
        String[] labels = {MetricConstants.SERVICE_NAME, MetricConstants.SERVICE_TYPE,
            MetricConstants.INVOCATION_URL};
        createMetrics(SynapseConstants.FAIL_SAFE_MODE_API, MetricConstants.COUNTER,
            MetricConstants.API_REQUEST_COUNT_TOTAL,
            "Total number of requests to an api", labels);
        createMetrics(SynapseConstants.FAIL_SAFE_MODE_API, MetricConstants.HISTOGRAM,
            MetricConstants.API_LATENCY_SECONDS,
            "Latency of requests to an api", labels);
        initializeApiErrorMetrics();
    }

    /**
     * Create the proxy services related metrics.
     */
    public void initializeProxyMetrics() {
        String[] labels = {MetricConstants.SERVICE_NAME, MetricConstants.SERVICE_TYPE};

        createMetrics(SynapseConstants.PROXY_SERVICE_TYPE, MetricConstants.COUNTER,
            MetricConstants.PROXY_REQUEST_COUNT_TOTAL,
            "Total number of requests to a proxy service", labels);
        createMetrics(SynapseConstants.PROXY_SERVICE_TYPE, MetricConstants.HISTOGRAM,
            MetricConstants.PROXY_LATENCY_SECONDS,
            "Latency of requests to a proxy service", labels);

        initializeProxyErrorMetrics();
    }

    /**
     * Create the inbound endpoint related metrics.
     */
    public void initializeInboundEndpointMetrics() {
        String[] labels = {MetricConstants.SERVICE_NAME, MetricConstants.SERVICE_TYPE};

        createMetrics("INBOUND_ENDPOINT", MetricConstants.COUNTER,
            MetricConstants.INBOUND_ENDPOINT_REQUEST_COUNT_TOTAL,
            "Total number of requests to an inbound endpoint.", labels);
        createMetrics("INBOUND_ENDPOINT", MetricConstants.HISTOGRAM,
            MetricConstants.INBOUND_ENDPOINT_LATENCY_SECONDS,
            "Latency of requests to an inbound endpoint.", labels);

        initializeInboundEndpointErrorMetrics();
    }

    /**
     * Create data services related metrics.
     */
    public void initializeDataServiceMetrics() {
        String[] labels = {MetricConstants.SERVICE_NAME, MetricConstants.SERVICE_TYPE};

        createMetrics("DATA_SERVICE", MetricConstants.COUNTER,
            MetricConstants.DATA_SERVICE_REQUEST_COUNT_TOTAL,
            "Total number of requests to a data service.", labels);
        createMetrics("DATA_SERVICE", MetricConstants.HISTOGRAM,
            MetricConstants.DATA_SERVICE_LATENCY_SECONDS,
            "Latency of requests to a data service.", labels);

        initializeDataServiceErrorMetrics();
    }

    /**
     * Create the metrics related to server startup.
     */
    public void initializeServeMetrics() {
        createMetrics(MetricConstants.SERVER, MetricConstants.GAUGE, MetricConstants.SERVER_UP,
            "Server Status", new String[]{MetricConstants.HOST, MetricConstants.PORT,
                MetricConstants.JAVA_HOME_LABEL, MetricConstants.JAVA_VERSION_LABEL});
    }

    /**
     * Create the metrics related to server version.
     */
    public void initializeServerVersionMetrics() {
        createMetrics(MetricConstants.VERSION, MetricConstants.GAUGE,
            MetricConstants.SERVER_VERSION,
            "Version and Update Level of Server",
            new String[]{MetricConstants.VERSION_LABEL, MetricConstants.UPDATE_LEVEL_LABEL});
    }

    /**
     * Create the metrics related to service deployment.
     */
    public void initializeArtifactDeploymentMetrics() {
        createMetrics(MetricConstants.SERVICE, MetricConstants.GAUGE, MetricConstants.SERVICE_UP,
            "Service Status",
            new String[]{MetricConstants.SERVICE_NAME, MetricConstants.SERVICE_TYPE});

    }

    /**
     * Load the user defined Histogram bucket upper limits configurations from the deployment.toml
     * file else assign the default bucket configuration values.  .
     *
     * @param configs The map of configs defined for Histogram bucket upper limits in the
     *                deployment.toml file
     */
    private void createBuckets(Map<String, Object> configs) {
        Object proxyConfigBuckets = configs.get(MetricConstants.METRIC_HANDLER + "." +
            MetricConstants.PROXY_LATENCY_BUCKETS);
        Object apiConfigBuckets = configs.get(MetricConstants.METRIC_HANDLER + "." +
            MetricConstants.API_LATENCY_BUCKETS);
        Object inboundEndpointConfigBuckets = configs.get(MetricConstants.METRIC_HANDLER + "." +
            MetricConstants.INBOUND_ENDPOINT_LATENCY_BUCKETS);
        Object dataServiceConfigBuckets = configs.get(MetricConstants.METRIC_HANDLER + "." +
            MetricConstants.DATA_SERVICE_LATENCY_BUCKETS);

        if (null != proxyConfigBuckets) {
            List<Object> list = Arrays.asList(proxyConfigBuckets);
            int size = list.size();
            List<Object> bucketList = (ArrayList) list.get(0);
            proxyLatencyBuckets = new double[size];
            for (int i = 0; i < size; i++) {
                proxyLatencyBuckets[i] = (double) bucketList.get(i);
            }
        } else {
            proxyLatencyBuckets = new double[]{0.19, 0.20, 0.25, 0.30, 0.35, 0.40, 0.50, 0.60, 1, 5};
        }
        if (null != apiConfigBuckets) {
            List<Object> list = Arrays.asList(apiConfigBuckets);
            int size = list.size();
            List<Object> bucketList = (ArrayList) list.get(0);
            apiLatencyBuckets = new double[size];
            for (int i = 0; i < size; i++) {
                apiLatencyBuckets[i] = (double) bucketList.get(i);
            }
        } else {
            apiLatencyBuckets = new double[]{0.19, 0.20, 0.25, 0.30, 0.35, 0.40, 0.50, 0.60, 1, 5};
        }
        if (null != inboundEndpointConfigBuckets) {
            List<Object> list = Arrays.asList(inboundEndpointConfigBuckets);
            int size = list.size();
            List<Object> bucketList = (ArrayList) list.get(0);
            inboundEndpointLatencyBuckets = new double[size];
            for (int i = 0; i < size; i++) {
                inboundEndpointLatencyBuckets[i] = (double) bucketList.get(i);
            }
        } else {
            inboundEndpointLatencyBuckets = new double[]{0.19, 0.20, 0.25, 0.30, 0.35, 0.40, 0.50, 0.60,
                1, 5};
        }
        if (null != dataServiceConfigBuckets) {
            List<Object> list = Arrays.asList(dataServiceConfigBuckets);
            int size = list.size();
            List<Object> bucketList = (ArrayList) list.get(0);
            dataServiceLatencyBuckets = new double[size];
            for (int i = 0; i < size; i++) {
                dataServiceLatencyBuckets[i] = (double) bucketList.get(i);
            }
        } else {
            dataServiceLatencyBuckets = new double[]{0.19, 0.20, 0.25, 0.30, 0.35, 0.40, 0.50, 0.60, 1,
                5};
        }
    }

    /**
     * Set the counter value with the appropriate label values.
     *
     * @param counter     The counter metric
     * @param serviceName The service name
     * @param serviceType The service type
     */
    public void setCounterValue(Counter counter, String serviceName, String serviceType) {
        if (serviceType.equals(SynapseConstants.FAIL_SAFE_MODE_API)) {
            counter.labelValues(serviceName, serviceType, "");
        } else {
            counter.labelValues(serviceName, serviceType);
        }
    }

    /**
     * Create the metrics related to failed API requests.
     */
    public void initializeApiErrorMetrics() {
        initErrorMetrics("API", MetricConstants.COUNTER,
            MetricConstants.API_REQUEST_COUNT_ERROR_TOTAL,
            "Total number of error requests to an api", new String[]{MetricConstants.SERVICE_NAME,
                MetricConstants.SERVICE_TYPE, MetricConstants.INVOCATION_URL});
    }

    /**
     * Create the metrics related to failed proxy services.
     */
    public void initializeProxyErrorMetrics() {
        initErrorMetrics("PROXY", MetricConstants.COUNTER,
            MetricConstants.PROXY_REQUEST_COUNT_ERROR_TOTAL,
            "Total number of error requests to a proxy service", new String[]
                {MetricConstants.SERVICE_NAME, MetricConstants.SERVICE_TYPE});
    }

    /**
     * Create the metrics related to failed inbound endpoints.
     */
    public void initializeInboundEndpointErrorMetrics() {
        initErrorMetrics("INBOUND_ENDPOINT", MetricConstants.COUNTER,
            MetricConstants.INBOUND_ENDPOINT_REQUEST_COUNT_ERROR_TOTAL, "Total number of error" +
                " requests when receiving the message by an inbound endpoint.",
            new String[]{MetricConstants.SERVICE_NAME, MetricConstants.SERVICE_TYPE});
    }

    /**
     * Create the metrics related to failed dataservices.
     */
    public void initializeDataServiceErrorMetrics() {
        initErrorMetrics("DATA_SERVICE", MetricConstants.COUNTER,
            MetricConstants.DATA_SERVICE_REQUEST_COUNT_ERROR_TOTAL, "Total number of error" +
                " requests when receiving the message by an inbound endpoint.",
            new String[]{MetricConstants.SERVICE_NAME, MetricConstants.SERVICE_TYPE});
    }
}

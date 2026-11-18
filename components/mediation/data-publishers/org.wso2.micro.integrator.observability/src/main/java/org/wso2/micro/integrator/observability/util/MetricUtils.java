/*
 * Copyright (c) 2023, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.micro.integrator.observability.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.config.mapper.ConfigParser;
import org.wso2.micro.integrator.observability.metric.handler.MetricReporter;
import org.wso2.micro.integrator.observability.metric.handler.prometheus.reporter.PrometheusReporter;
import org.wso2.micro.integrator.observability.metric.handler.prometheus.reporter.PrometheusReporterV1;

import java.util.Map;

public class MetricUtils {

    private static Log log = LogFactory.getLog(MetricUtils.class);
    private static final String METRIC_REPORTER = "metric_reporter";

    private static MetricReporter metricReporter = null;

    public static synchronized MetricReporter getMetricReporter() {
        if (metricReporter == null) {
            metricReporter = generateMetricReporter();
        }
        return  metricReporter;
    }

    /**
     * Load the MetricReporter class from the deployment.toml file if a user has defined a MetricReporter.
     * Use default PrometheusReporter if the user hasn't defined a MetricReporter or an error occurs
     * during custom MetricReporter class invocation.
     */

    public static MetricReporter generateMetricReporter() {
        Map<String, Object> configs = ConfigParser.getParsedConfigs();
        Object metricReporterClass = configs.get(MetricConstants.METRIC_HANDLER + "." + METRIC_REPORTER);
        Class loadedMetricClass;
        MetricReporter reporterInstance;

        if (metricReporterClass != null) {
            try {
                loadedMetricClass = Class.forName(metricReporterClass.toString());
                reporterInstance = (MetricReporter) loadedMetricClass.newInstance();
                if (log.isDebugEnabled()) {
                    log.debug("The class " + metricReporterClass + " loaded successfully");
                }
            } catch (IllegalAccessException | ClassNotFoundException | InstantiationException e) {
                log.error("Error in loading the class " + metricReporterClass.toString() +
                        " .Hence loading the default PrometheusReporter class ", e);
                reporterInstance = loadDefaultPrometheusReporter();
            }
        } else {
            reporterInstance = loadDefaultPrometheusReporter();
        }
        return reporterInstance;
    }

    private static MetricReporter loadDefaultPrometheusReporter() {
        boolean enablePrometheusLegacy = Boolean.parseBoolean(
            System.getProperty(MetricConstants.ENABLE_LEGACY_PROMETHEUS));
        MetricReporter reporterInstance;
        if (enablePrometheusLegacy) {
            if (log.isDebugEnabled()) {
                log.debug("Switched to legacy Prometheus API version (0.x) for metrics exposure.");
            }
            reporterInstance = new PrometheusReporter();
        } else {
            reporterInstance = new PrometheusReporterV1();
            if (log.isDebugEnabled()) {
                log.debug("Switched to Prometheus API version 1.x for metrics exposure.");
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("The class org.wso2.micro.integrator.obsrvability.handler.metrics.publisher.prometheus." +
                    "reporter.PrometheusReporter was loaded successfully");
        }
        return reporterInstance;
    }
}

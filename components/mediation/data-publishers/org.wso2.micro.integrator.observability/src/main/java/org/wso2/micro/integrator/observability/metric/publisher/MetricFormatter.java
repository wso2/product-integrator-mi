/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.micro.integrator.observability.metric.publisher;

import io.prometheus.client.Collector;
import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.Collector.Type;

import java.util.Enumeration;

/**
 * Metric Formatter class to format Prometheus Metrics
 */
public class MetricFormatter {

    /**
     * Return the type of metric
     *
     * @param type Metric Prometheus type
     */
    private static String typeString(Type type) {
        switch (type) {
            case GAUGE:
                return "gauge";
            case COUNTER:
                return "counter";
            case SUMMARY:
                return "summary";
            case HISTOGRAM:
                return "histogram";
            default:
                return "untyped";
        }
    }

    /**
     * Return a formatted metric
     *
     * @param metricsList the Prometheus Metric from Prometheus Default Registry
     * @return the metric converted into String format
     */
    public static String formatMetrics(Enumeration<MetricFamilySamples> metricsList) {

        StringBuilder metric = new StringBuilder();
        StringBuilder metricsValue = new StringBuilder();

        while (metricsList.hasMoreElements()) {
            MetricFamilySamples metricFamilySamples = metricsList.nextElement();
            metric.append("# HELP ").append(metricFamilySamples.name).append(' ')
                    .append(escapeText(metricFamilySamples.help)).append("\n").append("# TYPE ").
                    append(metricFamilySamples.name).append(' ').append(typeString(metricFamilySamples.type)).
                    append("\n");

            StringBuilder metricData = new StringBuilder();
            for (Collector.MetricFamilySamples.Sample sample : metricFamilySamples.samples) {
                StringBuilder labelNames = new StringBuilder();
                if (!sample.labelNames.isEmpty()) {
                    labelNames.append("{");

                    for (int i = 0; i < sample.labelNames.size(); ++i) {
                        labelNames.append(sample.labelNames.get(i)).append("=\"")
                                .append(escapeText(sample.labelValues.get(i)).replace("\"", "\\\"")).append('"');
                        if (i < sample.labelNames.size() - 1) {
                            labelNames.append(", ");
                        } else {
                            labelNames.append("}");
                        }
                    }
                }

                labelNames.append(' ').append(sample.value);
                if (sample.timestampMs != null) {
                    labelNames.append(' ').append(sample.timestampMs.toString());
                }
                metricData.append(metricFamilySamples.name).append(labelNames).append("\n");
            }
            metric.append(metricData);
        }
        metricsValue.append(metric);
        return metricsValue.toString();
    }

    private static String escapeText(String text) {
        return String.valueOf(text).replace("\\", "\\\\").replace("\n", "\\n");
    }
}

/*
 * Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.micro.integrator.observability.metric.handler.prometheus.reporter;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import org.junit.Assert;
import org.junit.Test;
import org.wso2.micro.integrator.observability.util.MetricConstants;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class PrometheusReporterComponentMetricTest {

    private static final String[] LABELS = {"kind", "name"};

    @Test
    public void testCounterIsRegisteredOnceAndUsedAgain() throws Exception {
        PrometheusReporter reporter = new PrometheusReporter();
        String name = "pd_legacy_files_total";

        Assert.assertTrue(reporter.createComponentMetric(MetricConstants.COUNTER, name, "Files done", LABELS));
        Object counter = metricMap(reporter).get(name);
        Assert.assertTrue("A second registration of the same shape is used again",
                reporter.createComponentMetric(MetricConstants.COUNTER, name, "Files done", LABELS));
        Assert.assertSame("Nothing is registered twice", counter, metricMap(reporter).get(name));

        reporter.incrementCount(name, new String[]{"INBOUND", "a"});
        reporter.incrementCount(name, new String[]{"INBOUND", "a"});
        Assert.assertEquals(2.0, ((Counter) counter).labels("INBOUND", "a").get(), 0.0);
    }

    @Test
    public void testOtherShapeGivesFalse() throws Exception {
        PrometheusReporter reporter = new PrometheusReporter();
        String name = "pd_legacy_failures_total";

        Assert.assertTrue(reporter.createComponentMetric(MetricConstants.COUNTER, name, "Failures", LABELS));
        Object counter = metricMap(reporter).get(name);
        Assert.assertFalse("Other label names give false",
                reporter.createComponentMetric(MetricConstants.COUNTER, name, "Failures", new String[]{"other"}));
        Assert.assertFalse("Another type gives false",
                reporter.createComponentMetric(MetricConstants.GAUGE, name, "Failures", LABELS));
        Assert.assertSame("The first registration stays", counter, metricMap(reporter).get(name));

        reporter.incrementCount(name, new String[]{"CONNECTION", "c1"});
        Assert.assertEquals(1.0, ((Counter) counter).labels("CONNECTION", "c1").get(), 0.0);
    }

    @Test
    public void testRefusedShapeIsLoggedOnce() throws Exception {
        PrometheusReporter reporter = new PrometheusReporter();
        String name = "pd_legacy_clash_total";
        List<String> errors = new ArrayList<>();
        Handler handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getLevel() == Level.SEVERE && record.getMessage().contains(name)) {
                    errors.add(record.getMessage());
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        Logger logger = Logger.getLogger(PrometheusReporter.class.getName());
        logger.addHandler(handler);
        try {
            Assert.assertTrue(reporter.createComponentMetric(MetricConstants.COUNTER, name, "Clash", LABELS));
            String[] other = {"other"};
            Assert.assertFalse(reporter.createComponentMetric(MetricConstants.COUNTER, name, "Clash", other));
            Assert.assertFalse(reporter.createComponentMetric(MetricConstants.COUNTER, name, "Clash", other));
            Assert.assertEquals("The same refused shape gives one ERROR", 1, errors.size());
            Assert.assertTrue("The ERROR names both shapes",
                    errors.get(0).contains("[kind, name]") && errors.get(0).contains("[other]"));

            Assert.assertFalse(reporter.createComponentMetric(MetricConstants.GAUGE, name, "Clash", LABELS));
            Assert.assertFalse(reporter.createComponentMetric(MetricConstants.GAUGE, name, "Clash", LABELS));
            Assert.assertEquals("Another refused shape gives its own ERROR", 2, errors.size());
        } finally {
            logger.removeHandler(handler);
        }
    }

    @Test
    public void testGaugeIsSet() throws Exception {
        PrometheusReporter reporter = new PrometheusReporter();
        String gauge = "pd_legacy_open_sessions";
        String counter = "pd_legacy_transport_faults_total";
        Assert.assertTrue(reporter.createComponentMetric(MetricConstants.GAUGE, gauge, "Open sessions", LABELS));
        Assert.assertTrue(reporter.createComponentMetric(MetricConstants.COUNTER, counter, "Faults", LABELS));

        Assert.assertTrue(reporter.setGauge(gauge, new String[]{"CONNECTION", "c1"}, 7));
        Assert.assertEquals(7.0, ((Gauge) metricMap(reporter).get(gauge)).labels("CONNECTION", "c1").get(), 0.0);

        Assert.assertFalse("Too few label values", reporter.setGauge(gauge, new String[]{"CONNECTION"}, 8));
        Assert.assertFalse("No label values", reporter.setGauge(gauge, null, 8));
        Assert.assertFalse("A counter is not a gauge", reporter.setGauge(counter, new String[]{"CONNECTION", "c1"}, 8));
        Assert.assertFalse("A name that is not registered", reporter.setGauge("pd_legacy_unknown", LABELS, 8));
        Assert.assertEquals(7.0, ((Gauge) metricMap(reporter).get(gauge)).labels("CONNECTION", "c1").get(), 0.0);
    }

    @Test
    public void testOtherTypeIsNotRegistered() throws Exception {
        PrometheusReporter reporter = new PrometheusReporter();
        String name = "pd_legacy_latency_seconds";

        Assert.assertFalse(reporter.createComponentMetric(MetricConstants.HISTOGRAM, name, "Latency", LABELS));
        Assert.assertFalse(metricMap(reporter).containsKey(name));
    }

    @Test
    public void testMetricMapIsConcurrent() throws Exception {
        Assert.assertTrue(metricMap(new PrometheusReporter()) instanceof ConcurrentHashMap);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> metricMap(PrometheusReporter reporter) throws Exception {
        Field field = PrometheusReporter.class.getDeclaredField("metricMap");
        field.setAccessible(true);
        return (Map<String, Object>) field.get(reporter);
    }
}

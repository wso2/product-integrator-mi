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

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.config.mapper.ConfigParser;
import org.wso2.micro.integrator.observability.metric.publisher.MetricFormatter;
import org.wso2.micro.integrator.observability.util.MetricConstants;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class PrometheusReporterComponentMetricTest {

    private static final String[] LABELS = {"kind", "name"};
    private static final String[] SERVICE_LABELS = {MetricConstants.SERVICE_NAME, MetricConstants.SERVICE_TYPE};
    private static final String[] API_LABELS =
            {MetricConstants.SERVICE_NAME, MetricConstants.SERVICE_TYPE, MetricConstants.INVOCATION_URL};

    private final PrometheusReporter reporter = new PrometheusReporter();
    private Object savedConfigs;

    @Before
    public void setUp() throws Exception {
        savedConfigs = setStatic(ConfigParser.class, "parsedConfigs", new HashMap<String, Object>());
    }

    @After
    public void tearDown() throws Exception {
        try {
            for (Object collector : metricMap(reporter).values()) {
                CollectorRegistry.defaultRegistry.unregister((Collector) collector);
            }
        } finally {
            setStatic(ConfigParser.class, "parsedConfigs", savedConfigs);
        }
    }

    @Test
    public void testCounterIsRegisteredOnceAndUsedAgain() throws Exception {
        String name = "test_legacy_files_total";

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
        String name = "test_legacy_failures_total";

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
        String name = "test_legacy_clash_total";
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
    public void testDuplicateLabelsDoNotPublishACounterOrGauge() {
        String healthy = "component_healthy";
        Assert.assertTrue(reporter.createComponentMetric("Gauge", healthy, "Active", new String[0]));
        Assert.assertTrue(reporter.setGauge(healthy, new String[0], 7));

        for (String type : new String[]{"Counter", "Gauge"}) {
            String name = "component_" + type;
            try {
                reporter.createComponentMetric(type, name, "Operations", new String[]{"file_id", "file_id"});
                Assert.fail("Duplicate label names must fail before registration");
            } catch (IllegalArgumentException expected) {
                Assert.assertTrue(reporter.createComponentMetric(type, name, "Operations", new String[]{"file_id"}));
            }
            if ("Gauge".equals(type)) {
                Assert.assertTrue(reporter.setGauge(name, new String[]{"one"}, 1));
            } else {
                reporter.incrementCount(name, new String[]{"one"});
            }
        }
        String text = MetricFormatter.formatMetrics(CollectorRegistry.defaultRegistry.metricFamilySamples());
        Assert.assertTrue(text.contains("component_healthy 7.0\n"));
        Assert.assertTrue(text.contains("component_Gauge{file_id=\"one\"} 1.0\n"));
        Assert.assertTrue(text.contains("component_Counter{file_id=\"one\"} 1.0\n"));
    }

    @Test
    public void testHelpAndLabelsInLegacyScrape() {
        String name = "component_state";
        String help = "Path \\ and \"quote\"\nnext";
        Assert.assertTrue(reporter.createComponentMetric("Gauge", name, help, new String[]{"connection", "kind"}));
        Assert.assertTrue(reporter.setGauge(name, new String[]{"a\\b\"c\nd", "file"}, 2));
        String text = MetricFormatter.formatMetrics(CollectorRegistry.defaultRegistry.metricFamilySamples());
        Assert.assertTrue(text.contains("# HELP component_state Path \\\\ and \"quote\"\\nnext\n"
                + "# TYPE component_state gauge\n"));
        Assert.assertTrue(text.contains("component_state{connection=\"a\\\\b\\\"c\\nd\", kind=\"file\"} 2.0\n"));
    }

    @Test
    public void testGaugeIsSet() throws Exception {
        String gauge = "test_legacy_open_sessions";
        String counter = "test_legacy_transport_faults_total";
        Assert.assertTrue(reporter.createComponentMetric(MetricConstants.GAUGE, gauge, "Open sessions", LABELS));
        Assert.assertTrue(reporter.createComponentMetric(MetricConstants.COUNTER, counter, "Faults", LABELS));

        Assert.assertTrue(reporter.setGauge(gauge, new String[]{"CONNECTION", "c1"}, 7));
        Assert.assertEquals(7.0, ((Gauge) metricMap(reporter).get(gauge)).labels("CONNECTION", "c1").get(), 0.0);

        Assert.assertFalse("Too few label values", reporter.setGauge(gauge, new String[]{"CONNECTION"}, 8));
        Assert.assertFalse("No label values", reporter.setGauge(gauge, null, 8));
        Assert.assertFalse("A counter is not a gauge", reporter.setGauge(counter, new String[]{"CONNECTION", "c1"}, 8));
        Assert.assertFalse("A name that is not registered", reporter.setGauge("test_legacy_unknown", LABELS, 8));
        Assert.assertEquals(7.0, ((Gauge) metricMap(reporter).get(gauge)).labels("CONNECTION", "c1").get(), 0.0);
    }

    @Test
    public void testOtherTypeIsNotRegistered() throws Exception {
        String name = "test_legacy_latency_seconds";

        Assert.assertFalse(reporter.createComponentMetric(MetricConstants.HISTOGRAM, name, "Latency", LABELS));
        Assert.assertFalse(metricMap(reporter).containsKey(name));
    }

    @Test
    public void testMetricMapIsConcurrent() throws Exception {
        Assert.assertTrue(metricMap(reporter) instanceof ConcurrentHashMap);
    }

    @Test
    public void testLabelNamesAreCopied() throws Exception {
        String name = "test_legacy_owned_names";
        String[] names = {"owner"};

        Assert.assertTrue(reporter.createComponentMetric(MetricConstants.GAUGE, name, "Owned names", names));
        names[0] = "changed";

        Assert.assertTrue("The registered shape stays the one of the registration",
                reporter.createComponentMetric(MetricConstants.GAUGE, name, "Owned names", new String[]{"owner"}));
        Assert.assertTrue(reporter.setGauge(name, new String[]{"first"}, 7));
        Assert.assertEquals("The metric keeps the label name of the registration",
                Collections.singletonList("owner"), labelNames(reporter, name));
    }

    @Test
    public void testLabelValuesAreCopied() throws Exception {
        String name = "test_legacy_owned_values";
        Assert.assertTrue(reporter.createComponentMetric(MetricConstants.GAUGE, name, "Owned values",
                new String[]{"owner"}));

        String[] values = {"first"};
        Assert.assertTrue(reporter.setGauge(name, values, 7));
        values[0] = "second";
        Assert.assertTrue(reporter.setGauge(name, values, 9));

        Map<List<String>, Double> samples = samples(reporter, name);
        Assert.assertEquals("Each call set its own series", 2, samples.size());
        Assert.assertEquals(7.0, samples.get(Collections.singletonList("first")), 0.0);
        Assert.assertEquals(9.0, samples.get(Collections.singletonList("second")), 0.0);
    }

    @Test
    public void testRefusedShapeIsCopied() throws Exception {
        String name = "test_legacy_owned_shape_total";
        try (Errors errors = new Errors(name)) {
            Assert.assertTrue(reporter.createComponentMetric(MetricConstants.COUNTER, name, "Owned shape", LABELS));
            String[] other = {"other"};
            Assert.assertFalse(reporter.createComponentMetric(MetricConstants.COUNTER, name, "Owned shape", other));
            other[0] = "changed";

            Assert.assertFalse(reporter.createComponentMetric(MetricConstants.COUNTER, name, "Owned shape",
                    new String[]{"other"}));
            Assert.assertEquals("The first refused shape is still known, so it gives no second ERROR", 1,
                    errors.count());

            Assert.assertFalse(reporter.createComponentMetric(MetricConstants.COUNTER, name, "Owned shape", other));
            Assert.assertEquals("The changed array is another refused shape", 2, errors.count());
        }
    }

    @Test
    public void testComponentMetricDoesNotTakeTheKeyOfAServerMetric() throws Exception {
        String key = "test_legacy_timer_lookup";
        try (Errors errors = new Errors(key)) {
            reporter.createMetrics("PROXY", MetricConstants.HISTOGRAM, key, "Latency", SERVICE_LABELS);
            Object histogram = metricMap(reporter).get(key);
            Assert.assertTrue(histogram instanceof Histogram);

            Assert.assertFalse("The key is in use by a metric of the server",
                    reporter.createComponentMetric(MetricConstants.COUNTER, key, "Files", LABELS));
            Assert.assertFalse(reporter.createComponentMetric(MetricConstants.COUNTER, key, "Files", LABELS));
            Assert.assertEquals("The refused registration gives one ERROR", 1, errors.count());

            Assert.assertSame("The metric of the server stays", histogram, metricMap(reporter).get(key));
            Object timer = reporter.getTimer(key, new String[]{"proxy", "proxy-service"});
            Assert.assertNotNull(timer);
            reporter.observeTime(timer);
            Assert.assertFalse("The refused metric is no gauge", reporter.setGauge(key, new String[]{"a", "b"}, 1));
            assertNoCollector(key);
        }
    }

    @Test
    public void testServerMetricDoesNotTakeTheKeyOfAComponentMetric() throws Exception {
        String key = "test_legacy_gauge_lookup";
        try (Errors errors = new Errors(key)) {
            Assert.assertTrue(reporter.createComponentMetric(MetricConstants.GAUGE, key, "Open sessions", LABELS));
            Assert.assertTrue(reporter.setGauge(key, new String[]{"CONNECTION", "c1"}, 7));
            Object gauge = metricMap(reporter).get(key);

            reporter.createMetrics("API", MetricConstants.HISTOGRAM, key, "Latency", API_LABELS);
            Assert.assertEquals("The refused registration gives an ERROR", 1, errors.count());
            Assert.assertSame("The metric of the component stays", gauge, metricMap(reporter).get(key));
            Assert.assertTrue("The same shape is used again",
                    reporter.createComponentMetric(MetricConstants.GAUGE, key, "Open sessions", LABELS));
            Assert.assertSame(gauge, metricMap(reporter).get(key));
            Assert.assertTrue(reporter.setGauge(key, new String[]{"CONNECTION", "c1"}, 8));
            Assert.assertEquals(8.0, samples(reporter, key).get(Arrays.asList("CONNECTION", "c1")), 0.0);
        }

        // The refused call registered nothing, so the metric of the server can still be registered.
        String free = "test_legacy_api_timer_lookup";
        reporter.createMetrics("API", MetricConstants.HISTOGRAM, free, "Latency", API_LABELS);
        Object timer = reporter.getTimer(free, new String[]{"api", "api", "/context"});
        Assert.assertNotNull(timer);
        reporter.observeTime(timer);
    }

    @Test
    public void testErrorMetricAndComponentMetricKeepTheirKeys() throws Exception {
        String key = "test_legacy_component_first_total";
        try (Errors errors = new Errors(key)) {
            Assert.assertTrue(reporter.createComponentMetric(MetricConstants.COUNTER, key, "Files", LABELS));
            Object counter = metricMap(reporter).get(key);

            reporter.initErrorMetrics("PROXY", MetricConstants.COUNTER, key, "Errors", SERVICE_LABELS);
            Assert.assertEquals("The refused registration gives an ERROR", 1, errors.count());
            Assert.assertSame("The metric of the component stays", counter, metricMap(reporter).get(key));
            reporter.incrementCount(key, new String[]{"INBOUND", "a"});
            Assert.assertTrue(reporter.createComponentMetric(MetricConstants.COUNTER, key, "Files", LABELS));
        }

        // The refused call registered nothing, so the error metric of the server can still be registered.
        String serverKey = "test_legacy_error_lookup";
        try (Errors errors = new Errors(serverKey)) {
            reporter.initErrorMetrics("PROXY", MetricConstants.COUNTER, serverKey, "Errors", SERVICE_LABELS);
            Object errorCounter = metricMap(reporter).get(serverKey);
            Assert.assertTrue(errorCounter instanceof Counter);

            Assert.assertFalse("The key is in use by a metric of the server",
                    reporter.createComponentMetric(MetricConstants.GAUGE, serverKey, "Open sessions", LABELS));
            Assert.assertEquals(1, errors.count());
            Assert.assertSame("The metric of the server stays", errorCounter, metricMap(reporter).get(serverKey));
            reporter.incrementCount(serverKey, new String[]{"proxy", "proxy-service"});
            assertNoCollector(serverKey);
        }
    }

    @Test
    public void testComponentRegistrationWaitsForAServerRegistration() throws Exception {
        String key = "test_legacy_held_lookup";
        HeldConfigs configs = new HeldConfigs();
        setStatic(ConfigParser.class, "parsedConfigs", configs);
        AtomicReference<Boolean> registered = new AtomicReference<>();
        // The server registration checked the key and holds before it registers its metric.
        Thread server = new Thread(() -> reporter.createMetrics("INBOUND_ENDPOINT", MetricConstants.HISTOGRAM, key,
                "Latency", SERVICE_LABELS));
        Thread component = new Thread(() -> registered.set(
                reporter.createComponentMetric(MetricConstants.COUNTER, key, "Files", LABELS)));
        AtomicReference<Throwable> workerFailure = new AtomicReference<>();
        Thread.UncaughtExceptionHandler handler = (thread, failure) -> workerFailure.compareAndSet(null, failure);
        server.setUncaughtExceptionHandler(handler);
        component.setUncaughtExceptionHandler(handler);
        try (Errors errors = new Errors(key)) {
            server.start();
            Assert.assertTrue("The server registration reads the configs", configs.entered.await(10, TimeUnit.SECONDS));
            Assert.assertFalse(metricMap(reporter).containsKey(key));

            component.start();
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (component.getState() != Thread.State.BLOCKED && System.nanoTime() < end) {
                Thread.sleep(10);
            }
            Assert.assertEquals("The component registration waits for the lock of the reporter", Thread.State.BLOCKED,
                    component.getState());
            Assert.assertFalse("Nothing is registered under the key yet", metricMap(reporter).containsKey(key));
            Assert.assertNull(registered.get());

            configs.release.countDown();
            server.join(10000);
            component.join(10000);
            Assert.assertFalse(server.isAlive() || component.isAlive());

            Assert.assertEquals("The key stays with the metric of the server", Boolean.FALSE, registered.get());
            Assert.assertTrue(metricMap(reporter).get(key) instanceof Histogram);
            Object timer = reporter.getTimer(key, new String[]{"inbound", "inbound-endpoint"});
            Assert.assertNotNull(timer);
            reporter.observeTime(timer);
            Assert.assertEquals(1, errors.count());
            assertNoCollector(key);
        } finally {
            configs.release.countDown();
            server.join(10000);
            component.join(10000);
            Assert.assertFalse("Workers must finish before teardown", server.isAlive() || component.isAlive());
            if (workerFailure.get() != null) {
                throw new AssertionError("Metric registration worker failed", workerFailure.get());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> metricMap(PrometheusReporter reporter) throws Exception {
        Field field = PrometheusReporter.class.getDeclaredField("metricMap");
        field.setAccessible(true);
        return (Map<String, Object>) field.get(reporter);
    }

    private static List<String> labelNames(PrometheusReporter reporter, String name) throws Exception {
        Collector collector = (Collector) metricMap(reporter).get(name);
        return new ArrayList<>(collector.collect().get(0).samples.get(0).labelNames);
    }

    // The value of each series of a metric, by its label values. A series that is collected twice fails.
    // A counter series exists at 0 before its first event, and an existing series keeps its value.
    @Test
    public void testCounterSeriesStartsAtZero() throws Exception {
        String counter = "test_legacy_failures_total";
        String gauge = "test_legacy_fail_states_pending";
        List<String> series = Arrays.asList("INBOUND", "a");
        Assert.assertTrue(reporter.createComponentMetric(MetricConstants.COUNTER, counter, "Failures", LABELS));
        Assert.assertTrue(reporter.createComponentMetric(MetricConstants.GAUGE, gauge, "Pending", LABELS));
        Assert.assertTrue("No series before the first event", counterSamples(reporter, counter).isEmpty());

        Assert.assertTrue(reporter.initCounter(counter, new String[]{"INBOUND", "a"}));
        Assert.assertEquals(Collections.singletonMap(series, 0.0), counterSamples(reporter, counter));

        reporter.incrementCount(counter, new String[]{"INBOUND", "a"});
        Assert.assertTrue(reporter.initCounter(counter, new String[]{"INBOUND", "a"}));
        Assert.assertEquals(Collections.singletonMap(series, 1.0), counterSamples(reporter, counter));

        Assert.assertFalse("A gauge is not a counter", reporter.initCounter(gauge, new String[]{"INBOUND", "a"}));
        Assert.assertFalse("Too few label values", reporter.initCounter(counter, new String[]{"INBOUND"}));
        Assert.assertFalse("No label values", reporter.initCounter(counter, null));
        Assert.assertFalse("A name that is not registered", reporter.initCounter("test_legacy_unknown", LABELS));
        Assert.assertEquals(Collections.singletonMap(series, 1.0), counterSamples(reporter, counter));
    }

    // The value samples of a counter. The library also gives a creation time sample for each series.
    private static Map<List<String>, Double> counterSamples(PrometheusReporter reporter, String name)
            throws Exception {
        Map<List<String>, Double> samples = new HashMap<>();
        Collector collector = (Collector) metricMap(reporter).get(name);
        for (Collector.MetricFamilySamples family : collector.collect()) {
            for (Collector.MetricFamilySamples.Sample sample : family.samples) {
                if (sample.name.endsWith("_total")) {
                    Assert.assertNull("Each series is collected once",
                            samples.put(new ArrayList<>(sample.labelValues), sample.value));
                }
            }
        }
        return samples;
    }

    private static Map<List<String>, Double> samples(PrometheusReporter reporter, String name) throws Exception {
        Map<List<String>, Double> samples = new HashMap<>();
        Collector collector = (Collector) metricMap(reporter).get(name);
        for (Collector.MetricFamilySamples family : collector.collect()) {
            for (Collector.MetricFamilySamples.Sample sample : family.samples) {
                Assert.assertNull("Each series is collected once",
                        samples.put(new ArrayList<>(sample.labelValues), sample.value));
            }
        }
        return samples;
    }

    // The metric library takes a name once. So a name that it takes here has no metric.
    private static void assertNoCollector(String name) {
        CollectorRegistry.defaultRegistry.unregister(Gauge.build().name(name).help("Probe").register());
    }

    private static Object setStatic(Class<?> type, String name, Object value) throws Exception {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        Object previous = field.get(null);
        field.set(null, value);
        return previous;
    }

    /**
     * Counts the ERROR lines of the reporter that name one metric.
     */
    private static class Errors extends Handler implements AutoCloseable {

        private final String name;
        private final AtomicInteger count = new AtomicInteger();

        Errors(String name) {
            this.name = name;
            Logger.getLogger(PrometheusReporter.class.getName()).addHandler(this);
        }

        int count() {
            return count.get();
        }

        @Override
        public void publish(LogRecord record) {
            if (record.getLevel() == Level.SEVERE && record.getMessage().contains(name)) {
                count.incrementAndGet();
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
            Logger.getLogger(PrometheusReporter.class.getName()).removeHandler(this);
        }
    }

    /**
     * The parsed configs of the server. The first read waits until the test lets it go. A registration of a
     * metric of the server reads the configs after it checked its key and before it registers the metric.
     */
    private static class HeldConfigs extends HashMap<String, Object> {

        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        @Override
        public Object get(Object key) {
            entered.countDown();
            try {
                if (!release.await(10, TimeUnit.SECONDS)) {
                    throw new AssertionError("Timed out waiting for the test to release the registration");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted while waiting for the test", e);
            }
            return super.get(key);
        }
    }
}

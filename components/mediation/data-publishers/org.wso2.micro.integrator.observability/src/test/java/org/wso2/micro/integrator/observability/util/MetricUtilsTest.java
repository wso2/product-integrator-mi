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

package org.wso2.micro.integrator.observability.util;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.config.mapper.ConfigParser;
import org.wso2.micro.integrator.observability.metric.handler.MetricReporter;
import org.wso2.micro.integrator.observability.metric.handler.prometheus.reporter.PrometheusReporterV1;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MetricUtilsTest {

    private Object savedConfigs;
    private Object savedReporter;

    @Before
    public void setUp() throws Exception {
        Map<String, Object> configs = new HashMap<>();
        configs.put(MetricConstants.METRIC_HANDLER + ".metric_reporter", SlowReporter.class.getName());
        savedConfigs = setStatic(ConfigParser.class, "parsedConfigs", configs);
        savedReporter = setStatic(MetricUtils.class, "metricReporter", null);
        SlowReporter.CREATED.set(0);
    }

    @After
    public void tearDown() throws Exception {
        setStatic(ConfigParser.class, "parsedConfigs", savedConfigs);
        setStatic(MetricUtils.class, "metricReporter", savedReporter);
    }

    @Test
    public void testTwoThreadsThatAskAtOnceGetOneReporter() throws Exception {
        CyclicBarrier start = new CyclicBarrier(2);
        Callable<MetricReporter> ask = () -> {
            start.await(10, TimeUnit.SECONDS);
            return MetricUtils.getMetricReporter();
        };
        ExecutorService threads = Executors.newFixedThreadPool(2);
        try {
            Future<MetricReporter> first = threads.submit(ask);
            Future<MetricReporter> second = threads.submit(ask);
            MetricReporter reporter = first.get(10, TimeUnit.SECONDS);

            Assert.assertTrue(reporter instanceof SlowReporter);
            Assert.assertSame("Both threads get the same reporter", reporter, second.get(10, TimeUnit.SECONDS));
            Assert.assertEquals("The reporter is made once", 1, SlowReporter.CREATED.get());
            Assert.assertSame(reporter, MetricUtils.getMetricReporter());
        } finally {
            threads.shutdownNow();
            Assert.assertTrue("No thread of the test runs after it",
                    threads.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private static Object setStatic(Class<?> type, String name, Object value) throws Exception {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        Object previous = field.get(null);
        field.set(null, value);
        return previous;
    }

    /**
     * A reporter that takes time to make, so that a second thread asks while the first one makes it.
     */
    public static class SlowReporter extends PrometheusReporterV1 {

        static final AtomicInteger CREATED = new AtomicInteger();

        public SlowReporter() {
            CREATED.incrementAndGet();
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}

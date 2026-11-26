/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
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
package org.wso2.micro.integrator.observability.metric.handler;

import junit.framework.TestCase;

/**
 * Unit tests for {@link MetricHandler}.
 */
public class MetricHandlerTest extends TestCase {

    public void testGetServiceInvokePortWithExplicitPort() {
        assertEquals(8290, MetricHandler.getServiceInvokePort("http://localhost:8290/services/"));
    }

    public void testGetServiceInvokePortWithoutExplicitPort() {
        assertEquals(-1, MetricHandler.getServiceInvokePort("http://scanner.example/"));
    }

    public void testGetServiceInvokePortWithMalformedPrefix() {
        assertEquals(-1, MetricHandler.getServiceInvokePort("not a valid URI"));
    }

    public void testGetServiceInvokePortWithHttpsPort() {
        assertEquals(443, MetricHandler.getServiceInvokePort("https://example.com:443/"));
    }

    public void testGetServiceInvokePortWithIpv6Address() {
        assertEquals(8290, MetricHandler.getServiceInvokePort("http://[2001:db8::1]:8290/api/v2/resource"));
    }

    public void testGetServiceInvokePortWithInvalidPort() {
        assertEquals(-1, MetricHandler.getServiceInvokePort("http://localhost:not-a-port/api/v2/resource"));
    }
}

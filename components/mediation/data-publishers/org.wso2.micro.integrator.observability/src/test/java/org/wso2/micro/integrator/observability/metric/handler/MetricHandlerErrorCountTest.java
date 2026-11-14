/*
 * Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com).
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

import org.apache.axis2.context.ConfigurationContext;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.api.API;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.rest.RESTConstants;
import org.junit.Before;
import org.junit.Test;
import org.wso2.micro.integrator.observability.util.MetricConstants;

import java.lang.reflect.Field;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests which error counter MetricHandler#handleError increments for each kind of artifact.
 */
public class MetricHandlerErrorCountTest {

    private static final String INBOUND_NAME = "OrderInbound";
    private static final String HTTP_SERVICE_PREFIX = "http://localhost:8290/";

    static {
        // MetricHandler reads the port offset in a static initializer.
        System.setProperty(MetricConstants.PORT_OFFSET, "10");
    }

    private MetricHandler metricHandler;
    private MetricReporter metricReporter;
    private SynapseConfiguration synapseConfiguration;

    @Before
    public void setUp() throws Exception {
        metricHandler = new MetricHandler();
        metricReporter = mock(MetricReporter.class);
        Field reporterField = MetricHandler.class.getDeclaredField("metricReporterInstance");
        reporterField.setAccessible(true);
        reporterField.set(metricHandler, metricReporter);
        synapseConfiguration = mock(SynapseConfiguration.class);
    }

    @Test
    public void testInboundErrorWithoutTransportInUrlIsCounted() throws Exception {
        // JMS, File, Kafka, etc. inbound endpoints do not set TransportInURL.
        Axis2MessageContext synCtx = createMessageContext();
        synCtx.setProperty(SynapseConstants.IS_INBOUND, true);
        synCtx.setProperty(SynapseConstants.INBOUND_ENDPOINT_NAME, INBOUND_NAME);

        metricHandler.handleError(synCtx);

        verifyInboundErrorCounted();
        verifyNoApiOrProxyErrorCounted();
    }

    @Test
    public void testHttpInboundDispatchedToApiIsCountedOnlyAsInboundError() throws Exception {
        Axis2MessageContext synCtx = createMessageContext();
        synCtx.setProperty(SynapseConstants.IS_INBOUND, true);
        synCtx.setProperty(SynapseConstants.INBOUND_ENDPOINT_NAME, INBOUND_NAME);
        synCtx.setProperty(RESTConstants.SYNAPSE_REST_API, "OrderAPI");
        setTransportInUrl(synCtx, "/orders");

        metricHandler.handleError(synCtx);

        verifyInboundErrorCounted();
        verifyNoApiOrProxyErrorCounted();
    }

    @Test
    public void testProxyErrorIsCountedAsProxyError() throws Exception {
        Axis2MessageContext synCtx = createMessageContext();
        synCtx.setProperty(SynapseConstants.PROXY_SERVICE, "OrderProxy");
        setTransportInUrl(synCtx, "/services/OrderProxy");

        metricHandler.handleError(synCtx);

        verify(metricReporter, times(1)).incrementCount(eq(MetricConstants.PROXY_REQUEST_COUNT_ERROR_TOTAL),
                aryEq(new String[]{"OrderProxy", SynapseConstants.PROXY_SERVICE_TYPE}));
        verify(metricReporter, never()).incrementCount(
                eq(MetricConstants.INBOUND_ENDPOINT_REQUEST_COUNT_ERROR_TOTAL), any());
    }

    @Test
    public void testApiErrorIsCountedWhenContextContainsServices() throws Exception {
        API api = new API("OrderServicesAPI", "/order-services");
        when(synapseConfiguration.getAPIs()).thenReturn(Collections.singletonList(api));
        Axis2MessageContext synCtx = createMessageContext();
        synCtx.setProperty(RESTConstants.SYNAPSE_REST_API, api.getName());
        setTransportInUrl(synCtx, "/order-services");

        metricHandler.handleError(synCtx);

        verify(metricReporter, times(1)).incrementCount(eq(MetricConstants.API_REQUEST_COUNT_ERROR_TOTAL),
                aryEq(new String[]{"OrderServicesAPI", SynapseConstants.FAIL_SAFE_MODE_API,
                        HTTP_SERVICE_PREFIX + "order-services"}));
    }

    @Test
    public void testApiErrorIsCountedWhenContextOverlapsServicePath() throws Exception {
        // An API context can share its prefix with a custom Axis2 service path (eg:- servicePath = "api").
        API api = new API("OrdersAPI", "/api/orders");
        when(synapseConfiguration.getAPIs()).thenReturn(Collections.singletonList(api));
        Axis2MessageContext synCtx = createMessageContext();
        synCtx.setProperty(RESTConstants.SYNAPSE_REST_API, api.getName());
        setTransportInUrl(synCtx, "/api/orders");
        ConfigurationContext configurationContext = mock(ConfigurationContext.class);
        when(configurationContext.getServicePath()).thenReturn("api");
        synCtx.getAxis2MessageContext().setConfigurationContext(configurationContext);

        metricHandler.handleError(synCtx);

        verify(metricReporter, times(1)).incrementCount(eq(MetricConstants.API_REQUEST_COUNT_ERROR_TOTAL),
                aryEq(new String[]{"OrdersAPI", SynapseConstants.FAIL_SAFE_MODE_API,
                        HTTP_SERVICE_PREFIX + "api/orders"}));
    }

    @Test
    public void testErrorUnderServicePathIsNotCountedAsApiError() throws Exception {
        API api = new API("OrdersAPI", "/orders");
        when(synapseConfiguration.getAPIs()).thenReturn(Collections.singletonList(api));
        Axis2MessageContext synCtx = createMessageContext();
        synCtx.setProperty(RESTConstants.SYNAPSE_REST_API, "SomeAPI");
        setTransportInUrl(synCtx, "/services/OrderDataService");

        metricHandler.handleError(synCtx);

        verifyNoApiOrProxyErrorCounted();
    }

    @Test
    public void testInboundWithoutEndpointNameIsNotCounted() throws Exception {
        // gRPC inbound and the WebSocket client relay set isInbound without the inbound endpoint name.
        Axis2MessageContext synCtx = createMessageContext();
        synCtx.setProperty(SynapseConstants.IS_INBOUND, true);

        metricHandler.handleRequestInFlow(synCtx);
        metricHandler.handleError(synCtx);

        verify(metricReporter, never()).incrementCount(any(), any());
        verify(metricReporter, never()).getTimer(any(), any());
        assertEquals(true, synCtx.getProperty(SynapseConstants.IS_ERROR_COUNT_ALREADY_PROCESSED));
    }

    @Test
    public void testErrorIsCountedOnlyOnce() throws Exception {
        Axis2MessageContext synCtx = createMessageContext();
        synCtx.setProperty(SynapseConstants.IS_INBOUND, true);
        synCtx.setProperty(SynapseConstants.INBOUND_ENDPOINT_NAME, INBOUND_NAME);

        metricHandler.handleError(synCtx);
        metricHandler.handleError(synCtx);

        verifyInboundErrorCounted();
    }

    private Axis2MessageContext createMessageContext() throws Exception {
        SynapseEnvironment synapseEnvironment = mock(SynapseEnvironment.class);
        when(synapseEnvironment.getSynapseConfiguration()).thenReturn(synapseConfiguration);
        org.apache.axis2.context.MessageContext axis2MessageContext = new org.apache.axis2.context.MessageContext();
        axis2MessageContext.setProperty(MetricConstants.SERVICE_PREFIX, HTTP_SERVICE_PREFIX);
        return new Axis2MessageContext(axis2MessageContext, synapseConfiguration, synapseEnvironment);
    }

    private void setTransportInUrl(Axis2MessageContext synCtx, String url) {
        synCtx.getAxis2MessageContext().setProperty(MetricConstants.TRANSPORT_IN_URL, url);
    }

    private void verifyInboundErrorCounted() {
        verify(metricReporter, times(1)).incrementCount(
                eq(MetricConstants.INBOUND_ENDPOINT_REQUEST_COUNT_ERROR_TOTAL),
                aryEq(new String[]{INBOUND_NAME, MetricConstants.INBOUND_ENDPOINT}));
    }

    private void verifyNoApiOrProxyErrorCounted() {
        verify(metricReporter, never()).incrementCount(eq(MetricConstants.API_REQUEST_COUNT_ERROR_TOTAL), any());
        verify(metricReporter, never()).incrementCount(eq(MetricConstants.PROXY_REQUEST_COUNT_ERROR_TOTAL), any());
    }
}

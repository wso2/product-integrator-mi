/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.micro.integrator.management.apis.security.handler;

import org.apache.synapse.MessageContext;
import org.apache.synapse.rest.RESTConstants;
import org.junit.Assert;
import org.junit.Test;
import org.wso2.micro.integrator.management.apis.Constants;
import java.util.ArrayList;
import java.util.List;

/**
 * InternalAPIDispatcher resolves the request path and caches it under RESTConstants.REST_FULL_REQUEST_PATH
 * before invoking any handler, and needsHandling() reads that same value back via
 * ApiUtils.getFullRequestPath(). These tests therefore set the property directly instead of re-deriving it
 * from the request target.
 */
public class SecurityHandlerAdapterTest {

    /**
     * Tests a handler configuration similar to the following with no resources defined.
     * <p>
     * <handler name="SampleInternalApiHandlerWithNoResources"
     *          class="internal.http.api.SampleInternalApiHandlerWithNoResources"/>
     */
    @Test
    public void testHandledWithNoResource() {

        MessageContext messageContext = new TestMessageContext();
        messageContext.setProperty(RESTConstants.REST_FULL_REQUEST_PATH, "/sectest/resource1");

        TestSecurityHandler internalAPIHandler = new TestSecurityHandler("/sectest");

        //test with no resources
        internalAPIHandler.setResources(new ArrayList<>());
        internalAPIHandler.invoke(messageContext);
        Assert.assertTrue("Handler should be engaged when no resources are explictely defined, but it was not engaged.",
                          internalAPIHandler.isHandleTriggered());
    }

    /**
     * Tests a handler configuration similar to the following.
     * <p>
     * <handler name="SampleInternalApiHandlerWithCustomResources"
     *          class="internal.http.api.SampleInternalApiHandlerWithCustomResources">
     *  <resources>
     *      <resource>/resource1</resource>
     *      <resource>/resource2</resource>
     *  </resources>
     *</handler>
     */
    @Test
    public void testHandledWithCustomResources() {

        MessageContext messageContext = new TestMessageContext();

        TestSecurityHandler internalAPIHandler = new TestSecurityHandler("/sectest");
        List<String> resources = new ArrayList<>();
        resources.add("/resource1");
        resources.add("/resource2");
        internalAPIHandler.setResources(resources);

        //set message context with matching resource
        messageContext.setProperty(RESTConstants.REST_FULL_REQUEST_PATH, "/sectest/resource1");
        internalAPIHandler.invoke(messageContext);
        Assert.assertTrue("Handler should be engaged since resource 1 was defined, but it was not engaged.",
                          internalAPIHandler.isHandleTriggered());

        //set message context with matching resource
        messageContext.setProperty(RESTConstants.REST_FULL_REQUEST_PATH, "/sectest/resource2");
        internalAPIHandler.invoke(messageContext);
        Assert.assertTrue("Handler should be engaged since resource 2 was defined, but it was not engaged.",
                          internalAPIHandler.isHandleTriggered());

        //set message context with matching resource but containing a sub resource
        messageContext.setProperty(RESTConstants.REST_FULL_REQUEST_PATH, "/sectest/resource2/resource22");
        internalAPIHandler.invoke(messageContext);
        Assert.assertTrue("Handler should be engaged since resource 2 was defined, but it was not engaged.",
                          internalAPIHandler.isHandleTriggered());

        //set message context with a resource that is not matching
        messageContext.setProperty(RESTConstants.REST_FULL_REQUEST_PATH, "/sectest/resource3");
        internalAPIHandler.invoke(messageContext);
        Assert.assertFalse("Handler should not be engaged since resource 3 was not defined, but it was engaged.",
                           internalAPIHandler.isHandleTriggered());

    }

    /**
     * Tests a handler configuration similar to the following.
     *
     * <handler name="SampleInternalApiHandlerWithAllResources"
     *          class="internal.http.api.SampleInternalApiHandlerWithAllResources">
     *  <resources>
     *      <resource>/</resource>
     *  </resources>
     *</handler>
     */
    @Test
    public void testHandledWithAllResources() {

        MessageContext messageContext = new TestMessageContext();

        TestSecurityHandler internalAPIHandler = new TestSecurityHandler("/sectest");
        List<String> resources = new ArrayList<>();
        resources.add("/");
        internalAPIHandler.setResources(resources);

        //set message context with matching resource
        messageContext.setProperty(RESTConstants.REST_FULL_REQUEST_PATH, "/sectest/resource1");
        internalAPIHandler.invoke(messageContext);
        Assert.assertTrue("Handler should be engaged since all resources are defined, but it was not engaged.",
                          internalAPIHandler.isHandleTriggered());
    }

    /**
     * The bare management API root context ("/management") is not handled by the security handler.
     */
    @Test
    public void testRootContextExemption() {

        MessageContext messageContext = new TestMessageContext();

        TestSecurityHandler internalAPIHandler = new TestSecurityHandler(Constants.REST_API_CONTEXT);
        internalAPIHandler.setResources(new ArrayList<>());

        messageContext.setProperty(RESTConstants.REST_FULL_REQUEST_PATH, Constants.REST_API_CONTEXT);
        internalAPIHandler.invoke(messageContext);
        Assert.assertFalse("The bare management API root context should not be handled by the security handler, but it was.",
                            internalAPIHandler.isHandleTriggered());
    }

        /**
     * A request path that does not start with the handler's context must not be handled, even when it ends
     * with a configured resource.
     */
    @Test
    public void testPathOutsideHandlerContextIsNotHandled() {

        MessageContext messageContext = new TestMessageContext();

        TestSecurityHandler internalAPIHandler = new TestSecurityHandler("/sectest");
        List<String> resources = new ArrayList<>();
        resources.add("/resource1");
        internalAPIHandler.setResources(resources);

        messageContext.setProperty(RESTConstants.REST_FULL_REQUEST_PATH, "/othercontext/resource1");
        internalAPIHandler.invoke(messageContext);
        Assert.assertFalse("Handler should not be engaged for a path outside its context, but it was engaged.",
                           internalAPIHandler.isHandleTriggered());
    }

    /**
     * Only the bare management API root context is exempt. A path below the root context must still be handled.
     */
    @Test
    public void testPathBelowRootContextIsHandled() {

        MessageContext messageContext = new TestMessageContext();

        TestSecurityHandler internalAPIHandler = new TestSecurityHandler(Constants.REST_API_CONTEXT);
        internalAPIHandler.setResources(new ArrayList<>());

        messageContext.setProperty(RESTConstants.REST_FULL_REQUEST_PATH,
                Constants.REST_API_CONTEXT + "/users");
        internalAPIHandler.invoke(messageContext);
        Assert.assertTrue("Handler should be engaged for a path below the root context, but it was not engaged.",
                          internalAPIHandler.isHandleTriggered());
    }
}

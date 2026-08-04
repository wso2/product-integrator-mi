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

package org.wso2.micro.integrator.initializer.dashboard;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Unit tests for ManagementEndpoint, which parses the management URL advertised to ICP
 * in the heartbeat payload.
 */
public class ManagementEndpointTest {

    @Test
    public void testFromUrl_HttpsUrlWithPort_ParsesHostnameAndPort() {
        ManagementEndpoint endpoint = ManagementEndpoint.fromUrl("https://mi.example.com:8443");

        assertEquals("Hostname should be taken from the URL", "mi.example.com", endpoint.getHostname());
        assertEquals("Port should be taken from the URL", 8443, endpoint.getPort());
    }

    @Test
    public void testFromUrl_HttpsUrlWithoutPort_UsesDefaultHttpsPort() {
        ManagementEndpoint endpoint = ManagementEndpoint.fromUrl("https://mi.example.com/");

        assertEquals("Hostname should be taken from the URL", "mi.example.com", endpoint.getHostname());
        assertEquals("Port should default to the HTTPS port", 443, endpoint.getPort());
    }

    @Test
    public void testFromUrl_UnsupportedUrlParts_ThrowsException() {
        assertInvalid("http://mi.example.com", "HTTPS");
        assertInvalid("https://mi.example.com/gateway", "must not contain a path");
        assertInvalid("https://mi.example.com/%2F", "must not contain a path");
        assertInvalid("https://user@mi.example.com", "user info");
        assertInvalid("https://mi.example.com?region=us", "query");
        assertInvalid("https://mi.example.com#management", "fragment");
    }

    @Test
    public void testFromUrl_MalformedUrls_ThrowsException() {
        assertInvalid("", "empty");
        assertInvalid("mi.example.com:443", "HTTPS");
        assertInvalid("https://mi.example.com:", "port");
        assertInvalid("https://mi.example.com:65536", "between 1 and 65535");
    }

    private static void assertInvalid(String value, String expectedMessage) {
        try {
            ManagementEndpoint.fromUrl(value);
            fail("Expected URL to be rejected");
        } catch (IllegalArgumentException e) {
            if (!e.getMessage().contains(expectedMessage)) {
                fail("Expected error containing '" + expectedMessage + "' but got: " + e.getMessage());
            }
        }
    }
}

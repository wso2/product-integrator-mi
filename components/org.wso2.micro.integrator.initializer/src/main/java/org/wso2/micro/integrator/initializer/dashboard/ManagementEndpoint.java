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

import java.net.URI;
import java.net.URISyntaxException;

final class ManagementEndpoint {

    private static final int DEFAULT_HTTPS_PORT = 443;

    private final String hostname;
    private final int port;

    ManagementEndpoint(String hostname, int port) {
        if (hostname == null || hostname.isBlank()) {
            throw new IllegalArgumentException("URL must include a hostname.");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("URL port must be between 1 and 65535.");
        }
        this.hostname = hostname;
        this.port = port;
    }

    String getHostname() {
        return hostname;
    }

    int getPort() {
        return port;
    }

    static ManagementEndpoint fromUrl(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("URL must not be empty.");
        }

        URI uri;
        try {
            uri = new URI(value.trim());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Value must be a valid absolute URL.");
        }

        if (!"https".equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException("Only HTTPS URLs are supported for the management URL.");
        }
        if (uri.getHost() == null || uri.getHost().isBlank()) {
            throw new IllegalArgumentException("URL must include a valid hostname.");
        }
        if (uri.getUserInfo() != null || uri.getQuery() != null || uri.getFragment() != null) {
            throw new IllegalArgumentException("URL must not include user info, a query, or a fragment.");
        }
        String path = uri.getRawPath();
        if (path != null && !path.isEmpty() && !"/".equals(path)) {
            throw new IllegalArgumentException("The management URL must not contain a path.");
        }
        if (uri.getRawAuthority().endsWith(":")) {
            throw new IllegalArgumentException("URL must include a port after ':'.");
        }

        int port = uri.getPort() == -1 ? DEFAULT_HTTPS_PORT : uri.getPort();
        return new ManagementEndpoint(uri.getHost(), port);
    }
}

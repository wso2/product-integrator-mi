/*
 *  Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.wso2.carbon.connector.core;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Health of connector connections, for the management API. A connector registers the health of each connection
 * and unregisters it with the same supplier object. Each connection lifetime must own a distinct supplier,
 * including when it shares health state with a previous connection of the same name.
 * Health values are strings, booleans, finite numbers, maps and lists of these values.
 */
public final class ConnectionHealthRegistry {

    private static final Map<String, Map<String, Supplier<Map<String, Object>>>> connectors =
            new ConcurrentHashMap<>();

    private ConnectionHealthRegistry() {
    }

    public static void register(String connector, String connection, Supplier<Map<String, Object>> health) {
        connectors.compute(connector, (name, connections) -> {
            if (connections == null) {
                connections = new ConcurrentHashMap<>();
            }
            connections.put(connection, health);
            return connections;
        });
    }

    public static void unregister(String connector, String connection, Supplier<Map<String, Object>> health) {
        connectors.computeIfPresent(connector, (name, connections) -> {
            if (connections.get(connection) == health) {
                connections.remove(connection);
            }
            return connections.isEmpty() ? null : connections;
        });
    }

    /**
     * The health of each connection of a connector, by connection name. The map is a copy that can not be
     * changed, so the caller can not change the registry through it.
     *
     * @param connector the name of the connector
     * @return the suppliers by connection name, or an empty map when none is registered
     */
    public static Map<String, Supplier<Map<String, Object>>> getConnections(String connector) {
        Map<String, Supplier<Map<String, Object>>> connections = connectors.get(connector);
        if (connections == null) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new HashMap<>(connections));
    }
}

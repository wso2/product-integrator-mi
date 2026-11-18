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
package org.wso2.carbon.inbound.endpoint.protocol.generic;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Polling consumers by inbound endpoint name, for the management API. The processor registers its consumer after
 * it constructs it and unregisters it by identity on destroy. So an older processor never unregisters the newer
 * consumer of the same name.
 */
public final class GenericPollingConsumerRegistry {

    private static final Map<String, GenericPollingConsumer> consumers = new ConcurrentHashMap<>();

    private GenericPollingConsumerRegistry() {
    }

    public static void register(String inboundEndpointName, GenericPollingConsumer consumer) {
        consumers.put(inboundEndpointName, consumer);
    }

    public static void unregister(String inboundEndpointName, GenericPollingConsumer consumer) {
        consumers.remove(inboundEndpointName, consumer);
    }

    public static GenericPollingConsumer get(String inboundEndpointName) {
        return consumers.get(inboundEndpointName);
    }
}

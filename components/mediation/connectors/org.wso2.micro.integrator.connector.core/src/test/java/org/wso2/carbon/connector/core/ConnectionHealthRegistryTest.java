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

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

public class ConnectionHealthRegistryTest {

    @Test
    public void testRegisteredConnectionIsRead() {
        Supplier<Map<String, Object>> health = Collections::emptyMap;
        ConnectionHealthRegistry.register("read", "c1", health);
        try {
            Map<String, Supplier<Map<String, Object>>> connections = ConnectionHealthRegistry.getConnections("read");
            Assert.assertEquals(1, connections.size());
            Assert.assertSame(health, connections.get("c1"));
        } finally {
            ConnectionHealthRegistry.unregister("read", "c1", health);
        }
        Assert.assertTrue(ConnectionHealthRegistry.getConnections("read").isEmpty());
    }

    @Test
    public void testUnknownConnectorHasNoConnection() {
        Assert.assertTrue(ConnectionHealthRegistry.getConnections("unknown").isEmpty());
    }

    @Test
    public void testCallerCanNotChangeTheRegistry() {
        Supplier<Map<String, Object>> health = Collections::emptyMap;
        ConnectionHealthRegistry.register("copy", "c1", health);
        try {
            Map<String, Supplier<Map<String, Object>>> connections = ConnectionHealthRegistry.getConnections("copy");
            try {
                connections.remove("c1");
                Assert.fail("The map of the read call can not be changed");
            } catch (UnsupportedOperationException expected) {
                // The registry stays as it is.
            }
            try {
                ConnectionHealthRegistry.getConnections("unknown").put("c2", health);
                Assert.fail("The empty map of the read call can not be changed");
            } catch (UnsupportedOperationException expected) {
                // The registry stays as it is.
            }
            Assert.assertSame(health, ConnectionHealthRegistry.getConnections("copy").get("c1"));
            Assert.assertTrue(ConnectionHealthRegistry.getConnections("unknown").isEmpty());
        } finally {
            ConnectionHealthRegistry.unregister("copy", "c1", health);
        }
    }

    @Test
    public void testUnregisterRemovesOnlyTheSameSupplier() {
        Supplier<Map<String, Object>> older = Collections::emptyMap;
        Supplier<Map<String, Object>> newer = Collections::emptyMap;
        ConnectionHealthRegistry.register("identity", "c1", older);
        ConnectionHealthRegistry.register("identity", "c1", newer);
        ConnectionHealthRegistry.register("identity", "c2", older);

        ConnectionHealthRegistry.unregister("identity", "c1", older);
        Assert.assertSame("An older supplier does not remove the newer one", newer,
                ConnectionHealthRegistry.getConnections("identity").get("c1"));
        Assert.assertSame(older, ConnectionHealthRegistry.getConnections("identity").get("c2"));

        ConnectionHealthRegistry.unregister("identity", "c1", newer);
        ConnectionHealthRegistry.unregister("identity", "c2", older);
        Assert.assertTrue(ConnectionHealthRegistry.getConnections("identity").isEmpty());
    }

    @Test
    public void testUnregisterOfAnUnknownConnectorDoesNothing() {
        ConnectionHealthRegistry.unregister("unknown", "c1", Collections::emptyMap);
        Assert.assertTrue(ConnectionHealthRegistry.getConnections("unknown").isEmpty());
    }
}

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

import org.apache.synapse.core.SynapseEnvironment;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class GenericPollingConsumerRegistryTest {

    private static final String NAME = "PollingHealthInbound";

    @After
    public void clear() {
        GenericPollingConsumerRegistry.unregister(NAME, GenericPollingConsumerRegistry.get(NAME));
    }

    @Test
    public void testDefaultHealthIsNull() {
        Assert.assertNull("A polling consumer has no health by default", newConsumer().getHealth());
    }

    @Test
    public void testRegisterAndGet() {
        GenericPollingConsumer consumer = newConsumer();
        GenericPollingConsumerRegistry.register(NAME, consumer);
        Assert.assertSame(consumer, GenericPollingConsumerRegistry.get(NAME));
        Assert.assertNull(GenericPollingConsumerRegistry.get("OtherInbound"));
    }

    @Test
    public void testUnregisterRemovesOnlyTheSameObject() {
        GenericPollingConsumer older = newConsumer();
        GenericPollingConsumer newer = newConsumer();
        Assert.assertNotSame(older, newer);
        Assert.assertEquals("The two consumers are equal, so only the identity tells them apart", older, newer);
        GenericPollingConsumerRegistry.register(NAME, older);
        GenericPollingConsumerRegistry.register(NAME, newer);

        GenericPollingConsumerRegistry.unregister(NAME, older);
        Assert.assertSame("An older consumer does not remove the newer one", newer,
                GenericPollingConsumerRegistry.get(NAME));

        GenericPollingConsumerRegistry.unregister(NAME, newer);
        Assert.assertNull(GenericPollingConsumerRegistry.get(NAME));
    }

    @Test
    public void testUnregisterBeforeTheNewerConsumerKeepsIt() {
        GenericPollingConsumer older = newConsumer();
        GenericPollingConsumer newer = newConsumer();
        GenericPollingConsumerRegistry.register(NAME, older);
        GenericPollingConsumerRegistry.unregister(NAME, older);
        Assert.assertNull("The owner removes its own consumer", GenericPollingConsumerRegistry.get(NAME));

        GenericPollingConsumerRegistry.register(NAME, newer);
        GenericPollingConsumerRegistry.unregister(NAME, older);
        Assert.assertSame("A second unregister of the older consumer does not remove the newer one", newer,
                GenericPollingConsumerRegistry.get(NAME));
    }

    @Test
    public void testProcessorRegistersAfterConstructAndUnregistersOnDestroy() {
        GenericProcessor processor = newProcessor();
        processor.init();
        GenericPollingConsumer consumer = GenericPollingConsumerRegistry.get(NAME);
        Assert.assertTrue("The processor registers the consumer that it constructed",
                consumer instanceof TestPollingConsumer);

        processor.destroy();
        Assert.assertTrue("The consumer is destroyed", ((TestPollingConsumer) consumer).destroyed);
        Assert.assertNull("The processor unregisters its consumer on destroy",
                GenericPollingConsumerRegistry.get(NAME));
    }

    @Test
    public void testDestroyOfAnOlderProcessorKeepsTheNewerConsumer() {
        GenericProcessor older = newProcessor();
        older.init();
        GenericPollingConsumer olderConsumer = GenericPollingConsumerRegistry.get(NAME);
        GenericProcessor newer = newProcessor();
        newer.init();
        GenericPollingConsumer newerConsumer = GenericPollingConsumerRegistry.get(NAME);
        Assert.assertNotNull("The second processor registers a consumer", newerConsumer);
        Assert.assertNotSame("The second processor registers its own consumer", olderConsumer, newerConsumer);
        Assert.assertEquals(olderConsumer, newerConsumer);

        older.destroy();
        Assert.assertSame("The destroy of the older processor keeps the newer consumer", newerConsumer,
                GenericPollingConsumerRegistry.get(NAME));

        newer.destroy();
        Assert.assertNull(GenericPollingConsumerRegistry.get(NAME));
    }

    private static GenericPollingConsumer newConsumer() {
        return new TestPollingConsumer(new Properties(), NAME, null, 1000L, "main", "fault", false, true);
    }

    private static GenericProcessor newProcessor() {
        return new GenericProcessor(NAME, TestPollingConsumer.class.getName(), new Properties(), 1000L, "main",
                "fault", null, false, true);
    }

    public static class TestPollingConsumer extends GenericPollingConsumer {

        private boolean destroyed;

        public TestPollingConsumer(Properties properties, String name, SynapseEnvironment synapseEnvironment,
                                   long scanInterval, String injectingSeq, String onErrorSeq, boolean coordination,
                                   boolean sequential) {
            super(properties, name, synapseEnvironment, scanInterval, injectingSeq, onErrorSeq, coordination,
                    sequential);
        }

        @Override
        public Object poll() {
            return null;
        }

        @Override
        public void resume() {
        }

        @Override
        public void pause() {
        }

        @Override
        public void destroy() {
            destroyed = true;
        }

        // Two consumers of this class are equal. Only the identity tells them apart.
        @Override
        public boolean equals(Object other) {
            return other instanceof TestPollingConsumer;
        }

        @Override
        public int hashCode() {
            return 1;
        }
    }
}

/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
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
package org.wso2.carbon.inbound.endpoint.protocol.generic;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.SynapseException;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.inbound.InboundProcessorParams;
import org.apache.synapse.libraries.LibClassLoader;
import org.apache.synapse.startup.quartz.StartUpController;
import org.apache.synapse.task.TaskStartupObserver;
import org.wso2.carbon.inbound.endpoint.common.InboundRequestProcessorImpl;
import org.wso2.carbon.inbound.endpoint.common.InboundTask;
import org.wso2.carbon.inbound.endpoint.protocol.PollingConstants;
import org.wso2.carbon.inbound.endpoint.protocol.Utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import static org.wso2.carbon.inbound.endpoint.protocol.generic.GenericConstants.CRON_EXPRESSION;

public class GenericProcessor extends InboundRequestProcessorImpl implements TaskStartupObserver {

    private GenericPollingConsumer pollingConsumer;
    private Properties properties;
    private String injectingSeq;
    private String onErrorSeq;
    private static final Log log = LogFactory.getLog(GenericProcessor.class);
    private StartUpController startUpController;
    private String classImpl;
    private boolean sequential;
    private String cronExpression;

    private static final String ENDPOINT_POSTFIX = "CLASS" + COMMON_ENDPOINT_POSTFIX;

    public GenericProcessor(String name, String classImpl, Properties properties, long scanInterval,
                            String injectingSeq, String onErrorSeq, SynapseEnvironment synapseEnvironment,
                            boolean coordination, boolean sequential) {
        this.name = name;
        this.properties = properties;
        this.interval = scanInterval;
        this.injectingSeq = injectingSeq;
        this.onErrorSeq = onErrorSeq;
        this.synapseEnvironment = synapseEnvironment;
        this.classImpl = classImpl;
        this.coordination = coordination;
        this.sequential = sequential;
    }

    public GenericProcessor(String name, String classImpl, Properties properties, long scanInterval,
                            String injectingSeq, String onErrorSeq, SynapseEnvironment synapseEnvironment,
                            boolean coordination, boolean sequential, boolean startInPauseMode) {
        this.name = name;
        this.properties = properties;
        this.interval = scanInterval;
        this.injectingSeq = injectingSeq;
        this.onErrorSeq = onErrorSeq;
        this.synapseEnvironment = synapseEnvironment;
        this.classImpl = classImpl;
        this.coordination = coordination;
        this.sequential = sequential;
        this.startInPausedMode = startInPauseMode;

    }
  
    public GenericProcessor(String name, String classImpl, Properties properties, String cronExpression,
                            String injectingSeq, String onErrorSeq, SynapseEnvironment synapseEnvironment,
                            boolean coordination, boolean sequential) {
        this.name = name;
        this.properties = properties;
        this.injectingSeq = injectingSeq;
        this.onErrorSeq = onErrorSeq;
        this.synapseEnvironment = synapseEnvironment;
        this.classImpl = classImpl;
        this.coordination = coordination;
        this.sequential = sequential;
        this.cronExpression = cronExpression;
    }

    public GenericProcessor(InboundProcessorParams params) {
        this.name = params.getName();
        this.properties = params.getProperties();
        if (properties.getProperty(PollingConstants.INBOUND_ENDPOINT_INTERVAL) != null) {
            this.interval = Long.parseLong(properties.getProperty(PollingConstants.INBOUND_ENDPOINT_INTERVAL));
        } else if (properties.getProperty(CRON_EXPRESSION) != null) {
            this.cronExpression = properties.getProperty(CRON_EXPRESSION);
        }
        this.coordination = true;
        if (properties.getProperty(PollingConstants.INBOUND_COORDINATION) != null) {
            this.coordination = Boolean.parseBoolean(properties.getProperty(PollingConstants.INBOUND_COORDINATION));
        }
        this.sequential = true;
        if (properties.getProperty(PollingConstants.INBOUND_ENDPOINT_SEQUENTIAL) != null) {
            this.sequential = Boolean
                    .parseBoolean(properties.getProperty(PollingConstants.INBOUND_ENDPOINT_SEQUENTIAL));
        }
        this.injectingSeq = params.getInjectingSeq();
        this.onErrorSeq = params.getOnErrorSeq();
        this.synapseEnvironment = params.getSynapseEnvironment();
        this.classImpl = params.getClassImpl();
        this.startInPausedMode = params.startInPausedMode();
    }

    public void init() {
        log.info("Inbound listener [" + name + "] is initializing"
                + (this.startInPausedMode ? " but will remain in suspended mode..." : "..."));
        Map<String, ClassLoader> libClassLoaders = SynapseConfiguration.getLibraryClassLoaders();
        Class c = null;
        if (libClassLoaders != null) {
            for (Map.Entry<String, ClassLoader> entry : libClassLoaders.entrySet()) {
                try {
                    if (entry.getValue() instanceof LibClassLoader) {
                        c = entry.getValue().loadClass(classImpl);
                        break;
                    }
                } catch (ClassNotFoundException e) {
                    if (log.isDebugEnabled()) {
                        log.debug("Class " + classImpl + " not found in the classloader of the library " + entry.getKey());
                    }
                }
            }
        }
        if (c == null) {
            try {
                c = Class.forName(classImpl);
            } catch (ClassNotFoundException e) {
                handleException(
                        "Class " + classImpl + " not found. Please check the required class is added to the classpath.", e);
            }
        }
        try {
            Constructor cons;
            if (cronExpression != null && !cronExpression.trim().isEmpty() && !cronExpression.equals("null")) {
                cons = c.getConstructor(Properties.class, String.class, SynapseEnvironment.class, String.class,
                        String.class, String.class, boolean.class, boolean.class);
                pollingConsumer = (GenericPollingConsumer) cons.newInstance(properties, name, synapseEnvironment,
                        cronExpression, injectingSeq, onErrorSeq, coordination, sequential);
            } else {
                cons = c.getConstructor(Properties.class, String.class, SynapseEnvironment.class, long.class,
                                String.class, String.class, boolean.class, boolean.class);
                pollingConsumer = (GenericPollingConsumer) cons.newInstance(properties, name, synapseEnvironment,
                        interval, injectingSeq, onErrorSeq, coordination, sequential);
            }
            GenericPollingConsumerRegistry.register(name, pollingConsumer);

        } catch (NoSuchMethodException e) {
            handleException("Required constructor is not implemented.", e);
        } catch (InvocationTargetException e) {
            handleException("Unable to create the consumer", e);
        } catch (Exception e) {
            handleException("Unable to create the consumer", e);
        }
        start();
    }

    private void handleException(String msg, Exception ex) {
        log.error(msg, ex);
        throw new SynapseException(ex);
    }

    public void start() {
        try {
            InboundTask task = new GenericTask(pollingConsumer, interval);
            start(task, ENDPOINT_POSTFIX);
        } catch (Exception e) {
            log.error("Could not start Generic Processor. Error starting up scheduler. Error: " + e
                    .getLocalizedMessage());
        }
    }

    /**
     * Stop the inbound polling processor This will be called when inbound is
     * undeployed/redeployed or when server stop
     */
    public void destroy() {
        try {
            super.destroy();
            //Terminate polling events
            pollingConsumer.destroy();
        } finally {
            GenericPollingConsumerRegistry.unregister(name, pollingConsumer);
        }
    }

    @Override
    public void pause() {
        try {
            pollingConsumer.pause();
        } catch (AbstractMethodError e) {
            if (log.isDebugEnabled()) {
                log.debug("Implement the 'pause()' method to enable graceful shutdown in your custom "
                        + "inbound endpoint: " + getName());
            }
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void update() {
        start();
    }

    @Override
    public boolean activate() {

        if (Utils.checkMethodImplementation(pollingConsumer.getClass(), "resume")) {
            // After the task is resumed via super.activate(), the resume() method of the corresponding polling
            // consumer (where the task is scheduled) will be invoked within the 'GenericTask.notifyLocalTaskResume' method.

            return super.activate();
        } else {
            throw new UnsupportedOperationException("Activation is not supported for Inbound Endpoint '" + getName()
                    + "'. To enable this functionality, ensure that the 'destroy()' and 'resume()' methods are "
                    + "properly implemented. If using a WSO2-released inbound, please upgrade to the latest version.");
        }
    }

    @Override
    public boolean deactivate() {

        if (Utils.checkMethodImplementation(pollingConsumer.getClass(), "destroy")
                && Utils.checkMethodImplementation(pollingConsumer.getClass(), "resume")) {
            // We check that both 'destroy' and 'resume' are implemented to ensure that existing customers who only
            // implemented 'destroy' do not end up in an inconsistent state due to a missing 'resume' implementation.

            // After the task is paused via super.deactivate(), the destroy() method of the corresponding polling
            // consumer (where the task is scheduled) will be invoked within the 'GenericTask.notifyLocalTaskPause' method.

            return super.deactivate();
        } else {
            throw new UnsupportedOperationException("Deactivation is not supported for Inbound Endpoint '"
                    + getName() + "'. To enable this functionality, ensure that the 'destroy()' and 'resume()' methods "
                    + "are properly implemented. If using a WSO2-released inbound, please upgrade to the latest version.");
        }
    }
}

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

import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.util.UUIDGenerator;
import org.apache.axis2.builder.Builder;
import org.apache.axis2.builder.BuilderUtil;
import org.apache.axis2.builder.SOAPBuilder;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.transport.TransportUtils;
import org.apache.commons.io.input.AutoCloseInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.mediators.base.SequenceMediator;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

public abstract class GenericPollingConsumer {

    protected Properties properties;
    protected String name;
    protected SynapseEnvironment synapseEnvironment;
    protected long scanInterval;
    protected String injectingSeq;
    protected String onErrorSeq;
    protected boolean coordination;
    protected boolean sequential;
    protected String cronExpression;

    private static final Log log = LogFactory.getLog(GenericPollingConsumer.class);

    public GenericPollingConsumer(Properties properties, String name, SynapseEnvironment synapseEnvironment,
                                  String cronExpression, String injectingSeq, String onErrorSeq, boolean coordination,
                                  boolean sequential) {
        this.properties = properties;
        this.name = name;
        this.synapseEnvironment = synapseEnvironment;
        this.injectingSeq = injectingSeq;
        this.onErrorSeq = onErrorSeq;
        this.coordination = coordination;
        this.sequential = sequential;
        this.cronExpression = cronExpression;
    }

    public GenericPollingConsumer(Properties properties, String name, SynapseEnvironment synapseEnvironment,
                                  long scanInterval, String injectingSeq, String onErrorSeq, boolean coordination,
                                  boolean sequential) {
        this.properties = properties;
        this.name = name;
        this.synapseEnvironment = synapseEnvironment;
        this.injectingSeq = injectingSeq;
        this.onErrorSeq = onErrorSeq;
        this.coordination = coordination;
        this.sequential = sequential;
        this.scanInterval = scanInterval;
    }
    public abstract Object poll();

    public abstract void resume();

    public abstract void pause();

    public void destroy() {
        log.info("Default destroy invoked. Not overwritten.");
    }

    /**
     * Health of the consumer for the management API. The default has no health to report.
     * Health values are strings, booleans, finite numbers, maps and lists of these values.
     *
     * @return the health map, or null when the consumer does not report health
     */
    public Map<String, Object> getHealth() {
        return null;
    }

    protected boolean injectMessage(String strMessage, String contentType) {
        InputStream in = new AutoCloseInputStream(new ByteArrayInputStream(strMessage.getBytes()));
        return injectMessage(in, contentType);
    }

    protected boolean injectMessage(InputStream in, String contentType) {
        try {
            org.apache.synapse.MessageContext msgCtx = createMessageContext();
            if (log.isDebugEnabled()) {
                log.debug("Processed Custom inbound EP Message of Content-type : " + contentType);
            }
            MessageContext axis2MsgCtx = ((org.apache.synapse.core.axis2.Axis2MessageContext) msgCtx)
                    .getAxis2MessageContext();
            msgCtx.setProperty(SynapseConstants.INBOUND_ENDPOINT_NAME, name);
            // Determine the message builder to use
            Builder builder;
            if (contentType == null) {
                log.debug("No content type specified. Using SOAP builder.");
                builder = new SOAPBuilder();
            } else {
                int index = contentType.indexOf(';');
                String type = index > 0 ? contentType.substring(0, index) : contentType;
                builder = BuilderUtil.getBuilderFromSelector(type, axis2MsgCtx);
                if (builder == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("No message builder found for type '" + type + "'. Falling back to SOAP.");
                    }
                    builder = new SOAPBuilder();
                }
            }
            OMElement documentElement = builder.processDocument(in, contentType, axis2MsgCtx);
            //Inject the message to the sequence.             
            msgCtx.setEnvelope(TransportUtils.createSOAPEnvelope(documentElement));
            if (injectingSeq == null || injectingSeq.equals("")) {
                log.error("Sequence name not specified. Sequence : " + injectingSeq);
                return false;
            }
            SequenceMediator seq = (SequenceMediator) synapseEnvironment.getSynapseConfiguration()
                    .getSequence(injectingSeq);
            if (seq != null) {
                if (log.isDebugEnabled()) {
                    log.debug("injecting message to sequence : " + injectingSeq);
                }
                seq.setErrorHandler(onErrorSeq);
                if (!seq.isInitialized()) {
                    seq.init(synapseEnvironment);
                }
                if (!synapseEnvironment.injectInbound(msgCtx, seq, sequential)) {
                    return false;
                }
            } else {
                log.error("Sequence: " + injectingSeq + " not found");
            }
        } catch (Exception e) {
            log.error("Error while processing the Custom Inbound EP Message.");
        }
        return true;
    }

    /**
     * Create the initial message context for the file
     */
    private org.apache.synapse.MessageContext createMessageContext() {
        org.apache.synapse.MessageContext msgCtx = synapseEnvironment.createMessageContext();
        MessageContext axis2MsgCtx = ((org.apache.synapse.core.axis2.Axis2MessageContext) msgCtx)
                .getAxis2MessageContext();
        axis2MsgCtx.setServerSide(true);
        axis2MsgCtx.setMessageID(UUIDGenerator.getUUID());
        return msgCtx;
    }

    protected Properties getInboundProperties() {
        return properties;
    }

    public String getCronExpression() {
        return cronExpression;
    }
}

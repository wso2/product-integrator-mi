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

package org.wso2.micro.integrator.initializer;

import org.apache.axis2.AxisFault;
import org.apache.axis2.addressing.RelatesTo;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.synapse.Mediator;
import org.apache.synapse.MessageContext;
import org.apache.synapse.ServerConfigurationInformation;
import org.apache.synapse.ServerContextInformation;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.SynapseException;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.core.axis2.AsyncCallback;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.core.axis2.Axis2SynapseEnvironment;
import org.apache.synapse.core.axis2.SynapseCallbackReceiver;
import org.apache.synapse.endpoints.AddressEndpoint;
import org.apache.synapse.mediators.AbstractMediator;
import org.apache.synapse.mediators.Value;
import org.apache.synapse.mediators.base.SequenceMediator;
import org.apache.synapse.mediators.builtin.CallMediator;
import org.apache.synapse.mediators.builtin.DropMediator;
import org.apache.synapse.mediators.filters.FilterMediator;
import org.apache.synapse.util.xpath.SynapseXPath;
import org.jaxen.JaxenException;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

/**
 * Exercises automation sequence completion using Synapse mediation with an in-memory call endpoint.
 */
public class MicroIntegratorSequenceControllerTest {

    private final SynapseConfiguration configuration = new SynapseConfiguration();
    private final Axis2SynapseEnvironment environment =
            new Axis2SynapseEnvironment(new ConfigurationContext(new AxisConfiguration()), configuration);
    private final List<String> events = new ArrayList<>();
    private final MicroIntegratorSequenceController controller =
            new MicroIntegratorSequenceController(() -> events.add("shutdown"));
    private MessageContext pendingCall;

    @After
    public void stopExecutor() {
        environment.getExecutorService().shutdownNow();
        configuration.getSynapseTimer().cancel();
    }

    @Test
    public void shutsDownAfterSuccessfulMainSequence() {
        inject(sequence("main", event("main")));
        assertEvents("main", "shutdown");
    }

    @Test
    public void shutsDownAfterFaultSequence() {
        sequence("fault", event("fault"));
        inject(failingSequence());
        assertEvents("fault", "shutdown");
    }

    @Test
    public void waitsForMainContinuationAndItsFaultSequence() throws AxisFault {
        sequence("fault", event("fault"));
        SequenceMediator main = sequence("main", nonBlockingCall(), event("main resumed"), failure());
        main.setErrorHandler("fault");

        inject(main);
        assertEvents("call");
        resume();
        assertEvents("call", "main resumed", "fault", "shutdown");
    }

    @Test
    public void waitsForFaultContinuation() throws AxisFault {
        sequence("fault", event("fault started"), nonBlockingCall(), event("fault done"));

        inject(failingSequence());
        assertEvents("fault started", "call");
        resume();
        assertEvents("fault started", "call", "fault done", "shutdown");
    }

    @Test
    public void shutsDownAfterDropInFaultSequence() {
        sequence("fault", event("before drop"), new DropMediator(), event("after drop"));
        inject(failingSequence());
        assertEvents("before drop", "shutdown");
    }

    @Test
    public void doesNotEndMainWhenFaultSequenceIsCalledNormally() {
        sequence("fault", event("shared sequence"));
        SequenceMediator reference = new SequenceMediator();
        reference.setKey(new Value("fault"));
        SequenceMediator main = sequence("main", reference, event("main continued"));
        main.setErrorHandler("fault");

        inject(main);
        assertEvents("shared sequence", "main continued", "shutdown");
    }

    @Test
    public void shutsDownAfterResumedNestedSequenceDrops() throws AxisFault {
        SequenceMediator nested = sequence("nested", nonBlockingCall(), new DropMediator(),
                event("after nested drop"));
        SequenceMediator reference = new SequenceMediator();
        reference.setKey(new Value("nested"));
        SequenceMediator main = sequence("main", reference, event("main continued"));

        inject(main);
        assertEvents("call");
        resume();
        assertEvents("call", "shutdown");
    }

    @Test
    public void resumesAtNextMediatorAfterNestedReference() throws AxisFault {
        SequenceMediator nested = sequence("nested", nonBlockingCall(), event("nested resumed"));
        SequenceMediator reference = new SequenceMediator();
        reference.setKey(new Value("nested"));
        reference.setMediatorPosition(1);
        SequenceMediator main = sequence("main", event("before nested"), reference,
                event("after nested"));

        inject(main);
        assertEvents("before nested", "call");
        resume();
        assertEvents("before nested", "call", "nested resumed", "after nested", "shutdown");
    }

    @Test
    public void stopsCyclicFaultWrapping() {
        SequenceMediator main = sequence("main", failure());
        SequenceMediator fault = sequence("fault", event("fault"));
        main.setErrorHandler("fault");
        fault.setErrorHandler("main");

        inject(main);
        assertEvents("fault", "shutdown");
    }

    @Test
    public void wrapsNestedSequenceFaultInsideFilter() throws JaxenException {
        sequence("nested-fault", event("nested fault"));
        SequenceMediator nested = sequence("nested", failure());
        nested.setErrorHandler("nested-fault");

        SequenceMediator reference = new SequenceMediator();
        reference.setKey(new Value("nested"));
        FilterMediator filter = new FilterMediator();
        filter.setXpath(new SynapseXPath("true()"));
        filter.setRegex(Pattern.compile("true"));
        filter.addChild(reference);

        inject(sequence("main", filter));
        assertEvents("nested fault", "shutdown");
    }

    @Test
    public void wrapsNestedSequenceOwnFault() {
        sequence("nested-fault", event("nested fault"));
        SequenceMediator nested = sequence("nested", failure());
        nested.setErrorHandler("nested-fault");
        SequenceMediator reference = new SequenceMediator();
        reference.setKey(new Value("nested"));

        inject(sequence("main", reference));
        assertEvents("nested fault", "shutdown");
    }

    private void inject(SequenceMediator main) {
        MessageContext context = environment.createMessageContext();
        context.setProperty(ServiceBusConstants.AUTOMATION_MODE_MAIN_SEQ_PROPERTY, main.getName());
        controller.inject(context, main);
    }

    private void resume() throws AxisFault {
        ServerContextInformation server = new ServerContextInformation(new ServerConfigurationInformation());
        SynapseCallbackReceiver receiver = new SynapseCallbackReceiver(configuration, server);
        Axis2MessageContext request = (Axis2MessageContext) pendingCall;
        Axis2MessageContext response = (Axis2MessageContext) environment.createMessageContext();
        String messageId = request.getMessageID();
        AsyncCallback callback = new AsyncCallback(request.getAxis2MessageContext(), request);
        callback.setTimeout(60_000);
        callback.setTimeoutType(SynapseConstants.ENDPOINT_TIMEOUT_TYPE.GLOBAL_TIMEOUT);
        receiver.addCallback(messageId, callback);
        response.getAxis2MessageContext().addRelatesTo(new RelatesTo(messageId));
        receiver.receive(response.getAxis2MessageContext());
    }

    private SequenceMediator failingSequence() {
        SequenceMediator main = sequence("main", failure());
        main.setErrorHandler("fault");
        return main;
    }

    private SequenceMediator sequence(String name, Mediator... children) {
        SequenceMediator sequence = new SequenceMediator();
        sequence.setName(name);
        Arrays.stream(children).forEach(sequence::addChild);
        sequence.init(environment);
        configuration.addSequence(name, sequence);
        return sequence;
    }

    private CallMediator nonBlockingCall() {
        CallMediator call = new CallMediator();
        call.setBlocking(false);
        call.setEndpoint(new AddressEndpoint() {
            @Override
            public void send(MessageContext context) {
                events.add("call");
                pendingCall = context;
            }
        });
        return call;
    }

    private Mediator event(String event) {
        return new AbstractMediator() {
            @Override
            public boolean mediate(MessageContext context) {
                events.add(event);
                return true;
            }
        };
    }

    private Mediator failure() {
        return new AbstractMediator() {
            @Override
            public boolean mediate(MessageContext context) {
                throw new SynapseException("Expected automation test fault");
            }
        };
    }

    private void assertEvents(String... expected) {
        assertEquals(Arrays.asList(expected), events);
    }
}

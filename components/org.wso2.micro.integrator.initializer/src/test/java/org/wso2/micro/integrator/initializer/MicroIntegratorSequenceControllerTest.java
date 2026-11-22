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

import org.apache.axiom.om.util.AXIOMUtil;
import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.addressing.RelatesTo;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.synapse.Mediator;
import org.apache.synapse.MessageContext;
import org.apache.synapse.ServerConfigurationInformation;
import org.apache.synapse.ServerContextInformation;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.SynapseException;
import org.apache.synapse.config.Entry;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.config.xml.AnonymousListMediator;
import org.apache.synapse.config.xml.SwitchCase;
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
import org.apache.synapse.mediators.builtin.ForEachMediator;
import org.apache.synapse.mediators.builtin.ValidateMediator;
import org.apache.synapse.mediators.filters.FilterMediator;
import org.apache.synapse.mediators.filters.SwitchMediator;
import org.apache.synapse.transport.passthru.PassThroughConstants;
import org.apache.synapse.transport.passthru.Pipe;
import org.apache.synapse.util.xpath.SynapseXPath;
import org.jaxen.JaxenException;
import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

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
    public void buildsDeferredResponseAfterRequestWasMarkedBuilt() throws AxisFault {
        SequenceMediator main = sequence("main", nonBlockingCall(), readPayload());

        inject(main);
        assertEvents("call");
        pendingCall.setProperty("message.builder.invoked", Boolean.TRUE);
        resume(new Pipe(null, "automation test response", null) {
            @Override
            public synchronized InputStream getInputStream() {
                String envelope = "<soap:Envelope xmlns:soap='http://schemas.xmlsoap.org/soap/envelope/'>"
                        + "<soap:Body><response>backend response</response></soap:Body>"
                        + "</soap:Envelope>";
                return new ByteArrayInputStream(envelope.getBytes(StandardCharsets.UTF_8));
            }
        });

        assertEvents("call", "backend response", "shutdown");
    }

    @Test
    public void routesMessageBuildFailureToSequenceErrorHandler() {
        sequence("fault", event("fault"));
        SequenceMediator main = sequence("main", event("before build"), readPayload());
        main.setErrorHandler("fault");
        Axis2MessageContext context = (Axis2MessageContext) environment.createMessageContext();
        configureDeferredPayload(context, new Pipe(null, "automation test request", null) {
            @Override
            public synchronized InputStream getInputStream() {
                throw new IllegalStateException("Expected automation test build failure");
            }
        });

        inject(main, context);

        assertEvents("before build", "fault", "shutdown");
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

    @Test
    public void preservesDeployedNestedChildrenAcrossAutomationRuns() throws JaxenException {
        Mediator beforeDrop = event("before drop");
        Mediator drop = new DropMediator();
        FilterMediator filter = new FilterMediator();
        filter.setXpath(new SynapseXPath("true()"));
        filter.setRegex(Pattern.compile("true"));
        filter.addChild(beforeDrop);
        filter.addChild(drop);
        SequenceMediator main = sequence("main", filter);

        inject(main);
        List<Mediator> childrenAfterFirstRun = new ArrayList<>(filter.getList());

        MessageContext secondContext = environment.createMessageContext();
        secondContext.setProperty(ServiceBusConstants.AUTOMATION_MODE_MAIN_SEQ_PROPERTY, main.getName());
        MicroIntegratorSequenceController secondController =
                new MicroIntegratorSequenceController(() -> events.add("second shutdown"));
        secondController.inject(secondContext, main);

        assertSame(filter, main.getChild(0));
        assertEquals(2, childrenAfterFirstRun.size());
        assertSame(beforeDrop, childrenAfterFirstRun.get(0));
        assertSame("First run replaced the deployed drop", drop, childrenAfterFirstRun.get(1));
        assertEquals(2, filter.getList().size());
        assertSame(beforeDrop, filter.getChild(0));
        assertSame("Second run replaced the deployed drop", drop, filter.getChild(1));
        assertEvents("before drop", "shutdown", "before drop", "second shutdown");
    }


    @Test
    public void shutsDownAfterDropInMatchingSwitchCase() throws JaxenException {
        Mediator beforeDrop = event("case");
        Mediator drop = new DropMediator();
        SwitchCase matching = switchBranch("selected", beforeDrop, drop);
        SwitchMediator mediator = switchSelecting("selected", matching);
        mediator.setDefaultCase(switchBranch(null, event("default")));

        inject(sequence("main", mediator, event("after switch")));

        assertEvents("case", "shutdown");
        assertSame(matching, mediator.getCases().get(0));
        assertEquals(Arrays.asList(beforeDrop, drop), matching.getCaseMediator().getList());
    }

    @Test
    public void shutsDownAfterDropInDefaultSwitchCase() throws JaxenException {
        Mediator beforeDrop = event("default");
        Mediator drop = new DropMediator();
        SwitchCase fallback = switchBranch(null, beforeDrop, drop);
        SwitchMediator mediator = switchSelecting("other", switchBranch("selected", event("case")));
        mediator.setDefaultCase(fallback);

        inject(sequence("main", mediator, event("after switch")));

        assertEvents("default", "shutdown");
        assertSame(fallback, mediator.getDefaultCase());
        assertEquals(Arrays.asList(beforeDrop, drop), fallback.getCaseMediator().getList());
    }

    @Test
    public void continuesAfterSuccessfulSwitchCase() throws JaxenException {
        SwitchMediator mediator = switchSelecting("selected", switchBranch("selected", event("case")));
        mediator.addCase(switchBranch(".*", event("later case")));
        mediator.setDefaultCase(switchBranch(null, event("default")));

        inject(sequence("main", mediator, event("after switch")));

        assertEvents("case", "after switch", "shutdown");
    }

    private SwitchMediator switchSelecting(String value, SwitchCase branch) throws JaxenException {
        SwitchMediator mediator = new SwitchMediator();
        mediator.setSource(new SynapseXPath("'" + value + "'"));
        mediator.addCase(branch);
        return mediator;
    }

    private SwitchCase switchBranch(String regex, Mediator... children) {
        SwitchCase branch = new SwitchCase();
        if (regex != null) {
            branch.setRegex(Pattern.compile(regex));
        }
        AnonymousListMediator list = new AnonymousListMediator();
        Arrays.stream(children).forEach(list::addChild);
        branch.setCaseMediator(list);
        return branch;
    }


    @Test
    public void shutsDownAfterDropInValidateOnFail() throws Exception {
        injectInvalidPayload(event("validate on-fail"), new DropMediator());
        assertEvents("validate on-fail", "shutdown");
    }

    @Test
    public void waitsForReferencedFaultInValidateOnFail() throws Exception {
        sequence("validate-fault", event("fault started"), nonBlockingCall(), event("fault finished"));
        SequenceMediator child = sequence("validate-child", failure());
        child.setErrorHandler("validate-fault");
        SequenceMediator reference = new SequenceMediator();
        reference.setKey(new Value("validate-child"));

        injectInvalidPayload(event("validate on-fail"), reference);
        assertEvents("validate on-fail", "fault started", "call");
        resume();
        assertEvents("validate on-fail", "fault started", "call", "fault finished", "shutdown");
    }

    @Test
    public void shutsDownAfterFaultInExpressionSelectedSequence() throws Exception {
        sequence("dynamic-fault", event("dynamic fault finished"));
        SequenceMediator child = sequence("dynamic-child", event("dynamic child entered"), failure());
        child.setErrorHandler("dynamic-fault");
        SequenceMediator reference = new SequenceMediator();
        reference.setKey(new Value(new SynapseXPath("get-property('childSequence')")));
        SequenceMediator main = sequence("main", reference, event("after dynamic child"));

        MessageContext context = environment.createMessageContext();
        context.setProperty("childSequence", child.getName());
        context.setProperty(ServiceBusConstants.AUTOMATION_MODE_MAIN_SEQ_PROPERTY, main.getName());
        controller.inject(context, main);

        assertEvents("dynamic child entered", "dynamic fault finished", "shutdown");
    }

    @Test
    public void continuesAfterDropConsumedByForEach() throws Exception {
        SequenceMediator iteration = sequence("iteration", event("iteration before drop"), new DropMediator(),
                event("iteration after drop"));
        ForEachMediator forEach = new ForEachMediator();
        forEach.setExpression(new SynapseXPath("//items/item"));
        forEach.setSequenceRef(iteration.getName());
        SequenceMediator main = sequence("main", forEach, event("after foreach"));

        MessageContext context = environment.createMessageContext();
        context.getEnvelope().getBody().addChild(AXIOMUtil.stringToOM("<items><item>one</item></items>"));
        context.setProperty(ServiceBusConstants.AUTOMATION_MODE_MAIN_SEQ_PROPERTY, main.getName());
        controller.inject(context, main);

        assertEvents("iteration before drop", "after foreach", "shutdown");
    }


    private void injectInvalidPayload(Mediator... onFail) throws Exception {
        String schemaKey = "validate-schema";
        Entry schema = new Entry(schemaKey);
        schema.setType(Entry.INLINE_XML);
        schema.setValue(AXIOMUtil.stringToOM(
                "<xs:schema xmlns:xs='http://www.w3.org/2001/XMLSchema'>"
                        + "<xs:element name='valid' type='xs:string'/>"
                        + "</xs:schema>"));
        configuration.addEntry(schemaKey, schema);

        ValidateMediator validate = new ValidateMediator();
        validate.setSchemaKeys(Arrays.asList(new Value(schemaKey)));
        Arrays.stream(onFail).forEach(validate::addChild);
        SequenceMediator main = sequence("main", validate, event("after validate"));
        MessageContext context = environment.createMessageContext();
        context.getEnvelope().getBody().addChild(AXIOMUtil.stringToOM("<invalid/>"));
        context.setProperty(ServiceBusConstants.AUTOMATION_MODE_MAIN_SEQ_PROPERTY, main.getName());
        controller.inject(context, main);
    }


    private void inject(SequenceMediator main) {
        inject(main, environment.createMessageContext());
    }

    private void inject(SequenceMediator main, MessageContext context) {
        context.setProperty(ServiceBusConstants.AUTOMATION_MODE_MAIN_SEQ_PROPERTY, main.getName());
        controller.inject(context, main);
    }

    private void resume() throws AxisFault {
        resume(null);
    }

    private void resume(Pipe responsePipe) throws AxisFault {
        ServerContextInformation server = new ServerContextInformation(new ServerConfigurationInformation());
        SynapseCallbackReceiver receiver = new SynapseCallbackReceiver(configuration, server);
        Axis2MessageContext request = (Axis2MessageContext) pendingCall;
        Axis2MessageContext response = (Axis2MessageContext) environment.createMessageContext();
        if (responsePipe != null) {
            configureDeferredPayload(response, responsePipe);
        }
        String messageId = request.getMessageID();
        AsyncCallback callback = new AsyncCallback(request.getAxis2MessageContext(), request);
        callback.setTimeout(60_000);
        callback.setTimeoutType(SynapseConstants.ENDPOINT_TIMEOUT_TYPE.GLOBAL_TIMEOUT);
        receiver.addCallback(messageId, callback);
        response.getAxis2MessageContext().addRelatesTo(new RelatesTo(messageId));
        receiver.receive(response.getAxis2MessageContext());
    }

    private void configureDeferredPayload(Axis2MessageContext context, Pipe pipe) {
        context.getAxis2MessageContext().setProperty(Constants.Configuration.CONTENT_TYPE, "text/xml");
        context.getAxis2MessageContext().setProperty(PassThroughConstants.PASS_THROUGH_PIPE, pipe);
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
            public boolean isContentAware() {
                return false;
            }

            @Override
            public boolean mediate(MessageContext context) {
                events.add(event);
                return true;
            }
        };
    }

    private Mediator readPayload() {
        return new AbstractMediator() {
            @Override
            public boolean isContentAware() {
                return true;
            }

            @Override
            public boolean mediate(MessageContext context) {
                events.add(context.getEnvelope().getBody().getFirstElement().getText());
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

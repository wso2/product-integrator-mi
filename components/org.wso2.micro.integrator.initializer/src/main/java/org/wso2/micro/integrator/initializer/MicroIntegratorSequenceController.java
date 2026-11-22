/*
 * Copyright (c) 2023, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
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
package org.wso2.micro.integrator.initializer;

import org.apache.synapse.ContinuationState;
import org.apache.synapse.Mediator;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.config.xml.AnonymousListMediator;
import org.apache.synapse.config.xml.SwitchCase;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.mediators.AbstractMediator;
import org.apache.synapse.mediators.FlowContinuableMediator;
import org.apache.synapse.mediators.Value;
import org.apache.synapse.mediators.ListMediator;
import org.apache.synapse.mediators.MediatorProperty;
import org.apache.synapse.mediators.filters.FilterMediator;
import org.apache.synapse.mediators.filters.SwitchMediator;
import org.apache.synapse.mediators.builtin.ForEachMediator;
import org.apache.synapse.mediators.base.SequenceMediator;
import org.apache.synapse.mediators.builtin.DropMediator;
import org.apache.synapse.mediators.builtin.ValidateMediator;
import org.apache.synapse.SynapseException;
import org.apache.synapse.transport.passthru.util.RelayUtils;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.xml.sax.SAXException;

import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class MicroIntegratorSequenceController {

    private static final String AUTOMATION_OWNER_PROPERTY =
            MicroIntegratorSequenceController.class.getName() + ".automationOwner";

    private final AtomicBoolean completed = new AtomicBoolean();
    private final Runnable shutdown;
    private final Map<String, String> wrappedSequences = new HashMap<>();
    private final Map<String, String> faultSequences = new HashMap<>();
    private final Set<String> wrappingSequences = new HashSet<>();

    MicroIntegratorSequenceController(Runnable shutdown) {
        this.shutdown = shutdown;
    }

    void inject(MessageContext context, SequenceMediator mainSequence) {
        SynapseConfiguration configuration = context.getConfiguration();
        String runName = SynapseConstants.PREFIX_HIDDEN_SEQUENCE_KEY + "automation-" + UUID.randomUUID();
        SequenceMediator main = new AutomationSequence(runName + "-main", mainSequence,
                context, configuration, runName, true);

        if (mainSequence.getErrorHandler() != null) {
            Mediator faultSequence = context.getSequence(mainSequence.getErrorHandler());
            if (faultSequence instanceof SequenceMediator) {
                SequenceMediator fault = new AutomationSequence(runName + "-fault",
                        (SequenceMediator) faultSequence, context, configuration, runName, true);
                // The private name is used only for onError dispatch and for resuming that fault flow.
                configuration.addSequence(fault.getName(), fault);
                main.setErrorHandler(fault.getName());
            }
        }

        // Synapse resolves named sequences from the configuration when a non-blocking call resumes.
        configuration.addSequence(main.getName(), main);
        context.setProperty(AUTOMATION_OWNER_PROPERTY, runName);
        context.getEnvironment().injectMessage(context, main);
    }

    private void complete(MessageContext context, boolean result) {
        // Non-blocking calls stop the current pass and resume on a Synapse callback.
        boolean suspended = !result
                && context.getContinuationStateStack() != null
                && !context.getContinuationStateStack().isEmpty();
        if (!suspended && result
                && completed.compareAndSet(false, true)) {
            shutdown.run();
        }
    }

    private class AutomationSequence extends SequenceMediator {

        private final String statisticsName;
        private final boolean terminal;

        AutomationSequence(String name, SequenceMediator original, MessageContext context,
                           SynapseConfiguration configuration, String runName, boolean terminal) {
            this.terminal = terminal;
            statisticsName = original.getSequenceNameForStatistics();
            if (original.getName() != null) {
                if (terminal) {
                    faultSequences.put(original.getName(), name);
                } else {
                    wrappedSequences.put(original.getName(), name);
                }
            }
            setName(name);
            setErrorHandler(original.getErrorHandler());
            if (original.getErrorHandler() != null) {
                String originalFaultName = original.getErrorHandler();
                String privateFaultName = faultSequences.get(originalFaultName);
                if (privateFaultName == null && !wrappingSequences.contains(originalFaultName)) {
                    Mediator faultSequence = context.getSequence(originalFaultName);
                    if (faultSequence instanceof SequenceMediator) {
                        wrappingSequences.add(originalFaultName);
                        privateFaultName = runName + "-nested-fault-" + UUID.randomUUID();
                        SequenceMediator wrappedFault = new AutomationSequence(privateFaultName,
                                (SequenceMediator) faultSequence, context, configuration, runName, true);
                        wrappingSequences.remove(originalFaultName);
                        faultSequences.put(originalFaultName, privateFaultName);
                        configuration.addSequence(privateFaultName, wrappedFault);
                        setErrorHandler(privateFaultName);
                    }
                } else if (privateFaultName != null) {
                    setErrorHandler(privateFaultName);
                }
            }
            setArtifactContainerName(original.getArtifactContainerName());
            setTraceState(original.getTraceState());
            configure(original.getAspectConfiguration());
            for (Mediator mediator : original.getList()) {
                addChild(wrapMediator(mediator, context, configuration, runName));
            }
        }

        // The deployed sequence owns the lifecycle of the shared mediators.
        @Override
        public boolean isInitialized() {
            return true;
        }

        @Override
        public void init(SynapseEnvironment environment) {
        }

        @Override
        public String getSequenceNameForStatistics() {
            return statisticsName;
        }

        @Override
        public boolean mediate(MessageContext context) {
            boolean result = super.mediate(context);
            if (terminal) {
                complete(context, result);
            }
            return result;
        }

        @Override
        public boolean mediate(MessageContext context, ContinuationState state) {
            boolean result = super.mediate(context, state);
            if (terminal) {
                complete(context, result);
            }
            return result;
        }

        private Mediator wrapMediator(Mediator mediator, MessageContext context,
                                      SynapseConfiguration configuration, String runName) {
            Mediator wrapped = wrapAutomationMediator(mediator, context, configuration, runName);
            // A sequence reference lets its target install its own fault handler before building.
            if (wrapped instanceof SequenceMediator && ((SequenceMediator) wrapped).getKey() != null) {
                return wrapped;
            }
            if (!mediator.isContentAware() && !wrapped.isContentAware()) {
                return wrapped;
            }
            // Keep one child per original position so Synapse can resume nested continuations.
            return wrapped instanceof FlowContinuableMediator
                    ? new ContentAwareFlowMediator(wrapped) : new ContentAwareMediator(wrapped);
        }

        private Mediator wrapAutomationMediator(Mediator mediator, MessageContext context,
                                                SynapseConfiguration configuration, String runName) {
            if (mediator instanceof SequenceMediator) {
                SequenceMediator sequenceReference = (SequenceMediator) mediator;
                Value key = sequenceReference.getKey();
                if (key != null && key.getExpression() != null) {
                    return copyReference(sequenceReference, new Value(key.getExpression()) {
                        @Override
                        public String evaluateValue(MessageContext messageContext) {
                            String name = key.evaluateValue(messageContext);
                            if (!runName.equals(messageContext.getProperty(AUTOMATION_OWNER_PROPERTY))) {
                                return name;
                            }
                            return resolveWrappedSequence(name, messageContext, configuration, runName);
                        }
                    });
                }
                if (key != null && key.getKeyValue() != null) {
                    String name = key.getKeyValue();
                    String privateName = resolveWrappedSequence(name, context, configuration, runName);
                    if (!name.equals(privateName)) {
                        return copyReference(sequenceReference, new Value(privateName));
                    }
                }
            }
            if (mediator instanceof DropMediator) {
                return new AutomationDropMediator(runName);
            }
            if (mediator instanceof ForEachMediator) {
                // ForEach consumes a drop in its iteration and continues the enclosing flow.
                return mediator;
            }
            if (mediator instanceof FilterMediator) {
                return copyFilter((FilterMediator) mediator, context, configuration, runName);
            }
            if (mediator instanceof SwitchMediator) {
                return copySwitch((SwitchMediator) mediator, context, configuration, runName);
            }
            if (mediator instanceof ValidateMediator) {
                return copyValidate((ValidateMediator) mediator, context, configuration, runName);
            }
            return mediator;
        }

        private FilterMediator copyFilter(FilterMediator original, MessageContext context,
                                          SynapseConfiguration configuration, String runName) {
            FilterMediator copy = new FilterMediator();
            copy.setSource(original.getSource());
            copy.setRegex(original.getRegex());
            if (original.getXpath() != null) {
                copy.setXpath(original.getXpath());
            }
            copy.setThenElementPresent(original.isThenElementPresent());
            copy.setThenKey(resolveWrappedSequence(original.getThenKey(), context, configuration, runName));
            copy.setElseKey(resolveWrappedSequence(original.getElseKey(), context, configuration, runName));
            copy.setTraceState(original.getTraceState());
            copy.setMediatorPosition(original.getMediatorPosition());
            copy.configure(original.getAspectConfiguration());
            for (Mediator child : original.getList()) {
                copy.addChild(wrapMediator(child, context, configuration, runName));
            }

            ListMediator originalElse = original.getElseMediator();
            if (originalElse != null) {
                AnonymousListMediator elseCopy = new AnonymousListMediator();
                elseCopy.setTraceState(originalElse.getTraceState());
                for (Mediator child : originalElse.getList()) {
                    elseCopy.addChild(wrapMediator(child, context, configuration, runName));
                }
                copy.setElseMediator(elseCopy);
            }
            return copy;
        }


        private SwitchMediator copySwitch(SwitchMediator original, MessageContext context,
                                          SynapseConfiguration configuration, String runName) {
            SwitchMediator copy = new SwitchMediator();
            copy.setSource(original.getSource());
            copy.setTraceState(original.getTraceState());
            copy.setMediatorPosition(original.getMediatorPosition());
            copy.configure(original.getAspectConfiguration());
            for (SwitchCase branch : original.getCases()) {
                copy.addCase(copySwitchCase(branch, context, configuration, runName));
            }
            if (original.getDefaultCase() != null) {
                copy.setDefaultCase(copySwitchCase(original.getDefaultCase(), context, configuration, runName));
            }
            return copy;
        }

        private SwitchCase copySwitchCase(SwitchCase original, MessageContext context,
                                          SynapseConfiguration configuration, String runName) {
            AnonymousListMediator originalBranch = original.getCaseMediator();
            AnonymousListMediator branch = new AnonymousListMediator();
            branch.setTraceState(originalBranch.getTraceState());
            branch.setMediatorPosition(originalBranch.getMediatorPosition());
            branch.configure(originalBranch.getAspectConfiguration());
            for (Mediator child : originalBranch.getList()) {
                branch.addChild(wrapMediator(child, context, configuration, runName));
            }
            SwitchCase copy = new SwitchCase();
            copy.setRegex(original.getRegex());
            copy.setCaseMediator(branch);
            return copy;
        }


        private ValidateMediator copyValidate(ValidateMediator original, MessageContext context,
                                              SynapseConfiguration configuration, String runName) {
            ValidateMediator copy = new ValidateMediator();
            copy.setSchemaKeys(original.getSchemaKeys());
            if (original.getSource() != null) {
                copy.setSource(original.getSource());
            }
            copy.setIsJSONSource(original.isJSONSource());
            copy.setResourceMap(original.getResourceMap());
            copy.setCacheSchema(original.isCacheSchema());
            copy.setTraceState(original.getTraceState());
            copy.setMediatorPosition(original.getMediatorPosition());
            copy.configure(original.getAspectConfiguration());
            for (MediatorProperty feature : original.getFeatures()) {
                try {
                    copy.addFeature(feature.getName(), Boolean.parseBoolean(feature.getValue()));
                } catch (SAXException e) {
                    throw new SynapseException("Could not copy validation feature " + feature.getName(), e);
                }
            }
            for (Mediator child : original.getList()) {
                copy.addChild(wrapMediator(child, context, configuration, runName));
            }
            return copy;
        }


        private String resolveWrappedSequence(String name, MessageContext context,
                                              SynapseConfiguration configuration, String runName) {
            if (name == null) {
                return null;
            }
            synchronized (MicroIntegratorSequenceController.this) {
                String privateName = wrappedSequences.get(name);
                if (privateName != null) {
                    return privateName;
                }
                Mediator referenced = context.getSequence(name);
                if (!(referenced instanceof SequenceMediator)) {
                    return name;
                }
                privateName = runName + "-sequence-" + UUID.randomUUID();
                wrappedSequences.put(name, privateName);
                try {
                    SequenceMediator wrapped = new AutomationSequence(privateName,
                            (SequenceMediator) referenced, context, configuration, runName, false);
                    configuration.addSequence(privateName, wrapped);
                    return privateName;
                } catch (RuntimeException e) {
                    wrappedSequences.remove(name);
                    throw e;
                }
            }
        }

        private SequenceMediator copyReference(SequenceMediator original, Value key) {
            SequenceMediator copy = new SequenceMediator();
            copy.setKey(key);
            copy.setName(original.getName());
            copy.setErrorHandler(original.getErrorHandler());
            copy.setArtifactContainerName(original.getArtifactContainerName());
            copy.setDynamic(original.isDynamic());
            copy.setRegistryKey(original.getRegistryKey());
            copy.setTraceState(original.getTraceState());
            copy.setMediatorPosition(original.getMediatorPosition());
            copy.configure(original.getAspectConfiguration());
            return copy;
        }
    }

    private static class ContentAwareMediator extends AbstractMediator {

        private final Mediator delegate;

        ContentAwareMediator(Mediator delegate) {
            this.delegate = delegate;
            if (delegate instanceof AbstractMediator) {
                setMediatorId(((AbstractMediator) delegate).getMediatorId());
            }
        }

        @Override
        public boolean mediate(MessageContext context) {
            buildMessage(context);
            return delegate.mediate(context);
        }

        @Override
        public String getType() {
            return delegate.getType();
        }

        @Override
        public Integer reportOpenStatistics(MessageContext context, boolean continuation) {
            return delegate.reportOpenStatistics(context, continuation);
        }

        @Override
        public void reportCloseStatistics(MessageContext context, Integer statisticId) {
            delegate.reportCloseStatistics(context, statisticId);
        }

        final void buildMessage(MessageContext context) {
            try {
                if (context instanceof Axis2MessageContext) {
                    // RelayUtils checks the current Axis2 message, which may differ from the original request.
                    RelayUtils.buildMessage(((Axis2MessageContext) context).getAxis2MessageContext(), false);
                } else if (context.getEnvelope() != null) {
                    context.getEnvelope().getBody().build();
                }
            } catch (Exception e) {
                throw new SynapseException("Error while building automation sequence message", e);
            }
        }
    }

    private static final class ContentAwareFlowMediator extends ContentAwareMediator
            implements FlowContinuableMediator {

        private final FlowContinuableMediator flow;

        ContentAwareFlowMediator(Mediator delegate) {
            super(delegate);
            flow = (FlowContinuableMediator) delegate;
        }

        @Override
        public boolean mediate(MessageContext context, ContinuationState state) {
            return flow.mediate(context, state);
        }
    }

    private class AutomationDropMediator extends DropMediator {

        private final String owner;

        AutomationDropMediator(String owner) {
            this.owner = owner;
        }

        @Override
        public boolean mediate(MessageContext context) {
            boolean result = super.mediate(context);
            if (owner.equals(context.getProperty(AUTOMATION_OWNER_PROPERTY))
                    && completed.compareAndSet(false, true)) {
                shutdown.run();
            }
            return result;
        }
    }
}

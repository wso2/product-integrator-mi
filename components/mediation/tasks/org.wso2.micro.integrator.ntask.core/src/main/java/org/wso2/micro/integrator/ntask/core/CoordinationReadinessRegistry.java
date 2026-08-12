/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.micro.integrator.ntask.core;

import java.util.Collection;

/**
 * The readiness owner — every "refuse readiness / readiness ERROR" lands here. One implementation,
 * JVM-local (readiness is per-node truth by design), provided as an OSGi service by TasksDSComponent so
 * every producing component and the management resource share the single instance.
 */
public interface CoordinationReadinessRegistry {

    // Idempotent; re-raising refreshes detail, keeps the original since.
    void raise(String name, ConditionClass clazz, String detail);

    void clear(String name);                            // idempotent

    boolean green();                                    // no raised condition

    Collection<Condition> snapshot();                   // immutable copy

    enum ConditionClass { GATE, CONTINUOUS }            // one-time activation
                                                        // gate vs continuous

    /**
     * A raised condition: name, clazz, detail, sinceEpochMillis (app clock).
     */
    class Condition {

        private final String name;
        private final ConditionClass clazz;
        private final String detail;
        private final long sinceEpochMillis;

        public Condition(String name, ConditionClass clazz, String detail, long sinceEpochMillis) {
            this.name = name;
            this.clazz = clazz;
            this.detail = detail;
            this.sinceEpochMillis = sinceEpochMillis;
        }

        public String getName() {
            return name;
        }

        public ConditionClass getClazz() {
            return clazz;
        }

        public String getDetail() {
            return detail;
        }

        public long getSinceEpochMillis() {
            return sinceEpochMillis;
        }
    }
}

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
package org.wso2.micro.integrator.ntask.core.internal;

import org.wso2.micro.integrator.ntask.core.CoordinationReadinessRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The single JVM-local {@link CoordinationReadinessRegistry} implementation. Raise and clear are single-map
 * operations, no compound check-then-act.
 */
public class CoordinationReadinessRegistryImpl implements CoordinationReadinessRegistry {

    private final ConcurrentHashMap<String, Condition> conditions = new ConcurrentHashMap<>();

    @Override
    public void raise(String name, ConditionClass clazz, String detail) {
        conditions.compute(name, (key, existing) -> new Condition(name, clazz, detail,
                existing == null ? System.currentTimeMillis() : existing.getSinceEpochMillis()));
    }

    @Override
    public void clear(String name) {
        conditions.remove(name);
    }

    @Override
    public boolean green() {
        return conditions.isEmpty();
    }

    @Override
    public Collection<Condition> snapshot() {
        return Collections.unmodifiableList(new ArrayList<>(conditions.values()));
    }
}

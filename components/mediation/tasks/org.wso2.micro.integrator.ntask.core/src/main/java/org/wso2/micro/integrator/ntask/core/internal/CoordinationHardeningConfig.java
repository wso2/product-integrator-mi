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

import org.wso2.micro.integrator.ntask.coordination.TaskCoordinationException;

import java.time.ZoneId;

/**
 * The one immutable coordination-hardening startup snapshot — the only product of the strict-parse step and
 * the only configuration source every hardened consumer reads thereafter. Master-off and unparseable-master
 * boots build no instance.
 */
public final class CoordinationHardeningConfig {

    private final boolean coordinationHardening;
    private final boolean taskDeleteBarrier;
    private final boolean synchronousInjection;
    private final ZoneId zoneId;
    private final long heartbeatIntervalMillis;
    private final int heartbeatMaxRetries;

    private CoordinationHardeningConfig(boolean coordinationHardening, boolean taskDeleteBarrier,
                                        boolean synchronousInjection, ZoneId zoneId, long heartbeatIntervalMillis,
                                        int heartbeatMaxRetries) {
        this.coordinationHardening = coordinationHardening;
        this.taskDeleteBarrier = taskDeleteBarrier;
        this.synchronousInjection = synchronousInjection;
        this.zoneId = zoneId;
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
        this.heartbeatMaxRetries = heartbeatMaxRetries;
    }

    /**
     * The one producer. Parameters are the raw toml values, each read exactly once from ConfigParser by the
     * caller (absent key -> null), plus the two resolved timing values. The hardened-profile validation runs
     * inside; any violation throws {@link TaskCoordinationException} naming the offending key and no
     * instance exists.
     */
    public static CoordinationHardeningConfig parse(String coordinationHardeningRaw, String taskDeleteBarrierRaw,
                                                    String synchronousInjectionRaw, long heartbeatIntervalMillis,
                                                    int heartbeatMaxRetries) throws TaskCoordinationException {

        if (!isStrictTrue(coordinationHardeningRaw)) {
            throw new TaskCoordinationException("[task_handling] coordination_hardening must be true to build the "
                    + "hardening snapshot. Found: [" + coordinationHardeningRaw + "]");
        }
        if (!isStrictTrue(taskDeleteBarrierRaw)) {
            throw new TaskCoordinationException("[task_handling] enable_task_delete_barrier must be true to arm "
                    + "coordination hardening. Found: [" + taskDeleteBarrierRaw + "]");
        }
        if (!isStrictTrue(synchronousInjectionRaw)) {
            throw new TaskCoordinationException("[task_handling] synchronous_injection must be true to arm "
                    + "coordination hardening. Found: [" + synchronousInjectionRaw + "]");
        }
        // ZoneId.systemDefault() is called exactly once here — the single capture the fingerprint and the
        // occurrence keys share.
        return new CoordinationHardeningConfig(true, true, true, ZoneId.systemDefault(), heartbeatIntervalMillis,
                heartbeatMaxRetries);
    }

    private static boolean isStrictTrue(String raw) {
        return raw != null && "true".equalsIgnoreCase(raw.trim());
    }

    public boolean isCoordinationHardening() {
        return coordinationHardening;
    }

    public boolean isTaskDeleteBarrier() {
        return taskDeleteBarrier;
    }

    public boolean isSynchronousInjection() {
        return synchronousInjection;
    }

    public ZoneId getZoneId() {
        return zoneId;
    }

    public long getHeartbeatIntervalMillis() {
        return heartbeatIntervalMillis;
    }

    public int getHeartbeatMaxRetries() {
        return heartbeatMaxRetries;
    }
}

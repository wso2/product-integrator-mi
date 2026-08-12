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

import org.wso2.micro.integrator.ntask.coordination.TaskCoordinationException;

/**
 * The boot lease controller — the one component every lease consumer calls. One implementation, owned
 * by TasksDSComponent's coordination wiring; installed only on ARMED nodes and read at call time via
 * TasksDSComponent.getBootLeaseController() (null on STOCK/REFUSING nodes).
 */
public interface BootLeaseController {

    /**
     * Blocking acquisition at boot: CAS the advertisement row at BOOT_ID/BOOT_STARTED_AT/
     * ELIGIBILITY='PENDING'. Returns only on success; refusal (living twin) throws and the component
     * does not start coordinated scheduling.
     */
    void acquire() throws TaskCoordinationException;

    /**
     * Piggybacked on the real heartbeat write (the Heartbeat Advertise renewal rule): the
     * strictly-forward BOOT_ID-conditioned UPDATED_AT touch, executed through ntask's own connector.
     * Invoked via the coordination-owned HeartbeatHook — never called directly by coordination code.
     * Records the monotonic instant of the last SUCCESSFUL renewal.
     */
    void renewOnHeartbeat();

    /**
     * Returns the single dispatch decision used by both Quartz firing and coordinator participation. Dispatch is
     * blocked unless the lease is PROVEN and its monotonic renewal age remains inside the takeover margin.
     */
    boolean isDispatchBlocked();

    /**
     * The monotonic source: nanos of the last successful renewal's monotonic START timestamp.
     */
    long lastSuccessfulRenewalNanos();

    /**
     * This lease generation's boot id token.
     */
    String bootId();

    /**
     * Boot-start timestamp written by the acquisition CAS; static per lease generation.
     */
    long bootStartedAt();

    /**
     * Starts the first enumeration of a task type in the current lease generation. The returned handle binds
     * failures to that generation. A later enumeration of the same type returns null and is steady state.
     */
    BootPassHandle beginTypeBootPass(String taskType);

    /**
     * Returns the still-open generation-bound handle for a task type. This allows nested startup registration to
     * retain boot-pass authority when the explicit caller handle is unavailable. It returns null after the type pass
     * closes or the lease generation changes. On stock and refusing nodes no controller exists, so the caller uses
     * the normal null-handle path.
     */
    BootPassHandle getOpenTypeBootPass(String taskType);

    /**
     * Generation-bound completion: the caller passes the BOOT_ID it captured BEFORE the enumeration
     * it observed. A stale bootId (lease lost + reacquired mid-enumeration) makes the call a logged
     * NO-OP — a stale completion can never publish PROVEN or touch a newer generation's failure set;
     * the newer generation's own rerun completes itself. Current generation + empty recorded-failure
     * set -&gt; the completion branch: ELIGIBLE publication (BOOT_ID-conditioned), then PROVEN.
     * Non-empty -&gt; remain PENDING/UNPROVEN with the red GATE condition boot-pass-incomplete naming
     * the entries; the only exit is re-acquisition's fresh generation.
     */
    void completeBootPass(String bootId);

    /**
     * Single-flight recovery entry (LAPSED -&gt; RECOVERING -&gt; PROVEN per the lease state machine;
     * external signals may WAKE it, never execute it).
     */
    void wakeRecovery();
}

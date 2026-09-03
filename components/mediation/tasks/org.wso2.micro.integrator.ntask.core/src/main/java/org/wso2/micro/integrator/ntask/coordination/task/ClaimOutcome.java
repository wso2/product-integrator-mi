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

package org.wso2.micro.integrator.ntask.coordination.task;

/**
 * Outcome of a fire-claim CAS attempt against COORDINATED_TASK_CLAIM. The state to reason mapping is
 * exhaustive and fixed: WON/{NONE}; LOST/{CLOSED, FP_MISMATCH, INCARNATION_MISMATCH, EPOCH_MISMATCH,
 * BOOT_MISMATCH, LOST_RACE}; INDETERMINATE/{NO_FENCE_ROW, EXCEPTION}. No other pairing is constructible
 * (private constructor; one static factory per reason). Only WON dispatches.
 */
public final class ClaimOutcome {

    public final ClaimState state;                      // never null
    public final ClaimReason reason;                    // never null

    public enum ClaimState { WON, LOST, INDETERMINATE }

    public enum ClaimReason {
        NONE,                  // state == WON
        // Zero-rows read-back classification, in EXACTLY this first-match
        // precedence order. Each reason maps to one state; the classification
        // order and the state mapping are independent pins — absence is
        // checked first but is NOT a LOST reason.
        NO_FENCE_ROW,          // 1. claim row absent         -> INDETERMINATE
        CLOSED,                // 2. STATUS = 'CLOSED'        -> LOST
        FP_MISMATCH,           // 3. SCHEDULE_FP differs      -> LOST
        INCARNATION_MISMATCH,  // 4. INCARNATION differs      -> LOST
        EPOCH_MISMATCH,        // 5. OWNER_EPOCH differs      -> LOST
        BOOT_MISMATCH,         // 6. OWNER_BOOT_ID differs    -> LOST
        LOST_RACE,             // 7. FIRE_KEY already >= ours -> LOST
        EXCEPTION              // SQL/connection/commit failure or ambiguity
                               //                             -> INDETERMINATE
    }

    private ClaimOutcome(ClaimState state, ClaimReason reason) {

        this.state = state;
        this.reason = reason;
    }

    public static ClaimOutcome won() {
        return new ClaimOutcome(ClaimState.WON, ClaimReason.NONE);
    }

    public static ClaimOutcome noFenceRow() {
        return new ClaimOutcome(ClaimState.INDETERMINATE, ClaimReason.NO_FENCE_ROW);
    }

    public static ClaimOutcome closed() {
        return new ClaimOutcome(ClaimState.LOST, ClaimReason.CLOSED);
    }

    public static ClaimOutcome fpMismatch() {
        return new ClaimOutcome(ClaimState.LOST, ClaimReason.FP_MISMATCH);
    }

    public static ClaimOutcome incarnationMismatch() {
        return new ClaimOutcome(ClaimState.LOST, ClaimReason.INCARNATION_MISMATCH);
    }

    public static ClaimOutcome epochMismatch() {
        return new ClaimOutcome(ClaimState.LOST, ClaimReason.EPOCH_MISMATCH);
    }

    public static ClaimOutcome bootMismatch() {
        return new ClaimOutcome(ClaimState.LOST, ClaimReason.BOOT_MISMATCH);
    }

    public static ClaimOutcome lostRace() {
        return new ClaimOutcome(ClaimState.LOST, ClaimReason.LOST_RACE);
    }

    public static ClaimOutcome exception() {
        return new ClaimOutcome(ClaimState.INDETERMINATE, ClaimReason.EXCEPTION);
    }

}

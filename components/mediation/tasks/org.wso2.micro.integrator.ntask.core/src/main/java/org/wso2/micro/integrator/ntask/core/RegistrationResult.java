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

/**
 * The registration doorway's result; the ONLY source of park/no-park truth. Caller logic is binary —
 * parked or not; the middle value exists for logs and telemetry.
 */
public enum RegistrationResult {
    REGISTERED_OR_VALIDATED,          // schedulable: fresh insert; exists+OPEN
                                      // validate/no-change; recurring
                                      // inserted+CLOSED reopen; boot-pass catch-up
    SAME_FP_CANCELLED_AND_VALIDATED,  // schedulable: a wave was cancelled (or
                                      // RETIRING -> RETIRE_REFUSED recorded) in
                                      // this transaction, then registered
    DIFFERENT_FP_PARKED               // NOT schedulable — that is the ONLY
                                      // invariant this value carries. Whether
                                      // a task row exists depends on the
                                      // matrix branch: the wave-park writes
                                      // no row; the inserted+CLOSED one-shot
                                      // park KEEPS its re-materialized row.
                                      // Covers
                                      // different-fp vs in-flight wave (+consent
                                      // ack), the one-shot CLOSED park, and
                                      // trigger-family-conflict
}

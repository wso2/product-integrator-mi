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
 * The explicit boot-pass/steady-state signal the registration doorway takes alongside its existing
 * inputs — per-node and in-process, never a config flag and never inferred. BOOT_PASS only from a
 * type's first startup enumeration in the current lease generation (beginTypeBootPass), including
 * re-acquisition's rerun; every other caller passes STEADY_STATE. On a BOOT_PASS registration, rows
 * may legitimately be missing or unbound and seeding/binding inline is the authorized repair; on a
 * STEADY_STATE registration a missing or unbound row is an invariant failure, alarmed, never
 * silently repaired.
 */
public enum RegistrationPhase { BOOT_PASS, STEADY_STATE }

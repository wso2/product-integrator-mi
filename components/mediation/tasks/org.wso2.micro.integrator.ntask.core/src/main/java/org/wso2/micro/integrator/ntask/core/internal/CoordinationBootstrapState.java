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

/**
 * The coordination bootstrap outcome decided once per JVM by the first activation. STOCK runs the shipped
 * stock coordination bootstrap; REFUSING_PROFILE_INVALID runs neither bootstrap (local tasks only);
 * ARMED runs the hardened sequence with the installed {@link CoordinationHardeningConfig} snapshot.
 */
public enum CoordinationBootstrapState {

    STOCK,
    REFUSING_PROFILE_INVALID,
    ARMED
}

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
 * The generation-bound pass handle beginTypeBootPass() returns — the LeaseToken callback rule applied
 * to the boot pass. Threaded as an EXPLICIT PARAMETER through the enumeration call chain; null ==
 * STEADY_STATE.
 */
public interface BootPassHandle {

    /**
     * The generation this handle is bound to.
     */
    String bootId();

    /**
     * Records into ITS generation's failure set. If the controller's current generation differs, this
     * is a logged no-op — a stale pass can never mutate a newer generation's state. Entry naming: a
     * per-task failure records the task name; a type-level failure records "type:&lt;taskType&gt;"
     * (recorded by initTaskManagersForType, which then RETHROWS — stock propagation unchanged).
     */
    void recordFailure(String name, Throwable cause);
}

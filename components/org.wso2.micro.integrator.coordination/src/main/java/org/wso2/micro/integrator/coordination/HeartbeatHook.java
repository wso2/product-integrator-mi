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

package org.wso2.micro.integrator.coordination;

/**
 * Callback interface owned by the coordination component so ntask can implement it without coordination
 * ever depending on ntask. No hook registered means both calls are no-ops and coordinator participation
 * is allowed — coordination behaves bit-for-bit as shipped.
 */
public interface HeartbeatHook {

    /**
     * Called by HeartBeatExecutionTask after each successful node heartbeat write, on the heartbeat
     * thread, in MEMBER and COORDINATOR roles alike. Implementations must be fast and must not throw
     * (the task wraps the call; a hook failure is logged and never breaks the heartbeat).
     *
     * @param epochMillis the app-clock timestamp the heartbeat write carried
     */
    void afterHeartbeat(long epochMillis);

    /**
     * Read by HeartBeatExecutionTask once per cycle, before the MEMBER/COORDINATOR dispatch — the
     * coordinator-election control, the executable form of Standby Coordinator Abstention. Same
     * contract: fast, must not throw; the task wraps the call and reads a throw as false — fail closed
     * for leadership, never for the heartbeat.
     *
     * @return whether this node may hold or win the coordinator role
     */
    boolean isCoordinatorParticipationAllowed();
}

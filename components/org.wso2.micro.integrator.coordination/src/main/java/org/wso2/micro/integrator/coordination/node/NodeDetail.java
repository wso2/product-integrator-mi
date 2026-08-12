/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
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

package org.wso2.micro.integrator.coordination.node;

/**
 * This class contains node heartbeat data of a specific node.
 */
public class NodeDetail {
    /**
     * Node ID of the belonging node.
     */
    private final String nodeId;

    /**
     * Group ID of the belonging node.
     */
    private final String groupId;

    /**
     * The last updated heartbeat value.
     */
    private final long lastHeartbeat;

    /**
     * Indicate if the node addition is already identified by the coordinator.
     */
    private final boolean isNewNode;

    /**
     * Indicate if the node is the coordinator node.
     */
    private boolean isCoordinator;

    /**
     * The heartbeat window this node advertised (Heartbeat Advertise). 0 when the node has no
     * advertisement row — the judge falls back to its local window.
     */
    private long heartbeatWindow;

    /**
     * The BOOT_ID observed on this node's advertisement row at read time; null when the node has no
     * advertisement row. Conditions the removeNode() advertisement cleanup.
     */
    private String advertisedBootId;

    /**
     * NodeDetail constructor.
     *
     * @param nodeId        node ID
     * @param groupId       cluster agent's socket address
     * @param lastHeartbeat last heartbeat received from the node
     * @param isCoordinator check if this node is the coordinator
     * @param isNewNode     true if new node
     */
    public NodeDetail(String nodeId, String groupId, boolean isCoordinator, long lastHeartbeat, boolean isNewNode ) {
        this.nodeId = nodeId;
        this.lastHeartbeat = lastHeartbeat;
        this.isNewNode = isNewNode;
        this.groupId = groupId;
        this.isCoordinator = isCoordinator;
    }
    /**
     * Getter method for Node ID.
     *
     * @return node ID
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * Getter method for last heartbeat.
     *
     * @return last heartbeat received form the node
     */
    public long getLastHeartbeat() {
        return lastHeartbeat;
    }

    /**
     * Getter method for isNewNode.
     *
     * @return true if this is a new node
     */
    public boolean isNewNode() {
        return isNewNode;
    }

    /**
     * Getter method for groupID.
     *
     * @return groupId
     */
    public String getGroupId() {
        return groupId;
    }

    /**
     * Getter method for isCoordinator.
     *
     * @return true if this is the coordinator
     */
    public boolean isCoordinator() {
        return isCoordinator;
    }

    /**
     * Getter method for the advertised heartbeat window.
     *
     * @return the advertised heartbeat window, or 0 when the node has no advertisement row
     */
    public long getHeartbeatWindow() {
        return heartbeatWindow;
    }

    public void setHeartbeatWindow(long heartbeatWindow) {
        this.heartbeatWindow = heartbeatWindow;
    }

    /**
     * Getter method for the observed advertisement BOOT_ID.
     *
     * @return the BOOT_ID observed at read time, or null when the node has no advertisement row
     */
    public String getAdvertisedBootId() {
        return advertisedBootId;
    }

    public void setAdvertisedBootId(String advertisedBootId) {
        this.advertisedBootId = advertisedBootId;
    }

}

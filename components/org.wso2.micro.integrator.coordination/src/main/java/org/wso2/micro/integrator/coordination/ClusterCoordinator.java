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

package org.wso2.micro.integrator.coordination;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.micro.integrator.coordination.exception.ClusterCoordinationException;
import org.wso2.micro.integrator.ndatasource.common.DataSourceException;

import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;

/**
 * Coordinator class to start running Clustering.
 */
public class ClusterCoordinator {

    private static final Log log = LogFactory.getLog(ClusterCoordinator.class);
    private RDBMSCoordinationStrategy rdbmsCoordinationStrategy;
    private boolean isDuplicateNode = false;

    public ClusterCoordinator(DataSource coordinationDatasource) throws DataSourceException {

        this.rdbmsCoordinationStrategy = new RDBMSCoordinationStrategy(coordinationDatasource);
    }

    public void startCoordinator() {

        rdbmsCoordinationStrategy.joinGroup();
        List<String> allNodeIds = this.getAllNodeIds();
        allNodeIds.remove(getThisNodeId());
        allNodeIds.forEach(id -> log.info("Connected with node [" + id + "]"));
    }

    public boolean checkDuplicateNodeExistence() {
        return rdbmsCoordinationStrategy.isDuplicatedNode();
    }

    public int getHeartbeatMaxRetryInterval() {
        return rdbmsCoordinationStrategy.getHeartbeatMaxRetryInterval();
    }

    public boolean isDuplicateNode() {
        return isDuplicateNode;
    }

    public void setDuplicateNode(boolean isDuplicateNode) {
        this.isDuplicateNode = isDuplicateNode;
    }

    /**
     * Registers an event listener to listen to cluster events.
     *
     * @param listener - Listener instance.
     */
    public void registerListener(MemberEventListener listener) {
        rdbmsCoordinationStrategy.registerEventListener(listener);
    }

    /**
     * Checks whether the current node is leader or not.
     *
     * @return - whether this node is leader or not.
     */
    public boolean isLeader() {
        boolean isLeader = false;
        try {
            isLeader = rdbmsCoordinationStrategy.isLeaderNode();
        } catch (ClusterCoordinationException ex) {
            log.error("Exception occurred while checking leader node.", ex);
        }
        return isLeader;
    }

    /**
     * Give the ids of all the nodes present in the cluster.
     *
     * @return - List of node ids.
     */
    public List<String> getAllNodeIds() {

        List<String> nodeIds = new ArrayList<>();
        try {
            rdbmsCoordinationStrategy.getAllNodeDetails().forEach(node -> nodeIds.add(node.getNodeId()));
        } catch (ClusterCoordinationException ex) {
            log.error("Exception occurred while retrieving all node Ids.", ex);
        }
        return nodeIds;
    }

    /**
     * The throwing live-member read: identical to {@link #getAllNodeIds()} minus the catch, for
     * callers whose step must refuse on a membership-read failure instead of vacuously passing.
     *
     * @return - List of node ids.
     * @throws ClusterCoordinationException when the membership read fails.
     */
    public List<String> getAllNodeIdsOrThrow() throws ClusterCoordinationException {

        List<String> nodeIds = new ArrayList<>();
        rdbmsCoordinationStrategy.getAllNodeDetails().forEach(node -> nodeIds.add(node.getNodeId()));
        return nodeIds;
    }

    /**
     * Returns the id of this node.
     *
     * @return - Id of this node.
     */
    public String getThisNodeId() {
        return rdbmsCoordinationStrategy.getThisNodeId();
    }

    /**
     * Registers the heartbeat hook on the coordination strategy. ntask holds this facade, never the
     * strategy.
     *
     * @param hook - the heartbeat hook.
     */
    public void registerHeartbeatHook(HeartbeatHook hook) {
        rdbmsCoordinationStrategy.registerHeartbeatHook(hook);
    }

}

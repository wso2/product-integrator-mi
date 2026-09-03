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
import org.wso2.micro.integrator.coordination.node.NodeDetail;
import org.wso2.micro.integrator.coordination.query.QueryManager;
import org.wso2.micro.integrator.coordination.query.QueryManager.DBQueries;
import org.wso2.micro.integrator.coordination.util.CommunicationBusContext;
import org.wso2.micro.integrator.coordination.util.MemberEvent;
import org.wso2.micro.integrator.coordination.util.MemberEventType;
import org.wso2.micro.integrator.coordination.util.RDBMSConstantUtils;
import org.wso2.micro.integrator.coordination.util.StringUtil;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

/**
 * The RDBMS based communication bus layer for the nodes. This layer handles the database level calls.
 */
public class RDBMSCommunicationBusContextImpl implements CommunicationBusContext {
    /**
     * The log class
     */
    private static final Log log = LogFactory.getLog(RDBMSCommunicationBusContextImpl.class);

    /**
     * The datasource which is used to be connected to the database.
     */
    private DataSource datasource;
    private String databaseType = null;
    private QueryManager queryManager;

    /**
     * Heartbeat Advertise read gate: the NODE_ADVERTISEMENT table is only touched on an ARMED node —
     * the master envelope disengages every mechanism, and on a master-off cluster the table may not
     * exist. Set when the heartbeat hook is registered.
     */
    private volatile boolean advertisementReadsEnabled;

    /**
     * One node's advertised identity observed at read time.
     */
    private static final class AdvertisedIdentity {

        private final long heartbeatWindow;
        private final String bootId;

        private AdvertisedIdentity(long heartbeatWindow, String bootId) {
            this.heartbeatWindow = heartbeatWindow;
            this.bootId = bootId;
        }
    }

    public RDBMSCommunicationBusContextImpl(DataSource dataSource) {
        this.datasource = dataSource;

        Connection connection = null;
        try {
            connection = getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            this.databaseType = metaData.getDatabaseProductName();
            this.queryManager = new QueryManager(this.databaseType);
        } catch (SQLException e) {
            throw new ClusterCoordinationException("Database communication failed", e);
        } finally {
            close(connection, "Getting coordination database information");
        }
    }

    public RDBMSCommunicationBusContextImpl() {

    }

    @Override
    public void storeMembershipEvent(String changedMember, String groupId, List<String> clusterNodes,
                                     int membershipEventType) throws ClusterCoordinationException {
        Connection connection = null;
        boolean isRolledBack = false; // Flag to track if rollback has occurred
        boolean isTransactionSuccessful = false;
        PreparedStatement storeMembershipEventPreparedStatement = null;
        String task = "Storing membership event: " + membershipEventType + " for member: " + changedMember
                + " in group " + groupId;
        try {
            connection = getConnection();
            storeMembershipEventPreparedStatement = connection
                    .prepareStatement(queryManager.getQuery(DBQueries.INSERT_MEMBERSHIP_EVENT));
            for (String clusterNode : clusterNodes) {
                storeMembershipEventPreparedStatement.setString(1, clusterNode);
                storeMembershipEventPreparedStatement.setString(2, groupId);
                storeMembershipEventPreparedStatement.setInt(3, membershipEventType);
                storeMembershipEventPreparedStatement.setString(4, changedMember);
                storeMembershipEventPreparedStatement.addBatch();
            }
            storeMembershipEventPreparedStatement.executeBatch();
            commitTransactionIfNotInterrupted(connection);
            isTransactionSuccessful = true;
            if (log.isDebugEnabled()) {
                log.debug(StringUtil.removeCRLFCharacters(task) + " executed successfully");
            }
        } catch (SQLException | InterruptedException e) {
            rollback(connection, task);
            isRolledBack = true; // Mark as rolled back
            log.warn("Transaction rolled back for task: " + task);

            throw new ClusterCoordinationException("Error storing membership change: " + membershipEventType +
                    " for member: " + changedMember + " in group " + groupId, e);
        } finally {
            if (!isTransactionSuccessful && !isRolledBack && Thread.currentThread().isInterrupted()) {
                rollback(connection, task);
                log.warn("Transaction rolled back for task: " + task);
            }
            close(storeMembershipEventPreparedStatement, task);
            close(connection, task);
        }
    }

    @Override
    public List<MemberEvent> readMemberShipEvents(String nodeID) throws ClusterCoordinationException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        PreparedStatement clearMembershipEvents = null;
        ResultSet resultSet = null;
        List<MemberEvent> membershipEvents = new ArrayList<MemberEvent>();
        String task = "retrieving membership events destined to: " + nodeID;
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(queryManager.getQuery(
                    DBQueries.SELECT_MEMBERSHIP_EVENT));
            preparedStatement.setString(1, nodeID);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                MemberEvent membershipEvent = new MemberEvent(MemberEventType.getTypeFromInt(
                        resultSet.getInt(RDBMSConstantUtils.MEMBERSHIP_CHANGE_TYPE)),
                                                              resultSet.getString(RDBMSConstantUtils.MEMBERSHIP_CHANGED_MEMBER_ID),
                                                              resultSet.getString(RDBMSConstantUtils.GROUP_ID));
                membershipEvents.add(membershipEvent);
            }
            clearMembershipEvents = connection.prepareStatement(queryManager.getQuery(
                    DBQueries.CLEAN_MEMBERSHIP_EVENTS_FOR_NODE));
            clearMembershipEvents.setString(1, nodeID);
            clearMembershipEvents.executeUpdate();
            connection.commit();
            if (log.isDebugEnabled()) {
                log.debug(task + " executed successfully");
            }
            return membershipEvents;
        } catch (SQLException e) {
            throw new ClusterCoordinationException("Error occurred while " + task, e);
        } finally {
            close(resultSet, task);
            close(preparedStatement, task);
            close(clearMembershipEvents, task);
            close(connection, task);
        }
    }

    public Map<String, String> getMessageProcessorStates() {
        Map<String, String> updatedStates = new HashMap<>();
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(queryManager.getQuery(
                    DBQueries.SELECT_MP_STATE_UPDATE_EVENT));
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String mpName = resultSet.getString("MP_NAME");
                String newState = resultSet.getString("MP_STATE");
                updatedStates.put(mpName, newState);
            }
        } catch (SQLException e) {
            String errMsg = RDBMSConstantUtils.MP_GET_ALL_STATES;
            log.warn("Error occurred while " + errMsg, e);
        } finally {
            close(resultSet, RDBMSConstantUtils.MP_GET_ALL_STATES);
            close(preparedStatement, RDBMSConstantUtils.MP_GET_ALL_STATES);
            close(connection, RDBMSConstantUtils.MP_GET_ALL_STATES);
        }
        return updatedStates;
    }


    @Override
    public String getCoordinatorNodeId(String groupId) throws ClusterCoordinationException {

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(queryManager.getQuery(
                    DBQueries.GET_COORDINATOR_NODE_ID));
            preparedStatement.setString(1, groupId);
            resultSet = preparedStatement.executeQuery();
            String coordinatorNodeId;
            if (resultSet.next()) {
                coordinatorNodeId = resultSet.getString(1);
                if (log.isDebugEnabled()) {
                    log.debug("Coordinator node ID: " + StringUtil.removeCRLFCharacters(coordinatorNodeId) +
                              " for group : " + StringUtil.removeCRLFCharacters(groupId));
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("No coordinator present in database for group "
                              + StringUtil.removeCRLFCharacters(groupId));
                }
                coordinatorNodeId = null;
            }
            if (log.isDebugEnabled()) {
                log.debug(RDBMSConstantUtils.TASK_GET_COORDINATOR_INFORMATION + " executed successfully");
            }
            return coordinatorNodeId;
        } catch (SQLException e) {
            String errMsg = RDBMSConstantUtils.TASK_GET_COORDINATOR_INFORMATION;
            throw new ClusterCoordinationException("Error occurred while " + errMsg, e);
        } finally {
            close(resultSet, RDBMSConstantUtils.TASK_GET_COORDINATOR_INFORMATION);
            close(preparedStatement, RDBMSConstantUtils.TASK_GET_COORDINATOR_INFORMATION);
            close(connection, RDBMSConstantUtils.TASK_GET_COORDINATOR_INFORMATION);
        }

    }

    @Override
    public boolean createCoordinatorEntry(String nodeId, String groupId) throws ClusterCoordinationException {
        Connection connection = null;
        boolean isRolledBack = false; // Flag to track if rollback has occurred
        boolean isTransactionSuccessful = false;
        PreparedStatement preparedStatement = null;
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(queryManager.getQuery(
                    DBQueries.INSERT_COORDINATOR_ROW));
            preparedStatement.setString(1, groupId);
            preparedStatement.setString(2, nodeId);
            preparedStatement.setLong(3, System.currentTimeMillis());
            int updateCount = preparedStatement.executeUpdate();
            commitTransactionIfNotInterrupted(connection);
            isTransactionSuccessful = true;
            if (log.isDebugEnabled()) {
                log.debug(RDBMSConstantUtils.TASK_ADD_MESSAGE_ID + " " + nodeId + " executed successfully");
            }
            return updateCount != 0;
        } catch (SQLException | InterruptedException e) {
            rollback(connection, RDBMSConstantUtils.TASK_ADD_MESSAGE_ID);
            isRolledBack = true; // Mark as rolled back
            log.warn("Transaction rolled back for task: " + RDBMSConstantUtils.TASK_ADD_MESSAGE_ID);
            return false;
        } finally {
            if (!isTransactionSuccessful && !isRolledBack && Thread.currentThread().isInterrupted()) {
                rollback(connection, RDBMSConstantUtils.TASK_ADD_MESSAGE_ID);
                log.warn("Transaction rolled back for task: " + RDBMSConstantUtils.TASK_ADD_MESSAGE_ID);
            }
            close(preparedStatement, RDBMSConstantUtils.TASK_ADD_MESSAGE_ID);
            close(connection, RDBMSConstantUtils.TASK_ADD_MESSAGE_ID);
        }
    }

    @Override
    public boolean checkIsCoordinator(String nodeId, String groupId) throws ClusterCoordinationException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(queryManager.getQuery(
                    DBQueries.GET_COORDINATOR_ROW_FOR_NODE_ID));
            preparedStatement.setString(1, nodeId);
            preparedStatement.setString(2, groupId);
            resultSet = preparedStatement.executeQuery();
            boolean isCoordinator;
            isCoordinator = resultSet.next();
            if (log.isDebugEnabled()) {
                log.debug(RDBMSConstantUtils.TASK_CHECK_IS_COORDINATOR + " instance id: " + nodeId
                          + " group ID: " + groupId + " executed successfully");
            }
            return isCoordinator;
        } catch (SQLException e) {
            String errMsg = RDBMSConstantUtils.TASK_CHECK_IS_COORDINATOR + " instance id: " + nodeId
                            + " group ID: " + groupId;
            throw new ClusterCoordinationException("Error occurred while " + errMsg, e);
        } finally {
            close(resultSet, RDBMSConstantUtils.TASK_CHECK_IS_COORDINATOR);
            close(preparedStatement, RDBMSConstantUtils.TASK_CHECK_IS_COORDINATOR);
            close(connection, RDBMSConstantUtils.TASK_CHECK_IS_COORDINATOR);
        }
    }

    @Override
    public boolean updateCoordinatorHeartbeat(String nodeId, String groupId, long currentHeartbeatTime)
            throws ClusterCoordinationException {
        Connection connection = null;
        boolean isRolledBack = false; // Flag to track if rollback has occurred
        boolean isTransactionSuccessful = false;
        PreparedStatement preparedStatementForCoordinatorUpdate = null;
        try {
            connection = getConnection();
            preparedStatementForCoordinatorUpdate = connection
                    .prepareStatement(queryManager.getQuery(DBQueries.UPDATE_COORDINATOR_HEARTBEAT));
            preparedStatementForCoordinatorUpdate.setLong(1, currentHeartbeatTime);
            preparedStatementForCoordinatorUpdate.setString(2, nodeId);
            preparedStatementForCoordinatorUpdate.setString(3, groupId);
            int updateCount = preparedStatementForCoordinatorUpdate.executeUpdate();
            commitTransactionIfNotInterrupted(connection);
            isTransactionSuccessful = true;
            if (log.isDebugEnabled()) {
                log.debug(RDBMSConstantUtils.TASK_UPDATE_COORDINATOR_HEARTBEAT + "node id " + nodeId
                        + " executed successfully");
            }
            return updateCount != 0;
        } catch (SQLException | InterruptedException e) {
            rollback(connection, RDBMSConstantUtils.TASK_UPDATE_COORDINATOR_HEARTBEAT);
            isRolledBack = true; // Mark as rolled back
            log.warn("Transaction rolled back for task: " + RDBMSConstantUtils.TASK_UPDATE_COORDINATOR_HEARTBEAT);
            throw new ClusterCoordinationException("Error occurred while "
                                                   + RDBMSConstantUtils.TASK_UPDATE_COORDINATOR_HEARTBEAT
                                                   + ". instance ID: " + nodeId + " group ID: " + groupId, e);
        } finally {
            if (!isTransactionSuccessful && !isRolledBack && Thread.currentThread().isInterrupted()) {
                rollback(connection, RDBMSConstantUtils.TASK_UPDATE_COORDINATOR_HEARTBEAT);
                log.warn("Transaction rolled back for task: " + RDBMSConstantUtils.TASK_UPDATE_COORDINATOR_HEARTBEAT);
            }
            close(preparedStatementForCoordinatorUpdate, RDBMSConstantUtils.TASK_UPDATE_COORDINATOR_HEARTBEAT);
            close(connection, RDBMSConstantUtils.TASK_UPDATE_COORDINATOR_HEARTBEAT);
        }
    }

    @Override
    public boolean checkIfCoordinatorValid(String groupId, String nodeId, int heartbeatMaxAge,
                                           long currentHeartbeatTime) throws ClusterCoordinationException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(queryManager.getQuery(
                    DBQueries.GET_COORDINATOR_HEARTBEAT));
            preparedStatement.setString(1, groupId);
            resultSet = preparedStatement.executeQuery();
            boolean isCoordinatorValid;
            if (resultSet.next()) {
                long coordinatorHeartbeat = resultSet.getLong(1);
                long heartbeatAge = currentHeartbeatTime - coordinatorHeartbeat;
                isCoordinatorValid = heartbeatAge <= heartbeatMaxAge;
                if (!isCoordinatorValid) {
                    log.info("Coordinator is invalid, because there is no heartbeat for " + heartbeatAge
                             + " millis when checked by nodeId: " + nodeId +
                             ". The heartbeat should have happened in " + heartbeatMaxAge);
                }
            } else {
                log.info("No valid coordinator present in database for group " + groupId +
                         " when checked by nodeId: " + nodeId);
                isCoordinatorValid = false;
            }
            return isCoordinatorValid;
        } catch (SQLException e) {
            String errMsg = RDBMSConstantUtils.TASK_CHECK_COORDINATOR_VALIDITY;
            throw new ClusterCoordinationException("Error occurred while " + errMsg, e);
        } finally {
            close(resultSet, RDBMSConstantUtils.TASK_CHECK_COORDINATOR_VALIDITY);
            close(preparedStatement, RDBMSConstantUtils.TASK_CHECK_COORDINATOR_VALIDITY);
            close(connection, RDBMSConstantUtils.TASK_CHECK_COORDINATOR_VALIDITY);
        }
    }

    @Override
    public void removeCoordinator(String groupId, int heartbeatMaxAge, long currentHeartbeatTime)
            throws ClusterCoordinationException {
        Connection connection = null;
        boolean isRolledBack = false; // Flag to track if rollback has occurred
        boolean isTransactionSuccessful = false;
        PreparedStatement preparedStatement = null;
        try {
            connection = getConnection();
            long thresholdTimeLimit = currentHeartbeatTime - heartbeatMaxAge;
            preparedStatement = connection.prepareStatement(queryManager.getQuery(DBQueries.DELETE_COORDINATOR));
            preparedStatement.setString(1, groupId);
            preparedStatement.setLong(2, thresholdTimeLimit);
            preparedStatement.executeUpdate();
            commitTransactionIfNotInterrupted(connection);
            isTransactionSuccessful = true;
            if (log.isDebugEnabled()) {
                log.debug(RDBMSConstantUtils.TASK_REMOVE_COORDINATOR + " of group " + groupId + " executed successfully");
            }
        } catch (SQLException | InterruptedException e) {
            rollback(connection, RDBMSConstantUtils.TASK_REMOVE_COORDINATOR);
            isRolledBack = true; // Mark as rolled back
            log.warn("Transaction rolled back for task: " + RDBMSConstantUtils.TASK_REMOVE_COORDINATOR);
            throw new ClusterCoordinationException("Error occurred while "
                    + RDBMSConstantUtils.TASK_REMOVE_COORDINATOR, e);
        } finally {
            if (!isTransactionSuccessful && !isRolledBack && Thread.currentThread().isInterrupted()) {
                rollback(connection, RDBMSConstantUtils.TASK_REMOVE_COORDINATOR);
                log.warn("Transaction rolled back for task: " + RDBMSConstantUtils.TASK_REMOVE_COORDINATOR);
            }
            close(preparedStatement, RDBMSConstantUtils.TASK_REMOVE_COORDINATOR);
            close(connection, RDBMSConstantUtils.TASK_REMOVE_COORDINATOR);
        }
    }

    @Override
    public void removeCoordinatorSelf(String groupId, String nodeId) throws ClusterCoordinationException {
        Connection connection = null;
        boolean isRolledBack = false; // Flag to track if rollback has occurred
        boolean isTransactionSuccessful = false;
        PreparedStatement preparedStatement = null;
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(queryManager.getQuery(
                    DBQueries.REMOVE_COORDINATOR_SELF));
            preparedStatement.setString(1, groupId);
            preparedStatement.setString(2, nodeId);
            // zero rows deleted is success — the row is own-node conditioned
            preparedStatement.executeUpdate();
            commitTransactionIfNotInterrupted(connection);
            isTransactionSuccessful = true;
            if (log.isDebugEnabled()) {
                log.debug(RDBMSConstantUtils.TASK_REMOVE_COORDINATOR_SELF + " of node " + nodeId + " in group "
                        + groupId + " executed successfully");
            }
        } catch (SQLException | InterruptedException e) {
            rollback(connection, RDBMSConstantUtils.TASK_REMOVE_COORDINATOR_SELF);
            isRolledBack = true; // Mark as rolled back
            log.warn("Transaction rolled back for task: " + RDBMSConstantUtils.TASK_REMOVE_COORDINATOR_SELF);
            throw new ClusterCoordinationException("Error occurred while "
                    + RDBMSConstantUtils.TASK_REMOVE_COORDINATOR_SELF + ". Node ID: " + nodeId + " and Group ID: "
                    + groupId, e);
        } finally {
            if (!isTransactionSuccessful && !isRolledBack && Thread.currentThread().isInterrupted()) {
                rollback(connection, RDBMSConstantUtils.TASK_REMOVE_COORDINATOR_SELF);
                log.warn("Transaction rolled back for task: " + RDBMSConstantUtils.TASK_REMOVE_COORDINATOR_SELF);
            }
            close(preparedStatement, RDBMSConstantUtils.TASK_REMOVE_COORDINATOR_SELF);
            close(connection, RDBMSConstantUtils.TASK_REMOVE_COORDINATOR_SELF);
        }
    }

    @Override
    public boolean updateNodeHeartbeat(String nodeId, String groupId, long currentHeartbeatTime)
            throws ClusterCoordinationException {
        Connection connection = null;
        boolean isRolledBack = false; // Flag to track if rollback has occurred
        boolean isTransactionSuccessful = false;
        PreparedStatement preparedStatementForNodeUpdate = null;

        try {
            connection = getConnection();
            preparedStatementForNodeUpdate = connection.prepareStatement(queryManager.getQuery(
                    DBQueries.UPDATE_NODE_HEARTBEAT));
            preparedStatementForNodeUpdate.setLong(1, currentHeartbeatTime);
            preparedStatementForNodeUpdate.setString(2, nodeId);
            preparedStatementForNodeUpdate.setString(3, groupId);
            int updateCount = preparedStatementForNodeUpdate.executeUpdate();
            commitTransactionIfNotInterrupted(connection);
            isTransactionSuccessful = true;
            if (log.isDebugEnabled()) {
                log.debug(RDBMSConstantUtils.TASK_UPDATE_NODE_HEARTBEAT + " of node " + nodeId + " executed successfully");
            }
            return updateCount != 0;
        } catch (SQLException | InterruptedException e) {
            rollback(connection, RDBMSConstantUtils.TASK_UPDATE_NODE_HEARTBEAT);
            isRolledBack = true; // Mark as rolled back
            log.warn("Transaction rolled back for task: " + RDBMSConstantUtils.TASK_UPDATE_NODE_HEARTBEAT);
            throw new ClusterCoordinationException("Error occurred while "
                    + RDBMSConstantUtils.TASK_UPDATE_NODE_HEARTBEAT + ". Node ID: " + nodeId + "and Group ID : "
                    + groupId, e);
        } finally {
            if (!isTransactionSuccessful && !isRolledBack && Thread.currentThread().isInterrupted()) {
                rollback(connection, RDBMSConstantUtils.TASK_UPDATE_NODE_HEARTBEAT);
                log.warn("Transaction rolled back for task: " + RDBMSConstantUtils.TASK_UPDATE_NODE_HEARTBEAT);
            }
            close(preparedStatementForNodeUpdate, RDBMSConstantUtils.TASK_UPDATE_NODE_HEARTBEAT);
            close(connection, RDBMSConstantUtils.TASK_UPDATE_NODE_HEARTBEAT);
        }
    }

    /**
     * Method to commit the transaction.
     *
     * @param connection Connection object
     *
     */
    private void commitTransactionIfNotInterrupted(Connection connection) throws InterruptedException, SQLException {
        // Check if the thread has been interrupted before committing
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Thread was interrupted, avoiding commit.");
        }
        // Commit the transaction
        connection.commit();
    }

    @Override
    public void createNodeHeartbeatEntry(String nodeId, String groupId) throws ClusterCoordinationException {
        Connection connection = null;
        boolean isRolledBack = false; // Flag to track if rollback has occurred
        boolean isTransactionSuccessful = false;
        PreparedStatement preparedStatement = null;
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(queryManager.getQuery(
                    DBQueries.INSERT_NODE_HEARTBEAT_ROW));
            preparedStatement.setString(1, nodeId);
            preparedStatement.setLong(2, System.currentTimeMillis());
            preparedStatement.setString(3, groupId);
            preparedStatement.executeUpdate();
            commitTransactionIfNotInterrupted(connection);
            isTransactionSuccessful = true;
            if (log.isDebugEnabled()) {
                log.debug(RDBMSConstantUtils.TASK_CREATE_NODE_HEARTBEAT + " of node " + nodeId + " executed successfully");
            }
        } catch (SQLException | InterruptedException e) {
            rollback(connection, RDBMSConstantUtils.TASK_CREATE_NODE_HEARTBEAT);
            isRolledBack = true; // Mark as rolled back
            log.warn("Transaction rolled back for task: " + RDBMSConstantUtils.TASK_CREATE_NODE_HEARTBEAT);
            throw new ClusterCoordinationException("Error occurred while "
                    + RDBMSConstantUtils.TASK_CREATE_NODE_HEARTBEAT
                    + ". Node ID: " + nodeId + " group ID " + groupId, e);
        } finally {
            if (!isTransactionSuccessful && !isRolledBack && Thread.currentThread().isInterrupted()) {
                rollback(connection, RDBMSConstantUtils.TASK_CREATE_NODE_HEARTBEAT);
                log.warn("Transaction rolled back for task: " + RDBMSConstantUtils.TASK_CREATE_NODE_HEARTBEAT);
            }
            close(preparedStatement, RDBMSConstantUtils.TASK_UPDATE_COORDINATOR_HEARTBEAT);
            close(connection, RDBMSConstantUtils.TASK_CREATE_NODE_HEARTBEAT);
        }
    }

    @Override
    public List<NodeDetail> getAllNodeData(String groupId) throws ClusterCoordinationException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        String coordinatorNodeId = getCoordinatorNodeId(groupId);
        if (coordinatorNodeId == null) {
            coordinatorNodeId = getCoordinatorNodeId(groupId);
        }
        ArrayList<NodeDetail> nodeDataList = new ArrayList<NodeDetail>();

        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(queryManager.getQuery(
                    DBQueries.GET_ALL_NODE_HEARTBEAT));
            preparedStatement.setString(1, groupId);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String nodeId = resultSet.getString(2);
                boolean isCoordinatorNode = false;
                if (coordinatorNodeId != null) {
                    isCoordinatorNode = coordinatorNodeId.equals(nodeId);
                }
                long lastHeartbeat = resultSet.getLong(3);
                boolean isNewNode = convertIntToBoolean(resultSet.getInt(4));
                NodeDetail heartBeatData = new NodeDetail(nodeId, groupId, isCoordinatorNode,
                                                          lastHeartbeat, isNewNode);
                nodeDataList.add(heartBeatData);
            }

        } catch (SQLException e) {
            String errMsg = RDBMSConstantUtils.TASK_GET_ALL_QUEUES;
            throw new ClusterCoordinationException("Error occurred while " + errMsg, e);
        } finally {
            close(resultSet, RDBMSConstantUtils.TASK_GET_ALL_QUEUES);
            close(preparedStatement, RDBMSConstantUtils.TASK_GET_ALL_QUEUES);
            close(connection, RDBMSConstantUtils.TASK_GET_ALL_QUEUES);
        }
        // Heartbeat Advertise: join the advertised windows in memory by NODE_ID — read fresh on every
        // call, never cached.
        Map<String, AdvertisedIdentity> advertisements = readNodeAdvertisements(groupId);
        for (NodeDetail nodeDetail : nodeDataList) {
            AdvertisedIdentity advertised = advertisements.get(nodeDetail.getNodeId());
            if (advertised != null) {
                nodeDetail.setHeartbeatWindow(advertised.heartbeatWindow);
                nodeDetail.setAdvertisedBootId(advertised.bootId);
            }
        }
        if (log.isDebugEnabled()) {
            log.debug(RDBMSConstantUtils.TASK_GET_ALL_QUEUES + " of group " + groupId + " executed successfully");
        }
        return nodeDataList;
    }

    /**
     * Turns advertisement reads on once the node is ARMED (the heartbeat hook is registered). Until
     * then no read touches NODE_ADVERTISEMENT and every judge falls back to its local window.
     */
    public void setAdvertisementReadsEnabled(boolean enabled) {
        this.advertisementReadsEnabled = enabled;
    }

    /**
     * Reads every advertised (window, boot id) of the group, keyed by NODE_ID. Empty when the gate is
     * off.
     */
    private Map<String, AdvertisedIdentity> readNodeAdvertisements(String groupId)
            throws ClusterCoordinationException {
        Map<String, AdvertisedIdentity> advertisements = new HashMap<>();
        if (!advertisementReadsEnabled) {
            return advertisements;
        }
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(queryManager.getQuery(
                    DBQueries.GET_NODE_ADVERTISEMENTS));
            preparedStatement.setString(1, groupId);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                advertisements.put(resultSet.getString(1),
                        new AdvertisedIdentity(resultSet.getLong(2), resultSet.getString(3)));
            }
        } catch (SQLException e) {
            throw new ClusterCoordinationException("Error occurred while "
                    + RDBMSConstantUtils.TASK_READ_NODE_ADVERTISEMENTS, e);
        } finally {
            close(resultSet, RDBMSConstantUtils.TASK_READ_NODE_ADVERTISEMENTS);
            close(preparedStatement, RDBMSConstantUtils.TASK_READ_NODE_ADVERTISEMENTS);
            close(connection, RDBMSConstantUtils.TASK_READ_NODE_ADVERTISEMENTS);
        }
        return advertisements;
    }

    /**
     * Reads one node's advertised (window, boot id). Null when the node has no advertisement row or
     * the gate is off.
     */
    private AdvertisedIdentity readNodeAdvertisement(String groupId, String nodeId)
            throws ClusterCoordinationException {
        if (!advertisementReadsEnabled) {
            return null;
        }
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(queryManager.getQuery(
                    DBQueries.GET_NODE_ADVERTISEMENT));
            preparedStatement.setString(1, groupId);
            preparedStatement.setString(2, nodeId);
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return new AdvertisedIdentity(resultSet.getLong(1), resultSet.getString(2));
            }
            return null;
        } catch (SQLException e) {
            throw new ClusterCoordinationException("Error occurred while "
                    + RDBMSConstantUtils.TASK_READ_NODE_ADVERTISEMENTS, e);
        } finally {
            close(resultSet, RDBMSConstantUtils.TASK_READ_NODE_ADVERTISEMENTS);
            close(preparedStatement, RDBMSConstantUtils.TASK_READ_NODE_ADVERTISEMENTS);
            close(connection, RDBMSConstantUtils.TASK_READ_NODE_ADVERTISEMENTS);
        }
    }

    /**
     * Reads one node's advertised heartbeat window. 0 when the node has no advertisement row or the
     * gate is off — the judge falls back to its local window.
     */
    public long getAdvertisedHeartbeatWindow(String groupId, String nodeId) throws ClusterCoordinationException {
        AdvertisedIdentity advertised = readNodeAdvertisement(groupId, nodeId);
        return advertised == null ? 0 : advertised.heartbeatWindow;
    }

    @Override
    public NodeDetail getRemovedNodeData(String nodeId, String groupId,
                                         String removedMemberId) throws ClusterCoordinationException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        PreparedStatement clearMembershipEvents = null;
        ResultSet resultSet = null;
        NodeDetail nodeDetail = null;
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(queryManager.getQuery(
                    DBQueries.SELECT_REMOVED_MEMBER_DETAILS));
            preparedStatement.setString(1, nodeId);
            preparedStatement.setString(2, removedMemberId);
            preparedStatement.setString(3, groupId);
            resultSet = preparedStatement.executeQuery();

            if (resultSet.next()) {
                nodeDetail = new NodeDetail(removedMemberId, groupId, false, 0, false);
            }
            clearMembershipEvents = connection
                    .prepareStatement(queryManager.getQuery(DBQueries.DELETE_REMOVED_MEMBER_DETAIL_FOR_NODE));
            clearMembershipEvents.setString(1, nodeId);
            clearMembershipEvents.setString(2, removedMemberId);
            clearMembershipEvents.setString(3, groupId);
            clearMembershipEvents.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            String errMsg = RDBMSConstantUtils.TASK_GET_ALL_QUEUES;
            throw new ClusterCoordinationException("Error occurred while " + errMsg, e);
        } finally {
            close(resultSet, RDBMSConstantUtils.TASK_GET_ALL_QUEUES);
            close(preparedStatement, RDBMSConstantUtils.TASK_GET_ALL_QUEUES);
            close(clearMembershipEvents, RDBMSConstantUtils.TASK_GET_ALL_QUEUES);
            close(connection, RDBMSConstantUtils.TASK_GET_ALL_QUEUES);
        }
        if (log.isDebugEnabled()) {
            log.debug(RDBMSConstantUtils.TASK_GET_ALL_QUEUES + " of removed nodes in group "
                      + StringUtil.removeCRLFCharacters(groupId) + " executed successfully");
        }
        return nodeDetail;
    }

    @Override
    public NodeDetail getNodeData(String nodeId, String groupId) throws ClusterCoordinationException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        String coordinatorNodeId = getCoordinatorNodeId(groupId);
        if (coordinatorNodeId == null) {
            coordinatorNodeId = getCoordinatorNodeId(groupId);
        }
        NodeDetail nodeDetail = null;

        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(queryManager.getQuery(DBQueries.GET_NODE_DATA));
            preparedStatement.setString(1, groupId);
            preparedStatement.setString(2, nodeId);
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                boolean isCoordinatorNode = nodeId.equals(coordinatorNodeId);
                long lastHeartbeat = resultSet.getLong(3);
                boolean isNewNode = convertIntToBoolean(resultSet.getInt(4));
                nodeDetail = new NodeDetail(nodeId, groupId, isCoordinatorNode, lastHeartbeat, isNewNode);
            }
        } catch (SQLException e) {
            String errMsg = RDBMSConstantUtils.TASK_GET_ALL_QUEUES;
            throw new ClusterCoordinationException("Error occurred while " + errMsg, e);
        } finally {
            close(resultSet, RDBMSConstantUtils.TASK_GET_ALL_QUEUES);
            close(preparedStatement, RDBMSConstantUtils.TASK_GET_ALL_QUEUES);
            close(connection, RDBMSConstantUtils.TASK_GET_ALL_QUEUES);
        }
        if (nodeDetail != null) {
            // Heartbeat Advertise: fresh in-memory join by NODE_ID, like getAllNodeData
            AdvertisedIdentity advertised = readNodeAdvertisement(groupId, nodeId);
            if (advertised != null) {
                nodeDetail.setHeartbeatWindow(advertised.heartbeatWindow);
                nodeDetail.setAdvertisedBootId(advertised.bootId);
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("getting node data of node " + StringUtil.removeCRLFCharacters(nodeId) +
                      " executed successfully");
        }
        return nodeDetail;
    }

    @Override
    public void removeNode(String nodeId, String groupId) throws ClusterCoordinationException {
        removeNode(nodeId, groupId, null);
    }

    /**
     * Removes a node's heartbeat row and, when an advertisement BOOT_ID was observed at judgment
     * time, that advertisement row — conditioned on the OBSERVED BOOT_ID, so a slow evictor can never
     * erase a restarted successor's live lease.
     *
     * @param nodeId                    node id of the evicted node
     * @param groupId                   group id of the evicted node
     * @param observedAdvertisementBootId the BOOT_ID observed on the node's advertisement row at
     *                                  judgment time, or null to skip the advertisement cleanup
     */
    public void removeNode(String nodeId, String groupId, String observedAdvertisementBootId)
            throws ClusterCoordinationException {
        Connection connection = null;
        boolean isRolledBack = false; // Flag to track if rollback has occurred
        boolean isTransactionSuccessful = false;
        PreparedStatement preparedStatement = null;
        PreparedStatement advertisementDelete = null;
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(queryManager.getQuery(
                    DBQueries.DELETE_NODE_HEARTBEAT));
            preparedStatement.setString(1, nodeId);
            preparedStatement.setString(2, groupId);
            preparedStatement.executeUpdate();
            if (observedAdvertisementBootId != null) {
                advertisementDelete = connection.prepareStatement(queryManager.getQuery(
                        DBQueries.DELETE_NODE_ADVERTISEMENT_OF_BOOT));
                advertisementDelete.setString(1, groupId);
                advertisementDelete.setString(2, nodeId);
                advertisementDelete.setString(3, observedAdvertisementBootId);
                advertisementDelete.executeUpdate();
            }
            commitTransactionIfNotInterrupted(connection);
            isTransactionSuccessful = true;
            if (log.isDebugEnabled()) {
                log.debug(RDBMSConstantUtils.TASK_REMOVE_NODE_HEARTBEAT + " of node "
                        + StringUtil.removeCRLFCharacters(nodeId) + " executed successfully");
            }
        } catch (SQLException | InterruptedException e) {
            rollback(connection, RDBMSConstantUtils.TASK_REMOVE_NODE_HEARTBEAT);
            isRolledBack = true; // Mark as rolled back
            log.warn("Transaction rolled back for task: " + RDBMSConstantUtils.TASK_REMOVE_NODE_HEARTBEAT);
            throw new ClusterCoordinationException("error occurred while "
                    + RDBMSConstantUtils.TASK_REMOVE_NODE_HEARTBEAT, e);
        } finally {
            if (!isTransactionSuccessful && !isRolledBack && Thread.currentThread().isInterrupted()) {
                rollback(connection, RDBMSConstantUtils.TASK_REMOVE_NODE_HEARTBEAT);
                log.warn("Transaction rolled back for task: " + RDBMSConstantUtils.TASK_REMOVE_NODE_HEARTBEAT);
            }
            close(advertisementDelete, RDBMSConstantUtils.TASK_REMOVE_NODE_HEARTBEAT);
            close(preparedStatement, RDBMSConstantUtils.TASK_REMOVE_NODE_HEARTBEAT);
            close(connection, RDBMSConstantUtils.TASK_REMOVE_NODE_HEARTBEAT);
        }
    }

    @Override
    public void insertRemovedNodeDetails(String removedMember, String groupId, List<String> clusterNodes)
            throws ClusterCoordinationException {
        Connection connection = null;
        boolean isRolledBack = false; // Flag to track if rollback has occurred
        boolean isTransactionSuccessful = false;
        PreparedStatement storeRemovedMembersPreparedStatement = null;
        String task = "Storing removed member: " + removedMember + " in group " + groupId;
        try {
            connection = getConnection();
            storeRemovedMembersPreparedStatement = connection
                    .prepareStatement(queryManager.getQuery(DBQueries.INSERT_REMOVED_MEMBER_DETAILS));

            for (String clusterNode : clusterNodes) {
                storeRemovedMembersPreparedStatement.setString(1, clusterNode);
                storeRemovedMembersPreparedStatement.setString(2, groupId);
                storeRemovedMembersPreparedStatement.setString(3, removedMember);
                storeRemovedMembersPreparedStatement.addBatch();
            }
            storeRemovedMembersPreparedStatement.executeBatch();
            commitTransactionIfNotInterrupted(connection);
            isTransactionSuccessful = true;
            if (log.isDebugEnabled()) {
                log.debug(StringUtil.removeCRLFCharacters(task) + " executed successfully");
            }
        } catch (SQLException | InterruptedException e) {
            rollback(connection, task);
            isRolledBack = true; // Mark as rolled back
            log.warn("Transaction rolled back for task: " + task);
            throw new ClusterCoordinationException(
                    "Error storing removed member: " + removedMember + " in group " + groupId, e);
        } finally {
            if (!isTransactionSuccessful && !isRolledBack && Thread.currentThread().isInterrupted()) {
                rollback(connection, task);
                log.warn("Transaction rolled back for task: " + task);
            }
            close(storeRemovedMembersPreparedStatement, task);
            close(connection, task);
        }
    }

    @Override
    public void markNodeAsNotNew(String nodeId, String groupId) throws ClusterCoordinationException {
        Connection connection = null;
        boolean isRolledBack = false; // Flag to track if rollback has occurred
        boolean isTransactionSuccessful = false;
        PreparedStatement preparedStatement = null;
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(queryManager.getQuery(DBQueries.MARK_NODE_NOT_NEW));
            preparedStatement.setString(1, nodeId);
            preparedStatement.setString(2, groupId);
            int updateCount = preparedStatement.executeUpdate();
            if (updateCount == 0) {
                log.warn("No record was updated while marking node as not new");
            }
            commitTransactionIfNotInterrupted(connection);
            isTransactionSuccessful = true;
            if (log.isDebugEnabled()) {
                log.debug(RDBMSConstantUtils.TASK_MARK_NODE_NOT_NEW + " of node "
                        + StringUtil.removeCRLFCharacters(nodeId) + " executed successfully");
            }
        } catch (SQLException | InterruptedException e) {
            rollback(connection, RDBMSConstantUtils.TASK_MARK_NODE_NOT_NEW);
            isRolledBack = true; // Mark as rolled back
            log.warn("Transaction rolled back for task: " + RDBMSConstantUtils.TASK_MARK_NODE_NOT_NEW);
            throw new ClusterCoordinationException("Error occurred while "
                    + RDBMSConstantUtils.TASK_MARK_NODE_NOT_NEW, e);
        } finally {
            if (!isTransactionSuccessful && !isRolledBack && Thread.currentThread().isInterrupted()) {
                rollback(connection, RDBMSConstantUtils.TASK_MARK_NODE_NOT_NEW);
                log.warn("Transaction rolled back for task: " + RDBMSConstantUtils.TASK_MARK_NODE_NOT_NEW);
            }
            close(preparedStatement, RDBMSConstantUtils.TASK_MARK_NODE_NOT_NEW);
            close(connection, RDBMSConstantUtils.TASK_MARK_NODE_NOT_NEW);
        }
    }


    @Override
    public void clearHeartBeatData() throws ClusterCoordinationException {
        Connection connection = null;
        PreparedStatement clearNodeHeartbeatData = null;
        PreparedStatement clearCoordinatorHeartbeatData = null;
        String task = "Clearing all heartbeat data";
        try {
            connection = getConnection();
            clearNodeHeartbeatData = connection.prepareStatement(queryManager.getQuery(
                    DBQueries.CLEAR_NODE_HEARTBEATS));
            clearNodeHeartbeatData.executeUpdate();

            clearCoordinatorHeartbeatData = connection.prepareStatement(queryManager.getQuery(
                    DBQueries.CLEAR_COORDINATOR_HEARTBEAT));
            clearCoordinatorHeartbeatData.executeUpdate();
            connection.commit();
            if (log.isDebugEnabled()) {
                log.debug(task + " executed successfully");
            }
        } catch (SQLException e) {
            rollback(connection, task);
            throw new ClusterCoordinationException("Error occurred while " + task, e);
        } finally {
            close(clearNodeHeartbeatData, task);
            close(clearCoordinatorHeartbeatData, task);
            close(connection, task);
        }
    }

    @Override
    public void clearMembershipEvents(String nodeID, String groupID) throws ClusterCoordinationException {
        Connection connection = null;
        PreparedStatement clearMembershipEvents = null;
        String task = "Clearing all membership events for node: " + nodeID;
        try {
            connection = getConnection();
            clearMembershipEvents = connection.prepareStatement(queryManager.getQuery(
                    DBQueries.CLEAN_MEMBERSHIP_EVENTS_FOR_NODE));
            clearMembershipEvents.setString(1, nodeID);
            clearMembershipEvents.executeUpdate();
            connection.commit();
            if (log.isDebugEnabled()) {
                log.debug(task + " executed successfully");
            }
        } catch (SQLException e) {
            rollback(connection, task);
            throw new ClusterCoordinationException("Error occurred while " + task, e);
        } finally {
            close(clearMembershipEvents, task);
            close(connection, task);
        }
    }

    /**
     * Get the connection to the database.
     */
    private Connection getConnection() throws SQLException {
        Connection connection = datasource.getConnection();
        connection.setAutoCommit(false);
        return connection;
    }

    /**
     * Close the resultset.
     *
     * @param resultSet The resultset which should be closed
     * @param task      The task which was running
     */
    private void close(ResultSet resultSet, String task) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                log.error("Closing result set failed after " + task, e);
            }
        }
    }

    /**
     * close the connection.
     *
     * @param connection The connection to be closed
     * @param task       The task which was running
     */
    private void close(Connection connection, String task) {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.rollback(); // to prevent inconsistencies in select statements
                connection.close();
            }
        } catch (SQLException e) {
            log.error("Failed to close connection after " + StringUtil.removeCRLFCharacters(task), e);
        }
    }

    /**
     * Close the prepared statement.
     *
     * @param preparedStatement The statement to be closed
     * @param task              The task which was running
     */
    private void close(Statement preparedStatement, String task) {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                log.error("Closing prepared statement failed after " + StringUtil.removeCRLFCharacters(task), e);
            }
        }
    }

    /**
     * The rollback method.
     *
     * @param connection The connection object which the rollback should be applied to
     * @param task       The task which was running
     */
    private void rollback(Connection connection, String task) {
        if (connection != null) {
            try {
                if (!connection.isClosed()) {
                    connection.rollback();
                }
            } catch (SQLException e) {
                log.warn("Rollback failed on " + StringUtil.removeCRLFCharacters(task), e);
            }
        }
    }

    /**
     * Convert a value to boolean.
     *
     * @param value the value to be converted to boolean
     * @return the converted boolean
     */
    private boolean convertIntToBoolean(int value) {
        return value != 0;
    }

}

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

package org.wso2.micro.integrator.ntask.coordination.task.store.connector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.micro.integrator.ntask.coordination.TaskCoordinationException;
import org.wso2.micro.integrator.ntask.coordination.task.CoordinatedTask;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.sql.DataSource;

import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.ACTIVATE_TASK;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.ADD_TASK;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.BARRIER_STATUS_FINALIZING;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.BARRIER_STATUS_OPEN;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.CLEAN_TASKS_OF_NODE;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.DELETE_TASK;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.DELETE_TASK_IF_STATE_MATCH;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.DELETE_TASK_IF_STATE_NOT_MATCH;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.DELETE_TASK_DELETE_BARRIER;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.DELETE_TASK_DELETE_BARRIER_ACKS;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.DELETE_TASK_DELETE_BARRIER_EXPECTED;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.DEADLINE_AT;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.DESTINED_NODE_ID;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.GET_ALL_ASSIGNED_INCOMPLETE_TASKS;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.GUARD_UUID;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.INSERT_TASK_DELETE_BARRIER;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.INSERT_TASK_DELETE_BARRIER_ACK;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.INSERT_TASK_DELETE_BARRIER_EXPECTED;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.INSERT_TASK_DELETE_GUARD;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.MP_STATE;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.NODE_ID;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.OWNER_NODE_ID;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.REMOVE_ASSIGNMENT_AND_UPDATE_STATE;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.REMOVE_TASKS_OF_NODE;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.RETRIEVE_ALL_TASKS;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.RETRIEVE_MP_STATE;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.RETRIEVE_TASKS_OF_NODE;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.RETRIEVE_TASK_STATE;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.RETRIEVE_UNASSIGNED_NOT_COMPLETED_TASKS;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.SELECT_OPEN_TASK_DELETE_BARRIERS;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.SELECT_OPEN_TASK_DELETE_BARRIER_BY_TASK_AND_GUARD;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.SELECT_MAX_TASK_DELETE_GUARD_UPDATED_AT;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.SELECT_TASK_DELETE_BARRIER_ACK_NODES;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.SELECT_TASK_DELETE_BARRIER;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.SELECT_TASK_DELETE_BARRIER_EXPECTED_NODES;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.SELECT_TASK_DELETE_GUARD;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.TASK_NAME;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.TASK_DELETE_PENDING_STATE;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.TASK_STATE;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UPDATE_ASSIGNMENT_AND_STATE;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UPDATE_TASK_DELETE_BARRIER_ACK;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UPDATE_TASK_DELETE_BARRIER_STATUS;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UPDATE_TASK_DELETE_BARRIER_TIMESTAMP;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UPDATE_TASK_DELETE_GUARD;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UPDATE_TASK_DELETE_GUARD_IF_MATCH;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UPDATE_TASK_DELETE_GUARD_TIMESTAMP_IF_MATCH;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UPDATE_TASK_STATE;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UPDATE_TASK_STATE_FOR_DESTINED_NODE;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UPDATE_TASK_STATUS_TO_DEACTIVATED;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.INSERT_MP_STATE;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UPDATE_MP_STATE;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UPDATE_TASK_STATUS_TO_DELETE_PENDING;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UPDATED_AT;

/**
 * The connector class which deals with underlying coordinated task table.
 */
public class RDMBSConnector {

    private static final Log LOG = LogFactory.getLog(RDMBSConnector.class);
    private static final String ERROR_MSG = "Error while doing data base operation.";
    private static final String EMPTY_LIST = "Provided list is empty ";
    private static final String SQL_INTEGRITY_VIOLATION_CODE = "23";
    private static final String DELETE_PENDING_STATE = TASK_DELETE_PENDING_STATE;
    private static final Set<Integer> DUPLICATE_KEY_ERROR_CODES = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(1, 1062, 2627, 2601, 803, -803)));
    private DataSource dataSource;

    /**
     * In memory view of a delete barrier row used during ACK/Finalize/Recovery flows.
     */
    private static class DeleteBarrierRecord {
        private final String taskName;
        private final String guardUuid;
        private final String ownerNodeId;
        private final long deadlineAt;
        private final long updatedAt;

        DeleteBarrierRecord(String taskName, String guardUuid, String ownerNodeId, long deadlineAt,
                            long updatedAt) {
            this.taskName = taskName;
            this.guardUuid = guardUuid;
            this.ownerNodeId = ownerNodeId;
            this.deadlineAt = deadlineAt;
            this.updatedAt = updatedAt;
        }
    }

    /**
     * Constructor.
     *
     * @param dataSource - The datasource config to initiate the connection.
     * @throws TaskCoordinationException - Exception.
     */
    public RDMBSConnector(DataSource dataSource) throws TaskCoordinationException {

        this.dataSource = dataSource;
        try (Connection connection = getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            String databaseType = metaData.getDatabaseProductName();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Successfully connected to : " + databaseType);
            }
        } catch (SQLException ex) {
            throw new TaskCoordinationException("Error while initializing RDBMS connection.", ex);
        }
    }

    /**
     * Removes the node id of the task and update the task state.
     *
     * @param tasks - List of coordinated tasks which needs to be updated.
     */
    public void unAssignAndUpdateState(List<String> tasks) throws TaskCoordinationException {

        if (tasks.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(EMPTY_LIST + "for un assignment removal.");
            }
            return;
        }
        try (Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(
                REMOVE_ASSIGNMENT_AND_UPDATE_STATE)) {
            for (String task : tasks) {
                preparedStatement.setString(1, task);
                preparedStatement.addBatch();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Removing the node assignment of task [" + task + "].");
                }
            }
            preparedStatement.executeBatch();
            if (LOG.isDebugEnabled()) {
                tasks.forEach(task -> LOG.debug("Successfully removed the node assignment of task [" + task + "]."));
            }
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * Sets the destined node id to null and state to none if running or to paused if deactivated.
     *
     * @param nodeId - Node Id which needs to be set to null.
     */
    public void unAssignAndUpdateState(String nodeId) throws TaskCoordinationException {

        try (Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(
                CLEAN_TASKS_OF_NODE)) {
            preparedStatement.setString(1, nodeId);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Un assigning the tasks of node [" + nodeId + "].");
            }
            preparedStatement.executeUpdate();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Successfully un assigned the tasks of node [" + nodeId + "].");
            }
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * Reads latest task delete guard updated time across all tasks.
     *
     * @return latest UPDATED_AT value from TASK_DELETE_GUARD, or -1 if table has no rows
     * @throws TaskCoordinationException when DB operations fail
     */
    public long getLatestDeleteGuardUpdatedAt() throws TaskCoordinationException {

        try (Connection connection = getConnection();
                PreparedStatement preparedStatement
                        = connection.prepareStatement(SELECT_MAX_TASK_DELETE_GUARD_UPDATED_AT);
                ResultSet resultSet = preparedStatement.executeQuery()) {
            if (!resultSet.next()) {
                return -1L;
            }
            long updatedAt = resultSet.getLong(1);
            if (resultSet.wasNull()) {
                return -1L;
            }
            return updatedAt;
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * Retrieves the list of task names.
     *
     * @param nodeID - Id of the node, for which the tasks need to be retrieved.
     * @param state  - State of the tasks which need to be retrieved.
     * @return - List of task names
     */
    public List<String> retrieveTaskNames(String nodeID, CoordinatedTask.States state)
            throws TaskCoordinationException {

        try (Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(
                RETRIEVE_TASKS_OF_NODE)) {
            preparedStatement.setString(1, nodeID);
            preparedStatement.setString(2, state.name());
            return query(preparedStatement, "for node [" + nodeID + "] with state [" + state.name() + "]");
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    private void printDebugLogs(List<Object> tasks, String msg) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(msg);
            tasks.forEach(LOG::debug);
        }
    }

    /**
     * Removes all the tasks assigned to the node.
     *
     * @param nodeId - The node id.
     */
    public void deleteTasks(String nodeId) throws TaskCoordinationException {

        try (Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(
                REMOVE_TASKS_OF_NODE)) {
            preparedStatement.setString(1, nodeId);
            preparedStatement.executeUpdate();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Removed all the tasks of node [" + nodeId + "].");
            }
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * Deactivates the task.
     *
     * @param name - Name of the task.
     */
    public void deactivateTask(String name) throws TaskCoordinationException {
        try (Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(
                UPDATE_TASK_STATUS_TO_DEACTIVATED)) {
            preparedStatement.setString(1, name);
            preparedStatement.executeUpdate();
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * Activates the task.
     *
     * @param name - Name of the task.
     */
    public void activateTask(String name) throws TaskCoordinationException {
        try (Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(
                ACTIVATE_TASK)) {
            preparedStatement.setString(1, name);
            preparedStatement.executeUpdate();
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * Retrieve the state of the task.
     *
     * @param name name of the task
     * @return state of the task
     * @throws TaskCoordinationException if something goes wrong while doing db read
     */
    public CoordinatedTask.States getTaskState(String name) throws TaskCoordinationException {
        return parseCoordinatedTaskState(getTaskStateValue(name), name);
    }

    /**
     * Retrieve raw DB task state value.
     *
     * @param name name of the task
     * @return raw state string or null if no row exists
     * @throws TaskCoordinationException if something goes wrong while doing db read
     */
    public String getTaskStateValue(String name) throws TaskCoordinationException {
        try (Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(
                RETRIEVE_TASK_STATE)) {
            preparedStatement.setString(1, name);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getString(TASK_STATE);
                }
            }
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
        return null;
    }

    /**
     * Remove the task entry.
     *
     * @param tasks - List of tasks to be removed.
     */
    public void deleteTasks(List<String> tasks) throws TaskCoordinationException {

        if (tasks.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(EMPTY_LIST + " for deleting tasks.");
            }
            return;
        }
        try (Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(
                DELETE_TASK)) {
            for (String task : tasks) {
                preparedStatement.setString(1, task);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            printDebugLogs(new ArrayList<>(tasks), "Following list of tasks were deleted.");
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * Remove task entries only when their state does not match the excluded state.
     *
     * @param tasks         - List of tasks to be removed.
     * @param excludedState - State value that should be skipped.
     * @return list of task names that were skipped because their state matched the excluded state.
     */
    public List<String> deleteTasksIfStateNotMatch(List<String> tasks, String excludedState)
            throws TaskCoordinationException {

        if (tasks.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(EMPTY_LIST + " for deleting tasks with state exclusion.");
            }
            return new ArrayList<>();
        }
        try (Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(
                DELETE_TASK_IF_STATE_NOT_MATCH)) {
            for (String task : tasks) {
                preparedStatement.setString(1, task);
                preparedStatement.setString(2, excludedState);
                preparedStatement.addBatch();
            }
            int[] results = preparedStatement.executeBatch();
            List<String> skippedTasks = new ArrayList<>();
            for (int i = 0; i < results.length; i++) {
                if (results[i] == 0) {
                    skippedTasks.add(tasks.get(i));
                }
            }
            printDebugLogs(new ArrayList<>(tasks), "Following list of tasks were conditionally deleted.");
            return skippedTasks;
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * Helper method to query data base and return task list.
     *
     * @param preparedStatement - Statement to be executed to retrieve the list of tasks.
     * @throws SQLException - Exception.
     */
    private List<String> query(PreparedStatement preparedStatement, String debug) throws SQLException {
        List<String> taskNames = new ArrayList<>();
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                taskNames.add(resultSet.getString(TASK_NAME));
            }
        }
        printDebugLogs(new ArrayList<>(taskNames), "Following list of tasks were retrieved " + debug);
        return taskNames;
    }

    /**
     * Helper method to query data base and return coordinated task list.
     *
     * @param preparedStatement - Statement to be executed to retrieve the list of tasks.
     * @throws SQLException - Exception.
     */
    private List<CoordinatedTask> queryTasks(PreparedStatement preparedStatement, String debug) throws SQLException {
        List<CoordinatedTask> tasks = new ArrayList<>();
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                CoordinatedTask task = new CoordinatedTask(resultSet.getString(TASK_NAME),
                        resultSet.getString(DESTINED_NODE_ID), CoordinatedTask.States.RUNNING);
                tasks.add(task);
            }
        }
        if (LOG.isDebugEnabled()) {
            printDebugLogs(tasks.stream().map(CoordinatedTask::getTaskName).collect(Collectors.toList()),
                    "Following list of tasks were retrieved " + debug);
        }
        return tasks;
    }

    /**
     * Helper method query data base and return task list.
     *
     * @param preparedStatement - Statement to be executed to retrieve the list of tasks.
     * @throws SQLException - Exception.
     */
    private List<CoordinatedTask> executeQuery(PreparedStatement preparedStatement) throws SQLException {

        List<CoordinatedTask> tasks = new ArrayList<>();
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                String taskName = resultSet.getString(TASK_NAME);
                CoordinatedTask.States taskState = parseCoordinatedTaskState(resultSet.getString(TASK_STATE), taskName);
                if (taskState == null) {
                    continue;
                }
                tasks.add(new CoordinatedTask(taskName, resultSet.getString(DESTINED_NODE_ID), taskState));
            }
        }
        printDebugLogs(new ArrayList<>(tasks),
                       "Following list of tasks were retrieved for assigned and incomplete tasks.");
        return tasks;
    }

    /**
     * Retrieve all the tasks.
     *
     * @return - List of available tasks.
     */
    public List<CoordinatedTask> getAllTaskNames() throws TaskCoordinationException {

        try (Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(
                RETRIEVE_ALL_TASKS)) {
            return queryTasks(preparedStatement, "for all available tasks names.");
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * Retrieve all assigned and incomplete tasks.
     *
     * @return - List of tasks.
     */
    public List<CoordinatedTask> getAllAssignedIncompleteTasks() throws TaskCoordinationException {

        try (Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(
                GET_ALL_ASSIGNED_INCOMPLETE_TASKS)) {
            return executeQuery(preparedStatement);
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * Add the task if doesn't exist already.
     *
     * @param taskName - The task which needs to be added.
     */
    public void addTaskIfNotExist(String taskName, CoordinatedTask.States state) throws TaskCoordinationException {

        try (Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(
                ADD_TASK)) {
            preparedStatement.setString(1, taskName);
            preparedStatement.setString(2, state.name());
            preparedStatement.executeUpdate();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Successfully added the task [" + taskName + "].");
            }
        } catch (SQLException ex) {
            if (isIntegrityViolation(ex)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Task [" + taskName + "] already exists.");
                }
            } else {
                throw new TaskCoordinationException(ERROR_MSG, ex);
            }
        }
    }

    public String getMessageProcessorState(String processorName) {
        try (Connection connection = getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(RETRIEVE_MP_STATE)) {
            preparedStatement.setString(1, processorName);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getString(MP_STATE);
                }
            }
        } catch (SQLException ex) {
            LOG.warn("Failed to get message processor state " +
                    "(Please sync registry for robust message processor state handle)");
        }
        return null;
    }

    public void addOrUpdateMPState(String processorName, String state) {
        try (Connection connection = getConnection();
             PreparedStatement insertStmt = connection.prepareStatement(INSERT_MP_STATE)) {

            insertStmt.setString(1, processorName);
            insertStmt.setString(2, state);
            insertStmt.executeUpdate();

            if (LOG.isDebugEnabled()) {
                LOG.debug("Successfully inserted processor [" + processorName + "] with state [" + state + "].");
            }

        } catch (SQLException ex) {
            if (ex.getSQLState() != null && ex.getSQLState().startsWith(SQL_INTEGRITY_VIOLATION_CODE)) {
                // Duplicate key -> perform update
                if (LOG.isDebugEnabled()) {
                    LOG.debug("MP [" + processorName + "] already exists. Attempting to update state.");
                }

                try (Connection connection = getConnection();
                     PreparedStatement updateStmt = connection.prepareStatement(UPDATE_MP_STATE)) {

                    updateStmt.setString(1, state);
                    updateStmt.setString(2, processorName);
                    updateStmt.executeUpdate();

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Successfully updated state of MP [" + processorName + "] to [" + state + "].");
                    }

                } catch (SQLException updateEx) {
                    LOG.warn("Failed to update existing message processor state " +
                            "(Please sync registry for robust message processor state handle)", updateEx);
                }

            } else {
                LOG.warn("Failed to Insert message processor state " +
                        "(Please sync registry for robust message processor state handle)");
            }
        }
    }


    /**
     * Updates the destined node id and state to none if it was in running.
     *
     * @param tasks - List of tasks to be updated.
     */
    public void updateAssignmentAndState(Map<String, String> tasks) throws TaskCoordinationException {

        if (tasks.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(EMPTY_LIST + " for update assignment and state change to none if running.");
            }
            return;
        }
        try (Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(
                UPDATE_ASSIGNMENT_AND_STATE)) {
            for (Map.Entry<String, String> entry : tasks.entrySet()) {
                preparedStatement.setString(1, entry.getValue());
                preparedStatement.setString(2, entry.getKey());
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            if (LOG.isDebugEnabled()) {
                tasks.forEach((task, destinedNode) -> LOG
                        .debug("Assigned the task [" + task + "] with destined node [" + destinedNode + "]"));
            }
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * Update the state of task.
     *
     * @param taskName     Name of the task.
     * @param updatedState Updated state.
     * @param destinedId   Destined Node Id.
     * @return True if update is successful.
     * @throws TaskCoordinationException when something goes wrong while updating.
     */
    public boolean updateTaskState(String taskName, CoordinatedTask.States updatedState, String destinedId)
            throws TaskCoordinationException {

        try (Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(
                UPDATE_TASK_STATE_FOR_DESTINED_NODE)) {
            preparedStatement.setString(1, updatedState.name());
            preparedStatement.setString(2, taskName);
            preparedStatement.setString(3, destinedId);
            int result = preparedStatement.executeUpdate();
            if (LOG.isDebugEnabled()) {
                LOG.debug((result == 1 ? "Updated" : "Unable to update") + " state to [" + updatedState + "] for task ["
                                  + "] with destined nodeId [" + destinedId + "]");
            }
            return result == 1;
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * Updates the stat of a task.
     *
     * @param tasks - Name of the task.
     * @param state - State to be updated.
     */
    public void updateTaskState(List<String> tasks, CoordinatedTask.States state) throws TaskCoordinationException {

        if (LOG.isDebugEnabled()) {
            LOG.debug(EMPTY_LIST + " for    update assignment and state change to none if running.");
            return;
        }
        try (Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(
                UPDATE_TASK_STATE)) {
            for (String task : tasks) {
                preparedStatement.setString(1, state.name());
                preparedStatement.setString(2, task);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            if (LOG.isDebugEnabled()) {
                tasks.stream().map(task -> "Paused task [" + task + "]").forEachOrdered(LOG::debug);
            }
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * Get All unassigned tasks except the completed ones.
     *
     * @return - List of unassigned and in complete tasks.
     */
    public List<String> retrieveAllUnAssignedAndIncompleteTasks() throws TaskCoordinationException {

        try (Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(
                RETRIEVE_UNASSIGNED_NOT_COMPLETED_TASKS)) {
            return query(preparedStatement, "for unassigned incomplete tasks");
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * Opens a new delete barrier for a task, updates guard token, and marks task row as delete pending.
     *
     * @param taskName        name of the task
     * @param guardUuid       barrier/guard token
     * @param ownerNodeId     owner node of this barrier
     * @param expectedNodeIds expected nodes for acknowledgements
     * @param deadlineAt      barrier deadline in epoch millis
     * @param updatedAt       update timestamp in epoch millis
     * @throws TaskCoordinationException if barrier creation fails
     */
    public void createDeleteBarrier(String taskName, String guardUuid, String ownerNodeId, List<String> expectedNodeIds,
                                    long deadlineAt, long updatedAt) throws TaskCoordinationException {
        Set<String> expectedNodes = sanitizeExpectedNodes(expectedNodeIds, ownerNodeId);

        Connection connection = null;
        try {
            connection = getTransactionalConnection();
            upsertGuard(connection, taskName, guardUuid, updatedAt);

            try (PreparedStatement insertBarrier = connection.prepareStatement(INSERT_TASK_DELETE_BARRIER)) {
                insertBarrier.setString(1, taskName);
                insertBarrier.setString(2, guardUuid);
                insertBarrier.setString(3, ownerNodeId);
                insertBarrier.setString(4, BARRIER_STATUS_OPEN);
                insertBarrier.setLong(5, deadlineAt);
                insertBarrier.setLong(6, updatedAt);
                insertBarrier.executeUpdate();
            }

            insertExpectedNodes(connection, taskName, guardUuid, expectedNodes);
            // Leader path should also mark task as delete-pending so recovery can safely finalize if leader dies.
            markTaskDeletePending(connection, taskName);
            connection.commit();
        } catch (SQLException ex) {
            rollbackQuietly(connection);
            throw new TaskCoordinationException(ERROR_MSG, ex);
        } finally {
            closeQuietly(connection);
        }
    }

    /**
     * Reads current guard token for the given task.
     *
     * @param taskName task name
     * @return current guard token or null when no guard row exists
     * @throws TaskCoordinationException when DB operation fails
     */
    public String getCurrentDeleteGuardUuid(String taskName) throws TaskCoordinationException {
        try (Connection connection = getConnection()) {
            return readGuardUuid(connection, taskName);
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * Attempts worker bootstrap for a delete barrier by CAS claiming guard ownership.
     * This operation updates/inserts guard and creates barrier rows atomically in one transaction.
     *
     * @param taskName task name
     * @param expectedGuardUuid guard observed by worker before CAS (nullable)
     * @param newGuardUuid new guard token candidate for bootstrap owner
     * @param ownerNodeId owner node id for barrier
     * @param expectedNodeIds expected nodes for acknowledgements
     * @param deadlineAt barrier deadline in epoch millis
     * @param updatedAt updated timestamp in epoch millis
     * @return true when this node won CAS and created barrier rows
     * @throws TaskCoordinationException when DB operation fails
     */
    public boolean tryCreateDeleteBarrierWithGuardCas(String taskName, String expectedGuardUuid, String newGuardUuid,
                                                      String ownerNodeId, List<String> expectedNodeIds, long deadlineAt,
                                                      long updatedAt) throws TaskCoordinationException {
        Set<String> expectedNodes = sanitizeExpectedNodes(expectedNodeIds, ownerNodeId);
        Connection connection = null;
        try {
            connection = getTransactionalConnection();
            boolean acquired = acquireGuardByCompareAndSet(connection, taskName, expectedGuardUuid, newGuardUuid,
                    updatedAt);
            if (!acquired) {
                connection.rollback();
                return false;
            }
            try (PreparedStatement insertBarrier = connection.prepareStatement(INSERT_TASK_DELETE_BARRIER)) {
                insertBarrier.setString(1, taskName);
                insertBarrier.setString(2, newGuardUuid);
                insertBarrier.setString(3, ownerNodeId);
                insertBarrier.setString(4, BARRIER_STATUS_OPEN);
                insertBarrier.setLong(5, deadlineAt);
                insertBarrier.setLong(6, updatedAt);
                insertBarrier.executeUpdate();
            }
            insertExpectedNodes(connection, taskName, newGuardUuid, expectedNodes);
            // Bootstrap barrier path is an exception flow. Mark task row pending and let recovery own final cleanup.
            markTaskDeletePending(connection, taskName);
            connection.commit();
            return true;
        } catch (SQLException ex) {
            rollbackQuietly(connection);
            throw new TaskCoordinationException(ERROR_MSG, ex);
        } finally {
            closeQuietly(connection);
        }
    }

    /**
     * Writes acknowledgement for latest open barrier of the task.
     *
     * @param taskName task name
     * @param nodeId   node id to ack
     * @param ackedAt  ack timestamp in epoch millis
     * @return true if an open barrier was found and acked, false otherwise
     * @throws TaskCoordinationException when DB operation fails
     */
    public boolean acknowledgeOpenDeleteBarrier(String taskName, String nodeId, long ackedAt)
            throws TaskCoordinationException {
        try (Connection connection = getConnection()) {
            // Read current guard first, then ACK only the matching OPEN barrier.
            // This avoids accidentally ACKing a stale OPEN barrier picked by timestamp ordering.
            String currentGuard = readGuardUuid(connection, taskName);
            if (currentGuard == null) {
                return false;
            }
            DeleteBarrierRecord barrier = readOpenBarrierByTaskAndGuard(connection, taskName, currentGuard);
            if (barrier == null) {
                return false;
            }
            upsertBarrierAck(connection, taskName, barrier.guardUuid, nodeId, ackedAt);
            try (PreparedStatement updateBarrier = connection.prepareStatement(UPDATE_TASK_DELETE_BARRIER_TIMESTAMP)) {
                updateBarrier.setLong(1, ackedAt);
                updateBarrier.setString(2, taskName);
                updateBarrier.setString(3, barrier.guardUuid);
                updateBarrier.executeUpdate();
            }
            return true;
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * Checks whether all expected nodes acknowledged the barrier.
     *
     * @param taskName  task name
     * @param guardUuid barrier token
     * @return true if all expected nodes acknowledged
     * @throws TaskCoordinationException when DB operation fails
     */
    public boolean areAllExpectedNodesAcked(String taskName, String guardUuid) throws TaskCoordinationException {
        try (Connection connection = getConnection()) {
            return !hasMissingAcks(connection, taskName, guardUuid);
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * Attempts to finalize barrier and remove coordinated task row atomically.
     *
     * @param taskName     task name
     * @param guardUuid    barrier token
     * @param currentTime  current time in epoch millis
     * @return true if task row was deleted and barrier was finalized
     * @throws TaskCoordinationException when DB operation fails
     */
    public boolean finalizeDeleteBarrier(String taskName, String guardUuid, long currentTime)
            throws TaskCoordinationException {
        return finalizeDeleteBarrier(taskName, guardUuid, currentTime, false);
    }

    /**
     * Attempts to finalize barrier and remove coordinated task row atomically.
     *
     * @param taskName     task name
     * @param guardUuid    barrier token
     * @param currentTime  current time in epoch millis
     * @param pendingOnly  when true, task row delete is allowed only if task state is DELETE_PENDING
     * @return true if task row was deleted and barrier was finalized
     * @throws TaskCoordinationException when DB operation fails
     */
    private boolean finalizeDeleteBarrier(String taskName, String guardUuid, long currentTime, boolean pendingOnly)
            throws TaskCoordinationException {
        Connection connection = null;
        try {
            connection = getTransactionalConnection();

            int claimed;

            // Update updated_at time for both open and finalized barriers
            try (PreparedStatement claimBarrier = connection.prepareStatement(UPDATE_TASK_DELETE_BARRIER_STATUS)) {
                claimBarrier.setString(1, BARRIER_STATUS_FINALIZING);
                claimBarrier.setLong(2, currentTime);
                claimBarrier.setString(3, taskName);
                claimBarrier.setString(4, guardUuid);
                claimBarrier.setString(5, BARRIER_STATUS_OPEN);
                claimed = claimBarrier.executeUpdate();
            }
            if (claimed == 0) {
                try (PreparedStatement claimBarrier = connection.prepareStatement(UPDATE_TASK_DELETE_BARRIER_STATUS)) {
                    claimBarrier.setString(1, BARRIER_STATUS_FINALIZING);
                    claimBarrier.setLong(2, currentTime);
                    claimBarrier.setString(3, taskName);
                    claimBarrier.setString(4, guardUuid);
                    claimBarrier.setString(5, BARRIER_STATUS_FINALIZING);
                    claimed = claimBarrier.executeUpdate();
                }
                if (claimed == 0) {
                    connection.rollback();
                    return false;
                }
            }

            DeleteBarrierRecord barrier = readBarrierByTaskAndGuard(connection, taskName, guardUuid);
            if (barrier == null) {
                connection.rollback();
                return false;
            }
            String currentGuard = readGuardUuid(connection, taskName);
            if (!guardUuid.equals(currentGuard)) {
                cleanupBarrierEntries(connection, taskName, guardUuid);
                connection.commit();
                return false;
            }

            // Recheck ACK completeness inside finalize transaction.
            // The leader wait loop runs outside this transaction and may exit on timeout.
            // Also, finalize can be triggered by recovery flow without going through waitForDeleteBarrier().
            // If ACKs are still missing before deadline, barrier is reopened and delete is omitted.
            // In that case this call returns false, and a later finalize attempt (recovery cleaner) retries it.
            // After deadline, delete proceeds even with missing ACKs to avoid permanent blocking when nodes fail.
            // This DB state check is the final gate before deleting task row state.
            boolean allAcked = !hasMissingAcks(connection, taskName, guardUuid);
            if (!allAcked && currentTime < barrier.deadlineAt) {
                try (PreparedStatement reopen = connection.prepareStatement(UPDATE_TASK_DELETE_BARRIER_STATUS)) {
                    reopen.setString(1, BARRIER_STATUS_OPEN);
                    reopen.setLong(2, currentTime);
                    reopen.setString(3, taskName);
                    reopen.setString(4, guardUuid);
                    reopen.setString(5, BARRIER_STATUS_FINALIZING);
                    reopen.executeUpdate();
                }
                connection.commit();
                return false;
            }

            // Final compare and set guard check before deleting task state.
            // This avoids stale wave delete races.
            int guardMatchedRows;
            try (PreparedStatement guardMatch = connection.prepareStatement(
                    UPDATE_TASK_DELETE_GUARD_TIMESTAMP_IF_MATCH)) {
                guardMatch.setLong(1, currentTime);
                guardMatch.setString(2, taskName);
                guardMatch.setString(3, guardUuid);
                guardMatchedRows = guardMatch.executeUpdate();
            }
            if (guardMatchedRows == 0) {
                cleanupBarrierEntries(connection, taskName, guardUuid);
                connection.commit();
                return false;
            }

            int deletedTaskRows;
            if (pendingOnly) {
                try (PreparedStatement deleteTask = connection.prepareStatement(DELETE_TASK_IF_STATE_MATCH)) {
                    deleteTask.setString(1, taskName);
                    deleteTask.setString(2, DELETE_PENDING_STATE);
                    deletedTaskRows = deleteTask.executeUpdate();
                }
            } else {
                try (PreparedStatement deleteTask = connection.prepareStatement(DELETE_TASK)) {
                    deleteTask.setString(1, taskName);
                    deletedTaskRows = deleteTask.executeUpdate();
                }
            }
            cleanupBarrierEntries(connection, taskName, guardUuid);
            connection.commit();
            return deletedTaskRows > 0;
        } catch (SQLException ex) {
            rollbackQuietly(connection);
            throw new TaskCoordinationException(ERROR_MSG, ex);
        } finally {
            closeQuietly(connection);
        }
    }

    /**
     * Recovers open barriers that are expired or owned by nodes no longer live.
     *
     * @param liveNodeIds list of currently live node ids
     * @param currentTime current time in epoch millis
     * @return recovered task names whose barriers were finalized
     * @throws TaskCoordinationException when DB operation fails
     */
    public List<String> recoverExpiredOrAbandonedDeleteBarriers(List<String> liveNodeIds, long currentTime)
            throws TaskCoordinationException {
        Set<String> liveNodes = new HashSet<>();
        if (liveNodeIds != null) {
            liveNodes.addAll(liveNodeIds);
        }
        List<DeleteBarrierRecord> openBarriers;
        try (Connection connection = getConnection()) {
            openBarriers = readOpenBarriers(connection);
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }

        Set<String> recoveredTaskNames = new HashSet<>();
        for (DeleteBarrierRecord barrier : openBarriers) {
            boolean ownerMissing = barrier.ownerNodeId == null || !liveNodes.contains(barrier.ownerNodeId);
            boolean deadlinePassed = currentTime >= barrier.deadlineAt;
            if (ownerMissing || deadlinePassed) {
                String recoveryReason = ownerMissing && deadlinePassed ? "owner-missing-and-deadline-passed"
                        : ownerMissing ? "owner-missing" : "deadline-passed";
                boolean finalized = finalizeDeleteBarrier(barrier.taskName, barrier.guardUuid, currentTime, true);
                if (finalized) {
                    recoveredTaskNames.add(barrier.taskName);
                    LOG.info("Recovery flow finalized delete barrier for task [" + barrier.taskName + "]"
                            + " with guard ["
                            + barrier.guardUuid + "] due to [" + recoveryReason + "]. Task row deleted: true.");
                } else {
                    LOG.info("Recovery flow finalized barrier metadata for task [" + barrier.taskName + "] "
                            + "with guard ["
                            + barrier.guardUuid + "] due to [" + recoveryReason + "]. Task row deleted: false.");
                }
            }
        }
        return new ArrayList<>(recoveredTaskNames);
    }

    /**
     * Marks a task row as delete-pending during bootstrap barrier flow.
     *
     * @param connection transactional connection
     * @param taskName task name
     * @throws SQLException when query execution fails
     */
    private void markTaskDeletePending(Connection connection, String taskName) throws SQLException {
        try (PreparedStatement markPending = connection.prepareStatement(UPDATE_TASK_STATUS_TO_DELETE_PENDING)) {
            markPending.setString(1, taskName);
            markPending.executeUpdate();
        }
    }

    /**
     * Normalizes and deduplicates expected node IDs for barrier creation.
     *
     * @param expectedNodeIds expected node IDs
     * @param ownerNodeId owner node id
     * @return normalized expected node set
     */
    private Set<String> sanitizeExpectedNodes(List<String> expectedNodeIds, String ownerNodeId) {
        Set<String> expectedNodes = new HashSet<>();
        if (expectedNodeIds != null) {
            expectedNodes.addAll(expectedNodeIds);
        }
        expectedNodes.remove(null);
        if (expectedNodes.isEmpty() && ownerNodeId != null) {
            expectedNodes.add(ownerNodeId);
        }
        return expectedNodes;
    }

    /**
     * Compares and sets guard token for worker bootstrap ownership.
     * If expected guard is null, this tries to insert a new guard row.
     * If expected guard is non-null, this updates only when current guard matches expected.
     *
     * @param connection transactional connection
     * @param taskName task name
     * @param expectedGuardUuid guard observed by worker before CAS (nullable)
     * @param newGuardUuid new guard token candidate
     * @param updatedAt update timestamp in epoch millis
     * @return true if guard ownership was acquired
     * @throws SQLException when query execution fails
     */
    private boolean acquireGuardByCompareAndSet(Connection connection, String taskName, String expectedGuardUuid,
                                                String newGuardUuid, long updatedAt) throws SQLException {
        if (expectedGuardUuid == null) {
            try (PreparedStatement insertGuard = connection.prepareStatement(INSERT_TASK_DELETE_GUARD)) {
                insertGuard.setString(1, taskName);
                insertGuard.setString(2, newGuardUuid);
                insertGuard.setLong(3, updatedAt);
                insertGuard.executeUpdate();
                return true;
            } catch (SQLException ex) {
                if (isIntegrityViolation(ex)) {
                    return false;
                }
                throw ex;
            }
        }

        try (PreparedStatement updateGuard = connection.prepareStatement(UPDATE_TASK_DELETE_GUARD_IF_MATCH)) {
            updateGuard.setString(1, newGuardUuid);
            updateGuard.setLong(2, updatedAt);
            updateGuard.setString(3, taskName);
            updateGuard.setString(4, expectedGuardUuid);
            return updateGuard.executeUpdate() > 0;
        }
    }

    /**
     * Updates guard token for a task, or inserts a new row if absent.
     * Handles duplicate key races by retrying as update.
     *
     * @param connection transactional connection
     * @param taskName task name
     * @param guardUuid guard token
     * @param updatedAt guard update time
     * @throws SQLException when query execution fails
     */
    private void upsertGuard(Connection connection, String taskName, String guardUuid, long updatedAt)
            throws SQLException {
        int updated;
        // Guard rows are durable per task once hot deployed. And guardUuid is rotated per hot deployment wave.
        // Update first is the common path and insert is only for first time which is rare.
        // Also, this avoids depending on integrity violation handling in the common path.
        try (PreparedStatement updateGuard = connection.prepareStatement(UPDATE_TASK_DELETE_GUARD)) {
            updateGuard.setString(1, guardUuid);
            updateGuard.setLong(2, updatedAt);
            updateGuard.setString(3, taskName);
            updated = updateGuard.executeUpdate();
        }

        if (updated > 0) {
            return;
        }
        try (PreparedStatement insertGuard = connection.prepareStatement(INSERT_TASK_DELETE_GUARD)) {
            insertGuard.setString(1, taskName);
            insertGuard.setString(2, guardUuid);
            insertGuard.setLong(3, updatedAt);
            insertGuard.executeUpdate();
        } catch (SQLException ex) {
            if (isIntegrityViolation(ex)) {
                try (PreparedStatement updateGuard = connection.prepareStatement(UPDATE_TASK_DELETE_GUARD)) {
                    updateGuard.setString(1, guardUuid);
                    updateGuard.setLong(2, updatedAt);
                    updateGuard.setString(3, taskName);
                    updateGuard.executeUpdate();
                }
            } else {
                throw ex;
            }
        }
    }

    /**
     * Inserts expected acknowledgement node set for a barrier.
     *
     * @param connection transactional connection
     * @param taskName task name
     * @param guardUuid guard token
     * @param expectedNodes expected node IDs
     * @throws SQLException when query execution fails
     */
    private void insertExpectedNodes(Connection connection, String taskName, String guardUuid
            , Set<String> expectedNodes) throws SQLException {
        if (expectedNodes.isEmpty()) {
            return;
        }
        try (PreparedStatement insertExpected = connection.prepareStatement(INSERT_TASK_DELETE_BARRIER_EXPECTED)) {
            for (String node : expectedNodes) {
                insertExpected.setString(1, taskName);
                insertExpected.setString(2, guardUuid);
                insertExpected.setString(3, node);
                insertExpected.addBatch();
            }
            insertExpected.executeBatch();
        }
    }

    /**
     * Reads open barrier for a specific task and guard token.
     *
     * @param connection connection
     * @param taskName task name
     * @param guardUuid guard token
     * @return open barrier or null
     * @throws SQLException when query execution fails
     */
    private DeleteBarrierRecord readOpenBarrierByTaskAndGuard(Connection connection, String taskName, String guardUuid)
            throws SQLException {
        try (PreparedStatement queryBarrier = connection.prepareStatement(
                SELECT_OPEN_TASK_DELETE_BARRIER_BY_TASK_AND_GUARD)) {
            queryBarrier.setString(1, taskName);
            queryBarrier.setString(2, guardUuid);
            queryBarrier.setString(3, BARRIER_STATUS_OPEN);
            try (ResultSet resultSet = queryBarrier.executeQuery()) {
                if (resultSet.next()) {
                    return mapBarrier(resultSet);
                }
            }
        }
        return null;
    }

    /**
     * Reads all currently open delete barriers.
     *
     * @param connection connection
     * @return list of open barriers
     * @throws SQLException when query execution fails
     */
    private List<DeleteBarrierRecord> readOpenBarriers(Connection connection) throws SQLException {
        List<DeleteBarrierRecord> barriers = new ArrayList<>();
        try (PreparedStatement queryBarrier = connection.prepareStatement(SELECT_OPEN_TASK_DELETE_BARRIERS)) {
            queryBarrier.setString(1, BARRIER_STATUS_OPEN);
            try (ResultSet resultSet = queryBarrier.executeQuery()) {
                while (resultSet.next()) {
                    barriers.add(mapBarrier(resultSet));
                }
            }
        }
        return barriers;
    }

    /**
     * Reads barrier row for a specific task and guard token.
     *
     * @param connection connection
     * @param taskName task name
     * @param guardUuid guard token
     * @return barrier row or null
     * @throws SQLException when query execution fails
     */
    private DeleteBarrierRecord readBarrierByTaskAndGuard(Connection connection, String taskName, String guardUuid)
            throws SQLException {
        try (PreparedStatement queryBarrier = connection.prepareStatement(SELECT_TASK_DELETE_BARRIER)) {
            queryBarrier.setString(1, taskName);
            queryBarrier.setString(2, guardUuid);
            try (ResultSet resultSet = queryBarrier.executeQuery()) {
                if (resultSet.next()) {
                    return mapBarrier(resultSet);
                }
            }
        }
        return null;
    }

    /**
     * Reads current guard token for a task.
     *
     * @param connection connection
     * @param taskName task name
     * @return guard token or null
     * @throws SQLException when query execution fails
     */
    private String readGuardUuid(Connection connection, String taskName) throws SQLException {
        try (PreparedStatement queryGuard = connection.prepareStatement(SELECT_TASK_DELETE_GUARD)) {
            queryGuard.setString(1, taskName);
            try (ResultSet resultSet = queryGuard.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getString(GUARD_UUID);
                }
            }
        }
        return null;
    }

    /**
     * Checks whether at least one expected node has not acknowledged the barrier.
     * Uses two simple selects (expected nodes and acked nodes) and computes missing acks in Java.
     *
     * @param connection connection
     * @param taskName task name
     * @param guardUuid guard token
     * @return true if there are missing acknowledgements
     * @throws SQLException when query execution fails
     */
    private boolean hasMissingAcks(Connection connection, String taskName, String guardUuid) throws SQLException {
        Set<String> expectedNodes = new HashSet<>();
        try (PreparedStatement queryExpectedNodes
                     = connection.prepareStatement(SELECT_TASK_DELETE_BARRIER_EXPECTED_NODES)) {
            queryExpectedNodes.setString(1, taskName);
            queryExpectedNodes.setString(2, guardUuid);
            try (ResultSet resultSet = queryExpectedNodes.executeQuery()) {
                while (resultSet.next()) {
                    expectedNodes.add(resultSet.getString(NODE_ID));
                }
            }
        }

        if (expectedNodes.isEmpty()) {
            return false;
        }

        try (PreparedStatement queryAckNodes = connection.prepareStatement(SELECT_TASK_DELETE_BARRIER_ACK_NODES)) {
            queryAckNodes.setString(1, taskName);
            queryAckNodes.setString(2, guardUuid);
            try (ResultSet resultSet = queryAckNodes.executeQuery()) {
                while (resultSet.next() && !expectedNodes.isEmpty()) {
                    expectedNodes.remove(resultSet.getString(NODE_ID));
                }
            }
        }
        return !expectedNodes.isEmpty();
    }

    /**
     * Parses a DB task state into coordinated state enum.
     * Unknown/internal states are ignored by returning null.
     *
     * @param state task state value from DB
     * @param taskName task name for logs
     * @return parsed state or null when value is not part of CoordinatedTask.States
     */
    private CoordinatedTask.States parseCoordinatedTaskState(String state, String taskName) {
        if (state == null) {
            return null;
        }
        try {
            return CoordinatedTask.States.valueOf(state);
        } catch (IllegalArgumentException ex) {
            LOG.info("Ignoring internal task state [" + state + "] for task [" + taskName
                    + "] while resolving coordinated tasks.");
            return null;
        }
    }

    /**
     * Inserts or updates acknowledgement row for a node and barrier.
     *
     * @param connection connection
     * @param taskName task name
     * @param guardUuid guard token
     * @param nodeId node identifier
     * @param ackedAt acknowledgement time
     * @throws SQLException when query execution fails
     */
    private void upsertBarrierAck(Connection connection, String taskName, String guardUuid, String nodeId, long ackedAt)
            throws SQLException {
        int updated;
        // Update first so repeated ACKs are handled normally.
        // This avoids depending on integrity violation handling in the common path.
        // Insert is only for the first ACK row.
        try (PreparedStatement updateAck = connection.prepareStatement(UPDATE_TASK_DELETE_BARRIER_ACK)) {
            updateAck.setLong(1, ackedAt);
            updateAck.setString(2, taskName);
            updateAck.setString(3, guardUuid);
            updateAck.setString(4, nodeId);
            updated = updateAck.executeUpdate();
        }
        if (updated > 0) {
            return;
        }
        try (PreparedStatement insertAck = connection.prepareStatement(INSERT_TASK_DELETE_BARRIER_ACK)) {
            insertAck.setString(1, taskName);
            insertAck.setString(2, guardUuid);
            insertAck.setString(3, nodeId);
            insertAck.setLong(4, ackedAt);
            insertAck.executeUpdate();
        } catch (SQLException ex) {
            if (isIntegrityViolation(ex)) {
                try (PreparedStatement updateAck = connection.prepareStatement(UPDATE_TASK_DELETE_BARRIER_ACK)) {
                    updateAck.setLong(1, ackedAt);
                    updateAck.setString(2, taskName);
                    updateAck.setString(3, guardUuid);
                    updateAck.setString(4, nodeId);
                    updateAck.executeUpdate();
                }
            } else {
                throw ex;
            }
        }
    }

    /**
     * Deletes helper rows for a barrier (ACK, EXPECTED, BARRIER).
     *
     * @param connection transactional connection
     * @param taskName task name
     * @param guardUuid guard token
     * @throws SQLException when query execution fails
     */
    private void cleanupBarrierEntries(Connection connection, String taskName, String guardUuid) throws SQLException {
        try (PreparedStatement deleteAcks = connection.prepareStatement(DELETE_TASK_DELETE_BARRIER_ACKS);
             PreparedStatement deleteExpected = connection.prepareStatement(DELETE_TASK_DELETE_BARRIER_EXPECTED);
             PreparedStatement deleteBarrier = connection.prepareStatement(DELETE_TASK_DELETE_BARRIER)) {
            deleteAcks.setString(1, taskName);
            deleteAcks.setString(2, guardUuid);
            deleteAcks.executeUpdate();

            deleteExpected.setString(1, taskName);
            deleteExpected.setString(2, guardUuid);
            deleteExpected.executeUpdate();

            deleteBarrier.setString(1, taskName);
            deleteBarrier.setString(2, guardUuid);
            deleteBarrier.executeUpdate();
        }
    }

    /**
     * Maps barrier query result row into an in memory record.
     *
     * @param resultSet query result
     * @return mapped barrier record
     * @throws SQLException when row read fails
     */
    private DeleteBarrierRecord mapBarrier(ResultSet resultSet) throws SQLException {
        return new DeleteBarrierRecord(resultSet.getString(TASK_NAME), resultSet.getString(GUARD_UUID),
                resultSet.getString(OWNER_NODE_ID), resultSet.getLong(DEADLINE_AT), resultSet.getLong(UPDATED_AT));
    }

    /**
     * Checks whether a SQLException chain represents an integrity/duplicate key violation.
     *
     * @param ex SQL exception
     * @return true when violation is identified
     */
    private boolean isIntegrityViolation(SQLException ex) {
        SQLException current = ex;
        while (current != null) {
            // Check SQLState integrity class first then fallback to vendor specific duplicate key codes.
            String sqlState = current.getSQLState();
            if (sqlState != null && sqlState.startsWith(SQL_INTEGRITY_VIOLATION_CODE)) {
                return true;
            }

            if (DUPLICATE_KEY_ERROR_CODES.contains(current.getErrorCode())) {
                return true;
            }
            current = current.getNextException();
        }
        return false;
    }

    /**
     * Creates a connection with auto-commit disabled for transactional operations.
     *
     * @return transactional connection
     * @throws SQLException when connection creation fails
     */
    private Connection getTransactionalConnection() throws SQLException {
        Connection connection = dataSource.getConnection();
        connection.setAutoCommit(false);
        return connection;
    }

    /**
     * Attempts rollback and suppresses rollback exceptions as warnings.
     *
     * @param connection transactional connection
     */
    private void rollbackQuietly(Connection connection) {
        if (connection != null) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                LOG.warn("Error while rolling back the transaction.", ex);
            }
        }
    }

    /**
     * Closes connection and suppresses close exceptions as warnings.
     *
     * @param connection connection
     */
    private void closeQuietly(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ex) {
                LOG.warn("Error while closing the connection.", ex);
            }
        }
    }

    /**
     * Get connection.
     *
     * @return - Connection with auto commit true.
     * @throws SQLException -
     */
    private Connection getConnection() throws SQLException {

        Connection connection = dataSource.getConnection();
        connection.setAutoCommit(true);
        return connection;
    }

}

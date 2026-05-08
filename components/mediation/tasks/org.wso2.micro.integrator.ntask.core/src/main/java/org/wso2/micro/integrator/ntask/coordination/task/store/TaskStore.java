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

package org.wso2.micro.integrator.ntask.coordination.task.store;

import org.wso2.micro.integrator.ntask.coordination.TaskCoordinationException;
import org.wso2.micro.integrator.ntask.coordination.task.CoordinatedTask;
import org.wso2.micro.integrator.ntask.coordination.task.store.connector.RDMBSConnector;

import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

/**
 * The layer which connects to the task data base.
 */
public class TaskStore {

    /**
     * Connector for the data base.
     */
    private RDMBSConnector rdmbsConnector;

    public TaskStore(DataSource dataSource) throws TaskCoordinationException {

        this.rdmbsConnector = new RDMBSConnector(dataSource);
    }

    /**
     * Removes the node id of the task update the task state.
     *
     * @param tasks - List of tasks which needs to be updated.
     */
    public void unAssignAndUpdateState(List<String> tasks) throws TaskCoordinationException {

        rdmbsConnector.unAssignAndUpdateState(tasks);
    }

    /**
     * Sets the destined node id to null and state to none if running or to paused if deactivated.
     *
     * @param nodeId - Node Id which needs to be set to null.
     */
    public void unAssignAndUpdateState(String nodeId) throws TaskCoordinationException {

        rdmbsConnector.unAssignAndUpdateState(nodeId);
    }

    /**
     * Retrieves latest delete guard updated time across all tasks.
     *
     * @return latest guard UPDATED_AT, or -1 when no guard row exists
     * @throws TaskCoordinationException if operation fails
     */
    public long getLatestDeleteGuardUpdatedAt() throws TaskCoordinationException {

        return rdmbsConnector.getLatestDeleteGuardUpdatedAt();
    }

    /**
     * Retrieves the list of tasks.
     *
     * @param nodeID - Id of the node, for which the tasks need to be retrieved.
     * @param state  - State of the tasks which need to be retrieved.
     * @return - List of tasks.
     */
    public List<String> retrieveTaskNames(String nodeID, CoordinatedTask.States state)
            throws TaskCoordinationException {

        return rdmbsConnector.retrieveTaskNames(nodeID, state);
    }

    /**
     * Removes all the tasks assigned to the node.
     *
     * @param nodeId - The node id.
     */
    public void deleteTasks(String nodeId) throws TaskCoordinationException {

        rdmbsConnector.deleteTasks(nodeId);
    }

    /**
     * Remove the task entry.
     *
     * @param coordinatedTasks - List of tasks to be removed.
     */
    public void deleteTasks(List<String> coordinatedTasks) throws TaskCoordinationException {

        rdmbsConnector.deleteTasks(coordinatedTasks);
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

        return rdmbsConnector.deleteTasksIfStateNotMatch(tasks, excludedState);
    }

    /**
     * Activates the task.
     *
     * @param taskName - Name of the task.
     */
    public void activateTask(String taskName) throws TaskCoordinationException {

        rdmbsConnector.activateTask(taskName);
    }

    /**
     * Deactivates the task.
     *
     * @param taskName - Name of the task.
     */
    public void deactivateTask(String taskName) throws TaskCoordinationException {

        rdmbsConnector.deactivateTask(taskName);
    }

    /**
     * Retrieve all the task names.
     *
     * @return - List of available tasks.
     */
    public List<CoordinatedTask> getAllTaskNames() throws TaskCoordinationException {

        return rdmbsConnector.getAllTaskNames();
    }

    /**
     * Retrieve all assigned and in completed tasks.
     *
     * @return - List of available tasks.
     */
    public List<CoordinatedTask> getAllAssignedIncompleteTasks() throws TaskCoordinationException {

        return rdmbsConnector.getAllAssignedIncompleteTasks();
    }

    /**
     * Add the task.
     *
     * @param task - The coordinated task which needs to be added.
     */
    public void addTaskIfNotExist(String task) throws TaskCoordinationException {

        rdmbsConnector.addTaskIfNotExist(task, CoordinatedTask.States.NONE);
    }

    /**
     * Add the task.
     *
     * @param task - The coordinated task which needs to be added.
     * @param state - Initial state of the task.
     */
    public void addTaskIfNotExist(String task, CoordinatedTask.States state) throws TaskCoordinationException {

        rdmbsConnector.addTaskIfNotExist(task, state);
    }

    /**
     * Updates the state and node id.
     *
     * @param tasks - List of tasks to be updated.
     */
    public void updateAssignmentAndState(Map<String, String> tasks) throws TaskCoordinationException {

        rdmbsConnector.updateAssignmentAndState(tasks);
    }

    /**
     * Updates the stat of a task.
     *
     * @param tasks - Names of the task.
     * @param state - State to be updated.
     */
    public void updateTaskState(List<String> tasks, CoordinatedTask.States state) throws TaskCoordinationException {

        rdmbsConnector.updateTaskState(tasks, state);
    }

    /**
     * Retrieve the state of the task.
     *
     * @param taskName name of the task
     * @return state of the task
     * @throws TaskCoordinationException if something goes wrong while doing db read
     */
    public CoordinatedTask.States getTaskState(String taskName) throws TaskCoordinationException {
        return rdmbsConnector.getTaskState(taskName);
    }

    /**
     * Retrieve raw task state value from DB.
     *
     * @param taskName name of the task
     * @return raw state value or null when task row does not exist
     * @throws TaskCoordinationException if something goes wrong while doing db read
     */
    public String getTaskStateValue(String taskName) throws TaskCoordinationException {
        return rdmbsConnector.getTaskStateValue(taskName);
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

        return rdmbsConnector.updateTaskState(taskName, updatedState, destinedId);
    }

    /**
     * Get All unassigned tasks except the completed ones.
     *
     * @return - List of unassigned and in complete tasks.
     */
    public List<String> retrieveAllUnAssignedAndIncompleteTasks() throws TaskCoordinationException {

        return rdmbsConnector.retrieveAllUnAssignedAndIncompleteTasks();
    }

    /**
     * Add or update the message processor state.
     *
     * @param mpName              Name of the message processor.
     * @param messageProcessorState State of the message processor.
     */
    public void addOrUpdateMessageProcessorState(String mpName, String messageProcessorState) {
        rdmbsConnector.addOrUpdateMPState(mpName, messageProcessorState);
    }

    public String getMessageProcessorTaskState(String taskName) {
        return rdmbsConnector.getMessageProcessorState(taskName);
    }

    /**
     * Opens a delete barrier for the given task.
     *
     * @param taskName task name
     * @param guardUuid barrier token
     * @param ownerNodeId owner node id
     * @param expectedNodeIds expected nodes for acknowledgement
     * @param deadlineAt barrier deadline in epoch millis
     * @param updatedAt updated timestamp in epoch millis
     * @throws TaskCoordinationException if operation fails
     */
    public void createDeleteBarrier(String taskName, String guardUuid, String ownerNodeId, List<String> expectedNodeIds,
                                    long deadlineAt, long updatedAt) throws TaskCoordinationException {
        rdmbsConnector.createDeleteBarrier(taskName, guardUuid, ownerNodeId, expectedNodeIds, deadlineAt, updatedAt);
    }

    /**
     * Reads the current guard token for a task.
     *
     * @param taskName task name
     * @return current guard token or null when no guard exists
     * @throws TaskCoordinationException if operation fails
     */
    public String getCurrentDeleteGuardUuid(String taskName) throws TaskCoordinationException {
        return rdmbsConnector.getCurrentDeleteGuardUuid(taskName);
    }

    /**
     * Attempts worker bootstrap of delete barrier by compare and set claiming the guard token.
     * Only one worker can win and create the barrier for a task wave.
     *
     * @param taskName task name
     * @param expectedGuardUuid guard observed by worker before CAS (nullable)
     * @param newGuardUuid candidate guard token for bootstrap owner
     * @param ownerNodeId bootstrap owner node id
     * @param expectedNodeIds expected nodes for acknowledgement
     * @param deadlineAt barrier deadline in epoch millis
     * @param updatedAt updated timestamp in epoch millis
     * @return true if this worker won CAS and created barrier rows
     * @throws TaskCoordinationException if operation fails
     */
    public boolean tryCreateDeleteBarrierWithGuardCas(String taskName, String expectedGuardUuid, String newGuardUuid,
                                                      String ownerNodeId, List<String> expectedNodeIds, long deadlineAt,
                                                      long updatedAt) throws TaskCoordinationException {
        return rdmbsConnector.tryCreateDeleteBarrierWithGuardCas(taskName, expectedGuardUuid, newGuardUuid, ownerNodeId,
                expectedNodeIds, deadlineAt, updatedAt);
    }

    /**
     * Acknowledges open barrier for a task from current node.
     *
     * @param taskName task name
     * @param nodeId node id
     * @param ackedAt ack timestamp in epoch millis
     * @return true if open barrier was found and acknowledged
     * @throws TaskCoordinationException if operation fails
     */
    public boolean acknowledgeOpenDeleteBarrier(String taskName, String nodeId, long ackedAt)
            throws TaskCoordinationException {
        return rdmbsConnector.acknowledgeOpenDeleteBarrier(taskName, nodeId, ackedAt);
    }

    /**
     * Checks if all expected nodes acknowledged barrier for a task.
     *
     * @param taskName task name
     * @param guardUuid barrier token
     * @return true if all expected nodes acknowledged
     * @throws TaskCoordinationException if operation fails
     */
    public boolean areAllExpectedNodesAcked(String taskName, String guardUuid) throws TaskCoordinationException {
        return rdmbsConnector.areAllExpectedNodesAcked(taskName, guardUuid);
    }

    /**
     * Finalizes delete barrier and attempts task row removal from .
     *
     * @param taskName task name
     * @param guardUuid barrier token
     * @param currentTime current time in epoch millis
     * @return true when task row delete path completed
     * @throws TaskCoordinationException if operation fails
     */
    public boolean finalizeDeleteBarrier(String taskName, String guardUuid, long currentTime)
            throws TaskCoordinationException {
        return rdmbsConnector.finalizeDeleteBarrier(taskName, guardUuid, currentTime);
    }

    /**
     * Recovers expired/abandoned open barriers.
     *
     * @param liveNodeIds currently live nodes
     * @param currentTime current time in epoch millis
     * @return recovered task names whose delete barriers were finalized
     * @throws TaskCoordinationException if operation fails
     */
    public List<String> recoverExpiredOrAbandonedDeleteBarriers(List<String> liveNodeIds, long currentTime)
            throws TaskCoordinationException {
        return rdmbsConnector.recoverExpiredOrAbandonedDeleteBarriers(liveNodeIds, currentTime);
    }

}

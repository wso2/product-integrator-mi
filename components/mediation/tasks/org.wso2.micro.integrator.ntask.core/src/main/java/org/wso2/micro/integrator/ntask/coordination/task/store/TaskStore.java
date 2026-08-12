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
import org.wso2.micro.integrator.ntask.coordination.task.ClaimOutcome;
import org.wso2.micro.integrator.ntask.coordination.task.CoordinatedTask;
import org.wso2.micro.integrator.ntask.coordination.task.store.connector.RDMBSConnector;
import org.wso2.micro.integrator.ntask.core.RegistrationPhase;
import org.wso2.micro.integrator.ntask.core.RegistrationResult;

import java.util.List;
import java.util.Map;
import java.util.Set;
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
     * The single registration doorway (armed nodes): the registration transaction matrix. The caller
     * computes the canonical fingerprint and trigger family from the local definition BEFORE the call.
     */
    public RegistrationResult addTaskIfNotExist(String taskName, CoordinatedTask.States initialState,
                                                String scheduleFingerprint, String triggerFamily, RegistrationPhase phase)
            throws TaskCoordinationException {

        return rdmbsConnector.addTaskIfNotExist(taskName, initialState, scheduleFingerprint, triggerFamily, phase);
    }

    /**
     * The cleaner's repair — claim-status-only branches, no fingerprint input BY DESIGN:
     * re-materializes a missing task row, never schedules, never parks/unparks.
     */
    public void repairMissingTaskRow(String taskName) throws TaskCoordinationException {

        rdmbsConnector.repairMissingTaskRow(taskName);
    }

    /**
     * Membership-rejoin hand-back: this node's surviving RUNNING rows driven back to
     * NONE (destined kept) so the restarted scheduler pass re-schedules them.
     */
    public int handBackNodeRunningTasks(String nodeId) throws TaskCoordinationException {

        return rdmbsConnector.handBackNodeRunningTasks(nodeId);
    }

    /**
     * Hardened wave creation: the read-then-branch claim guard stamp + barrier/expected rows + this
     * node's own ack, one lease-fenced transaction.
     */
    public RDMBSConnector.WaveHandle createOrJoinDeleteBarrier(String taskName, String candidateGuardUuid,
                                                               String ownerNodeId, List<String> expectedNodeIds,
                                                               long deadlineAt, long updatedAt)
            throws TaskCoordinationException {

        return rdmbsConnector.createOrJoinDeleteBarrier(taskName, candidateGuardUuid, ownerNodeId, expectedNodeIds,
                deadlineAt, updatedAt);
    }

    /**
     * Hardened barrier acknowledgement: the wave is located through the claim row's DELETE_GUARD and
     * the ack write is boot-lease-fenced inside its own transaction.
     */
    public boolean acknowledgeOpenDeleteBarrierHardened(String taskName, String nodeId, long ackedAt)
            throws TaskCoordinationException {

        return rdmbsConnector.acknowledgeOpenDeleteBarrierHardened(taskName, nodeId, ackedAt);
    }

    /**
     * The single guarded finalize with in-transaction Barrier Liveness classification, used identically
     * by the leader path and the cleaner.
     */
    public RDMBSConnector.FinalizeReport finalizeDeleteBarrierClassified(String taskName, String guardUuid, long now,
                                                                         long localWindowMillis)
            throws TaskCoordinationException {

        return rdmbsConnector.finalizeDeleteBarrierClassified(taskName, guardUuid, now, localWindowMillis);
    }

    /**
     * Hardened cleaner recovery: re-classifies every in-flight wave each cycle through the one guarded
     * finalize; a LIVE holdout leaves the wave OPEN.
     */
    public RDMBSConnector.BarrierRecoveryReport recoverInFlightDeleteBarriersClassified(String localNodeId,
                                                                                        long localWindowMillis,
                                                                                        long now)
            throws TaskCoordinationException {

        return rdmbsConnector.recoverInFlightDeleteBarriersClassified(localNodeId, localWindowMillis, now);
    }

    /**
     * Barrier row read for the classifier and the skip-wait cross-node signal.
     */
    public RDMBSConnector.DeleteBarrierView getDeleteBarrier(String taskName, String guardUuid)
            throws TaskCoordinationException {

        return rdmbsConnector.getDeleteBarrier(taskName, guardUuid);
    }

    /**
     * All barrier rows in the given status.
     */
    public List<RDMBSConnector.DeleteBarrierView> getDeleteBarriersByStatus(String status)
            throws TaskCoordinationException {

        return rdmbsConnector.getDeleteBarriersByStatus(status);
    }

    /**
     * EXPECTED node ids of a wave.
     */
    public List<String> getBarrierExpectedNodes(String taskName, String guardUuid) throws TaskCoordinationException {

        return rdmbsConnector.getBarrierExpectedNodes(taskName, guardUuid);
    }

    /**
     * ACKed node ids of a wave.
     */
    public List<String> getBarrierAckNodes(String taskName, String guardUuid) throws TaskCoordinationException {

        return rdmbsConnector.getBarrierAckNodes(taskName, guardUuid);
    }

    /**
     * Peer LAST_HEARTBEAT from the cluster heartbeat table (the classifier's advisory read).
     */
    public Long getPeerLastHeartbeat(String groupId, String nodeId) throws TaskCoordinationException {

        return rdmbsConnector.getPeerLastHeartbeat(groupId, nodeId);
    }

    /**
     * Ownership publication (i): the leader assignment transition — the task-row DESTINED_NODE_ID
     * write and the expected-epoch claim CAS publishing the TARGET's advertised boot id, one
     * transaction per task.
     */
    public boolean assignTaskWithLeaderPublication(String taskName, String targetNodeId, String groupId,
                                                   long localWindowMillis, long now)
            throws TaskCoordinationException {

        return rdmbsConnector.assignTaskWithLeaderPublication(taskName, targetNodeId, groupId, localWindowMillis,
                now);
    }

    /**
     * Ownership publication (ii): the target scheduling transition — destined-to-me RUNNING update +
     * the CAS publishing THIS node's own live boot id; the JobDetail is built or rebuilt only after
     * this commits.
     */
    public RDMBSConnector.PublishedOwnership publishTargetOwnershipAndRun(String taskName, String localNodeId,
                                                                          String localScheduleFp,
                                                                          String localTriggerFamily,
                                                                          long localWindowMillis, long now)
            throws TaskCoordinationException {

        return rdmbsConnector.publishTargetOwnershipAndRun(taskName, localNodeId, localScheduleFp,
                localTriggerFamily, localWindowMillis, now);
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
     * The Fenced Writes state transition — ownership plus fence currency (the writing job's claim
     * tuple). Zero rows means fenced or already-applied.
     *
     * @param taskName       name of the task
     * @param updatedState   state to write
     * @param destinedNodeId the writing node's id
     * @param incarnation    the writing job's stamped incarnation
     * @param ownerEpoch     the writing job's stamped owner epoch
     * @param ownerBootId    the writing job's stamped owner boot id
     * @return true if exactly one row was updated
     * @throws TaskCoordinationException when something goes wrong while updating
     */
    public boolean updateTaskStateFenced(String taskName, CoordinatedTask.States updatedState, String destinedNodeId,
                                         int incarnation, long ownerEpoch, String ownerBootId)
            throws TaskCoordinationException {

        return rdmbsConnector.updateTaskStateFenced(taskName, updatedState, destinedNodeId, incarnation, ownerEpoch,
                ownerBootId);
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
     * Attempts the atomic fire-claim CAS for one occurrence of a coordinated task. Never throws;
     * every SQL/connection failure is returned as INDETERMINATE/EXCEPTION. Only WON dispatches.
     *
     * @param taskName      task name
     * @param nodeId        this node's id
     * @param fireKey       occurrence key computed on the firing thread
     * @param ownerEpoch    owner epoch stamped in the job's fence tuple
     * @param incarnation   incarnation stamped in the job's fence tuple
     * @param bootId        owner boot id stamped in the job's fence tuple
     * @param scheduleFingerprint    canonical schedule fingerprint stamped in the job's fence tuple
     * @param triggerFamily trigger family carried by the job's fence tuple
     * @param now           app-clock epoch millis
     * @return the classified claim outcome
     */
    public ClaimOutcome claimFire(String taskName, String nodeId, long fireKey, long ownerEpoch, int incarnation,
                                  String bootId, String scheduleFingerprint, String triggerFamily, long now) {

        return rdmbsConnector.claimFire(taskName, nodeId, fireKey, ownerEpoch, incarnation, bootId, scheduleFingerprint,
                triggerFamily, now);
    }

    /**
     * The activation-readiness anti-join (never a row count): names every current task row lacking an
     * OPEN claim row with a non-null fingerprint and a non-null trigger family, classified per task as
     * missing / unexpectedly-CLOSED / unbound.
     *
     * @return task name to violation classification, empty when the invariant holds
     * @throws TaskCoordinationException if operation fails
     */
    public Map<String, String> getActivationReadinessViolations() throws TaskCoordinationException {
        return rdmbsConnector.getActivationReadinessViolations();
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

    /**
     * Reads this node's advertisement row (boot lease). Null when absent.
     */
    public RDMBSConnector.NodeAdvertisement getNodeAdvertisement(String groupId, String nodeId)
            throws TaskCoordinationException {
        return rdmbsConnector.getNodeAdvertisement(groupId, nodeId);
    }

    /**
     * Inserts the complete advertisement value set for a fresh lease generation.
     *
     * @return true when inserted, false on a unique-violation race
     */
    public boolean insertNodeAdvertisement(String groupId, String nodeId, long heartbeatWindow, String bootId,
                                           long bootStartedAt, String configFingerprint, long updatedAt)
            throws TaskCoordinationException {
        return rdmbsConnector.insertNodeAdvertisement(groupId, nodeId, heartbeatWindow, bootId, bootStartedAt,
                configFingerprint, updatedAt);
    }

    /**
     * The boot lease takeover CAS, conditioned on the exact observed tuple.
     *
     * @return the update count; 1 = lease won
     */
    public int takeoverNodeAdvertisement(String newBootId, long heartbeatWindow, String configFingerprint,
                                         long bootStartedAt, long updatedAt, String groupId, String nodeId,
                                         String expectedBootId, long expectedHeartbeatWindow, long expectedUpdatedAt)
            throws TaskCoordinationException {
        return rdmbsConnector.takeoverNodeAdvertisement(newBootId, heartbeatWindow, configFingerprint, bootStartedAt,
                updatedAt, groupId, nodeId, expectedBootId, expectedHeartbeatWindow, expectedUpdatedAt);
    }

    /**
     * The monotonic strictly-forward BOOT_ID-conditioned renewal touch, with the same-connection
     * read-back verification on zero rows.
     */
    public RDMBSConnector.AdvertisementWriteResult touchNodeAdvertisement(String groupId, String nodeId, String bootId,
                                                                          long mintedUpdatedAt)
            throws TaskCoordinationException {
        return rdmbsConnector.touchNodeAdvertisement(groupId, nodeId, bootId, mintedUpdatedAt);
    }

    /**
     * Publishes ELIGIBILITY for this boot's row, headed by the monotonic renewal touch.
     */
    public RDMBSConnector.AdvertisementWriteResult publishEligibility(String groupId, String nodeId, String bootId,
                                                                      long mintedUpdatedAt, String eligibility)
            throws TaskCoordinationException {
        return rdmbsConnector.publishEligibility(groupId, nodeId, bootId, mintedUpdatedAt, eligibility);
    }

    /**
     * The shutdown release: BOOT_ID-conditioned delete of this boot's own advertisement row.
     *
     * @return the delete count
     */
    public int releaseNodeAdvertisement(String groupId, String nodeId, String bootId)
            throws TaskCoordinationException {
        return rdmbsConnector.releaseNodeAdvertisement(groupId, nodeId, bootId);
    }

    /**
     * Retrieves every task currently destined to the given node, in deterministic name order.
     */
    public List<String> retrieveTaskNamesDestinedToNode(String nodeId) throws TaskCoordinationException {
        return rdmbsConnector.retrieveTaskNamesDestinedToNode(nodeId);
    }

    /**
     * One startup-handback task transition — the destination-conditioned unassignment, one transaction
     * per task, headed by the monotonic renewal touch.
     */
    public RDMBSConnector.HandbackResult handBackDestinedTask(String taskName, String expectedDestinedNodeId,
                                                              String groupId, String nodeId, String bootId,
                                                              long mintedUpdatedAt) throws TaskCoordinationException {
        return rdmbsConnector.handBackDestinedTask(taskName, expectedDestinedNodeId, groupId, nodeId, bootId,
                mintedUpdatedAt);
    }

    /**
     * Replaces this node's previous running-task observation with the tasks seen in the current scheduler
     * cycle. The timestamp is supplied by the caller so every row from one publish uses the same cycle time.
     *
     * @param nodeId       node that owns the observation rows
     * @param runningTasks coordinated tasks currently running on that node
     * @param cycleTime    publish time for this observation batch, in epoch milliseconds
     */
    public void recordObservations(String nodeId, List<String> runningTasks, long cycleTime)
            throws TaskCoordinationException {
        rdmbsConnector.recordObservations(nodeId, runningTasks, cycleTime);
    }

    /**
     * Returns recent observations grouped as task name to node ids, which is the shape the leader needs
     * to decide whether a task is running on more than one node.
     */
    public Map<String, Set<String>> readFreshObservationsByTask(long minObservedAt) throws TaskCoordinationException {
        return rdmbsConnector.readFreshObservationsByTask(minObservedAt);
    }

    /**
     * Returns duplicate execution episodes that have been detected but not yet closed.
     */
    public List<RDMBSConnector.DuplicationEpisode> getOpenDuplicationEpisodes() throws TaskCoordinationException {
        return rdmbsConnector.getOpenDuplicationEpisodes();
    }

    /**
     * Starts tracking a newly observed duplicate execution for one task.
     */
    public void openDuplicationEpisode(String taskName, String nodes, String destinedNode, long detectedAt,
                                       String kind) throws TaskCoordinationException {
        rdmbsConnector.openDuplicationEpisode(taskName, nodes, destinedNode, detectedAt, kind);
    }

    /**
     * Marks an open duplicate execution episode as sustained after it survives the grace period.
     */
    public void markDuplicationEpisodeSustained(String taskName) throws TaskCoordinationException {
        rdmbsConnector.markDuplicationEpisodeSustained(taskName);
    }

    /**
     * Closes the open duplicate execution episode once the task is no longer observed on multiple nodes.
     */
    public void closeDuplicationEpisode(String taskName, long clearedAt) throws TaskCoordinationException {
        rdmbsConnector.closeDuplicationEpisode(taskName, clearedAt);
    }

    /**
     * The operator reconfigure — the only generation-changing operation: guarded close over the full
     * expected tuple, checked reopen onto the computed target fingerprint, unassignment.
     */
    public RDMBSConnector.ReconfigureResult reconfigureTask(String taskName, int expectedIncarnation,
                                                            String expectedScheduleFingerprint, String targetScheduleFingerprint,
                                                            String targetFamily)
            throws TaskCoordinationException {

        return rdmbsConnector.reconfigureTask(taskName, expectedIncarnation, expectedScheduleFingerprint, targetScheduleFingerprint, targetFamily);
    }

    /**
     * The operator task-retire — the operator-initiated delete wave, born STATUS='RETIRING' with the
     * caller-stable operationId as the wave's durable guard.
     */
    public RDMBSConnector.RetireResult retireTask(String taskName, int expectedIncarnation, String operationId,
                                                  List<String> expectedNodeIds, long deadlineAt, long now)
            throws TaskCoordinationException {

        return rdmbsConnector.retireTask(taskName, expectedIncarnation, operationId, expectedNodeIds, deadlineAt,
                now);
    }

    /**
     * The RETIRING responder's one indexed read: in-flight RETIRING waves where the node is in
     * EXPECTED and has no ACK row.
     */
    public List<RDMBSConnector.RetiringWaveRef> getUnansweredRetiringWaves(String nodeId)
            throws TaskCoordinationException {

        return rdmbsConnector.getUnansweredRetiringWaves(nodeId);
    }

    /**
     * The responder's consent: this node's boot-fenced ack, guard-scoped to the wave it read.
     */
    public boolean ackRetiringWave(String taskName, String guardUuid, String nodeId, long ackedAt)
            throws TaskCoordinationException {

        return rdmbsConnector.ackRetiringWave(taskName, guardUuid, nodeId, ackedAt);
    }

    /**
     * The responder's durable dissent: RETIRING -> RETIRE_REFUSED + the claim-guard clear, task-locked.
     */
    public boolean refuseRetiringWave(String taskName, String guardUuid) throws TaskCoordinationException {

        return rdmbsConnector.refuseRetiringWave(taskName, guardUuid);
    }

}

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
import org.wso2.micro.integrator.ntask.coordination.task.BarrierLivenessClassifier;
import org.wso2.micro.integrator.ntask.coordination.task.ClaimOutcome;
import org.wso2.micro.integrator.ntask.coordination.task.CoordinatedTask;
import org.wso2.micro.integrator.ntask.core.BootLeaseController;
import org.wso2.micro.integrator.ntask.core.CoordinationReadinessRegistry;
import org.wso2.micro.integrator.ntask.core.RegistrationPhase;
import org.wso2.micro.integrator.ntask.core.RegistrationResult;
import org.wso2.micro.integrator.ntask.core.impl.EpisodeLog;
import org.wso2.micro.integrator.ntask.core.internal.BootLeaseControllerImpl;
import org.wso2.micro.integrator.ntask.core.internal.TasksDSComponent;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransactionRollbackException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import javax.sql.DataSource;

import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.ACTIVATE_TASK;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.ADD_TASK;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.BARRIER_STATUS_FINALIZING;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.BARRIER_STATUS_OPEN;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.BARRIER_STATUS_RETIRE_REFUSED;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.BARRIER_STATUS_RETIRING;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.BIND_RESIDUAL_UNBOUND_CLAIM;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.CLEAR_CLAIM_DELETE_GUARD_CLOSED_IF_MATCH;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.CLEAR_CLAIM_DELETE_GUARD_OPEN_IF_MATCH;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.CLOSE_CLAIM_FOR_GUARD;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.CLOSE_CLAIM_FOR_RECONFIGURE;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.HAND_BACK_NODE_RUNNING_TASKS;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.HAND_BACK_SURVIVING_RUNNING_TASK;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.REOPEN_CLAIM_ONESHOT_BIND_FAMILY;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.REOPEN_CLAIM_ONESHOT_IF_FAMILY_MATCH;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.RETRIEVE_UNANSWERED_RETIRING_WAVES;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.STAMP_CLAIM_DELETE_GUARD_CLOSED_IF_MATCH;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.STAMP_CLAIM_DELETE_GUARD_CLOSED_WHEN_NULL;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.INSERT_CLAIM_ROW;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.LOCK_TASK_ROW;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.NORMALIZE_DELETE_PENDING_TASK;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.PUBLISH_CLAIM_OWNER_LEADER_ASSIGN;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.PUBLISH_CLAIM_OWNER_TARGET_SCHEDULE;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.REOPEN_CLAIM_RECURRING_BIND_FAMILY;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.REOPEN_CLAIM_RECURRING_IF_FAMILY_MATCH;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.SELECT_CLAIM_FOR_REGISTRATION;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.SELECT_PEER_LAST_HEARTBEAT;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.SELECT_TASK_ROW;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.STAMP_CLAIM_DELETE_GUARD_OPEN_WHEN_NULL;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.TRIGGER_FAMILY_RECURRING;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.CLAIM_FIRE;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.CLAIM_STATUS_CLOSED;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.CLAIM_STATUS_OPEN;
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
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.SELECT_ACTIVATION_READINESS_VIOLATIONS;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.SELECT_CLAIM_BIND_STATE;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.SELECT_CLAIM_ROW;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.SELECT_OPEN_TASK_DELETE_BARRIERS;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.SELECT_OPEN_TASK_DELETE_BARRIER_BY_TASK_AND_GUARD;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.SELECT_MAX_TASK_DELETE_GUARD_UPDATED_AT;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.SELECT_TASK_DELETE_BARRIER_ACK_NODES;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.SELECT_TASK_DELETE_BARRIER;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.SELECT_TASK_DELETE_BARRIER_EXPECTED_NODES;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.SELECT_TASK_DELETE_GUARD;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.TASK_NAME;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.TASK_DELETE_PENDING_STATE;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.TRIGGER_FAMILY_ONESHOT;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.TASK_STATE;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UPDATE_ASSIGNMENT_AND_STATE;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UPDATE_TASK_DELETE_BARRIER_ACK;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UPDATE_TASK_DELETE_BARRIER_STATUS;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UPDATE_TASK_DELETE_BARRIER_TIMESTAMP;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UPDATE_TASK_DELETE_GUARD;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UPDATE_TASK_DELETE_GUARD_IF_MATCH;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UPDATE_TASK_DELETE_GUARD_TIMESTAMP_IF_MATCH;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UPDATE_TASK_STATE;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UPDATE_TASK_STATE_FENCED;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UPDATE_TASK_STATE_FOR_DESTINED_NODE;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UPDATE_TASK_STATUS_TO_DEACTIVATED;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.INSERT_MP_STATE;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UPDATE_MP_STATE;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UPDATE_TASK_STATUS_TO_DELETE_PENDING;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UPDATED_AT;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.DELETE_NODE_ADVERTISEMENT_OF_BOOT;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.INSERT_NODE_ADVERTISEMENT;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.RETRIEVE_TASKS_DESTINED_TO_NODE;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.SELECT_NODE_ADVERTISEMENT;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.TAKEOVER_NODE_ADVERTISEMENT;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.TOUCH_NODE_ADVERTISEMENT;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UNASSIGN_CLAIM_OWNER;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UNASSIGN_TASK_IF_DESTINED;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UPDATE_NODE_ADVERTISEMENT_ELIGIBILITY;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.DELETE_STALE_RUNNING_TASK_OBSERVATIONS_OF_NODE;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.INSERT_RUNNING_TASK_OBSERVATION;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.UPDATE_RUNNING_TASK_OBSERVATION;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.SELECT_FRESH_RUNNING_TASK_OBSERVATIONS;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.INSERT_DUPLICATION_EVENT;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.SELECT_OPEN_DUPLICATION_EVENTS;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.MARK_DUPLICATION_EVENT_SUSTAINED;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.CLOSE_DUPLICATION_EVENT;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.DETECTED_AT;
import static org.wso2.micro.integrator.ntask.coordination.task.store.connector.TaskQueryHelper.SEVERITY;

/**
 * The connector class which deals with underlying coordinated task table.
 */
public class RDMBSConnector {

    private static final Log LOG = LogFactory.getLog(RDMBSConnector.class);
    private static final String ERROR_MSG = "Error while doing data base operation.";
    private static final String EMPTY_LIST = "Provided list is empty ";
    private static final String SQL_INTEGRITY_VIOLATION_CODE = "23";
    // Statement timeout for best-effort monitoring queries. Connection acquisition is controlled by the
    // datasource/pool; this timeout only applies after a PreparedStatement is created.
    private static final int OBSERVATION_DB_TIMEOUT_SECONDS = 5;
    private static final String DELETE_PENDING_STATE = TASK_DELETE_PENDING_STATE;
    private static final Set<Integer> DUPLICATE_KEY_ERROR_CODES = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(1, 1062, 2627, 2601, 803, -803)));
    private static final Set<Integer> SERIALIZATION_FAILURE_ERROR_CODES = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(1213, 1205, 60, 8177, -911, -913)));
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
     * Claim-history tripwire cache. Best-effort by nature: a restore from 100 to 50 followed by a
     * legitimate claim of 101 is observed as 100 -> 101 and trips nothing, and this in-memory cache is
     * lost on restart. It catches some lossy-DB events, never all — the single-writer ops requirement
     * (one writable primary at any instant) is the real defence. Recurring tasks are keyed by task name
     * alone, across incarnations and fingerprints (their keys are never legitimately reset); one-shots
     * by (task name, incarnation) (their deliberate reopen resets to -1, so only within one incarnation
     * is a drop meaningful).
     */
    private final Map<String, Long> lastSeenFireKey = new ConcurrentHashMap<>();

    /**
     * Attempts the atomic fire-claim CAS for one occurrence of a coordinated task. Runs on the
     * auto-commit connection; returns WON only when the single-row UPDATE reports exactly one row.
     * Zero rows are classified by a read-back on the same connection in the defined first-match
     * precedence order. This method never throws: every SQL/connection failure is returned as
     * INDETERMINATE/EXCEPTION, and an ambiguous result is never retried for the same fire.
     *
     * @param taskName      task name
     * @param nodeId        this node's id
     * @param fireKey       occurrence key computed on the firing thread
     * @param ownerEpoch    owner epoch stamped in the job's fence tuple
     * @param incarnation   incarnation stamped in the job's fence tuple
     * @param bootId        owner boot id stamped in the job's fence tuple
     * @param scheduleFingerprint    canonical schedule fingerprint stamped in the job's fence tuple
     * @param triggerFamily trigger family carried by the job's fence tuple (DB column stays
     *                      authoritative at bind); feeds the family-keyed tripwire
     * @param now           app-clock epoch millis
     * @return the classified claim outcome; only WON dispatches
     */
    public ClaimOutcome claimFire(String taskName, String nodeId, long fireKey, long ownerEpoch, int incarnation,
                                  String bootId, String scheduleFingerprint, String triggerFamily, long now) {

        try (Connection connection = getConnection()) {
            try (PreparedStatement preparedStatement = connection.prepareStatement(CLAIM_FIRE)) {
                preparedStatement.setLong(1, fireKey);
                preparedStatement.setString(2, nodeId);
                preparedStatement.setLong(3, now);
                preparedStatement.setString(4, taskName);
                preparedStatement.setLong(5, fireKey);
                preparedStatement.setLong(6, ownerEpoch);
                preparedStatement.setInt(7, incarnation);
                preparedStatement.setString(8, bootId);
                preparedStatement.setString(9, scheduleFingerprint);
                if (preparedStatement.executeUpdate() == 1) {
                    observeClaimFireKey(taskName, triggerFamily, incarnation, fireKey);
                    return ClaimOutcome.won();
                }
            }
            try (PreparedStatement readBack = connection.prepareStatement(SELECT_CLAIM_ROW)) {
                readBack.setString(1, taskName);
                try (ResultSet resultSet = readBack.executeQuery()) {
                    if (!resultSet.next()) {
                        return ClaimOutcome.noFenceRow();
                    }
                    long rowFireKey = resultSet.getLong(1);
                    int rowIncarnation = resultSet.getInt(2);
                    long rowOwnerEpoch = resultSet.getLong(3);
                    String rowBootId = resultSet.getString(4);
                    String rowScheduleFp = resultSet.getString(5);
                    String rowStatus = resultSet.getString(6);
                    observeClaimFireKey(taskName, triggerFamily, rowIncarnation, rowFireKey);
                    if (CLAIM_STATUS_CLOSED.equals(rowStatus)) {
                        return ClaimOutcome.closed();
                    }
                    if (!Objects.equals(scheduleFingerprint, rowScheduleFp)) {
                        return ClaimOutcome.fpMismatch();
                    }
                    if (rowIncarnation != incarnation) {
                        return ClaimOutcome.incarnationMismatch();
                    }
                    if (rowOwnerEpoch != ownerEpoch) {
                        return ClaimOutcome.epochMismatch();
                    }
                    if (!Objects.equals(bootId, rowBootId)) {
                        return ClaimOutcome.bootMismatch();
                    }
                    if (rowFireKey >= fireKey) {
                        return ClaimOutcome.lostRace();
                    }
                    // zero rows the read-back cannot explain — ambiguity, never a specific diagnosis
                    return ClaimOutcome.exception();
                }
            }
        } catch (Exception ex) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Claim CAS failed for task [" + taskName + "].", ex);
            }
            return ClaimOutcome.exception();
        }
    }

    private void observeClaimFireKey(String taskName, String triggerFamily, int incarnation, long observedFireKey) {

        String key = TRIGGER_FAMILY_ONESHOT.equals(triggerFamily) ? taskName + '\u0000' + incarnation : taskName;
        Long seen = lastSeenFireKey.get(key);
        if (seen != null && observedFireKey < seen) {
            LOG.error("COORDINATED_TASK_CLAIM went backwards for [" + taskName + "] (" + seen + " -> "
                    + observedFireKey + "): the coordination DB lost committed writes (failover to stale replica / "
                    + "restore). Duplicate dispatch is possible until the DB is verified. Check replication.");
        }
        lastSeenFireKey.put(key, observedFireKey);
    }

    /**
     * The bind protocol for a residual unbound claim row, run on the boot pass on the CALLER's
     * connection (the registration transaction): binds the fingerprint AND the trigger family in one
     * NULL-to-value CAS — a non-null fingerprint or family is never overwritten. Zero rows are
     * classified by a read-back on the same connection: same values = already bound (no-op); a
     * different fingerprint or family, or a CLOSED/missing row, throws — the caller rolls the
     * registration transaction back.
     *
     * @param connection    the caller's transaction connection (task lock already held)
     * @param taskName      task name
     * @param scheduleFingerprint    canonical fingerprint computed from the deployed artifact
     * @param triggerFamily trigger family derived from the deployed artifact's TriggerInfo
     * @throws TaskCoordinationException on any classification failure or SQL error
     */
    public void bindResidualUnboundClaim(Connection connection, String taskName, String scheduleFingerprint,
                                         String triggerFamily) throws TaskCoordinationException {

        try {
            try (PreparedStatement preparedStatement = connection.prepareStatement(BIND_RESIDUAL_UNBOUND_CLAIM)) {
                preparedStatement.setString(1, scheduleFingerprint);
                preparedStatement.setString(2, triggerFamily);
                preparedStatement.setString(3, taskName);
                if (preparedStatement.executeUpdate() == 1) {
                    return;
                }
            }
            try (PreparedStatement readBack = connection.prepareStatement(SELECT_CLAIM_BIND_STATE)) {
                readBack.setString(1, taskName);
                try (ResultSet resultSet = readBack.executeQuery()) {
                    if (!resultSet.next()) {
                        throw new TaskCoordinationException("Invariant failure: no claim row exists for task ["
                                + taskName + "] at the boot-pass bind.");
                    }
                    String rowScheduleFp = resultSet.getString(1);
                    String rowTriggerFamily = resultSet.getString(2);
                    String rowStatus = resultSet.getString(3);
                    if (!CLAIM_STATUS_OPEN.equals(rowStatus)) {
                        throw new TaskCoordinationException("Invariant failure: the claim row for task [" + taskName
                                + "] is [" + rowStatus + "] at the boot-pass bind.");
                    }
                    if (Objects.equals(scheduleFingerprint, rowScheduleFp)
                            && Objects.equals(triggerFamily, rowTriggerFamily)) {
                        return;
                    }
                    if (!Objects.equals(scheduleFingerprint, rowScheduleFp)) {
                        throw new TaskCoordinationException("The boot-pass bind for task [" + taskName
                                + "] found a different stored fingerprint: deployed [" + scheduleFingerprint + "], stored ["
                                + rowScheduleFp + "].");
                    }
                    throw new TaskCoordinationException("The boot-pass bind for task [" + taskName
                            + "] found a different stored trigger family: deployed [" + triggerFamily + "], stored ["
                            + rowTriggerFamily + "] — the trigger family is immutable after bind.");
                }
            }
        } catch (SQLException ex) {
            throw new TaskCoordinationException("Error while binding the claim row for task [" + taskName + "].", ex);
        }
    }

    /**
     * The activation-readiness anti-join (never a row count — CLOSED tombstones legitimately outnumber
     * task rows after the first genuine delete): names every current task row lacking an OPEN claim
     * row with a non-null fingerprint and a non-null trigger family, classified per task as missing /
     * unexpectedly-CLOSED / unbound.
     *
     * @return task name to violation classification, empty when the invariant holds
     * @throws TaskCoordinationException on a database error
     */
    public Map<String, String> getActivationReadinessViolations() throws TaskCoordinationException {

        Map<String, String> violations = new LinkedHashMap<>();
        try (Connection connection = getConnection();
             PreparedStatement preparedStatement =
                     connection.prepareStatement(SELECT_ACTIVATION_READINESS_VIOLATIONS);
             ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                String taskName = resultSet.getString(1);
                String claimTaskName = resultSet.getString(2);
                String status = resultSet.getString(3);
                if (claimTaskName == null) {
                    violations.put(taskName, "missing claim row");
                } else if (!CLAIM_STATUS_OPEN.equals(status)) {
                    violations.put(taskName, "unexpectedly " + status + " claim row");
                } else {
                    violations.put(taskName, "unbound claim row (NULL fingerprint or trigger family)");
                }
            }
        } catch (SQLException ex) {
            throw new TaskCoordinationException("Error while running the activation-readiness anti-join query.", ex);
        }
        return violations;
    }

    /**
     * In-memory view of this node's NODE_ADVERTISEMENT row.
     */
    public static class NodeAdvertisement {

        private final String bootId;
        private final long heartbeatWindow;
        private final String configFingerprint;
        private final String eligibility;
        private final long bootStartedAt;
        private final long updatedAt;

        NodeAdvertisement(String bootId, long heartbeatWindow, String configFingerprint, String eligibility,
                          long bootStartedAt, long updatedAt) {
            this.bootId = bootId;
            this.heartbeatWindow = heartbeatWindow;
            this.configFingerprint = configFingerprint;
            this.eligibility = eligibility;
            this.bootStartedAt = bootStartedAt;
            this.updatedAt = updatedAt;
        }

        public String getBootId() {
            return bootId;
        }

        public long getHeartbeatWindow() {
            return heartbeatWindow;
        }

        public String getConfigFingerprint() {
            return configFingerprint;
        }

        public String getEligibility() {
            return eligibility;
        }

        public long getBootStartedAt() {
            return bootStartedAt;
        }

        public long getUpdatedAt() {
            return updatedAt;
        }
    }

    /**
     * Outcome of a lease-token-conditioned advertisement write. APPLIED = the strictly-forward touch
     * changed the row; SELF_TOUCH_ADOPTED = zero rows but the same-connection read-back proved a benign
     * concurrent self-touch (BOOT_ID mine AND UPDATED_AT at least the minted value) — treated as
     * success, the observed value adopted; IDENTITY_LOST = another BOOT_ID, row gone, or a smaller
     * value — the identity is no longer this process's.
     */
    public static final class AdvertisementWriteResult {

        public enum Verdict { APPLIED, SELF_TOUCH_ADOPTED, IDENTITY_LOST }

        private final Verdict verdict;
        private final long adoptedUpdatedAt;

        private AdvertisementWriteResult(Verdict verdict, long adoptedUpdatedAt) {
            this.verdict = verdict;
            this.adoptedUpdatedAt = adoptedUpdatedAt;
        }

        public Verdict getVerdict() {
            return verdict;
        }

        public long getAdoptedUpdatedAt() {
            return adoptedUpdatedAt;
        }
    }

    /**
     * Outcome of one startup-handback task transition. HANDED_BACK = task unassigned (claim epoch
     * advanced when an OPEN claim row existed); STALE_SNAPSHOT = the destination-conditioned task
     * write hit zero rows (a resolver moved the task between the read and the write) — the claim was
     * skipped entirely; IDENTITY_LOST = the heading advertisement touch proved the identity is no
     * longer this process's.
     */
    public static final class HandbackResult {

        public enum Verdict { HANDED_BACK, STALE_SNAPSHOT, IDENTITY_LOST }

        private final Verdict verdict;
        private final long adoptedUpdatedAt;

        private HandbackResult(Verdict verdict, long adoptedUpdatedAt) {
            this.verdict = verdict;
            this.adoptedUpdatedAt = adoptedUpdatedAt;
        }

        public Verdict getVerdict() {
            return verdict;
        }

        public long getAdoptedUpdatedAt() {
            return adoptedUpdatedAt;
        }
    }

    /**
     * Reads this node's advertisement row. Plain SELECT on the auto-commit connection.
     *
     * @return the row, or null when absent
     */
    public NodeAdvertisement getNodeAdvertisement(String groupId, String nodeId) throws TaskCoordinationException {

        try (Connection connection = getConnection()) {
            return readNodeAdvertisement(connection, groupId, nodeId);
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * Inserts the complete advertisement value set for a fresh lease generation. Single-statement,
     * own-connection insert: a unique violation only (a concurrent boot won the insert race) returns
     * false; every other integrity error is fatal, never "already present".
     *
     * @return true when inserted, false on a unique-violation race
     */
    public boolean insertNodeAdvertisement(String groupId, String nodeId, long heartbeatWindow, String bootId,
                                           long bootStartedAt, String configFingerprint, long updatedAt)
            throws TaskCoordinationException {

        try (Connection connection = getConnection();
                PreparedStatement preparedStatement = connection.prepareStatement(INSERT_NODE_ADVERTISEMENT)) {
            preparedStatement.setString(1, groupId);
            preparedStatement.setString(2, nodeId);
            preparedStatement.setLong(3, heartbeatWindow);
            preparedStatement.setString(4, bootId);
            preparedStatement.setLong(5, bootStartedAt);
            preparedStatement.setString(6, configFingerprint);
            preparedStatement.setLong(7, updatedAt);
            preparedStatement.executeUpdate();
            return true;
        } catch (SQLException ex) {
            if (isUniqueViolation(ex)) {
                return false;
            }
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * The takeover CAS, conditioned on the exact observed tuple. Single-statement auto-commit UPDATE;
     * 1 row = lease won with the advertisement complete, 0 rows = a race — the caller re-reads.
     *
     * @return the update count
     */
    public int takeoverNodeAdvertisement(String newBootId, long heartbeatWindow, String configFingerprint,
                                         long bootStartedAt, long updatedAt, String groupId, String nodeId,
                                         String expectedBootId, long expectedHeartbeatWindow, long expectedUpdatedAt)
            throws TaskCoordinationException {

        try (Connection connection = getConnection();
                PreparedStatement preparedStatement = connection.prepareStatement(TAKEOVER_NODE_ADVERTISEMENT)) {
            preparedStatement.setString(1, newBootId);
            preparedStatement.setLong(2, heartbeatWindow);
            preparedStatement.setString(3, configFingerprint);
            preparedStatement.setLong(4, bootStartedAt);
            preparedStatement.setLong(5, updatedAt);
            preparedStatement.setString(6, groupId);
            preparedStatement.setString(7, nodeId);
            preparedStatement.setString(8, expectedBootId);
            preparedStatement.setLong(9, expectedHeartbeatWindow);
            preparedStatement.setLong(10, expectedUpdatedAt);
            return preparedStatement.executeUpdate();
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * The monotonic strictly-forward BOOT_ID-conditioned renewal touch. Zero rows are verified before
     * any latch: the row is read back on the same connection and a benign concurrent self-touch
     * (BOOT_ID mine AND UPDATED_AT at least the minted value) is success.
     */
    public AdvertisementWriteResult touchNodeAdvertisement(String groupId, String nodeId, String bootId,
                                                           long mintedUpdatedAt) throws TaskCoordinationException {

        try (Connection connection = getConnection()) {
            int touched = executeAdvertisementTouch(connection, groupId, nodeId, bootId, mintedUpdatedAt);
            if (touched == 1) {
                return new AdvertisementWriteResult(AdvertisementWriteResult.Verdict.APPLIED, mintedUpdatedAt);
            }
            NodeAdvertisement row = readNodeAdvertisement(connection, groupId, nodeId);
            if (row != null && bootId.equals(row.getBootId()) && row.getUpdatedAt() >= mintedUpdatedAt) {
                return new AdvertisementWriteResult(AdvertisementWriteResult.Verdict.SELF_TOUCH_ADOPTED,
                        row.getUpdatedAt());
            }
            return new AdvertisementWriteResult(AdvertisementWriteResult.Verdict.IDENTITY_LOST, mintedUpdatedAt);
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * Publishes ELIGIBILITY for this boot's row: a single-advertisement-row identity-protecting
     * mutation, so the transaction's FIRST statement is the monotonic renewal touch itself as the
     * lease-token-conditioned head, then the BOOT_ID-conditioned eligibility update.
     */
    public AdvertisementWriteResult publishEligibility(String groupId, String nodeId, String bootId,
                                                       long mintedUpdatedAt, String eligibility)
            throws TaskCoordinationException {

        Connection connection = null;
        try {
            connection = getTransactionalConnection();
            AdvertisementWriteResult.Verdict verdict = AdvertisementWriteResult.Verdict.APPLIED;
            long adopted = mintedUpdatedAt;
            int touched = executeAdvertisementTouch(connection, groupId, nodeId, bootId, mintedUpdatedAt);
            if (touched != 1) {
                NodeAdvertisement row = readNodeAdvertisement(connection, groupId, nodeId);
                if (row == null || !bootId.equals(row.getBootId()) || row.getUpdatedAt() < mintedUpdatedAt) {
                    rollbackQuietly(connection);
                    return new AdvertisementWriteResult(AdvertisementWriteResult.Verdict.IDENTITY_LOST,
                            mintedUpdatedAt);
                }
                verdict = AdvertisementWriteResult.Verdict.SELF_TOUCH_ADOPTED;
                adopted = row.getUpdatedAt();
            }
            try (PreparedStatement preparedStatement = connection
                    .prepareStatement(UPDATE_NODE_ADVERTISEMENT_ELIGIBILITY)) {
                preparedStatement.setString(1, eligibility);
                preparedStatement.setString(2, groupId);
                preparedStatement.setString(3, nodeId);
                preparedStatement.setString(4, bootId);
                if (preparedStatement.executeUpdate() != 1) {
                    rollbackQuietly(connection);
                    return new AdvertisementWriteResult(AdvertisementWriteResult.Verdict.IDENTITY_LOST, adopted);
                }
            }
            connection.commit();
            return new AdvertisementWriteResult(verdict, adopted);
        } catch (SQLException ex) {
            rollbackQuietly(connection);
            throw new TaskCoordinationException(ERROR_MSG, ex);
        } finally {
            closeQuietly(connection);
        }
    }

    /**
     * The shutdown release: BOOT_ID-conditioned delete of this boot's own advertisement row — a stale
     * process can never delete its successor's row.
     *
     * @return the delete count
     */
    public int releaseNodeAdvertisement(String groupId, String nodeId, String bootId)
            throws TaskCoordinationException {

        try (Connection connection = getConnection();
                PreparedStatement preparedStatement = connection.prepareStatement(DELETE_NODE_ADVERTISEMENT_OF_BOOT)) {
            preparedStatement.setString(1, groupId);
            preparedStatement.setString(2, nodeId);
            preparedStatement.setString(3, bootId);
            return preparedStatement.executeUpdate();
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * Retrieves every task currently destined to the given node, in deterministic name order.
     */
    public List<String> retrieveTaskNamesDestinedToNode(String nodeId) throws TaskCoordinationException {

        try (Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(
                RETRIEVE_TASKS_DESTINED_TO_NODE)) {
            preparedStatement.setString(1, nodeId);
            return query(preparedStatement, "destined to node [" + nodeId + "]");
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * One startup-handback task transition — the destination-conditioned unassignment, one caller-owned
     * transaction per task, headed by the monotonic renewal touch as the lease-token-conditioned head.
     * Global lock order: ADVERTISEMENT (own row) then TASK then CLAIM. Zero rows on the task write =
     * stale snapshot, the claim is skipped entirely. An unexpected claim-CAS count rolls the whole task
     * transition back and throws — fail closed, retried by the caller.
     */
    public HandbackResult handBackDestinedTask(String taskName, String expectedDestinedNodeId, String groupId,
                                               String nodeId, String bootId, long mintedUpdatedAt)
            throws TaskCoordinationException {

        Connection connection = null;
        try {
            connection = getTransactionalConnection();
            long adopted = mintedUpdatedAt;
            int touched = executeAdvertisementTouch(connection, groupId, nodeId, bootId, mintedUpdatedAt);
            if (touched != 1) {
                NodeAdvertisement row = readNodeAdvertisement(connection, groupId, nodeId);
                if (row == null || !bootId.equals(row.getBootId()) || row.getUpdatedAt() < mintedUpdatedAt) {
                    rollbackQuietly(connection);
                    return new HandbackResult(HandbackResult.Verdict.IDENTITY_LOST, mintedUpdatedAt);
                }
                adopted = row.getUpdatedAt();
            }
            int taskRows;
            try (PreparedStatement preparedStatement = connection.prepareStatement(UNASSIGN_TASK_IF_DESTINED)) {
                preparedStatement.setString(1, taskName);
                preparedStatement.setString(2, expectedDestinedNodeId);
                taskRows = preparedStatement.executeUpdate();
            }
            if (taskRows == 0) {
                connection.commit();
                return new HandbackResult(HandbackResult.Verdict.STALE_SNAPSHOT, adopted);
            }
            boolean claimOpen = false;
            long ownerEpoch = 0;
            int incarnation = 0;
            try (PreparedStatement preparedStatement = connection.prepareStatement(SELECT_CLAIM_ROW)) {
                preparedStatement.setString(1, taskName);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        incarnation = resultSet.getInt(2);
                        ownerEpoch = resultSet.getLong(3);
                        claimOpen = CLAIM_STATUS_OPEN.equals(resultSet.getString(6));
                    }
                }
            }
            if (claimOpen) {
                long nextEpoch;
                try {
                    nextEpoch = Math.addExact(ownerEpoch, 1L);
                } catch (ArithmeticException ex) {
                    rollbackQuietly(connection);
                    throw new TaskCoordinationException("OWNER_EPOCH overflow for task [" + taskName
                            + "] — refusing the startup handback instead of wrapping a fencing token.", ex);
                }
                int claimRows;
                try (PreparedStatement preparedStatement = connection.prepareStatement(UNASSIGN_CLAIM_OWNER)) {
                    preparedStatement.setLong(1, nextEpoch);
                    preparedStatement.setString(2, taskName);
                    preparedStatement.setInt(3, incarnation);
                    preparedStatement.setLong(4, ownerEpoch);
                    claimRows = preparedStatement.executeUpdate();
                }
                if (claimRows != 1) {
                    rollbackQuietly(connection);
                    throw new TaskCoordinationException("Claim owner CAS returned an unexpected row count for task ["
                            + taskName + "] during the startup handback; the whole task transition was rolled back.");
                }
            }
            connection.commit();
            return new HandbackResult(HandbackResult.Verdict.HANDED_BACK, adopted);
        } catch (SQLException ex) {
            rollbackQuietly(connection);
            throw new TaskCoordinationException(ERROR_MSG, ex);
        } finally {
            closeQuietly(connection);
        }
    }

    private int executeAdvertisementTouch(Connection connection, String groupId, String nodeId, String bootId,
                                          long mintedUpdatedAt) throws SQLException {

        try (PreparedStatement preparedStatement = connection.prepareStatement(TOUCH_NODE_ADVERTISEMENT)) {
            preparedStatement.setLong(1, mintedUpdatedAt);
            preparedStatement.setString(2, groupId);
            preparedStatement.setString(3, nodeId);
            preparedStatement.setString(4, bootId);
            preparedStatement.setLong(5, mintedUpdatedAt);
            return preparedStatement.executeUpdate();
        }
    }

    private NodeAdvertisement readNodeAdvertisement(Connection connection, String groupId, String nodeId)
            throws SQLException {

        try (PreparedStatement preparedStatement = connection.prepareStatement(SELECT_NODE_ADVERTISEMENT)) {
            preparedStatement.setString(1, groupId);
            preparedStatement.setString(2, nodeId);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (!resultSet.next()) {
                    return null;
                }
                return new NodeAdvertisement(resultSet.getString(1), resultSet.getLong(2), resultSet.getString(3),
                        resultSet.getString(4), resultSet.getLong(5), resultSet.getLong(6));
            }
        }
    }

    /**
     * Classifies unique violations ONLY (PostgreSQL 23505; MySQL 1062; Oracle ORA-00001; MSSQL
     * 2627/2601; DB2 -803) — every other integrity error (NOT NULL, FK, CHECK) is fatal, never
     * "already present".
     */
    private boolean isUniqueViolation(SQLException ex) {

        SQLException current = ex;
        while (current != null) {
            if ("23505".equals(current.getSQLState()) || DUPLICATE_KEY_ERROR_CODES.contains(current.getErrorCode())) {
                return true;
            }
            current = current.getNextException();
        }
        return false;
    }

    /**
     * Classifies serialization failures ONLY (ISO SQLState 40001; PostgreSQL also 40P01; MySQL
     * deadlock 1213 / lock-wait 1205; Oracle ORA-00060 / ORA-08177; MSSQL 1205; DB2 -911/-913;
     * any SQLTransactionRollbackException) — the database rolled the victim's transaction back
     * entirely, so nothing partial remains and re-running the transaction is the correct reaction.
     * Every other error stays fatal: an integrity error means the data is wrong, not the timing.
     */
    private boolean isSerializationFailure(SQLException ex) {

        SQLException current = ex;
        while (current != null) {
            if (current instanceof SQLTransactionRollbackException
                    || "40001".equals(current.getSQLState()) || "40P01".equals(current.getSQLState())
                    || SERIALIZATION_FAILURE_ERROR_CODES.contains(current.getErrorCode())) {
                return true;
            }
            current = current.getNextException();
        }
        return false;
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
     * Helper method to query data base and return node id list.
     *
     * @param preparedStatement - Statement to be executed to retrieve the list of node ids.
     * @throws SQLException - Exception.
     */
    private List<String> queryNodeIds(PreparedStatement preparedStatement, String debug) throws SQLException {
        List<String> nodeIds = new ArrayList<>();
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                nodeIds.add(resultSet.getString(NODE_ID));
            }
        }
        printDebugLogs(new ArrayList<>(nodeIds), "Following list of nodes were retrieved " + debug);
        return nodeIds;
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
    /**
     * The Fenced Writes state transition: applies only while this node still owns the task and the
     * writing job's fence tuple is current against the claim row. Zero rows means fenced or
     * already-applied — the caller logs it through the episode limiter.
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

        try (Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(
                UPDATE_TASK_STATE_FENCED)) {
            preparedStatement.setString(1, updatedState.name());
            preparedStatement.setString(2, taskName);
            preparedStatement.setString(3, destinedNodeId);
            preparedStatement.setString(4, taskName);
            preparedStatement.setInt(5, incarnation);
            preparedStatement.setLong(6, ownerEpoch);
            preparedStatement.setString(7, ownerBootId);
            int result = preparedStatement.executeUpdate();
            if (LOG.isDebugEnabled()) {
                LOG.debug((result == 1 ? "Updated" : "Unable to update") + " state to [" + updatedState
                        + "] for task [" + taskName + "] under the fence of node [" + destinedNodeId + "]");
            }
            return result == 1;
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

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

        if (tasks.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(EMPTY_LIST + " — skipping state update to [" + state + "].");
            }
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
     * Writes this node's current running task snapshot into RUNNING_TASK_OBSERVATION. Rows for tasks
     * still running here are updated or inserted with the same timestamp, and older rows for this node
     * are deleted in the same transaction so readers see either the old snapshot or the full new one.
     *
     * @param nodeId       node that is publishing the snapshot
     * @param runningTasks coordinated tasks currently running on that node
     * @param cycleTime    timestamp to store for every row in this snapshot, in epoch milliseconds
     */
    public void recordObservations(String nodeId, List<String> runningTasks, long cycleTime)
            throws TaskCoordinationException {

        Connection connection = null;
        try {
            connection = getTransactionalConnection();
            for (String task : runningTasks) {
                int updated;
                try (PreparedStatement update = connection.prepareStatement(UPDATE_RUNNING_TASK_OBSERVATION)) {
                    update.setQueryTimeout(OBSERVATION_DB_TIMEOUT_SECONDS);
                    update.setLong(1, cycleTime);
                    update.setString(2, nodeId);
                    update.setString(3, task);
                    updated = update.executeUpdate();
                }
                if (updated == 0) {
                    try (PreparedStatement insert = connection.prepareStatement(INSERT_RUNNING_TASK_OBSERVATION)) {
                        insert.setQueryTimeout(OBSERVATION_DB_TIMEOUT_SECONDS);
                        insert.setString(1, nodeId);
                        insert.setString(2, task);
                        insert.setLong(3, cycleTime);
                        insert.executeUpdate();
                    } catch (SQLException ex) {
                        // Another writer may have inserted the same (node, task) row after our UPDATE
                        // found nothing. Treat that as success because the row now exists.
                        if (!isIntegrityViolation(ex)) {
                            throw ex;
                        }
                    }
                }
            }
            try (PreparedStatement prune = connection.prepareStatement(
                    DELETE_STALE_RUNNING_TASK_OBSERVATIONS_OF_NODE)) {
                prune.setQueryTimeout(OBSERVATION_DB_TIMEOUT_SECONDS);
                prune.setString(1, nodeId);
                prune.setLong(2, cycleTime);
                prune.executeUpdate();
            }
            connection.commit();
        } catch (SQLException ex) {
            rollbackQuietly(connection);
            throw new TaskCoordinationException(ERROR_MSG, ex);
        } finally {
            closeQuietly(connection);
        }
    }

    /**
     * Reads observations that are still recent enough to count as "currently running", grouped by task.
     * The leader uses the grouped result to find tasks observed on multiple nodes.
     *
     * @param minObservedAt oldest accepted OBSERVED_AT value, in epoch milliseconds
     * @return task name to node ids that recently reported running that task
     */
    public Map<String, Set<String>> readFreshObservationsByTask(long minObservedAt) throws TaskCoordinationException {

        Map<String, Set<String>> byTask = new HashMap<>();
        try (Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(
                SELECT_FRESH_RUNNING_TASK_OBSERVATIONS)) {
            preparedStatement.setQueryTimeout(OBSERVATION_DB_TIMEOUT_SECONDS);
            preparedStatement.setLong(1, minObservedAt);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    byTask.computeIfAbsent(resultSet.getString(TASK_NAME), key -> new HashSet<>())
                            .add(resultSet.getString(NODE_ID));
                }
            }
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
        return byTask;
    }

    /**
     * Returns duplicate execution episodes that are still open. Closed episodes are kept for history and
     * are intentionally ignored by the active detector.
     *
     * @return open duplicate episodes with task name, detection time, and severity
     */
    public List<DuplicationEpisode> getOpenDuplicationEpisodes() throws TaskCoordinationException {

        List<DuplicationEpisode> episodes = new ArrayList<>();
        try (Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(
                SELECT_OPEN_DUPLICATION_EVENTS); ResultSet resultSet = preparedStatement.executeQuery()) {
            while (resultSet.next()) {
                episodes.add(new DuplicationEpisode(resultSet.getString(TASK_NAME), resultSet.getLong(DETECTED_AT),
                        resultSet.getString(SEVERITY)));
            }
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
        return episodes;
    }

    /**
     * Creates the first record for a newly detected duplicate task. The episode stays open until a later
     * detection cycle clears it, and severity remains null until the overlap is classified.
     */
    public void openDuplicationEpisode(String taskName, String nodes, String destinedNode, long detectedAt,
                                       String kind) throws TaskCoordinationException {

        try (Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(
                INSERT_DUPLICATION_EVENT)) {
            preparedStatement.setQueryTimeout(OBSERVATION_DB_TIMEOUT_SECONDS);
            preparedStatement.setString(1, taskName);
            preparedStatement.setString(2, nodes);
            preparedStatement.setString(3, destinedNode);
            preparedStatement.setLong(4, detectedAt);
            preparedStatement.setString(5, kind);
            preparedStatement.executeUpdate();
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * Marks the open episode as sustained after the same duplicate execution remains visible past the
     * configured threshold.
     */
    public void markDuplicationEpisodeSustained(String taskName) throws TaskCoordinationException {

        try (Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(
                MARK_DUPLICATION_EVENT_SUSTAINED)) {
            preparedStatement.setQueryTimeout(OBSERVATION_DB_TIMEOUT_SECONDS);
            preparedStatement.setString(1, taskName);
            preparedStatement.executeUpdate();
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * Closes an episode when the duplicate overlap disappears. Episodes that were never marked sustained
     * are classified as transient during this close.
     */
    public void closeDuplicationEpisode(String taskName, long clearedAt) throws TaskCoordinationException {

        try (Connection connection = getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(
                CLOSE_DUPLICATION_EVENT)) {
            preparedStatement.setQueryTimeout(OBSERVATION_DB_TIMEOUT_SECONDS);
            preparedStatement.setLong(1, clearedAt);
            preparedStatement.setString(2, taskName);
            preparedStatement.executeUpdate();
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * Small immutable view of an open duplicate episode used by the leader detector.
     */
    public static final class DuplicationEpisode {
        private final String taskName;
        private final long detectedAt;
        private final String severity;

        public DuplicationEpisode(String taskName, long detectedAt, String severity) {
            this.taskName = taskName;
            this.detectedAt = detectedAt;
            this.severity = severity;
        }

        public String getTaskName() {
            return taskName;
        }

        public long getDetectedAt() {
            return detectedAt;
        }

        public String getSeverity() {
            return severity;
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

    // ------------------------------------------------------------------------------------------------
    // Coordination-hardening (master envelope) paths. Everything below runs ONLY on an ARMED node —
    // a coordination_hardening=false node runs the shipped methods above bit-for-bit. On the patched
    // wave protocol the claim row is the guard authority: the shipped TASK_DELETE_GUARD table is
    // neither written nor read, no task row is ever marked DELETE_PENDING while a wave is open, and
    // the deadline CLASSIFIES the missing nodes — it never authorizes a delete.
    // ------------------------------------------------------------------------------------------------

    // The skew contract: all lease/liveness math uses local epoch millis under the NTP ops
    // requirement — maximum pairwise skew 500 ms.
    private static final long SKEW_ALLOWANCE_MILLIS = 500L;

    private static final String ANTI_JOIN_INVARIANT_CONDITION = "anti-join-invariant";
    private static final String FINGERPRINT_CONFLICT_CONDITION = "fingerprint-conflict";
    private static final String FINGERPRINT_BIND_COMPLETENESS_CONDITION = "fingerprint-bind-completeness";
    private static final String TRIGGER_FAMILY_CONFLICT_CONDITION = "trigger-family-conflict";
    private static final String ORPHAN_TASK_ROW_CONDITION = "orphan-task-row";
    private static final String GUARD_ORPHAN_CONDITION = "guard-orphan";
    private static final String PARKED_TASK_CONDITION = "parked-task";
    private static final String DURABLE_FINALIZING_WAVE_CONDITION = "durable-finalizing-wave";
    private static final String WAVE_HOLDOUT_CONDITION = "wave-holdout";
    // RETIRE_REFUSED rows carry the 7-day terminal retention (children cleaned with them)
    private static final long RETIRE_REFUSED_RETENTION_MILLIS = 7L * 24 * 60 * 60 * 1000;

    /**
     * The parking path's episode limiter (lazily bound to the lease node id).
     */
    private volatile EpisodeLog parkEpisodes;

    /**
     * The cleaner recovery's log-once-per-state-change memory: (task, guard) to the last logged
     * classification summary.
     */
    private final Map<String, String> lastRecoveryStateByWave = new ConcurrentHashMap<>();

    /**
     * The lease fence inputs of one caller-owned fenced transaction, minted by the one lease
     * controller.
     */
    private static final class LeaseFence {

        private final BootLeaseControllerImpl controller;
        private final String groupId;
        private final String nodeId;
        private final String bootId;
        private final long minted;

        private LeaseFence(BootLeaseControllerImpl controller, String groupId, String nodeId, String bootId,
                           long minted) {
            this.controller = controller;
            this.groupId = groupId;
            this.nodeId = nodeId;
            this.bootId = bootId;
            this.minted = minted;
        }
    }

    /**
     * Completed-wave handling: the orphan-task-row clear — GATE conditions must have an exit. Cleared only when the
     * raised condition's detail names THIS task and this node just observed/produced the resolution
     * (reopen, retire, or the row provably absent). A second genuinely-orphaned task re-raises on its
     * own next registration or fire veto.
     */
    private void clearOrphanTaskRowConditionFor(String taskName) {

        for (CoordinationReadinessRegistry.Condition condition : readiness().snapshot()) {
            if (ORPHAN_TASK_ROW_CONDITION.equals(condition.getName())
                    && condition.getDetail() != null
                    && condition.getDetail().contains("[" + taskName + "]")) {
                readiness().clear(ORPHAN_TASK_ROW_CONDITION);
                return;
            }
        }
    }

    private LeaseFence acquireLeaseFence(String operation) throws TaskCoordinationException {

        BootLeaseController controller = TasksDSComponent.getBootLeaseController();
        if (!(controller instanceof BootLeaseControllerImpl)) {
            throw new TaskCoordinationException(operation
                    + " requires the boot lease controller of an armed node; none is installed.");
        }
        BootLeaseControllerImpl impl = (BootLeaseControllerImpl) controller;
        String bootId = impl.bootId();
        if (bootId == null) {
            throw new TaskCoordinationException(operation + " requires an acquired boot lease.");
        }
        return new LeaseFence(impl, impl.getGroupId(), impl.getNodeId(), bootId, impl.mintAdvertisementTouch());
    }

    /**
     * The BOOT_ID-conditioned advertisement touch every fenced transaction opens with, on the
     * transaction's OWN connection — an external pre-check on another connection is insufficient
     * (TOCTOU). Zero rows are disambiguated by the same-connection read-back (a benign concurrent
     * self-touch is success, the observed value adopted); anything else fails closed on the stale
     * boot.
     *
     * @return the adopted touch value to publish to the controller AFTER commit
     */
    private long openWithLeaseFence(Connection connection, LeaseFence fence, String operation)
            throws SQLException, TaskCoordinationException {

        int touched = executeAdvertisementTouch(connection, fence.groupId, fence.nodeId, fence.bootId, fence.minted);
        if (touched == 1) {
            return fence.minted;
        }
        NodeAdvertisement row = readNodeAdvertisement(connection, fence.groupId, fence.nodeId);
        if (row != null && fence.bootId.equals(row.getBootId()) && row.getUpdatedAt() >= fence.minted) {
            return row.getUpdatedAt();
        }
        throw new TaskCoordinationException(operation
                + " refused: the advertisement row no longer carries this boot id — a stale boot fails closed.");
    }

    private CoordinationReadinessRegistry readiness() {

        return TasksDSComponent.getReadinessRegistry();
    }

    private void parkEpisode(LeaseFence fence, String taskName, String reason, String message) {

        EpisodeLog episodes = parkEpisodes;
        if (episodes == null) {
            synchronized (this) {
                if (parkEpisodes == null) {
                    parkEpisodes = new EpisodeLog(fence == null ? "local" : fence.nodeId);
                }
                episodes = parkEpisodes;
            }
        }
        episodes.episodeWarn(taskName, reason, message);
    }

    /**
     * In-memory view of the claim row as read under the registration/finalize transactions.
     */
    private static final class ClaimRow {

        private final String status;
        private final String scheduleFingerprint;
        private final String triggerFamily;
        private final int incarnation;
        private final long ownerEpoch;
        private final String deleteGuard;

        private ClaimRow(String status, String scheduleFingerprint, String triggerFamily, int incarnation, long ownerEpoch,
                         String deleteGuard) {
            this.status = status;
            this.scheduleFingerprint = scheduleFingerprint;
            this.triggerFamily = triggerFamily;
            this.incarnation = incarnation;
            this.ownerEpoch = ownerEpoch;
            this.deleteGuard = deleteGuard;
        }
    }

    private ClaimRow readClaimRow(Connection connection, String taskName) throws SQLException {

        try (PreparedStatement preparedStatement = connection.prepareStatement(SELECT_CLAIM_FOR_REGISTRATION)) {
            preparedStatement.setString(1, taskName);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (!resultSet.next()) {
                    return null;
                }
                return new ClaimRow(resultSet.getString(1), resultSet.getString(2), resultSet.getString(3),
                        resultSet.getInt(4), resultSet.getLong(5), resultSet.getString(6));
            }
        }
    }

    private void lockTaskRow(Connection connection, String taskName) throws SQLException {

        // the conditional self-lock UPDATE by primary key — a lock attempt only, its count is ignored
        try (PreparedStatement preparedStatement = connection.prepareStatement(LOCK_TASK_ROW)) {
            preparedStatement.setString(1, taskName);
            preparedStatement.executeUpdate();
        }
    }

    /**
     * @return the task row's state, or null when the row is absent (the branch signal of the
     * lock-then-SELECT shape)
     */
    private String readTaskRowState(Connection connection, String taskName) throws SQLException {

        try (PreparedStatement preparedStatement = connection.prepareStatement(SELECT_TASK_ROW)) {
            preparedStatement.setString(1, taskName);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (!resultSet.next()) {
                    return null;
                }
                return resultSet.getString(2);
            }
        }
    }

    private void insertTaskRow(Connection connection, String taskName, CoordinatedTask.States state)
            throws SQLException {

        try (PreparedStatement preparedStatement = connection.prepareStatement(ADD_TASK)) {
            preparedStatement.setString(1, taskName);
            preparedStatement.setString(2, state.name());
            preparedStatement.executeUpdate();
        }
    }

    private Long readPeerLastHeartbeat(Connection connection, String groupId, String nodeId) throws SQLException {

        try (PreparedStatement preparedStatement = connection.prepareStatement(SELECT_PEER_LAST_HEARTBEAT)) {
            preparedStatement.setString(1, groupId);
            preparedStatement.setString(2, nodeId);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (!resultSet.next()) {
                    return null;
                }
                long lastHeartbeat = resultSet.getLong(1);
                return resultSet.wasNull() ? null : lastHeartbeat;
            }
        }
    }

    /**
     * Public in-memory view of a delete-barrier row, status included (Barrier Liveness reads).
     */
    public static final class DeleteBarrierView {

        private final String taskName;
        private final String guardUuid;
        private final String ownerNodeId;
        private final String status;
        private final long deadlineAt;
        private final long updatedAt;

        DeleteBarrierView(String taskName, String guardUuid, String ownerNodeId, String status, long deadlineAt,
                          long updatedAt) {
            this.taskName = taskName;
            this.guardUuid = guardUuid;
            this.ownerNodeId = ownerNodeId;
            this.status = status;
            this.deadlineAt = deadlineAt;
            this.updatedAt = updatedAt;
        }

        public String getTaskName() {
            return taskName;
        }

        public String getGuardUuid() {
            return guardUuid;
        }

        public String getOwnerNodeId() {
            return ownerNodeId;
        }

        public String getStatus() {
            return status;
        }

        public long getDeadlineAt() {
            return deadlineAt;
        }

        public long getUpdatedAt() {
            return updatedAt;
        }
    }

    private static boolean isInFlightBarrierStatus(String status) {

        return BARRIER_STATUS_OPEN.equals(status) || BARRIER_STATUS_FINALIZING.equals(status)
                || BARRIER_STATUS_RETIRING.equals(status);
    }

    private DeleteBarrierView readBarrierView(Connection connection, String taskName, String guardUuid)
            throws SQLException {

        try (PreparedStatement preparedStatement = connection.prepareStatement(SELECT_TASK_DELETE_BARRIER)) {
            preparedStatement.setString(1, taskName);
            preparedStatement.setString(2, guardUuid);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (!resultSet.next()) {
                    return null;
                }
                return mapBarrierView(resultSet);
            }
        }
    }

    private List<DeleteBarrierView> readBarrierViewsByStatus(Connection connection, String status)
            throws SQLException {

        List<DeleteBarrierView> views = new ArrayList<>();
        try (PreparedStatement preparedStatement = connection.prepareStatement(SELECT_OPEN_TASK_DELETE_BARRIERS)) {
            preparedStatement.setString(1, status);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    views.add(mapBarrierView(resultSet));
                }
            }
        }
        return views;
    }

    private DeleteBarrierView mapBarrierView(ResultSet resultSet) throws SQLException {

        return new DeleteBarrierView(resultSet.getString(TASK_NAME), resultSet.getString(GUARD_UUID),
                resultSet.getString(OWNER_NODE_ID), resultSet.getString(TaskQueryHelper.STATUS),
                resultSet.getLong(DEADLINE_AT), resultSet.getLong(UPDATED_AT));
    }

    /**
     * Barrier row read for the classifier and the skip-wait cross-node signal, own connection.
     */
    public DeleteBarrierView getDeleteBarrier(String taskName, String guardUuid) throws TaskCoordinationException {

        try (Connection connection = getConnection()) {
            return readBarrierView(connection, taskName, guardUuid);
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * All barrier rows in the given status, own connection.
     */
    public List<DeleteBarrierView> getDeleteBarriersByStatus(String status) throws TaskCoordinationException {

        try (Connection connection = getConnection()) {
            return readBarrierViewsByStatus(connection, status);
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * EXPECTED node ids of a wave, own connection.
     */
    public List<String> getBarrierExpectedNodes(String taskName, String guardUuid) throws TaskCoordinationException {

        try (Connection connection = getConnection();
                PreparedStatement preparedStatement =
                        connection.prepareStatement(SELECT_TASK_DELETE_BARRIER_EXPECTED_NODES)) {
            preparedStatement.setString(1, taskName);
            preparedStatement.setString(2, guardUuid);
            return queryNodeIds(preparedStatement, "expected nodes of wave [" + guardUuid + "]");
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * ACKed node ids of a wave, own connection.
     */
    public List<String> getBarrierAckNodes(String taskName, String guardUuid) throws TaskCoordinationException {

        try (Connection connection = getConnection();
                PreparedStatement preparedStatement =
                        connection.prepareStatement(SELECT_TASK_DELETE_BARRIER_ACK_NODES)) {
            preparedStatement.setString(1, taskName);
            preparedStatement.setString(2, guardUuid);
            return queryNodeIds(preparedStatement, "acked nodes of wave [" + guardUuid + "]");
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    /**
     * Peer LAST_HEARTBEAT from the cluster heartbeat table, own connection (the classifier's advisory
     * read; the authoritative variant runs on the finalize transaction's connection).
     */
    public Long getPeerLastHeartbeat(String groupId, String nodeId) throws TaskCoordinationException {

        try (Connection connection = getConnection()) {
            return readPeerLastHeartbeat(connection, groupId, nodeId);
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
    }

    private void countCheckedUpdate(Connection connection, String sql, String failureMessage, String... params)
            throws SQLException, TaskCoordinationException {

        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            for (int i = 0; i < params.length; i++) {
                preparedStatement.setString(i + 1, params[i]);
            }
            if (preparedStatement.executeUpdate() != 1) {
                throw new TaskCoordinationException(failureMessage);
            }
        }
    }

    /**
     * Result of the read-then-branch wave creation: the effective guard, and whether this call
     * created the wave or joined an in-flight one.
     */
    public static final class WaveHandle {

        private final String guardUuid;
        private final boolean created;

        private WaveHandle(String guardUuid, boolean created) {
            this.guardUuid = guardUuid;
            this.created = created;
        }

        public String getGuardUuid() {
            return guardUuid;
        }

        public boolean isCreated() {
            return created;
        }

        // Completed-wave handling: the wave's delete already completed cluster-wide — no task row remains, so a
        // late undeploy (a node that was down when the wave finalized) has no store work left.
        static WaveHandle alreadyCompleted() {
            return new WaveHandle(null, false);
        }

        public boolean isAlreadyCompleted() {
            return guardUuid == null;
        }
    }

    /**
     * Hardened wave creation — the required read-then-branch guard stamp, one form for every
     * creation path (leader, worker bootstrap, skip-wait hint): the wave's guard UUID is stamped on
     * the CLAIM row (durable provenance for the finalize), the barrier/expected rows are inserted, and
     * this node's own ack is written — all in one lease-fenced transaction (every ack write opens with
     * the BOOT_ID-conditioned advertisement touch). The task row is untouched — no DELETE_PENDING
     * marking: the old definition may keep running until deletion is proven, and a cancelled wave has
     * nothing to restore. Branches: claim guard NULL -> stamp (the only healthy non-conflict state);
     * non-null guard naming an in-flight OPEN/FINALIZING/RETIRING barrier -> JOIN (ack it, never
     * restamp); non-null guard on an OPEN claim naming no in-flight barrier -> the guard-orphan
     * corruption class -> refuse + readiness (background recovery owns the repair); an
     * undeploy-initiated wave meeting a CLOSED claim under a still-present task row -> abort with the
     * exists+CLOSED readiness alarm. Zero rows on the stamp CAS -> re-read and re-branch.
     */
    public WaveHandle createOrJoinDeleteBarrier(String taskName, String candidateGuardUuid, String ownerNodeId,
                                                List<String> expectedNodeIds, long deadlineAt, long updatedAt)
            throws TaskCoordinationException {

        Set<String> expectedNodes = sanitizeExpectedNodes(expectedNodeIds, ownerNodeId);
        LeaseFence fence = acquireLeaseFence("Delete-barrier creation for task [" + taskName + "]");
        Connection connection = null;
        try {
            connection = getTransactionalConnection();
            long adopted = openWithLeaseFence(connection, fence,
                    "Delete-barrier creation for task [" + taskName + "]");
            for (int attempt = 0; attempt < 3; attempt++) {
                ClaimRow claim = readClaimRow(connection, taskName);
                if (claim == null) {
                    rollbackQuietly(connection);
                    readiness().raise(ANTI_JOIN_INVARIANT_CONDITION,
                            CoordinationReadinessRegistry.ConditionClass.CONTINUOUS,
                            "no claim row exists for task [" + taskName + "] at delete-wave creation — the "
                                    + "creating node's own armed boot pass is the seeding authority");
                    throw new TaskCoordinationException("Refusing the delete wave for task [" + taskName
                            + "]: no claim row exists to bind the wave to.");
                }
                if (claim.deleteGuard == null) {
                    if (!CLAIM_STATUS_OPEN.equals(claim.status)) {
                        // Completed-wave handling: only a still-present task row is an orphan. With no task row the
                        // delete this wave would perform has already completed cluster-wide — a late
                        // undeploy from a node that was down at finalize has nothing left to do.
                        boolean taskRowPresent = readTaskRowView(connection, taskName) != null;
                        rollbackQuietly(connection);
                        if (!taskRowPresent) {
                            clearOrphanTaskRowConditionFor(taskName);
                            return WaveHandle.alreadyCompleted();
                        }
                        readiness().raise(ORPHAN_TASK_ROW_CONDITION,
                                CoordinationReadinessRegistry.ConditionClass.GATE,
                                "task [" + taskName + "] has a live task row over a " + claim.status
                                        + " claim — sanctioned exits: operator reconfigure (reopen) or "
                                        + "task-retire (delete)");
                        throw new TaskCoordinationException("Refusing the undeploy wave for task [" + taskName
                                + "]: the claim row is " + claim.status + " under a still-present task row.");
                    }
                    int stamped;
                    try (PreparedStatement preparedStatement =
                            connection.prepareStatement(STAMP_CLAIM_DELETE_GUARD_OPEN_WHEN_NULL)) {
                        preparedStatement.setString(1, candidateGuardUuid);
                        preparedStatement.setString(2, taskName);
                        preparedStatement.setInt(3, claim.incarnation);
                        stamped = preparedStatement.executeUpdate();
                    }
                    if (stamped != 1) {
                        continue;   // zero rows on the CAS -> re-read and re-branch
                    }
                    try (PreparedStatement insertBarrier = connection.prepareStatement(INSERT_TASK_DELETE_BARRIER)) {
                        insertBarrier.setString(1, taskName);
                        insertBarrier.setString(2, candidateGuardUuid);
                        insertBarrier.setString(3, ownerNodeId);
                        insertBarrier.setString(4, BARRIER_STATUS_OPEN);
                        insertBarrier.setLong(5, deadlineAt);
                        insertBarrier.setLong(6, updatedAt);
                        insertBarrier.executeUpdate();
                    }
                    insertExpectedNodes(connection, taskName, candidateGuardUuid, expectedNodes);
                    upsertBarrierAck(connection, taskName, candidateGuardUuid, ownerNodeId, updatedAt);
                    connection.commit();
                    fence.controller.adoptPublishedTouch(adopted);
                    return new WaveHandle(candidateGuardUuid, true);
                }
                DeleteBarrierView wave = readBarrierView(connection, taskName, claim.deleteGuard);
                if (wave != null && isInFlightBarrierStatus(wave.getStatus())) {
                    // an in-flight wave — join, never restamp
                    upsertBarrierAck(connection, taskName, claim.deleteGuard, ownerNodeId, updatedAt);
                    connection.commit();
                    fence.controller.adoptPublishedTouch(adopted);
                    return new WaveHandle(claim.deleteGuard, false);
                }
                // Completed-wave handling: read before the rollback — the non-OPEN refusal below may only fire
                // over a still-present task row; absent means the delete already completed.
                boolean taskRowPresent = CLAIM_STATUS_OPEN.equals(claim.status)
                        || readTaskRowView(connection, taskName) != null;
                rollbackQuietly(connection);
                if (CLAIM_STATUS_OPEN.equals(claim.status)) {
                    readiness().raise(GUARD_ORPHAN_CONDITION,
                            CoordinationReadinessRegistry.ConditionClass.CONTINUOUS,
                            "the OPEN claim of task [" + taskName + "] carries DELETE_GUARD ["
                                    + claim.deleteGuard + "] naming no in-flight barrier row — background "
                                    + "recovery owns the guard-orphan repair");
                    throw new TaskCoordinationException("Refusing the delete wave for task [" + taskName
                            + "]: the claim's delete guard names no in-flight barrier (guard-orphan).");
                }
                if (!taskRowPresent) {
                    clearOrphanTaskRowConditionFor(taskName);
                    return WaveHandle.alreadyCompleted();
                }
                readiness().raise(ORPHAN_TASK_ROW_CONDITION, CoordinationReadinessRegistry.ConditionClass.GATE,
                        "task [" + taskName + "] has a live task row over a " + claim.status
                                + " claim — sanctioned exits: operator reconfigure (reopen) or task-retire "
                                + "(delete)");
                throw new TaskCoordinationException("Refusing the undeploy wave for task [" + taskName
                        + "]: the claim row is " + claim.status + " under a still-present task row.");
            }
            rollbackQuietly(connection);
            throw new TaskCoordinationException("Could not stamp the delete guard for task [" + taskName
                    + "] after repeated re-branches.");
        } catch (SQLException ex) {
            rollbackQuietly(connection);
            throw new TaskCoordinationException(ERROR_MSG, ex);
        } catch (TaskCoordinationException ex) {
            rollbackQuietly(connection);
            throw ex;
        } finally {
            closeQuietly(connection);
        }
    }

    /**
     * Hardened barrier acknowledgement: the wave is located through the CLAIM row's DELETE_GUARD (the
     * guard authority on the patched path) and the ack write opens with the BOOT_ID-conditioned
     * advertisement touch inside the same transaction — a corpse ack fails closed on the stale boot.
     *
     * @return true when an in-flight wave was found and acked
     */
    /**
     * Membership-rejoin hand-back. becameUnresponsive killed this node's coordinated
     * scheduler and paused every locally running job, but a coordination-DB stall lapses no lease —
     * so the node's RUNNING rows still carry it as destined while no local Quartz edge exists.
     * Drive exactly those rows back to NONE (destination retained because those rows have no local Quartz
     * edge left) so the restarted
     * scheduler pass re-schedules them. Fenced like every hardened write.
     */
    public int handBackNodeRunningTasks(String nodeId) throws TaskCoordinationException {

        LeaseFence fence = acquireLeaseFence("Membership-rejoin hand-back of node [" + nodeId + "]");
        Connection connection = null;
        try {
            connection = getTransactionalConnection();
            long adopted = openWithLeaseFence(connection, fence,
                    "Membership-rejoin hand-back of node [" + nodeId + "]");
            int handedBack;
            try (PreparedStatement handBack = connection.prepareStatement(HAND_BACK_NODE_RUNNING_TASKS)) {
                handBack.setString(1, nodeId);
                handedBack = handBack.executeUpdate();
            }
            connection.commit();
            fence.controller.adoptPublishedTouch(adopted);
            return handedBack;
        } catch (SQLException ex) {
            rollbackQuietly(connection);
            throw new TaskCoordinationException(ERROR_MSG, ex);
        } finally {
            closeQuietly(connection);
        }
    }

    public boolean acknowledgeOpenDeleteBarrierHardened(String taskName, String nodeId, long ackedAt)
            throws TaskCoordinationException {

        LeaseFence fence = acquireLeaseFence("Delete-barrier acknowledgement for task [" + taskName + "]");
        Connection connection = null;
        try {
            connection = getTransactionalConnection();
            long adopted = openWithLeaseFence(connection, fence,
                    "Delete-barrier acknowledgement for task [" + taskName + "]");
            ClaimRow claim = readClaimRow(connection, taskName);
            if (claim == null || claim.deleteGuard == null) {
                rollbackQuietly(connection);
                return false;
            }
            DeleteBarrierView wave = readBarrierView(connection, taskName, claim.deleteGuard);
            if (wave == null || !isInFlightBarrierStatus(wave.getStatus())) {
                rollbackQuietly(connection);
                return false;
            }
            upsertBarrierAck(connection, taskName, claim.deleteGuard, nodeId, ackedAt);
            try (PreparedStatement updateBarrier = connection.prepareStatement(UPDATE_TASK_DELETE_BARRIER_TIMESTAMP)) {
                updateBarrier.setLong(1, ackedAt);
                updateBarrier.setString(2, taskName);
                updateBarrier.setString(3, claim.deleteGuard);
                updateBarrier.executeUpdate();
            }
            connection.commit();
            fence.controller.adoptPublishedTouch(adopted);
            return true;
        } catch (SQLException ex) {
            rollbackQuietly(connection);
            throw new TaskCoordinationException(ERROR_MSG, ex);
        } finally {
            closeQuietly(connection);
        }
    }

    /**
     * Outcome of one guarded finalize attempt.
     */
    public static final class FinalizeReport {

        public enum Outcome { FINALIZED, HOLDOUT, SUPERSEDED, NO_TASK_ROW, WAVE_GONE }

        private final Outcome outcome;
        private final List<String> holdoutNodes;

        private FinalizeReport(Outcome outcome, List<String> holdoutNodes) {
            this.outcome = outcome;
            this.holdoutNodes = holdoutNodes;
        }

        public Outcome getOutcome() {
            return outcome;
        }

        public List<String> getHoldoutNodes() {
            return holdoutNodes;
        }
    }

    /**
     * The single guarded finalize, used identically by the leader path and the cleaner — one transaction
     * on one connection, required statement order: (1) the boot fence (advertisement own-row touch);
     * (2) the task self-lock + SELECT; (3) non-locking reads of the wave, EXPECTED, ACK rows and the
     * per-missing-node peer advertisement/heartbeat, classification computed in Java (now passed in —
     * the app-clock rule); (4) any LIVE holdout (or liveness read failure) rolls back with NOTHING
     * written — the wave stays exactly as it was, OPEN with its collected acks (this replaces the
     * shipped reopen branch and the shipped deadline-override comparison); (5) the claim close CAS,
     * read-then-skip (STATUS='CLOSED' with THIS wave's exact guard skips only the already-satisfied
     * flip; a different guard is never the success branch) — this CAS SUBSUMES the shipped standalone
     * guard-table CAS; (6) the barrier in-flight -> FINALIZING claim tag; (7) the unconditional task
     * row delete (pendingOnly retired); (8) barrier bookkeeping cleanup; (9) COMMIT.
     */
    public FinalizeReport finalizeDeleteBarrierClassified(String taskName, String guardUuid, long now,
                                                          long localWindowMillis) throws TaskCoordinationException {

        LeaseFence fence = acquireLeaseFence("Guarded delete finalize for task [" + taskName + "]");
        Connection connection = null;
        try {
            connection = getTransactionalConnection();
            long adopted = openWithLeaseFence(connection, fence,
                    "Guarded delete finalize for task [" + taskName + "]");
            lockTaskRow(connection, taskName);
            if (readTaskRowState(connection, taskName) == null) {
                rollbackQuietly(connection);
                return new FinalizeReport(FinalizeReport.Outcome.NO_TASK_ROW, Collections.emptyList());
            }
            DeleteBarrierView wave = readBarrierView(connection, taskName, guardUuid);
            if (wave == null || !isInFlightBarrierStatus(wave.getStatus())) {
                rollbackQuietly(connection);
                return new FinalizeReport(FinalizeReport.Outcome.WAVE_GONE, Collections.emptyList());
            }
            // The existing barrier table has no CREATED_AT column; min(UPDATED_AT, DEADLINE_AT)
            // is a durable upper bound of the creation instant — consent may be withheld briefly,
            // never granted wrongly.
            long waveCreatedUpperBound = Math.min(wave.getUpdatedAt(), wave.getDeadlineAt());
            Set<String> missingNodes = new HashSet<>();
            try (PreparedStatement queryExpected =
                    connection.prepareStatement(SELECT_TASK_DELETE_BARRIER_EXPECTED_NODES)) {
                queryExpected.setString(1, taskName);
                queryExpected.setString(2, guardUuid);
                try (ResultSet resultSet = queryExpected.executeQuery()) {
                    while (resultSet.next()) {
                        missingNodes.add(resultSet.getString(NODE_ID));
                    }
                }
            }
            try (PreparedStatement queryAcks = connection.prepareStatement(SELECT_TASK_DELETE_BARRIER_ACK_NODES)) {
                queryAcks.setString(1, taskName);
                queryAcks.setString(2, guardUuid);
                try (ResultSet resultSet = queryAcks.executeQuery()) {
                    while (resultSet.next()) {
                        missingNodes.remove(resultSet.getString(NODE_ID));
                    }
                }
            }
            List<String> holdoutNodes = new ArrayList<>();
            for (String node : missingNodes) {
                BarrierLivenessClassifier.NodeClass nodeClass;
                try {
                    NodeAdvertisement advertisement = readNodeAdvertisement(connection, fence.groupId, node);
                    Long lastHeartbeat = readPeerLastHeartbeat(connection, fence.groupId, node);
                    nodeClass = BarrierLivenessClassifier.classifyNode(lastHeartbeat, advertisement,
                            waveCreatedUpperBound, localWindowMillis, now);
                } catch (Throwable readFailure) {
                    nodeClass = BarrierLivenessClassifier.NodeClass.LIVE;
                }
                if (nodeClass == BarrierLivenessClassifier.NodeClass.LIVE) {
                    holdoutNodes.add(node);
                }
            }
            if (!holdoutNodes.isEmpty()) {
                connection.rollback();
                return new FinalizeReport(FinalizeReport.Outcome.HOLDOUT, holdoutNodes);
            }
            ClaimRow claim = readClaimRow(connection, taskName);
            boolean skipClose = false;
            if (claim != null && CLAIM_STATUS_CLOSED.equals(claim.status)) {
                if (guardUuid.equals(claim.deleteGuard)) {
                    // the read-then-skip rule: the already-satisfied status flip is skipped, audited
                    LOG.info("Guarded finalize for task [" + taskName + "] found the claim already CLOSED under "
                            + "this wave's exact guard [" + guardUuid + "]; skipping the close flip.");
                    skipClose = true;
                } else {
                    connection.rollback();
                    return new FinalizeReport(FinalizeReport.Outcome.SUPERSEDED, Collections.emptyList());
                }
            }
            if (!skipClose) {
                int closed;
                try (PreparedStatement closeClaim = connection.prepareStatement(CLOSE_CLAIM_FOR_GUARD)) {
                    closeClaim.setString(1, taskName);
                    closeClaim.setString(2, guardUuid);
                    closed = closeClaim.executeUpdate();
                }
                if (closed == 0) {
                    // superseded: a registration cancelled the wave or a newer wave owns the guard —
                    // the shipped stale-wave self-clean, kept
                    cleanupBarrierEntries(connection, taskName, guardUuid);
                    connection.commit();
                    fence.controller.adoptPublishedTouch(adopted);
                    return new FinalizeReport(FinalizeReport.Outcome.SUPERSEDED, Collections.emptyList());
                }
            }
            int claimed;
            try (PreparedStatement claimBarrier = connection.prepareStatement(UPDATE_TASK_DELETE_BARRIER_STATUS)) {
                claimBarrier.setString(1, BARRIER_STATUS_FINALIZING);
                claimBarrier.setLong(2, now);
                claimBarrier.setString(3, taskName);
                claimBarrier.setString(4, guardUuid);
                claimBarrier.setString(5, wave.getStatus());
                claimed = claimBarrier.executeUpdate();
            }
            if (claimed == 0) {
                rollbackQuietly(connection);
                throw new TaskCoordinationException("Invariant alarm: the barrier claim of task [" + taskName
                        + "] wave [" + guardUuid + "] was held concurrently under the task lock.");
            }
            int deletedTaskRows;
            try (PreparedStatement deleteTask = connection.prepareStatement(DELETE_TASK)) {
                deleteTask.setString(1, taskName);
                deletedTaskRows = deleteTask.executeUpdate();
            }
            if (deletedTaskRows == 0) {
                rollbackQuietly(connection);
                throw new TaskCoordinationException("Invariant alarm: the task row of [" + taskName
                        + "] vanished under its own lock during the guarded finalize.");
            }
            cleanupBarrierEntries(connection, taskName, guardUuid);
            connection.commit();
            fence.controller.adoptPublishedTouch(adopted);
            LOG.info("Guarded finalize completed for task [" + taskName + "] with guard [" + guardUuid
                    + "]. Task row deleted, claim CLOSED.");
            return new FinalizeReport(FinalizeReport.Outcome.FINALIZED, Collections.emptyList());
        } catch (SQLException ex) {
            rollbackQuietly(connection);
            throw new TaskCoordinationException(ERROR_MSG, ex);
        } catch (TaskCoordinationException ex) {
            rollbackQuietly(connection);
            throw ex;
        } finally {
            closeQuietly(connection);
        }
    }

    /**
     * The cleaner recovery's report: finalized task names, plus whether a LIVE holdout was classified
     * on a wave this node initiated or joined (the skip-wait hint's cross-cycle signal).
     */
    public static final class BarrierRecoveryReport {

        private final List<String> finalizedTasks;
        private final boolean liveHoldoutOnLocalWave;

        private BarrierRecoveryReport(List<String> finalizedTasks, boolean liveHoldoutOnLocalWave) {
            this.finalizedTasks = finalizedTasks;
            this.liveHoldoutOnLocalWave = liveHoldoutOnLocalWave;
        }

        public List<String> getFinalizedTasks() {
            return finalizedTasks;
        }

        public boolean isLiveHoldoutOnLocalWave() {
            return liveHoldoutOnLocalWave;
        }
    }

    /**
     * Hardened cleaner recovery: re-classifies every in-flight wave each cycle through the SAME
     * guarded finalize as the leader path (there is exactly one finalizer — the stock pendingOnly call
     * is gone from this path). A LIVE holdout leaves the wave OPEN, logged once per state change, not
     * per cycle. There are no chained retry waves and no re-answer protocol: the same wave waits until
     * its holdout acks, dies, is boot-replaced, or is cancelled by a same-fingerprint registration. A
     * durably observed FINALIZING row is outside interference — readiness alarm + re-run the guarded
     * finalize (its FINALIZING -> FINALIZING re-claim absorbs it).
     */
    public BarrierRecoveryReport recoverInFlightDeleteBarriersClassified(String localNodeId, long localWindowMillis,
                                                                         long now) throws TaskCoordinationException {

        List<DeleteBarrierView> waves = new ArrayList<>();
        try (Connection connection = getConnection()) {
            waves.addAll(readBarrierViewsByStatus(connection, BARRIER_STATUS_OPEN));
            // recovery finalizes an all-answered RETIRING wave like OPEN; a live holdout leaves it
            // RETIRING until it acks, dies, is boot-replaced, or a dissenting registration refuses it
            waves.addAll(readBarrierViewsByStatus(connection, BARRIER_STATUS_RETIRING));
            List<DeleteBarrierView> durableFinalizing = readBarrierViewsByStatus(connection,
                    BARRIER_STATUS_FINALIZING);
            for (DeleteBarrierView finalizing : durableFinalizing) {
                readiness().raise(DURABLE_FINALIZING_WAVE_CONDITION,
                        CoordinationReadinessRegistry.ConditionClass.CONTINUOUS,
                        "a durably observed FINALIZING wave exists for task [" + finalizing.getTaskName()
                                + "] guard [" + finalizing.getGuardUuid() + "] — no commit path of this code "
                                + "leaves one; outside interference suspected");
            }
            waves.addAll(durableFinalizing);
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
        List<String> finalizedTasks = new ArrayList<>();
        boolean liveHoldoutOnLocalWave = false;
        Map<String, List<String>> holdoutWaves = new TreeMap<>();
        for (DeleteBarrierView wave : waves) {
            FinalizeReport report;
            try {
                report = finalizeDeleteBarrierClassified(wave.getTaskName(), wave.getGuardUuid(), now,
                        localWindowMillis);
            } catch (TaskCoordinationException ex) {
                LOG.error("Recovery finalize failed for task [" + wave.getTaskName() + "] wave ["
                        + wave.getGuardUuid() + "].", ex);
                continue;
            }
            String waveKey = wave.getTaskName() + '\u0000' + wave.getGuardUuid();
            String summary = report.getOutcome()
                    + (report.getHoldoutNodes().isEmpty() ? "" : " holdouts=" + report.getHoldoutNodes());
            String previous = lastRecoveryStateByWave.put(waveKey, summary);
            if (!summary.equals(previous)) {
                LOG.info("Delete-wave recovery state for task [" + wave.getTaskName() + "] guard ["
                        + wave.getGuardUuid() + "]: " + summary);
            }
            if (report.getOutcome() == FinalizeReport.Outcome.FINALIZED) {
                finalizedTasks.add(wave.getTaskName());
            }
            if (report.getOutcome() != FinalizeReport.Outcome.HOLDOUT) {
                lastRecoveryStateByWave.remove(waveKey);
            } else if (localNodeId != null && (localNodeId.equals(wave.getOwnerNodeId())
                    || getBarrierAckNodes(wave.getTaskName(), wave.getGuardUuid()).contains(localNodeId))) {
                liveHoldoutOnLocalWave = true;
                // the continuous wave-holdout condition names task + node for genuine undeploy waves
                // this node initiated or joined, past their deadline with a live non-acking member
                if (BARRIER_STATUS_OPEN.equals(wave.getStatus()) && now >= wave.getDeadlineAt()) {
                    holdoutWaves.put(wave.getTaskName(), report.getHoldoutNodes());
                }
            }
        }
        if (holdoutWaves.isEmpty()) {
            readiness().clear(WAVE_HOLDOUT_CONDITION);
        } else {
            readiness().raise(WAVE_HOLDOUT_CONDITION, CoordinationReadinessRegistry.ConditionClass.CONTINUOUS,
                    "OPEN delete wave(s) past deadline with live non-acking member(s): " + holdoutWaves
                            + " — exit: the holdout acking, dying, being boot-replaced, or a same-fp "
                            + "registration cancelling the wave");
        }
        cleanExpiredRetireRefusals(now);
        return new BarrierRecoveryReport(finalizedTasks, liveHoldoutOnLocalWave);
    }

    /**
     * RETIRE_REFUSED rows carry the 7-day terminal retention (children cleaned with them). After
     * retention cleanup a repeated operationId finds no wave and runs a fresh wave — safe, the barrier
     * re-proves absence.
     */
    private void cleanExpiredRetireRefusals(long now) {

        List<DeleteBarrierView> refused;
        try (Connection connection = getConnection()) {
            refused = readBarrierViewsByStatus(connection, BARRIER_STATUS_RETIRE_REFUSED);
        } catch (SQLException ex) {
            LOG.warn("The RETIRE_REFUSED retention read failed; retrying next cycle.", ex);
            return;
        }
        for (DeleteBarrierView wave : refused) {
            if (now - wave.getUpdatedAt() < RETIRE_REFUSED_RETENTION_MILLIS) {
                continue;
            }
            Connection connection = null;
            try {
                connection = getTransactionalConnection();
                lockTaskRow(connection, wave.getTaskName());
                cleanupBarrierEntries(connection, wave.getTaskName(), wave.getGuardUuid());
                connection.commit();
                LOG.info("Retention-cleaned RETIRE_REFUSED wave of task [" + wave.getTaskName() + "] guard ["
                        + wave.getGuardUuid() + "] after the 7-day terminal retention.");
            } catch (SQLException ex) {
                rollbackQuietly(connection);
                LOG.warn("The RETIRE_REFUSED retention cleanup failed for task [" + wave.getTaskName()
                        + "]; retrying next cycle.", ex);
            } finally {
                closeQuietly(connection);
            }
        }
    }

    /**
     * In-memory view of the task row (state + destination) as read under the operator transactions.
     */
    private static final class TaskRowView {

        private final String state;
        private final String destinedNodeId;

        private TaskRowView(String state, String destinedNodeId) {
            this.state = state;
            this.destinedNodeId = destinedNodeId;
        }
    }

    private TaskRowView readTaskRowView(Connection connection, String taskName) throws SQLException {

        try (PreparedStatement preparedStatement = connection.prepareStatement(SELECT_TASK_ROW)) {
            preparedStatement.setString(1, taskName);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (!resultSet.next()) {
                    return null;
                }
                return new TaskRowView(resultSet.getString(2), resultSet.getString(3));
            }
        }
    }

    /**
     * The state named by the refusal prose — the tuple/guard/incarnation actually read.
     */
    private Map<String, Object> observedOf(TaskRowView row, ClaimRow claim, String waveStatus) {

        Map<String, Object> observed = new LinkedHashMap<>();
        observed.put("taskRowPresent", row != null);
        if (row != null) {
            observed.put("taskState", row.state);
            observed.put("destinedNodeId", row.destinedNodeId);
        }
        if (claim != null) {
            observed.put("claimStatus", claim.status);
            observed.put("scheduleFp", claim.scheduleFingerprint);
            observed.put("triggerFamily", claim.triggerFamily);
            observed.put("incarnation", claim.incarnation);
            observed.put("deleteGuard", claim.deleteGuard);
        }
        if (waveStatus != null) {
            observed.put("waveStatus", waveStatus);
        }
        return observed;
    }

    /**
     * Outcome of the operator reconfigure — the generation CHANGE.
     */
    public static final class ReconfigureResult {

        public enum Outcome {
            RECONFIGURED, ALREADY_APPLIED, REOPENED, NO_CHANGE, STATE_CONFLICT, TRIGGER_FAMILY_IMMUTABLE,
            RETIRE_IN_PROGRESS, GUARD_ORPHAN, UNKNOWN_TASK
        }

        private final Outcome outcome;
        private final String oldFp;
        private final String newFp;
        private final int incarnation;
        private final Map<String, Object> observed;
        private final String detail;

        private ReconfigureResult(Outcome outcome, String oldFp, String newFp, int incarnation,
                                  Map<String, Object> observed, String detail) {
            this.outcome = outcome;
            this.oldFp = oldFp;
            this.newFp = newFp;
            this.incarnation = incarnation;
            this.observed = observed;
            this.detail = detail;
        }

        public Outcome getOutcome() {
            return outcome;
        }

        public String getOldFp() {
            return oldFp;
        }

        public String getNewFp() {
            return newFp;
        }

        public int getIncarnation() {
            return incarnation;
        }

        public Map<String, Object> getObserved() {
            return observed;
        }

        public String getDetail() {
            return detail;
        }
    }

    /**
     * Outcome of the operator task-retire — the operator-initiated delete wave.
     */
    public static final class RetireResult {

        public enum Outcome {
            RETIRING_CREATED, RETIRING_JOINED, RETIRED, RETIRE_REFUSED_REPORTED, STATE_CONFLICT, GUARD_ORPHAN,
            UNKNOWN_TASK
        }

        private final Outcome outcome;
        private final Map<String, Object> observed;
        private final String detail;

        private RetireResult(Outcome outcome, Map<String, Object> observed, String detail) {
            this.outcome = outcome;
            this.observed = observed;
            this.detail = detail;
        }

        public Outcome getOutcome() {
            return outcome;
        }

        public Map<String, Object> getObserved() {
            return observed;
        }

        public String getDetail() {
            return detail;
        }
    }

    /**
     * One RETIRING wave this node has not answered yet.
     */
    public static final class RetiringWaveRef {

        private final String taskName;
        private final String guardUuid;

        private RetiringWaveRef(String taskName, String guardUuid) {
            this.taskName = taskName;
            this.guardUuid = guardUuid;
        }

        public String getTaskName() {
            return taskName;
        }

        public String getGuardUuid() {
            return guardUuid;
        }
    }

    /**
     * The operator reconfigure — one transaction, global lock order: advertisement touch -> task-row
     * self-lock -> guard read -> prove -> branch (the read-then-prove shape, never a single OR/EXISTS
     * statement): guard NULL -> plain close (the CAS over the full expected tuple) -> reopen (checked
     * nextIncarnation, the computed target fingerprint, guard NULL; FIRE_KEY PRESERVED for a recurring
     * task, -1 only for a deliberate one-shot reopen) -> unassign (transition (iii); an already-NULL
     * observed destination is skipped as already-satisfied) -> commit, auditing the old and new tuple.
     * OPEN/FINALIZING or RETIRING guard -> conflict, refuse; any other observed guard on an OPEN claim
     * is the guard-orphan corruption class -> refuse, naming background recovery. The exists+CLOSED
     * repair skips the close and runs the guarded reopen alone — the sanctioned exit for a
     * legitimately re-deployed row the rolling window left over a tombstone, EXEMPT from the same-fp
     * no-change refusal. Any zero/unexpected row count rolls the whole transaction back.
     */
    public ReconfigureResult reconfigureTask(String taskName, int expectedIncarnation, String expectedScheduleFingerprint,
                                             String targetScheduleFingerprint, String targetFamily)
            throws TaskCoordinationException {

        int nextIncarnation;
        try {
            nextIncarnation = Math.addExact(expectedIncarnation, 1);
        } catch (ArithmeticException overflow) {
            readiness().raise(ANTI_JOIN_INVARIANT_CONDITION,
                    CoordinationReadinessRegistry.ConditionClass.CONTINUOUS,
                    "the INCARNATION of task [" + taskName + "] would overflow on reconfigure — refusing to "
                            + "wrap a fencing token");
            throw new TaskCoordinationException("INCARNATION overflow for task [" + taskName
                    + "] — refusing the reconfigure instead of wrapping a fencing token.", overflow);
        }
        LeaseFence fence = acquireLeaseFence("Reconfigure of task [" + taskName + "]");
        Connection connection = null;
        try {
            connection = getTransactionalConnection();
            long adopted = openWithLeaseFence(connection, fence, "Reconfigure of task [" + taskName + "]");
            lockTaskRow(connection, taskName);
            TaskRowView row = readTaskRowView(connection, taskName);
            ClaimRow claim = readClaimRow(connection, taskName);
            if (claim == null) {
                rollbackQuietly(connection);
                if (row == null) {
                    return new ReconfigureResult(ReconfigureResult.Outcome.UNKNOWN_TASK, null, null, 0, null,
                            "no task row and no claim row exist under the name [" + taskName + "]");
                }
                readiness().raise(ANTI_JOIN_INVARIANT_CONDITION,
                        CoordinationReadinessRegistry.ConditionClass.CONTINUOUS,
                        "task [" + taskName + "] has a task row but no claim row at the operator reconfigure");
                return new ReconfigureResult(ReconfigureResult.Outcome.STATE_CONFLICT, null, null, 0,
                        observedOf(row, null, null), "a task row exists with no claim row (anti-join invariant)");
            }
            String waveStatus = null;
            if (claim.deleteGuard != null) {
                DeleteBarrierView wave = readBarrierView(connection, taskName, claim.deleteGuard);
                waveStatus = wave == null ? null : wave.getStatus();
                if (wave != null && BARRIER_STATUS_RETIRING.equals(wave.getStatus())) {
                    rollbackQuietly(connection);
                    return new ReconfigureResult(ReconfigureResult.Outcome.RETIRE_IN_PROGRESS, null, null, 0,
                            observedOf(row, claim, waveStatus), "an in-flight retire wave owns the task");
                }
                if (wave != null && isInFlightBarrierStatus(wave.getStatus())) {
                    rollbackQuietly(connection);
                    return new ReconfigureResult(ReconfigureResult.Outcome.STATE_CONFLICT, null, null, 0,
                            observedOf(row, claim, waveStatus), "a live undeploy wave owns the task");
                }
                if (CLAIM_STATUS_OPEN.equals(claim.status)) {
                    rollbackQuietly(connection);
                    readiness().raise(GUARD_ORPHAN_CONDITION,
                            CoordinationReadinessRegistry.ConditionClass.CONTINUOUS,
                            "the OPEN claim of task [" + taskName + "] carries DELETE_GUARD ["
                                    + claim.deleteGuard + "] naming no in-flight barrier row — background "
                                    + "recovery owns the guard-orphan repair");
                    return new ReconfigureResult(ReconfigureResult.Outcome.GUARD_ORPHAN, null, null, 0,
                            observedOf(row, claim, waveStatus),
                            "the OPEN claim's guard names no in-flight barrier — background recovery's "
                                    + "audited transaction is the one legal repair");
                }
                // a CLOSED claim's row-less guard is history — the CLOSED-repair branch proceeds
            }
            if (claim.triggerFamily != null && !claim.triggerFamily.equals(targetFamily)) {
                rollbackQuietly(connection);
                return new ReconfigureResult(ReconfigureResult.Outcome.TRIGGER_FAMILY_IMMUTABLE, null, null, 0,
                        observedOf(row, claim, waveStatus), "TRIGGER_FAMILY is immutable — deploy the new "
                        + "definition under a new task name");
            }
            if (CLAIM_STATUS_CLOSED.equals(claim.status)) {
                if (row == null || claim.incarnation != expectedIncarnation
                        || !Objects.equals(claim.scheduleFingerprint, expectedScheduleFingerprint)) {
                    rollbackQuietly(connection);
                    return new ReconfigureResult(ReconfigureResult.Outcome.STATE_CONFLICT, null, null, 0,
                            observedOf(row, claim, waveStatus),
                            "the CLOSED claim does not match the expected tuple under a still-present row");
                }
                runOperatorReopen(connection, taskName, claim, nextIncarnation, targetScheduleFingerprint, targetFamily);
                runOperatorUnassign(connection, taskName, row.destinedNodeId, nextIncarnation, claim.ownerEpoch);
                connection.commit();
                fence.controller.adoptPublishedTouch(adopted);
                LOG.info("Reconfigure REOPENED task [" + taskName + "]: (CLOSED, incarnation ["
                        + expectedIncarnation + "], fp [" + claim.scheduleFingerprint + "]) -> (OPEN, incarnation ["
                        + nextIncarnation + "], fp [" + targetScheduleFingerprint + "]).");
                clearOrphanTaskRowConditionFor(taskName);
                return new ReconfigureResult(ReconfigureResult.Outcome.REOPENED, claim.scheduleFingerprint, targetScheduleFingerprint,
                        nextIncarnation, null, null);
            }
            // OPEN, guard NULL
            if (row == null) {
                rollbackQuietly(connection);
                return new ReconfigureResult(ReconfigureResult.Outcome.STATE_CONFLICT, null, null, 0,
                        observedOf(null, claim, waveStatus), "the claim is OPEN but no task row exists");
            }
            if (claim.incarnation == nextIncarnation && targetScheduleFingerprint.equals(claim.scheduleFingerprint)) {
                rollbackQuietly(connection);
                return new ReconfigureResult(ReconfigureResult.Outcome.ALREADY_APPLIED, null, null,
                        nextIncarnation, null, null);
            }
            if (claim.incarnation != expectedIncarnation || !Objects.equals(claim.scheduleFingerprint, expectedScheduleFingerprint)) {
                rollbackQuietly(connection);
                return new ReconfigureResult(ReconfigureResult.Outcome.STATE_CONFLICT, null, null, 0,
                        observedOf(row, claim, waveStatus), "the OPEN claim does not match the expected tuple");
            }
            if (expectedScheduleFingerprint.equals(targetScheduleFingerprint)) {
                rollbackQuietly(connection);
                LOG.info("Reconfigure of task [" + taskName + "] refused as an audited no-op: the expected "
                        + "fingerprint equals the computed target on the OPEN row — nothing to reconfigure.");
                return new ReconfigureResult(ReconfigureResult.Outcome.NO_CHANGE, null, null, 0,
                        observedOf(row, claim, waveStatus), "the expected fingerprint equals the computed "
                        + "target on the OPEN row — nothing to reconfigure");
            }
            int closed;
            try (PreparedStatement close = connection.prepareStatement(CLOSE_CLAIM_FOR_RECONFIGURE)) {
                close.setString(1, taskName);
                close.setInt(2, expectedIncarnation);
                close.setString(3, expectedScheduleFingerprint);
                closed = close.executeUpdate();
            }
            if (closed != 1) {
                rollbackQuietly(connection);
                throw new TaskCoordinationException("The reconfigure close CAS of task [" + taskName
                        + "] hit an unexpected row count under the task lock; the transaction was rolled back.");
            }
            runOperatorReopen(connection, taskName, claim, nextIncarnation, targetScheduleFingerprint, targetFamily);
            runOperatorUnassign(connection, taskName, row.destinedNodeId, nextIncarnation, claim.ownerEpoch);
            connection.commit();
            fence.controller.adoptPublishedTouch(adopted);
            LOG.info("Reconfigured task [" + taskName + "]: (incarnation [" + expectedIncarnation + "], fp ["
                    + expectedScheduleFingerprint + "]) -> (incarnation [" + nextIncarnation + "], fp [" + targetScheduleFingerprint + "]).");
            return new ReconfigureResult(ReconfigureResult.Outcome.RECONFIGURED, expectedScheduleFingerprint, targetScheduleFingerprint,
                    nextIncarnation, null, null);
        } catch (SQLException ex) {
            rollbackQuietly(connection);
            throw new TaskCoordinationException(ERROR_MSG, ex);
        } catch (TaskCoordinationException ex) {
            rollbackQuietly(connection);
            throw ex;
        } finally {
            closeQuietly(connection);
        }
    }

    /**
     * The reopen half of the operator operations: checked nextIncarnation, the computed target
     * fingerprint, guard NULL. FIRE_KEY is PRESERVED for any recurring definition (the absolute-key
     * rule); the only -1 reopen is the deliberate one-shot reopen, whose refire IS the operator intent.
     */
    private void runOperatorReopen(Connection connection, String taskName, ClaimRow claim, int nextIncarnation,
                                   String targetScheduleFingerprint, String targetFamily)
            throws SQLException, TaskCoordinationException {

        boolean recurring = TRIGGER_FAMILY_RECURRING.equals(targetFamily);
        String reopenSql = recurring
                ? (claim.triggerFamily == null ? REOPEN_CLAIM_RECURRING_BIND_FAMILY
                        : REOPEN_CLAIM_RECURRING_IF_FAMILY_MATCH)
                : (claim.triggerFamily == null ? REOPEN_CLAIM_ONESHOT_BIND_FAMILY
                        : REOPEN_CLAIM_ONESHOT_IF_FAMILY_MATCH);
        int reopened;
        try (PreparedStatement reopen = connection.prepareStatement(reopenSql)) {
            int index = 1;
            reopen.setInt(index++, nextIncarnation);
            reopen.setLong(index++, claim.ownerEpoch);
            reopen.setString(index++, targetScheduleFingerprint);
            if (claim.triggerFamily == null) {
                reopen.setString(index++, targetFamily);
            }
            reopen.setString(index++, taskName);
            if (claim.triggerFamily != null) {
                reopen.setString(index++, claim.triggerFamily);
            }
            reopen.setInt(index, claim.incarnation);
            reopened = reopen.executeUpdate();
        }
        if (reopened != 1) {
            rollbackQuietly(connection);
            throw new TaskCoordinationException("The operator reopen of task [" + taskName
                    + "] hit an unexpected row count; the transaction was rolled back.");
        }
    }

    /**
     * The unassignment (transition (iii)) inside the operator transaction — CLAIM was already acquired
     * at the close, so this is a lock re-use; an already-NULL observed destination is skipped as
     * already-satisfied, and a zero-row task write is the stale snapshot that skips the claim entirely.
     */
    private void runOperatorUnassign(Connection connection, String taskName, String observedDestined,
                                     int nextIncarnation, long ownerEpoch)
            throws SQLException, TaskCoordinationException {

        if (observedDestined == null) {
            return;
        }
        int taskRows;
        try (PreparedStatement unassign = connection.prepareStatement(UNASSIGN_TASK_IF_DESTINED)) {
            unassign.setString(1, taskName);
            unassign.setString(2, observedDestined);
            taskRows = unassign.executeUpdate();
        }
        if (taskRows == 0) {
            return;
        }
        long nextEpoch;
        try {
            nextEpoch = Math.addExact(ownerEpoch, 1L);
        } catch (ArithmeticException ex) {
            rollbackQuietly(connection);
            throw new TaskCoordinationException("OWNER_EPOCH overflow for task [" + taskName
                    + "] — refusing the operator unassignment instead of wrapping a fencing token.", ex);
        }
        int claimRows;
        try (PreparedStatement unassignClaim = connection.prepareStatement(UNASSIGN_CLAIM_OWNER)) {
            unassignClaim.setLong(1, nextEpoch);
            unassignClaim.setString(2, taskName);
            unassignClaim.setInt(3, nextIncarnation);
            unassignClaim.setLong(4, ownerEpoch);
            claimRows = unassignClaim.executeUpdate();
        }
        if (claimRows != 1) {
            rollbackQuietly(connection);
            throw new TaskCoordinationException("The operator unassignment claim CAS of task [" + taskName
                    + "] hit an unexpected row count; the transaction was rolled back.");
        }
    }

    /**
     * The operator task-retire — an operator-initiated delete wave, born STATUS='RETIRING' with the
     * caller-stable operationId as the wave's durable guard. Guard stamped via the normal
     * read-then-branch (one HTTP meaning per (operationId, durable state)); a CLOSED claim under a
     * still-present row stamps through the CLOSED variants (c)/(d) — the universal invariant. The
     * creator writes NO ack: every live patched node, this one included, answers through the RETIRING
     * responder from its local deployed view.
     */
    public RetireResult retireTask(String taskName, int expectedIncarnation, String operationId,
                                   List<String> expectedNodeIds, long deadlineAt, long now)
            throws TaskCoordinationException {

        LeaseFence fence = acquireLeaseFence("Task retire of [" + taskName + "]");
        Set<String> expectedNodes = sanitizeExpectedNodes(expectedNodeIds, fence.nodeId);
        Connection connection = null;
        try {
            connection = getTransactionalConnection();
            long adopted = openWithLeaseFence(connection, fence, "Task retire of [" + taskName + "]");
            for (int attempt = 0; attempt < 3; attempt++) {
                lockTaskRow(connection, taskName);
                TaskRowView row = readTaskRowView(connection, taskName);
                ClaimRow claim = readClaimRow(connection, taskName);
                if (row == null) {
                    rollbackQuietly(connection);
                    if (claim == null) {
                        return new RetireResult(RetireResult.Outcome.UNKNOWN_TASK, null,
                                "no task row and no claim row exist under the name [" + taskName + "]");
                    }
                    if (CLAIM_STATUS_CLOSED.equals(claim.status) && claim.incarnation == expectedIncarnation
                            && operationId.equals(claim.deleteGuard)) {
                        // the lost-response successor — the tombstone retains the historical guard, so
                        // guard = operationId is the positive identification
                        clearOrphanTaskRowConditionFor(taskName);
                        return new RetireResult(RetireResult.Outcome.RETIRED, null, null);
                    }
                    if (CLAIM_STATUS_OPEN.equals(claim.status)) {
                        LOG.error("Task retire of [" + taskName + "] found the task row absent under an OPEN "
                                + "claim — an invariant conflict, never blind success.");
                    }
                    return new RetireResult(RetireResult.Outcome.STATE_CONFLICT, observedOf(null, claim, null),
                            "the task row is absent and the claim does not prove this operation completed");
                }
                if (claim == null) {
                    rollbackQuietly(connection);
                    readiness().raise(ANTI_JOIN_INVARIANT_CONDITION,
                            CoordinationReadinessRegistry.ConditionClass.CONTINUOUS,
                            "task [" + taskName + "] has a task row but no claim row at the operator retire");
                    return new RetireResult(RetireResult.Outcome.STATE_CONFLICT, observedOf(row, null, null),
                            "a task row exists with no claim row (anti-join invariant)");
                }
                DeleteBarrierView myWave = readBarrierView(connection, taskName, operationId);
                if (myWave != null) {
                    rollbackQuietly(connection);
                    if (BARRIER_STATUS_RETIRING.equals(myWave.getStatus())) {
                        if (operationId.equals(claim.deleteGuard) && claim.incarnation == expectedIncarnation) {
                            // the same durable operation: create and restamp nothing, join it
                            return new RetireResult(RetireResult.Outcome.RETIRING_JOINED, null, null);
                        }
                        return new RetireResult(RetireResult.Outcome.STATE_CONFLICT,
                                observedOf(row, claim, myWave.getStatus()),
                                "a RETIRING wave exists under this operation id with a different claim tuple");
                    }
                    if (BARRIER_STATUS_RETIRE_REFUSED.equals(myWave.getStatus())) {
                        // my operation's terminal outcome — report the refusal, create nothing
                        return new RetireResult(RetireResult.Outcome.RETIRE_REFUSED_REPORTED, null, null);
                    }
                    return new RetireResult(RetireResult.Outcome.STATE_CONFLICT,
                            observedOf(row, claim, myWave.getStatus()),
                            "a non-retire wave exists under this operation id");
                }
                if (claim.incarnation != expectedIncarnation) {
                    rollbackQuietly(connection);
                    return new RetireResult(RetireResult.Outcome.STATE_CONFLICT, observedOf(row, claim, null),
                            "the claim incarnation does not match the expected incarnation");
                }
                String stampSql;
                String observedGuard = claim.deleteGuard;
                if (observedGuard == null) {
                    stampSql = CLAIM_STATUS_OPEN.equals(claim.status) ? STAMP_CLAIM_DELETE_GUARD_OPEN_WHEN_NULL
                            : STAMP_CLAIM_DELETE_GUARD_CLOSED_WHEN_NULL;
                } else {
                    DeleteBarrierView bound = readBarrierView(connection, taskName, observedGuard);
                    if (bound != null && BARRIER_STATUS_RETIRING.equals(bound.getStatus())) {
                        rollbackQuietly(connection);
                        return new RetireResult(RetireResult.Outcome.STATE_CONFLICT,
                                observedOf(row, claim, bound.getStatus()),
                                "another operator's retire owns the task");
                    }
                    if (bound != null && isInFlightBarrierStatus(bound.getStatus())) {
                        rollbackQuietly(connection);
                        return new RetireResult(RetireResult.Outcome.STATE_CONFLICT,
                                observedOf(row, claim, bound.getStatus()),
                                "a live undeploy wave owns the task");
                    }
                    if (CLAIM_STATUS_OPEN.equals(claim.status)) {
                        rollbackQuietly(connection);
                        readiness().raise(GUARD_ORPHAN_CONDITION,
                                CoordinationReadinessRegistry.ConditionClass.CONTINUOUS,
                                "the OPEN claim of task [" + taskName + "] carries DELETE_GUARD ["
                                        + observedGuard + "] naming no in-flight barrier row — background "
                                        + "recovery owns the guard-orphan repair");
                        return new RetireResult(RetireResult.Outcome.GUARD_ORPHAN,
                                observedOf(row, claim, bound == null ? null : bound.getStatus()),
                                "the OPEN claim's guard names no in-flight barrier — background recovery's "
                                        + "audited transaction is the one legal repair");
                    }
                    // a historical CLOSED guard is replaced only after the read classifies it terminal
                    stampSql = STAMP_CLAIM_DELETE_GUARD_CLOSED_IF_MATCH;
                }
                int stamped;
                try (PreparedStatement stamp = connection.prepareStatement(stampSql)) {
                    stamp.setString(1, operationId);
                    stamp.setString(2, taskName);
                    stamp.setInt(3, expectedIncarnation);
                    if (observedGuard != null) {
                        stamp.setString(4, observedGuard);
                    }
                    stamped = stamp.executeUpdate();
                }
                if (stamped != 1) {
                    continue;   // zero rows on the CAS -> re-read and re-branch
                }
                try (PreparedStatement insertBarrier = connection.prepareStatement(INSERT_TASK_DELETE_BARRIER)) {
                    insertBarrier.setString(1, taskName);
                    insertBarrier.setString(2, operationId);
                    insertBarrier.setString(3, fence.nodeId);
                    insertBarrier.setString(4, BARRIER_STATUS_RETIRING);
                    insertBarrier.setLong(5, deadlineAt);
                    insertBarrier.setLong(6, now);
                    insertBarrier.executeUpdate();
                }
                insertExpectedNodes(connection, taskName, operationId, expectedNodes);
                connection.commit();
                fence.controller.adoptPublishedTouch(adopted);
                LOG.info("Node [" + fence.nodeId + "] created RETIRING wave for task [" + taskName
                        + "] with operation id [" + operationId + "], expecting nodes " + expectedNodes + ".");
                return new RetireResult(RetireResult.Outcome.RETIRING_CREATED, null, null);
            }
            rollbackQuietly(connection);
            throw new TaskCoordinationException("Could not stamp the retire guard for task [" + taskName
                    + "] after repeated re-branches.");
        } catch (SQLException ex) {
            rollbackQuietly(connection);
            throw new TaskCoordinationException(ERROR_MSG, ex);
        } catch (TaskCoordinationException ex) {
            rollbackQuietly(connection);
            throw ex;
        } finally {
            closeQuietly(connection);
        }
    }

    /**
     * The RETIRING responder's one indexed read: in-flight RETIRING waves where the node is in
     * EXPECTED and has no ACK row. Empty means nothing to answer.
     */
    public List<RetiringWaveRef> getUnansweredRetiringWaves(String nodeId) throws TaskCoordinationException {

        List<RetiringWaveRef> waves = new ArrayList<>();
        try (Connection connection = getConnection();
                PreparedStatement preparedStatement =
                        connection.prepareStatement(RETRIEVE_UNANSWERED_RETIRING_WAVES)) {
            preparedStatement.setString(1, nodeId);
            preparedStatement.setString(2, nodeId);
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    waves.add(new RetiringWaveRef(resultSet.getString(1), resultSet.getString(2)));
                }
            }
        } catch (SQLException ex) {
            throw new TaskCoordinationException(ERROR_MSG, ex);
        }
        return waves;
    }

    /**
     * The responder's consent: this node's ack, guard-scoped to the wave read in step 1, via the
     * boot-fenced ack transaction — a woken corpse cannot consent. The ack keeps its truthful meaning:
     * this node holds nothing under that name.
     *
     * @return true when the wave was still RETIRING and the ack was written
     */
    public boolean ackRetiringWave(String taskName, String guardUuid, String nodeId, long ackedAt)
            throws TaskCoordinationException {

        LeaseFence fence = acquireLeaseFence("Retire-wave acknowledgement for task [" + taskName + "]");
        Connection connection = null;
        try {
            connection = getTransactionalConnection();
            long adopted = openWithLeaseFence(connection, fence,
                    "Retire-wave acknowledgement for task [" + taskName + "]");
            DeleteBarrierView wave = readBarrierView(connection, taskName, guardUuid);
            if (wave == null || !BARRIER_STATUS_RETIRING.equals(wave.getStatus())) {
                rollbackQuietly(connection);
                return false;
            }
            upsertBarrierAck(connection, taskName, guardUuid, nodeId, ackedAt);
            try (PreparedStatement updateBarrier = connection.prepareStatement(UPDATE_TASK_DELETE_BARRIER_TIMESTAMP)) {
                updateBarrier.setLong(1, ackedAt);
                updateBarrier.setString(2, taskName);
                updateBarrier.setString(3, guardUuid);
                updateBarrier.executeUpdate();
            }
            connection.commit();
            fence.controller.adoptPublishedTouch(adopted);
            return true;
        } catch (SQLException ex) {
            rollbackQuietly(connection);
            throw new TaskCoordinationException(ERROR_MSG, ex);
        } finally {
            closeQuietly(connection);
        }
    }

    /**
     * The responder's durable dissent — the SAME task-locked transaction as the registration presence
     * veto: RETIRING -> RETIRE_REFUSED + the claim-guard clear, count-checked. A 0-row flip against an
     * already-RETIRE_REFUSED (or gone) wave is skipped — idempotent.
     *
     * @return true when this call recorded the refusal
     */
    public boolean refuseRetiringWave(String taskName, String guardUuid) throws TaskCoordinationException {

        LeaseFence fence = acquireLeaseFence("Retire-wave presence veto for task [" + taskName + "]");
        Connection connection = null;
        try {
            connection = getTransactionalConnection();
            long adopted = openWithLeaseFence(connection, fence,
                    "Retire-wave presence veto for task [" + taskName + "]");
            lockTaskRow(connection, taskName);
            ClaimRow claim = readClaimRow(connection, taskName);
            int refused = flipRetiringToRefused(connection, taskName, guardUuid);
            if (refused != 1) {
                rollbackQuietly(connection);
                return false;
            }
            if (claim == null || !guardUuid.equals(claim.deleteGuard)) {
                rollbackQuietly(connection);
                throw new TaskCoordinationException("The retire presence veto of task [" + taskName
                        + "] found a RETIRING wave whose guard is not bound on the claim; the transaction "
                        + "was rolled back.");
            }
            clearClaimGuardStatusAware(connection, taskName, claim.status, guardUuid);
            connection.commit();
            fence.controller.adoptPublishedTouch(adopted);
            return true;
        } catch (SQLException ex) {
            rollbackQuietly(connection);
            throw new TaskCoordinationException(ERROR_MSG, ex);
        } catch (TaskCoordinationException ex) {
            rollbackQuietly(connection);
            throw ex;
        } finally {
            closeQuietly(connection);
        }
    }

    /**
     * The RETIRING -> RETIRE_REFUSED status flip, shared by the registration presence veto and the
     * RETIRING responder's dissent.
     */
    private int flipRetiringToRefused(Connection connection, String taskName, String guardUuid)
            throws SQLException {

        try (PreparedStatement flip = connection.prepareStatement(UPDATE_TASK_DELETE_BARRIER_STATUS)) {
            flip.setString(1, BARRIER_STATUS_RETIRE_REFUSED);
            flip.setLong(2, System.currentTimeMillis());
            flip.setString(3, taskName);
            flip.setString(4, guardUuid);
            flip.setString(5, BARRIER_STATUS_RETIRING);
            return flip.executeUpdate();
        }
    }

    /**
     * The single registration doorway — the registration transaction matrix. One transaction, one
     * connection, task row first, under the global ADVERTISEMENT -> TASK -> CLAIM -> BARRIER lock
     * order. The canonical fingerprint and trigger family are computed from the local definition by
     * the CALLER and carried into the transaction — never re-derived inside. The wave-interaction rule
     * (the linearization's second half) branches on the claim's DELETE_GUARD and the exact barrier
     * BEFORE any local scheduling: same-fingerprint cancels the wave in this transaction (the cancel
     * IS the write finalize observes); same-fingerprint against RETIRING records the durable
     * RETIRE_REFUSED presence veto; a different fingerprint parks + consents (acks the wave — this
     * node holds nothing of the old definition). A unique-specific error on the task insert rolls the
     * entire transaction back and retries from step 1; a serialization failure (this transaction was
     * rolled back as the database's deadlock victim) on ANY statement does the same; every other
     * error is fatal.
     */
    public RegistrationResult addTaskIfNotExist(String taskName, CoordinatedTask.States initialState,
                                                String scheduleFingerprint, String triggerFamily, RegistrationPhase phase)
            throws TaskCoordinationException {

        TaskCoordinationException lastRaceLoser = null;
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                return registerThroughMatrix(taskName, initialState, scheduleFingerprint, triggerFamily, phase);
            } catch (RegistrationRetry retry) {
                if (retry.serializationFailure) {
                    lastRaceLoser = new TaskCoordinationException("Registration of task [" + taskName
                            + "] was rolled back by the database as a serialization (deadlock) victim on every "
                            + "attempt.", retry.cause);
                    LOG.warn("Registration of task [" + taskName + "] was rolled back by the database as a "
                            + "serialization (deadlock) victim on attempt " + (attempt + 1)
                            + " of 3; the doorway transaction retries from step 1. Cause: "
                            + retry.cause.getMessage());
                    if (attempt < 2) {
                        // the colliding writer (typically the leader's assignment cycle) commits within
                        // milliseconds; a short jittered pause keeps the retry out of the same window
                        sleepBeforeSerializationRetry();
                    }
                } else {
                    lastRaceLoser = new TaskCoordinationException("Concurrent registration won the insert race for "
                            + "task [" + taskName + "].", retry.cause);
                }
            }
        }
        throw lastRaceLoser;
    }

    private static void sleepBeforeSerializationRetry() {
        try {
            Thread.sleep(50L + ThreadLocalRandom.current().nextLong(150L));
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Internal signal: roll back the entire doorway transaction and retry from step 1 — either a
     * concurrent registration won the task-row insert race, or the database rolled this transaction
     * back as a serialization (deadlock) victim.
     */
    private static final class RegistrationRetry extends Exception {

        private final SQLException cause;
        private final boolean serializationFailure;

        private RegistrationRetry(SQLException cause, boolean serializationFailure) {
            this.cause = cause;
            this.serializationFailure = serializationFailure;
        }
    }

    private RegistrationResult registerThroughMatrix(String taskName, CoordinatedTask.States initialState,
                                                     String scheduleFingerprint, String triggerFamily, RegistrationPhase phase)
            throws TaskCoordinationException, RegistrationRetry {

        LeaseFence fence = acquireLeaseFence("Registration of task [" + taskName + "]");
        Connection connection = null;
        try {
            connection = getTransactionalConnection();
            // step 0: the lease-token-conditioned advertisement touch on this node's own row — BEFORE
            // the task lock (the lock order's ADVERTISEMENT head): this transaction may write barrier
            // rows, and durable state from a stale boot must fail closed here, not at finalize
            long adopted = openWithLeaseFence(connection, fence, "Registration of task [" + taskName + "]");
            // step 1: the conditional self-lock UPDATE by primary key — count ignored
            lockTaskRow(connection, taskName);
            // step 2: SELECT the row by primary key on the same connection — the branch signal
            boolean taskRowExists = readTaskRowState(connection, taskName) != null;
            ClaimRow claim = readClaimRow(connection, taskName);

            // the wave-interaction rule: a wave in flight means someone is deciding this task's absence
            boolean waveResolved = false;
            if (claim != null && claim.deleteGuard != null) {
                DeleteBarrierView wave = readBarrierView(connection, taskName, claim.deleteGuard);
                if (wave != null && isInFlightBarrierStatus(wave.getStatus())) {
                    if (!Objects.equals(scheduleFingerprint, claim.scheduleFingerprint)) {
                        // different fingerprint, any in-flight wave -> park + consent: no task-row
                        // insert, no local scheduling; the consent ack is written in this same
                        // transaction (truthfully: this node holds nothing of the old definition)
                        upsertBarrierAck(connection, taskName, wave.getGuardUuid(), fence.nodeId,
                                System.currentTimeMillis());
                        connection.commit();
                        fence.controller.adoptPublishedTouch(adopted);
                        readiness().raise(PARKED_TASK_CONDITION,
                                CoordinationReadinessRegistry.ConditionClass.CONTINUOUS,
                                "task [" + taskName + "] parked: its definition's fingerprint differs from the "
                                        + "one an in-flight delete wave is deciding on; this node consented to "
                                        + "the wave");
                        parkEpisode(fence, taskName, "WAVE_PARK", "Registration of task [" + taskName
                                + "] parked against in-flight delete wave [" + wave.getGuardUuid()
                                + "] (different fingerprint); consent ack written.");
                        return RegistrationResult.DIFFERENT_FP_PARKED;
                    }
                    // Renewed presence keeps the row, but the wave's undeploy already
                    // deleted the initiating node's local Quartz job — hand a surviving RUNNING row
                    // back to the scheduler (RUNNING -> NONE, destined node kept) so the owner's
                    // normal scheduler pass re-schedules it; non-RUNNING states untouched. Task-row
                    // defined position in the transaction, under the task lock taken at step 1; commits or
                    // rolls back with the cancel/veto below.
                    try (PreparedStatement handBack = connection.prepareStatement(HAND_BACK_SURVIVING_RUNNING_TASK)) {
                        handBack.setString(1, taskName);
                        handBack.executeUpdate();
                    }
                    if (BARRIER_STATUS_RETIRING.equals(wave.getStatus())) {
                        // the presence veto: RETIRING -> RETIRE_REFUSED + the claim-guard clear, in
                        // this transaction — the refusal must never depend on the retire handler
                        // surviving
                        int refused = flipRetiringToRefused(connection, taskName, wave.getGuardUuid());
                        if (refused != 1) {
                            rollbackQuietly(connection);
                            throw new TaskCoordinationException("The RETIRING -> RETIRE_REFUSED presence veto of "
                                    + "task [" + taskName + "] hit an unexpected row count.");
                        }
                        clearClaimGuardStatusAware(connection, taskName, claim.status, wave.getGuardUuid());
                        LOG.error("Registration of task [" + taskName + "] refused in-flight retire wave ["
                                + wave.getGuardUuid() + "]: the artifact is present on this node.");
                    } else {
                        // same fingerprint, undeploy wave (OPEN / orphaned FINALIZING) -> renewed
                        // presence: cancel the wave in this transaction — CLAIM (guard clear) before
                        // BARRIER (row removal) in the global order
                        clearClaimGuardStatusAware(connection, taskName, claim.status, wave.getGuardUuid());
                        cleanupBarrierEntries(connection, taskName, wave.getGuardUuid());
                        try (PreparedStatement normalize = connection.prepareStatement(NORMALIZE_DELETE_PENDING_TASK)) {
                            normalize.setString(1, taskName);
                            normalize.executeUpdate();
                        }
                        LOG.error("Registration of task [" + taskName + "] cancelled in-flight delete wave ["
                                + wave.getGuardUuid() + "]: renewed presence with the same fingerprint.");
                    }
                    waveResolved = true;
                }
            }

            // family rule for every matrix row: a NULL stored family binds from the incoming
            // definition; otherwise a mismatch is never a fingerprint case — it parks with its own
            // readiness name (family is immutable after bind; the exit is undeploy-everywhere ->
            // task-retire -> redeploy under a new name)
            if (claim != null && claim.triggerFamily != null && !claim.triggerFamily.equals(triggerFamily)) {
                connection.commit();
                fence.controller.adoptPublishedTouch(adopted);
                readiness().raise(TRIGGER_FAMILY_CONFLICT_CONDITION,
                        CoordinationReadinessRegistry.ConditionClass.CONTINUOUS,
                        "task [" + taskName + "] parked: deployed trigger family [" + triggerFamily
                                + "] conflicts with the bound family [" + claim.triggerFamily + "] — undeploy "
                                + "(or rename) the conflicting artifact from EVERY live node, task-retire the "
                                + "row, then deploy under a new task name");
                parkEpisode(fence, taskName, "TRIGGER_FAMILY_CONFLICT", "Registration of task [" + taskName
                        + "] parked: trigger family [" + triggerFamily + "] conflicts with bound family ["
                        + claim.triggerFamily + "].");
                return RegistrationResult.DIFFERENT_FP_PARKED;
            }

            if (taskRowExists) {
                return finishExistsBranch(connection, fence, adopted, taskName, scheduleFingerprint, triggerFamily, phase,
                        claim, waveResolved);
            }
            // absent -> attempt the INSERT; a unique-specific error means a concurrent registration
            // won the race -> roll back the entire transaction and retry from step 1
            try {
                insertTaskRow(connection, taskName, initialState);
            } catch (SQLException insertFailure) {
                if (isUniqueViolation(insertFailure)) {
                    rollbackQuietly(connection);
                    throw new RegistrationRetry(insertFailure, false);
                }
                throw insertFailure;
            }
            return finishInsertedBranch(connection, fence, adopted, taskName, scheduleFingerprint, triggerFamily, phase,
                    claim, waveResolved);
        } catch (SQLException ex) {
            rollbackQuietly(connection);
            if (isSerializationFailure(ex)) {
                // the victim's transaction is rolled back in full — nothing partial remains, so the
                // whole doorway transaction (lease-fence touch included) re-runs from step 0
                throw new RegistrationRetry(ex, true);
            }
            throw new TaskCoordinationException(ERROR_MSG, ex);
        } catch (TaskCoordinationException ex) {
            rollbackQuietly(connection);
            throw ex;
        } finally {
            closeQuietly(connection);
        }
    }

    /**
     * The status-aware guard clear of a cancelling/refusing registration: targets the claim state
     * actually read under the task lock; exactly one affected claim row or the transaction rolls back.
     */
    private void clearClaimGuardStatusAware(Connection connection, String taskName, String claimStatus,
                                            String guardUuid) throws SQLException, TaskCoordinationException {

        String sql = CLAIM_STATUS_CLOSED.equals(claimStatus) ? CLEAR_CLAIM_DELETE_GUARD_CLOSED_IF_MATCH
                : CLEAR_CLAIM_DELETE_GUARD_OPEN_IF_MATCH;
        try {
            countCheckedUpdate(connection, sql, "The claim-guard clear of task [" + taskName + "] wave ["
                    + guardUuid + "] hit an unexpected row count.", taskName, guardUuid);
        } catch (TaskCoordinationException ex) {
            rollbackQuietly(connection);
            throw ex;
        }
    }

    private RegistrationResult finishExistsBranch(Connection connection, LeaseFence fence, long adopted,
                                                  String taskName, String scheduleFingerprint, String triggerFamily,
                                                  RegistrationPhase phase, ClaimRow claim, boolean waveResolved)
            throws SQLException, TaskCoordinationException {

        if (claim == null) {
            // exists + claim missing: boot pass seeds inline (the authorized seeding — the sole way
            // pre-existing tasks gain claim rows); steady state is an invariant failure
            if (phase == RegistrationPhase.BOOT_PASS) {
                insertClaimRow(connection, taskName, scheduleFingerprint, triggerFamily);
                connection.commit();
                fence.controller.adoptPublishedTouch(adopted);
                return RegistrationResult.REGISTERED_OR_VALIDATED;
            }
            rollbackQuietly(connection);
            readiness().raise(ANTI_JOIN_INVARIANT_CONDITION,
                    CoordinationReadinessRegistry.ConditionClass.CONTINUOUS,
                    "task [" + taskName + "] has a task row but no claim row at a STEADY_STATE registration — "
                            + "repaired only by the next boot pass");
            throw new TaskCoordinationException("Invariant failure: no claim row exists for task [" + taskName
                    + "] at a STEADY_STATE registration.");
        }
        if (CLAIM_STATUS_CLOSED.equals(claim.status)) {
            // a live task row over a tombstone is broken state; sanctioned exits are the operator
            // endpoints' CLOSED branches — automatic repair stays deliberately absent
            rollbackQuietly(connection);
            readiness().raise(ORPHAN_TASK_ROW_CONDITION, CoordinationReadinessRegistry.ConditionClass.GATE,
                    "task [" + taskName + "] has a live task row over a CLOSED claim — sanctioned exits: "
                            + "operator reconfigure (reopen) or task-retire (delete)");
            LOG.error("Registration of task [" + taskName + "] found a live task row over a CLOSED claim "
                    + "(CLOSED-over-live-task).");
            throw new TaskCoordinationException("Invariant failure: task [" + taskName
                    + "] carries a live task row over a CLOSED claim.");
        }
        // OPEN
        if (claim.scheduleFingerprint == null) {
            if (phase == RegistrationPhase.BOOT_PASS) {
                // the boot-pass catch-up bind (the NULL-to-value CAS)
                bindResidualUnboundClaim(connection, taskName, scheduleFingerprint, triggerFamily);
                connection.commit();
                fence.controller.adoptPublishedTouch(adopted);
                return waveResolved ? RegistrationResult.SAME_FP_CANCELLED_AND_VALIDATED
                        : RegistrationResult.REGISTERED_OR_VALIDATED;
            }
            rollbackQuietly(connection);
            readiness().raise(FINGERPRINT_BIND_COMPLETENESS_CONDITION,
                    CoordinationReadinessRegistry.ConditionClass.GATE,
                    "task [" + taskName + "] carries an unbound (NULL fingerprint) OPEN claim at a STEADY_STATE "
                            + "registration — readiness guaranteed non-NULL fingerprints before activation");
            throw new TaskCoordinationException("Invariant failure: task [" + taskName
                    + "] carries an unbound claim row at a STEADY_STATE registration.");
        }
        if (!scheduleFingerprint.equals(claim.scheduleFingerprint)) {
            // park — boot pass and steady state alike: commit nothing new, do NOT schedule locally;
            // publication honesty makes the park unconditional (the ownership CAS carries
            // AND SCHEDULE_FP = ?, so a mismatched local definition can be neither published nor
            // scheduled against a durable tuple describing a different definition)
            connection.commit();
            fence.controller.adoptPublishedTouch(adopted);
            readiness().raise(FINGERPRINT_CONFLICT_CONDITION,
                    CoordinationReadinessRegistry.ConditionClass.CONTINUOUS,
                    "task [" + taskName + "] parked: deployed schedule fingerprint [" + scheduleFingerprint
                            + "] conflicts with the bound fingerprint [" + claim.scheduleFingerprint + "] — the old "
                            + "definition keeps running wherever it matches; resolution is a completed "
                            + "redeploy cycle or the operator reconfigure");
            parkEpisode(fence, taskName, "FP_MISMATCH_PARK", "Registration of task [" + taskName
                    + "] parked: deployed fingerprint [" + scheduleFingerprint + "] conflicts with bound fingerprint ["
                    + claim.scheduleFingerprint + "].");
            return RegistrationResult.DIFFERENT_FP_PARKED;
        }
        // same fingerprint: validate only — scheduling gated on this presence transaction's commit
        connection.commit();
        fence.controller.adoptPublishedTouch(adopted);
        if (parkEpisodes != null) {
            parkEpisodes.cleared(taskName);
        }
        // Completed-wave handling: a task row observed over an OPEN matching claim is the opposite of the orphan
        // state — a raise naming this task is resolved (the peer's reopen landed first, or the
        // condition was stale)
        clearOrphanTaskRowConditionFor(taskName);
        return waveResolved ? RegistrationResult.SAME_FP_CANCELLED_AND_VALIDATED
                : RegistrationResult.REGISTERED_OR_VALIDATED;
    }

    private RegistrationResult finishInsertedBranch(Connection connection, LeaseFence fence, long adopted,
                                                    String taskName, String scheduleFingerprint, String triggerFamily,
                                                    RegistrationPhase phase, ClaimRow claim, boolean waveResolved)
            throws SQLException, TaskCoordinationException {

        if (claim == null) {
            // genuinely new -> INSERT claim; commit both
            insertClaimRow(connection, taskName, scheduleFingerprint, triggerFamily);
            connection.commit();
            fence.controller.adoptPublishedTouch(adopted);
            return RegistrationResult.REGISTERED_OR_VALIDATED;
        }
        if (CLAIM_STATUS_OPEN.equals(claim.status)) {
            if (claim.scheduleFingerprint == null) {
                if (phase == RegistrationPhase.BOOT_PASS) {
                    // re-materialize + bind inline
                    bindResidualUnboundClaim(connection, taskName, scheduleFingerprint, triggerFamily);
                    connection.commit();
                    fence.controller.adoptPublishedTouch(adopted);
                    return waveResolved ? RegistrationResult.SAME_FP_CANCELLED_AND_VALIDATED
                            : RegistrationResult.REGISTERED_OR_VALIDATED;
                }
                rollbackQuietly(connection);
                readiness().raise(FINGERPRINT_BIND_COMPLETENESS_CONDITION,
                        CoordinationReadinessRegistry.ConditionClass.GATE,
                        "task [" + taskName + "] carries an unbound (NULL fingerprint) OPEN claim at a "
                                + "STEADY_STATE registration");
                throw new TaskCoordinationException("Invariant failure: task [" + taskName
                        + "] carries an unbound claim row at a STEADY_STATE registration.");
            }
            if (!scheduleFingerprint.equals(claim.scheduleFingerprint)) {
                // the offline-swap case: commit the task row; park — boot pass and steady state alike
                connection.commit();
                fence.controller.adoptPublishedTouch(adopted);
                readiness().raise(FINGERPRINT_CONFLICT_CONDITION,
                        CoordinationReadinessRegistry.ConditionClass.CONTINUOUS,
                        "task [" + taskName + "] parked (task row re-materialized): deployed schedule "
                                + "fingerprint [" + scheduleFingerprint + "] conflicts with the bound fingerprint ["
                                + claim.scheduleFingerprint + "] — one clean redeploy cycle or the operator "
                                + "reconfigure converges it");
                parkEpisode(fence, taskName, "FP_MISMATCH_PARK", "Registration of task [" + taskName
                        + "] parked after re-materializing its row: deployed fingerprint [" + scheduleFingerprint
                        + "] conflicts with bound fingerprint [" + claim.scheduleFingerprint + "].");
                return RegistrationResult.DIFFERENT_FP_PARKED;
            }
            // normal restart repair: task row re-materialized, claim untouched
            // (key/incarnation/epoch/fp preserved)
            connection.commit();
            fence.controller.adoptPublishedTouch(adopted);
            if (parkEpisodes != null) {
                parkEpisodes.cleared(taskName);
            }
            return waveResolved ? RegistrationResult.SAME_FP_CANCELLED_AND_VALIDATED
                    : RegistrationResult.REGISTERED_OR_VALIDATED;
        }
        // inserted + CLOSED: split by trigger type
        if (TRIGGER_FAMILY_RECURRING.equals(triggerFamily)) {
            // recurring -> reopen: nextIncarnation = Math.addExact(observed, 1) computed in Java under
            // the held lock (overflow refuses before JDBC); FIRE_KEY PRESERVED — always, definition
            // changes included; OWNER_BOOT_ID clears to NULL (ownership is stamped later by the
            // guarded publication transitions)
            int nextIncarnation;
            try {
                nextIncarnation = Math.addExact(claim.incarnation, 1);
            } catch (ArithmeticException overflow) {
                rollbackQuietly(connection);
                readiness().raise(ANTI_JOIN_INVARIANT_CONDITION,
                        CoordinationReadinessRegistry.ConditionClass.CONTINUOUS,
                        "the INCARNATION of task [" + taskName + "] would overflow on reopen — refusing to wrap "
                                + "a fencing token");
                throw new TaskCoordinationException("INCARNATION overflow for task [" + taskName
                        + "] — refusing the reopen instead of wrapping a fencing token.", overflow);
            }
            String reopenSql = claim.triggerFamily == null ? REOPEN_CLAIM_RECURRING_BIND_FAMILY
                    : REOPEN_CLAIM_RECURRING_IF_FAMILY_MATCH;
            int reopened;
            try (PreparedStatement reopen = connection.prepareStatement(reopenSql)) {
                int index = 1;
                reopen.setInt(index++, nextIncarnation);
                reopen.setLong(index++, claim.ownerEpoch);
                reopen.setString(index++, scheduleFingerprint);
                if (claim.triggerFamily == null) {
                    reopen.setString(index++, triggerFamily);
                }
                reopen.setString(index++, taskName);
                if (claim.triggerFamily != null) {
                    reopen.setString(index++, claim.triggerFamily);
                }
                reopen.setInt(index, claim.incarnation);
                reopened = reopen.executeUpdate();
            }
            if (reopened != 1) {
                rollbackQuietly(connection);
                throw new TaskCoordinationException("The recurring reopen of task [" + taskName
                        + "] hit an unexpected row count; the transaction was rolled back.");
            }
            connection.commit();
            fence.controller.adoptPublishedTouch(adopted);
            if (parkEpisodes != null) {
                parkEpisodes.cleared(taskName);
            }
            clearOrphanTaskRowConditionFor(taskName);
            return RegistrationResult.REGISTERED_OR_VALIDATED;
        }
        // one-shot -> park: commit the task row, claim untouched; the operator reconfigure reopen is
        // the deliberate act — automatic reopen would refire occurrence 0 from a stale artifact
        connection.commit();
        fence.controller.adoptPublishedTouch(adopted);
        readiness().raise(PARKED_TASK_CONDITION, CoordinationReadinessRegistry.ConditionClass.CONTINUOUS,
                "one-shot task [" + taskName + "] re-registered over a CLOSED claim and parked — the operator "
                        + "reconfigure reopen is the deliberate act");
        parkEpisode(fence, taskName, "ONESHOT_CLOSED_PARK", "Registration of one-shot task [" + taskName
                + "] parked over its CLOSED claim; reconfigure is the deliberate reopen.");
        return RegistrationResult.DIFFERENT_FP_PARKED;
    }

    private void insertClaimRow(Connection connection, String taskName, String scheduleFingerprint, String triggerFamily)
            throws SQLException {

        try (PreparedStatement preparedStatement = connection.prepareStatement(INSERT_CLAIM_ROW)) {
            preparedStatement.setString(1, taskName);
            preparedStatement.setLong(2, 0L);
            preparedStatement.setString(3, scheduleFingerprint);
            preparedStatement.setString(4, triggerFamily);
            preparedStatement.executeUpdate();
        }
    }

    /**
     * The cleaner's repair — claim-status-only branches, no fingerprint input BY DESIGN: the cleaner
     * holds no artifact and must not be routed into branches that need one. Claim ABSENT: episode
     * alarm + skip (never a blind artifact-less seed — the creating node's own armed boot pass is the
     * seeding authority). Claim OPEN: re-materialize the task row only (the task table stores no
     * schedule; the claim is untouched). Claim CLOSED: skip — the delete completed, and a cleaner
     * re-add would be the resurrection the tombstone exists to stop. Re-materializes rows only — it
     * neither schedules nor unparks anything.
     */
    public void repairMissingTaskRow(String taskName) throws TaskCoordinationException {

        LeaseFence fence = acquireLeaseFence("Missing-task-row repair of [" + taskName + "]");
        Connection connection = null;
        try {
            connection = getTransactionalConnection();
            long adopted = openWithLeaseFence(connection, fence,
                    "Missing-task-row repair of [" + taskName + "]");
            lockTaskRow(connection, taskName);
            boolean taskRowExists = readTaskRowState(connection, taskName) != null;
            ClaimRow claim = readClaimRow(connection, taskName);
            if (claim == null) {
                rollbackQuietly(connection);
                readiness().raise(ANTI_JOIN_INVARIANT_CONDITION,
                        CoordinationReadinessRegistry.ConditionClass.CONTINUOUS,
                        "deployed task [" + taskName + "] has no claim row — the creating node's own armed boot "
                                + "pass is the seeding authority; the cleaner never seeds without the artifact");
                parkEpisode(fence, taskName, "REPAIR_CLAIM_ABSENT", "Missing-task-row repair skipped task ["
                        + taskName + "]: no claim row exists (deployed-but-rowless).");
                return;
            }
            if (CLAIM_STATUS_CLOSED.equals(claim.status)) {
                rollbackQuietly(connection);
                return;
            }
            if (!taskRowExists) {
                insertTaskRow(connection, taskName, CoordinatedTask.States.NONE);
                LOG.info("Missing-task-row repair re-materialized the task row of [" + taskName + "].");
            }
            connection.commit();
            fence.controller.adoptPublishedTouch(adopted);
        } catch (SQLException ex) {
            rollbackQuietly(connection);
            throw new TaskCoordinationException(ERROR_MSG, ex);
        } finally {
            closeQuietly(connection);
        }
    }

    /**
     * Ownership publication (i) — the LEADER assignment transition: one caller-owned transaction per
     * task; the task-row DESTINED_NODE_ID write and the expected-epoch claim CAS commit together. The
     * CAS publishes the target node's currently-advertised boot ID, which invalidates the previous owner
     * immediately, before the target polls; the freshness EXISTS revalidates the same fresh ELIGIBLE
     * tuple the resolver selected, at transaction time. A stale publisher loses the CAS; zero rows
     * roll the whole task transition back (the resolver retries next cycle). A task with no claim row
     * (the mixed hour) is assigned task-row-only — its fires veto NO_FENCE_ROW until seeded.
     *
     * @return true when the assignment committed
     */
    public boolean assignTaskWithLeaderPublication(String taskName, String targetNodeId, String groupId,
                                                   long localWindowMillis, long now)
            throws TaskCoordinationException {

        Connection connection = null;
        try {
            connection = getTransactionalConnection();
            int taskRows;
            try (PreparedStatement assign = connection.prepareStatement(UPDATE_ASSIGNMENT_AND_STATE)) {
                assign.setString(1, targetNodeId);
                assign.setString(2, taskName);
                taskRows = assign.executeUpdate();
            }
            if (taskRows == 0) {
                rollbackQuietly(connection);
                return false;
            }
            ClaimRow claim = readClaimRow(connection, taskName);
            if (claim == null) {
                connection.commit();
                return true;
            }
            if (!CLAIM_STATUS_OPEN.equals(claim.status) || claim.scheduleFingerprint == null) {
                rollbackQuietly(connection);
                return false;
            }
            NodeAdvertisement target = readNodeAdvertisement(connection, groupId, targetNodeId);
            if (target == null) {
                rollbackQuietly(connection);
                return false;
            }
            long nextEpoch;
            try {
                nextEpoch = Math.addExact(claim.ownerEpoch, 1L);
            } catch (ArithmeticException overflow) {
                rollbackQuietly(connection);
                throw new TaskCoordinationException("OWNER_EPOCH overflow for task [" + taskName
                        + "] — refusing the assignment instead of wrapping a fencing token.", overflow);
            }
            long freshnessFloor = now - (Math.max(localWindowMillis, target.getHeartbeatWindow())
                    + SKEW_ALLOWANCE_MILLIS);
            int cas;
            try (PreparedStatement publish = connection.prepareStatement(PUBLISH_CLAIM_OWNER_LEADER_ASSIGN)) {
                publish.setLong(1, nextEpoch);
                publish.setString(2, target.getBootId());
                publish.setString(3, taskName);
                publish.setInt(4, claim.incarnation);
                publish.setLong(5, claim.ownerEpoch);
                publish.setString(6, claim.scheduleFingerprint);
                publish.setString(7, groupId);
                publish.setString(8, targetNodeId);
                publish.setString(9, target.getBootId());
                publish.setLong(10, freshnessFloor);
                cas = publish.executeUpdate();
            }
            if (cas != 1) {
                rollbackQuietly(connection);
                return false;
            }
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
     * The fence tuple published by ownership transition (ii) — the values for the JobDataMap are
     * KNOWN from the successful update; the JobDetail is built or rebuilt only after commit.
     */
    public static final class PublishedOwnership {

        private final long ownerEpoch;
        private final int incarnation;
        private final String ownerBootId;
        private final String scheduleFingerprint;
        private final String triggerFamily;

        private PublishedOwnership(long ownerEpoch, int incarnation, String ownerBootId, String scheduleFingerprint,
                                   String triggerFamily) {
            this.ownerEpoch = ownerEpoch;
            this.incarnation = incarnation;
            this.ownerBootId = ownerBootId;
            this.scheduleFingerprint = scheduleFingerprint;
            this.triggerFamily = triggerFamily;
        }

        public long getOwnerEpoch() {
            return ownerEpoch;
        }

        public int getIncarnation() {
            return incarnation;
        }

        public String getOwnerBootId() {
            return ownerBootId;
        }

        public String getScheduleFp() {
            return scheduleFingerprint;
        }

        public String getTriggerFamily() {
            return triggerFamily;
        }
    }

    /**
     * Ownership publication (ii) — the TARGET scheduling transition, in scheduleCoordinatedTask (and
     * restart pickup): validate destined-to-me + RUNNING-state update, then the expected-epoch CAS
     * publishing THIS node's OWN live boot ID; commit; the caller THEN builds — or for resume paths
     * REBUILDS — the Quartz JobDetail from the just-published tuple. The CAS carries the LOCAL
     * definition's fingerprint (publication honesty: a mismatched local definition can be neither
     * published nor scheduled) and requires this node's own advertisement row fresh AND ELIGIBLE.
     *
     * @return the published fence tuple, or null when the transition lost (nothing was scheduled)
     */
    public PublishedOwnership publishTargetOwnershipAndRun(String taskName, String localNodeId,
                                                           String localScheduleFp, String localTriggerFamily,
                                                           long localWindowMillis, long now)
            throws TaskCoordinationException {

        LeaseFence fence = acquireLeaseFence("Target ownership publication for task [" + taskName + "]");
        Connection connection = null;
        try {
            connection = getTransactionalConnection();
            int running;
            try (PreparedStatement run = connection.prepareStatement(UPDATE_TASK_STATE_FOR_DESTINED_NODE)) {
                run.setString(1, CoordinatedTask.States.RUNNING.name());
                run.setString(2, taskName);
                run.setString(3, localNodeId);
                running = run.executeUpdate();
            }
            if (running != 1) {
                rollbackQuietly(connection);
                return null;
            }
            ClaimRow claim = readClaimRow(connection, taskName);
            if (claim == null || !CLAIM_STATUS_OPEN.equals(claim.status)) {
                rollbackQuietly(connection);
                return null;
            }
            NodeAdvertisement mine = readNodeAdvertisement(connection, fence.groupId, fence.nodeId);
            if (mine == null || !fence.bootId.equals(mine.getBootId())) {
                rollbackQuietly(connection);
                return null;
            }
            long nextEpoch;
            try {
                nextEpoch = Math.addExact(claim.ownerEpoch, 1L);
            } catch (ArithmeticException overflow) {
                rollbackQuietly(connection);
                throw new TaskCoordinationException("OWNER_EPOCH overflow for task [" + taskName
                        + "] — refusing the scheduling publication instead of wrapping a fencing token.",
                        overflow);
            }
            long freshnessFloor = now - (Math.max(localWindowMillis, mine.getHeartbeatWindow())
                    + SKEW_ALLOWANCE_MILLIS);
            int cas;
            try (PreparedStatement publish = connection.prepareStatement(PUBLISH_CLAIM_OWNER_TARGET_SCHEDULE)) {
                publish.setLong(1, nextEpoch);
                publish.setString(2, fence.bootId);
                publish.setString(3, taskName);
                publish.setInt(4, claim.incarnation);
                publish.setLong(5, claim.ownerEpoch);
                publish.setString(6, localScheduleFp);
                publish.setString(7, taskName);
                publish.setString(8, localNodeId);
                publish.setString(9, fence.groupId);
                publish.setString(10, fence.nodeId);
                publish.setString(11, fence.bootId);
                publish.setLong(12, freshnessFloor);
                cas = publish.executeUpdate();
            }
            if (cas != 1) {
                rollbackQuietly(connection);
                return null;
            }
            connection.commit();
            return new PublishedOwnership(nextEpoch, claim.incarnation, fence.bootId, localScheduleFp,
                    claim.triggerFamily != null ? claim.triggerFamily : localTriggerFamily);
        } catch (SQLException ex) {
            rollbackQuietly(connection);
            throw new TaskCoordinationException(ERROR_MSG, ex);
        } finally {
            closeQuietly(connection);
        }
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

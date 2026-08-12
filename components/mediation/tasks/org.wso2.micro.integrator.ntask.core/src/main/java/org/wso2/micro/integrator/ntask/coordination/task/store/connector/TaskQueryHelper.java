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

import org.wso2.micro.integrator.ntask.coordination.task.CoordinatedTask;

/**
 * The class which contains all the data base queries for the task database.
 */
public class TaskQueryHelper {

    //task table Name
    public static final String TABLE_NAME = "COORDINATED_TASK_TABLE";
    public static final String TASK_DELETE_GUARD_TABLE = "TASK_DELETE_GUARD";
    public static final String TASK_DELETE_BARRIER_TABLE = "TASK_DELETE_BARRIER";
    public static final String TASK_DELETE_BARRIER_EXPECTED_TABLE = "TASK_DELETE_BARRIER_EXPECTED";
    public static final String TASK_DELETE_BARRIER_ACK_TABLE = "TASK_DELETE_BARRIER_ACK";

    //message processor table Name
    public static final String MP_TABLE_NAME = "MP_STATE_TABLE";

    //MP table columns
    public static final String MP_NAME = "MP_NAME";
    public static final String MP_STATE = "MP_STATE";

    // Task table columns
    public static final String TASK_NAME = "TASK_NAME";
    public static final String DESTINED_NODE_ID = "DESTINED_NODE_ID";
    public static final String TASK_STATE = "TASK_STATE";
    public static final String GUARD_UUID = "GUARD_UUID";
    public static final String OWNER_NODE_ID = "OWNER_NODE_ID";
    public static final String STATUS = "STATUS";
    public static final String DEADLINE_AT = "DEADLINE_AT";
    public static final String UPDATED_AT = "UPDATED_AT";
    public static final String NODE_ID = "NODE_ID";
    public static final String ACKED_AT = "ACKED_AT";

    public static final String BARRIER_STATUS_OPEN = "OPEN";
    public static final String BARRIER_STATUS_FINALIZING = "FINALIZING";
    public static final String BARRIER_STATUS_RETIRING = "RETIRING";
    public static final String BARRIER_STATUS_RETIRE_REFUSED = "RETIRE_REFUSED";
    public static final String TASK_DELETE_PENDING_STATE = "DELETE_PENDING";

    // Latest known running-task snapshot per (node, task), used by monitoring and duplicate detection.
    public static final String RUNNING_TASK_OBSERVATION_TABLE = "RUNNING_TASK_OBSERVATION";
    public static final String OBSERVED_AT = "OBSERVED_AT";

    // Keep the update unconditional. Writes for a node are already serialized by the observation writer,
    // and a timestamp guard would make future timestamps sticky after a clock step.
    static final String UPDATE_RUNNING_TASK_OBSERVATION =
            "UPDATE " + RUNNING_TASK_OBSERVATION_TABLE + " SET " + OBSERVED_AT + " = ? WHERE " + NODE_ID + " = ? AND "
                    + TASK_NAME + " = ?";

    static final String INSERT_RUNNING_TASK_OBSERVATION =
            "INSERT INTO " + RUNNING_TASK_OBSERVATION_TABLE + " (" + NODE_ID + ", " + TASK_NAME + ", " + OBSERVED_AT
                    + ") VALUES (?,?,?)";

    static final String DELETE_STALE_RUNNING_TASK_OBSERVATIONS_OF_NODE =
            "DELETE FROM " + RUNNING_TASK_OBSERVATION_TABLE + " WHERE " + NODE_ID + " = ? AND " + OBSERVED_AT + " < ?";

    static final String SELECT_FRESH_RUNNING_TASK_OBSERVATIONS =
            "SELECT " + NODE_ID + ", " + TASK_NAME + ", " + OBSERVED_AT + " FROM " + RUNNING_TASK_OBSERVATION_TABLE
                    + " WHERE " + OBSERVED_AT +  " >= ?";

    // History of duplicate-execution episodes opened and closed by the leader.
    public static final String TASK_DUPLICATION_EVENT_TABLE = "TASK_DUPLICATION_EVENT";
    public static final String NODES = "NODES";
    public static final String DESTINED_NODE = "DESTINED_NODE";
    public static final String DETECTED_AT = "DETECTED_AT";
    public static final String CLEARED_AT = "CLEARED_AT";
    public static final String SEVERITY = "SEVERITY";
    public static final String TASK_KIND = "TASK_KIND";
    public static final String SEVERITY_SUSTAINED = "SUSTAINED";
    public static final String SEVERITY_TRANSIENT = "TRANSIENT";

    static final String INSERT_DUPLICATION_EVENT =
            "INSERT INTO " + TASK_DUPLICATION_EVENT_TABLE + " (" + TASK_NAME + ", " + NODES + ", " + DESTINED_NODE
                    + ", " + DETECTED_AT + ", " + TASK_KIND + ") VALUES (?,?,?,?,?)";

    static final String SELECT_OPEN_DUPLICATION_EVENTS =
            "SELECT " + TASK_NAME + ", " + DETECTED_AT + ", " + SEVERITY + " FROM " + TASK_DUPLICATION_EVENT_TABLE
                    + " WHERE " + CLEARED_AT + " IS NULL";

    static final String MARK_DUPLICATION_EVENT_SUSTAINED =
            "UPDATE " + TASK_DUPLICATION_EVENT_TABLE + " SET " + SEVERITY + " = '" + SEVERITY_SUSTAINED + "' WHERE "
                    + TASK_NAME + " = ? AND " + CLEARED_AT + " IS NULL";

    static final String CLOSE_DUPLICATION_EVENT =
            "UPDATE " + TASK_DUPLICATION_EVENT_TABLE + " SET " + CLEARED_AT + " = ?, " + SEVERITY + " = CASE WHEN "
                    + SEVERITY + " IS NULL THEN '" + SEVERITY_TRANSIENT + "' ELSE " + SEVERITY + " END WHERE "
                    + TASK_NAME + " = ? AND " + CLEARED_AT + " IS NULL";

    // Claim fence table: one permanent row per task, the dispatch fence of the claim gate.
    public static final String CLAIM_TABLE_NAME = "COORDINATED_TASK_CLAIM";
    public static final String FIRE_KEY = "FIRE_KEY";
    public static final String INCARNATION = "INCARNATION";
    public static final String OWNER_EPOCH = "OWNER_EPOCH";
    public static final String OWNER_BOOT_ID = "OWNER_BOOT_ID";
    public static final String SCHEDULE_FP = "SCHEDULE_FP";
    public static final String CLAIMED_AT = "CLAIMED_AT";
    public static final String CLAIM_STATUS_OPEN = "OPEN";
    public static final String CLAIM_STATUS_CLOSED = "CLOSED";
    public static final String DELETE_GUARD = "DELETE_GUARD";
    public static final String TRIGGER_FAMILY = "TRIGGER_FAMILY";
    public static final String TRIGGER_FAMILY_RECURRING = "RECURRING";
    public static final String TRIGGER_FAMILY_ONESHOT = "ONESHOT";

    // the claim CAS; executeUpdate() == 1 -> claim won
    static final String CLAIM_FIRE =
            "UPDATE " + CLAIM_TABLE_NAME + " SET " + FIRE_KEY + " = ?, " + NODE_ID + " = ?, " + CLAIMED_AT
                    + " = ? WHERE " + TASK_NAME + " = ? AND " + FIRE_KEY + " < ? AND " + STATUS + " = '"
                    + CLAIM_STATUS_OPEN + "' AND " + OWNER_EPOCH + " = ? AND " + INCARNATION + " = ? AND "
                    + OWNER_BOOT_ID + " = ? AND " + SCHEDULE_FP + " = ?";

    // zero-rows read-back of the claim CAS, on the same auto-commit connection discipline
    static final String SELECT_CLAIM_ROW =
            "SELECT " + FIRE_KEY + ", " + INCARNATION + ", " + OWNER_EPOCH + ", " + OWNER_BOOT_ID + ", "
                    + SCHEDULE_FP + ", " + STATUS + " FROM " + CLAIM_TABLE_NAME + " WHERE " + TASK_NAME + " = ?";

    // BIND: boot-pass NULL-to-value CAS for residual unbound rows. Binds the fingerprint AND the
    // trigger family (derived from the deployed artifact's TriggerInfo) in one statement. Zero rows ->
    // read back: same values = already bound; DIFFERENT fingerprint = hard readiness failure naming
    // both hashes; DIFFERENT family = hard readiness failure naming the immutability rule;
    // CLOSED/missing = invariant failure. Never overwrite a non-null fingerprint or family.
    static final String BIND_RESIDUAL_UNBOUND_CLAIM =
            "UPDATE " + CLAIM_TABLE_NAME + " SET " + SCHEDULE_FP + " = ?, " + TRIGGER_FAMILY + " = ? WHERE "
                    + TASK_NAME + " = ? AND " + STATUS + " = '" + CLAIM_STATUS_OPEN + "' AND " + SCHEDULE_FP
                    + " IS NULL AND " + TRIGGER_FAMILY + " IS NULL";

    // zero-rows read-back of the bind CAS, on the caller's connection
    static final String SELECT_CLAIM_BIND_STATE =
            "SELECT " + SCHEDULE_FP + ", " + TRIGGER_FAMILY + ", " + STATUS + " FROM " + CLAIM_TABLE_NAME + " WHERE "
                    + TASK_NAME + " = ?";

    // Activation readiness is an anti-join invariant query, never a row count (CLOSED tombstones
    // legitimately outnumber task rows after the first genuine delete): every current task row must
    // have an OPEN claim row with a non-null fingerprint AND a non-null trigger family; violations are
    // reported per task as missing / unexpectedly-CLOSED / unbound.
    static final String SELECT_ACTIVATION_READINESS_VIOLATIONS =
            "SELECT t." + TASK_NAME + ", c." + TASK_NAME + ", c." + STATUS + ", c." + SCHEDULE_FP + ", c."
                    + TRIGGER_FAMILY + " FROM " + TABLE_NAME + " t LEFT JOIN " + CLAIM_TABLE_NAME + " c ON t."
                    + TASK_NAME + " = c." + TASK_NAME + " WHERE c." + TASK_NAME + " IS NULL OR c." + STATUS + " <> '"
                    + CLAIM_STATUS_OPEN + "' OR c." + SCHEDULE_FP + " IS NULL OR c." + TRIGGER_FAMILY + " IS NULL";

    // Barrier creation binds the wave to the fence: stamps the wave's guard UUID on the claim row. The
    // required shape is READ-THEN-BRANCH — one form, used by every creation path; a single OR/EXISTS
    // statement is forbidden. Zero rows on either CAS -> re-read and re-branch.
    static final String STAMP_CLAIM_DELETE_GUARD_OPEN_WHEN_NULL =
            "UPDATE " + CLAIM_TABLE_NAME + " SET " + DELETE_GUARD + " = ? WHERE " + TASK_NAME + " = ? AND " + STATUS
                    + " = '" + CLAIM_STATUS_OPEN + "' AND " + INCARNATION + " = ? AND " + DELETE_GUARD + " IS NULL";

    static final String STAMP_CLAIM_DELETE_GUARD_OPEN_IF_MATCH =
            "UPDATE " + CLAIM_TABLE_NAME + " SET " + DELETE_GUARD + " = ? WHERE " + TASK_NAME + " = ? AND " + STATUS
                    + " = '" + CLAIM_STATUS_OPEN + "' AND " + INCARNATION + " = ? AND " + DELETE_GUARD + " = ?";

    // CLOSED variants for a RETIRE wave — the universal invariant: every in-flight barrier is durably
    // bound by the claim row's exact DELETE_GUARD, a retire wave over a CLOSED claim included.
    static final String STAMP_CLAIM_DELETE_GUARD_CLOSED_WHEN_NULL =
            "UPDATE " + CLAIM_TABLE_NAME + " SET " + DELETE_GUARD + " = ? WHERE " + TASK_NAME + " = ? AND " + STATUS
                    + " = '" + CLAIM_STATUS_CLOSED + "' AND " + INCARNATION + " = ? AND " + DELETE_GUARD + " IS NULL";

    static final String STAMP_CLAIM_DELETE_GUARD_CLOSED_IF_MATCH =
            "UPDATE " + CLAIM_TABLE_NAME + " SET " + DELETE_GUARD + " = ? WHERE " + TASK_NAME + " = ? AND " + STATUS
                    + " = '" + CLAIM_STATUS_CLOSED + "' AND " + INCARNATION + " = ? AND " + DELETE_GUARD + " = ?";

    // The guarded close, run INSIDE the guarded finalize transaction together with the task-row delete.
    // DELETE_GUARD-conditioned so a late-recovered OLD wave can never close a re-registered incarnation;
    // guard-only conditioning is provably sufficient because reopen clears the guard.
    static final String CLOSE_CLAIM_FOR_GUARD =
            "UPDATE " + CLAIM_TABLE_NAME + " SET " + STATUS + " = '" + CLAIM_STATUS_CLOSED + "' WHERE " + TASK_NAME
                    + " = ? AND " + STATUS + " = '" + CLAIM_STATUS_OPEN + "' AND " + DELETE_GUARD + " = ?";

    // The operator reconfigure's close: a CAS over the FULL expected tuple with the branch's guard
    // predicate (only the guard-NULL branch ever closes — every non-NULL guard refuses upstream).
    static final String CLOSE_CLAIM_FOR_RECONFIGURE =
            "UPDATE " + CLAIM_TABLE_NAME + " SET " + STATUS + " = '" + CLAIM_STATUS_CLOSED + "' WHERE " + TASK_NAME
                    + " = ? AND " + STATUS + " = '" + CLAIM_STATUS_OPEN + "' AND " + INCARNATION + " = ? AND "
                    + SCHEDULE_FP + " = ? AND " + DELETE_GUARD + " IS NULL";

    // The RETIRING responder's one indexed read: in-flight RETIRING waves where this node is in
    // EXPECTED and has no ACK row — built from the shipped EXPECTED/ACK shapes.
    static final String RETRIEVE_UNANSWERED_RETIRING_WAVES =
            "SELECT b." + TASK_NAME + ", b." + GUARD_UUID + " FROM " + TASK_DELETE_BARRIER_TABLE + " b, "
                    + TASK_DELETE_BARRIER_EXPECTED_TABLE + " e WHERE b." + TASK_NAME + " = e." + TASK_NAME
                    + " AND b." + GUARD_UUID + " = e." + GUARD_UUID + " AND b." + STATUS + " = '"
                    + BARRIER_STATUS_RETIRING + "' AND e." + NODE_ID + " = ? AND NOT EXISTS (SELECT 1 FROM "
                    + TASK_DELETE_BARRIER_ACK_TABLE + " a WHERE a." + TASK_NAME + " = b." + TASK_NAME + " AND a."
                    + GUARD_UUID + " = b." + GUARD_UUID + " AND a." + NODE_ID + " = ?)";

    // Re-registration after a completed close: new incarnation (nextIncarnation = Math.addExact(observed, 1),
    // computed in Java under the held lock; overflow refuses before JDBC), same transaction as the task-row
    // re-add. RECURRING reopen: FIRE_KEY is PRESERVED across EVERY recurring reopen, fingerprint changes
    // included — recurring reopen SQL never writes -1. OWNER_BOOT_ID clears to NULL: ownership is stamped
    // later by the guarded publication transitions, and a NULL boot means no claim can win until one does.
    // Variant (a) — stored family NULL (residual unbound row): the CAS binds it here.
    static final String REOPEN_CLAIM_RECURRING_BIND_FAMILY =
            "UPDATE " + CLAIM_TABLE_NAME + " SET " + STATUS + " = '" + CLAIM_STATUS_OPEN + "', " + INCARNATION
                    + " = ?, " + OWNER_EPOCH + " = ?, " + OWNER_BOOT_ID + " = NULL, " + SCHEDULE_FP + " = ?, "
                    + TRIGGER_FAMILY + " = ?, " + DELETE_GUARD + " = NULL WHERE " + TASK_NAME + " = ? AND " + STATUS
                    + " = '" + CLAIM_STATUS_CLOSED + "' AND " + TRIGGER_FAMILY + " IS NULL AND " + INCARNATION
                    + " = ?";

    // Variant (b) — stored family known: the CAS verifies it (a mismatched family zero-rows and rolls
    // back — the recurring<->one-shot immutability refusal enforced in the statement itself).
    static final String REOPEN_CLAIM_RECURRING_IF_FAMILY_MATCH =
            "UPDATE " + CLAIM_TABLE_NAME + " SET " + STATUS + " = '" + CLAIM_STATUS_OPEN + "', " + INCARNATION
                    + " = ?, " + OWNER_EPOCH + " = ?, " + OWNER_BOOT_ID + " = NULL, " + SCHEDULE_FP + " = ?, "
                    + DELETE_GUARD + " = NULL WHERE " + TASK_NAME + " = ? AND " + STATUS + " = '"
                    + CLAIM_STATUS_CLOSED + "' AND " + TRIGGER_FAMILY + " = ? AND " + INCARNATION + " = ?";

    // one-shot deliberate reopen (the reconfigure CLOSED branch): identity is the incarnation and the
    // refire IS the operator intent, so FIRE_KEY resets to -1 in the same statement shape — one fire per
    // incarnation. Same two family variants.
    static final String REOPEN_CLAIM_ONESHOT_BIND_FAMILY =
            "UPDATE " + CLAIM_TABLE_NAME + " SET " + STATUS + " = '" + CLAIM_STATUS_OPEN + "', " + INCARNATION
                    + " = ?, " + FIRE_KEY + " = -1, " + OWNER_EPOCH + " = ?, " + OWNER_BOOT_ID + " = NULL, "
                    + SCHEDULE_FP + " = ?, " + TRIGGER_FAMILY + " = ?, " + DELETE_GUARD + " = NULL WHERE " + TASK_NAME
                    + " = ? AND " + STATUS + " = '" + CLAIM_STATUS_CLOSED + "' AND " + TRIGGER_FAMILY
                    + " IS NULL AND " + INCARNATION + " = ?";

    static final String REOPEN_CLAIM_ONESHOT_IF_FAMILY_MATCH =
            "UPDATE " + CLAIM_TABLE_NAME + " SET " + STATUS + " = '" + CLAIM_STATUS_OPEN + "', " + INCARNATION
                    + " = ?, " + FIRE_KEY + " = -1, " + OWNER_EPOCH + " = ?, " + OWNER_BOOT_ID + " = NULL, "
                    + SCHEDULE_FP + " = ?, " + DELETE_GUARD + " = NULL WHERE " + TASK_NAME + " = ? AND " + STATUS
                    + " = '" + CLAIM_STATUS_CLOSED + "' AND " + TRIGGER_FAMILY + " = ? AND " + INCARNATION + " = ?";

    // Boot lease advertisement: one row per node, acquired / renewed / released, never blindly
    // upserted. Identity values are static per lease generation; ELIGIBILITY is the one dynamic
    // column; UPDATED_AT refreshes on every renewal.
    public static final String NODE_ADVERTISEMENT_TABLE = "NODE_ADVERTISEMENT";
    public static final String GROUP_ID = "GROUP_ID";
    public static final String HEARTBEAT_WINDOW = "HEARTBEAT_WINDOW";
    public static final String BOOT_ID = "BOOT_ID";
    public static final String BOOT_STARTED_AT = "BOOT_STARTED_AT";
    public static final String ELIGIBILITY = "ELIGIBILITY";
    public static final String CONFIG_FINGERPRINT = "CONFIG_FINGERPRINT";
    public static final String ELIGIBILITY_PENDING = "PENDING";
    public static final String ELIGIBILITY_ELIGIBLE = "ELIGIBLE";
    public static final String ELIGIBILITY_LAPSED = "LAPSED";
    public static final String ELIGIBILITY_RECOVERING = "RECOVERING";
    public static final String ELIGIBILITY_TERMINAL = "TERMINAL";

    static final String SELECT_NODE_ADVERTISEMENT =
            "SELECT " + BOOT_ID + ", " + HEARTBEAT_WINDOW + ", " + CONFIG_FINGERPRINT + ", " + ELIGIBILITY + ", "
                    + BOOT_STARTED_AT + ", " + UPDATED_AT + " FROM " + NODE_ADVERTISEMENT_TABLE + " WHERE " + GROUP_ID
                    + " = ? AND " + NODE_ID + " = ?";

    // A fresh generation is never born ELIGIBLE — PENDING is also the DDL default, so no write path
    // can produce a fresh ELIGIBLE row.
    static final String INSERT_NODE_ADVERTISEMENT =
            "INSERT INTO " + NODE_ADVERTISEMENT_TABLE + " (" + GROUP_ID + ", " + NODE_ID + ", " + HEARTBEAT_WINDOW
                    + ", " + BOOT_ID + ", " + BOOT_STARTED_AT + ", " + ELIGIBILITY + ", " + CONFIG_FINGERPRINT + ", "
                    + UPDATED_AT + ") VALUES (?,?,?,?,?,'" + ELIGIBILITY_PENDING + "',?,?)";

    // The takeover CAS, conditioned on the exact observed tuple — a new owner never briefly wears its
    // predecessor's window or fingerprint.
    static final String TAKEOVER_NODE_ADVERTISEMENT =
            "UPDATE " + NODE_ADVERTISEMENT_TABLE + " SET " + BOOT_ID + " = ?, " + HEARTBEAT_WINDOW + " = ?, "
                    + CONFIG_FINGERPRINT + " = ?, " + ELIGIBILITY + " = '" + ELIGIBILITY_PENDING + "', "
                    + BOOT_STARTED_AT + " = ?, " + UPDATED_AT + " = ? WHERE " + GROUP_ID + " = ? AND " + NODE_ID
                    + " = ? AND " + BOOT_ID + " = ? AND " + HEARTBEAT_WINDOW + " = ? AND " + UPDATED_AT + " = ?";

    // The monotonic renewal touch: strictly forward so it can never be a value-no-op (a value-identical
    // UPDATE returns 0 under MySQL useAffectedRows).
    static final String TOUCH_NODE_ADVERTISEMENT =
            "UPDATE " + NODE_ADVERTISEMENT_TABLE + " SET " + UPDATED_AT + " = ? WHERE " + GROUP_ID + " = ? AND "
                    + NODE_ID + " = ? AND " + BOOT_ID + " = ? AND " + UPDATED_AT + " < ?";

    static final String UPDATE_NODE_ADVERTISEMENT_ELIGIBILITY =
            "UPDATE " + NODE_ADVERTISEMENT_TABLE + " SET " + ELIGIBILITY + " = ? WHERE " + GROUP_ID + " = ? AND "
                    + NODE_ID + " = ? AND " + BOOT_ID + " = ?";

    // Shutdown release — BOOT_ID-conditioned so a stale process can never delete its successor's row.
    static final String DELETE_NODE_ADVERTISEMENT_OF_BOOT =
            "DELETE FROM " + NODE_ADVERTISEMENT_TABLE + " WHERE " + GROUP_ID + " = ? AND " + NODE_ID + " = ? AND "
                    + BOOT_ID + " = ?";

    // Startup handback (destination-conditioned unassignment transition) support.
    static final String RETRIEVE_TASKS_DESTINED_TO_NODE =
            "SELECT " + TASK_NAME + " FROM " + TABLE_NAME + " WHERE " + DESTINED_NODE_ID + " = ? ORDER BY "
                    + TASK_NAME;

    static final String UNASSIGN_CLAIM_OWNER =
            "UPDATE " + CLAIM_TABLE_NAME + " SET " + OWNER_EPOCH + " = ?, " + OWNER_BOOT_ID + " = NULL WHERE "
                    + TASK_NAME + " = ? AND " + STATUS + " = '" + CLAIM_STATUS_OPEN + "' AND " + INCARNATION
                    + " = ? AND " + OWNER_EPOCH + " = ?";

    // The registration transaction's task self-lock: a conditional self-assignment UPDATE by primary
    // key, used as a lock attempt ONLY — its count is ignored (neither a caught exception nor a no-op
    // UPDATE count can carry the branch signal: PG aborts the transaction on any error, and MySQL
    // useAffectedRows makes a self-assignment's count 0 for an existing row). The same-connection
    // SELECT after it carries the branch.
    static final String LOCK_TASK_ROW =
            "UPDATE " + TABLE_NAME + " SET " + TASK_NAME + " = " + TASK_NAME + " WHERE " + TASK_NAME + " = ?";

    static final String SELECT_TASK_ROW =
            "SELECT " + TASK_NAME + ", " + TASK_STATE + ", " + DESTINED_NODE_ID + " FROM " + TABLE_NAME + " WHERE "
                    + TASK_NAME + " = ?";

    // The registration matrix reads the full claim row (status, fp, family, incarnation, epoch, guard)
    // after taking the task lock — the wave-interaction rule branches on DELETE_GUARD + the exact
    // barrier before any local scheduling.
    static final String SELECT_CLAIM_FOR_REGISTRATION =
            "SELECT " + STATUS + ", " + SCHEDULE_FP + ", " + TRIGGER_FAMILY + ", " + INCARNATION + ", " + OWNER_EPOCH
                    + ", " + DELETE_GUARD + ", " + FIRE_KEY + ", " + OWNER_BOOT_ID + " FROM " + CLAIM_TABLE_NAME
                    + " WHERE " + TASK_NAME + " = ?";

    // claim-row INSERT used by the registration matrix (task row is inserted FIRST in the same
    // transaction; this runs for the inserted+claim-absent case and the exists+claim-missing
    // boot-pass seed — the sole way pre-existing tasks gain claim rows; there is no set-based script).
    // Family derived from the registering TriggerInfo by the shipped trigger-construction predicate
    // (AbstractQuartzTaskManager: cronExpression != null -> RECURRING (cron); cronExpression == null
    // && repeatCount == 0 -> ONESHOT (the bare once-only trigger, interval ignored); repeatCount == -1
    // or > 0 -> RECURRING (interval)). Immutable thereafter.
    static final String INSERT_CLAIM_ROW =
            "INSERT INTO " + CLAIM_TABLE_NAME + " (" + TASK_NAME + ", " + FIRE_KEY + ", " + OWNER_EPOCH + ", "
                    + SCHEDULE_FP + ", " + TRIGGER_FAMILY + ") VALUES (?, -1, ?, ?, ?)";

    // A cancelling registration clears the claim's matching DELETE_GUARD — guard-UUID-conditioned AND
    // status-aware: the clear targets the claim state actually read under the task lock, two prepared
    // variants; exactly one affected claim row or the transaction rolls back; CLAIM before BARRIER in
    // the global lock order.
    static final String CLEAR_CLAIM_DELETE_GUARD_OPEN_IF_MATCH =
            "UPDATE " + CLAIM_TABLE_NAME + " SET " + DELETE_GUARD + " = NULL WHERE " + TASK_NAME + " = ? AND "
                    + STATUS + " = '" + CLAIM_STATUS_OPEN + "' AND " + DELETE_GUARD + " = ?";

    static final String CLEAR_CLAIM_DELETE_GUARD_CLOSED_IF_MATCH =
            "UPDATE " + CLAIM_TABLE_NAME + " SET " + DELETE_GUARD + " = NULL WHERE " + TASK_NAME + " = ? AND "
                    + STATUS + " = '" + CLAIM_STATUS_CLOSED + "' AND " + DELETE_GUARD + " = ?";

    // Mixed-roll only: a wave created by an UNPATCHED node has marked the task row DELETE_PENDING —
    // the patched cancel path normalizes that row's state to NONE.
    static final String NORMALIZE_DELETE_PENDING_TASK =
            "UPDATE " + TABLE_NAME + " SET " + TASK_STATE + " = '" + CoordinatedTask.States.NONE + "' WHERE "
                    + TASK_NAME + " = ? AND " + TASK_STATE + " = '" + TASK_DELETE_PENDING_STATE + "'";

    // A renewed-presence wave cancellation hands a surviving RUNNING row back to the
    // scheduler — the wave's undeploy already deleted the initiating node's local Quartz job, so
    // RUNNING there has no scheduler edge. Destined node kept; PAUSED and every other non-RUNNING
    // state untouched.
    static final String HAND_BACK_SURVIVING_RUNNING_TASK =
            "UPDATE " + TABLE_NAME + " SET " + TASK_STATE + " = '" + CoordinatedTask.States.NONE + "' WHERE "
                    + TASK_NAME + " = ? AND " + TASK_STATE + " = '" + CoordinatedTask.States.RUNNING + "'";

    // Membership-rejoin hand-back: becameUnresponsive paused every locally running
    // job of this node without a lease transition, so its RUNNING rows have no local Quartz edge
    // left. Driven back to NONE (destined node kept) so the restarted scheduler pass re-schedules
    // them; other nodes' rows and every non-RUNNING state untouched.
    static final String HAND_BACK_NODE_RUNNING_TASKS =
            "UPDATE " + TABLE_NAME + " SET " + TASK_STATE + " = '" + CoordinatedTask.States.NONE + "' WHERE "
                    + DESTINED_NODE_ID + " = ? AND " + TASK_STATE + " = '" + CoordinatedTask.States.RUNNING + "'";

    // GUARDED owner publication (topology split): the leader and the target are DIFFERENT JVMs, so
    // there are TWO transitions, each an expected-epoch CAS on its own connection (SELECT current
    // epoch, compute expected+1 in Java, run the statement; 1 row = proceed, 0 = abort/rollback).
    // Alias-free: TASK_NAME is a parameter INTO each subquery.
    //
    // (i) LEADER assignment transition — same transaction as the DESTINED_NODE_ID write; publishes the
    //     TARGET'S currently-advertised boot ID, which invalidates the previous owner immediately,
    //     before the target polls. The EXISTS revalidates the same fresh ELIGIBLE tuple the resolver
    //     selected, at transaction time — a node that lapsed after the snapshot loses the assignment
    //     here.
    static final String PUBLISH_CLAIM_OWNER_LEADER_ASSIGN =
            "UPDATE " + CLAIM_TABLE_NAME + " SET " + OWNER_EPOCH + " = ?, " + OWNER_BOOT_ID + " = ? WHERE "
                    + TASK_NAME + " = ? AND " + STATUS + " = '" + CLAIM_STATUS_OPEN + "' AND " + INCARNATION
                    + " = ? AND " + OWNER_EPOCH + " = ? AND " + SCHEDULE_FP + " = ? AND EXISTS (SELECT 1 FROM "
                    + NODE_ADVERTISEMENT_TABLE + " WHERE " + GROUP_ID + " = ? AND " + NODE_ID + " = ? AND " + BOOT_ID
                    + " = ? AND " + UPDATED_AT + " >= ? AND " + ELIGIBILITY + " = '" + ELIGIBILITY_ELIGIBLE + "')";

    // (ii) TARGET scheduling transition — in scheduleCoordinatedTask (and restart pickup): validate
    //      destined-to-me + RUNNING-state update, then the same CAS shape publishing ITS OWN live boot
    //      ID; commit; THEN build — or for resume paths REBUILD — the Quartz JobDetail from the
    //      just-published tuple. MY OWN advertisement row must be fresh AND ELIGIBLE too — a kernel
    //      correctness fence, never only a routing hint: without it a mid-initialization node could
    //      schedule work the leader assigned against a stale ELIGIBLE snapshot.
    static final String PUBLISH_CLAIM_OWNER_TARGET_SCHEDULE =
            "UPDATE " + CLAIM_TABLE_NAME + " SET " + OWNER_EPOCH + " = ?, " + OWNER_BOOT_ID + " = ? WHERE "
                    + TASK_NAME + " = ? AND " + STATUS + " = '" + CLAIM_STATUS_OPEN + "' AND " + INCARNATION
                    + " = ? AND " + OWNER_EPOCH + " = ? AND " + SCHEDULE_FP + " = ? AND EXISTS (SELECT 1 FROM "
                    + TABLE_NAME + " WHERE " + TASK_NAME + " = ? AND " + DESTINED_NODE_ID + " = ?)"
                    + " AND EXISTS (SELECT 1 FROM " + NODE_ADVERTISEMENT_TABLE + " WHERE " + GROUP_ID + " = ? AND "
                    + NODE_ID + " = ? AND " + BOOT_ID + " = ? AND " + UPDATED_AT + " >= ? AND " + ELIGIBILITY
                    + " = '" + ELIGIBILITY_ELIGIBLE + "')";

    // Caller-connection variant of the peer heartbeat read for Barrier Liveness classification — the
    // authoritative reads the finalize decision stands on run as plain non-locking SELECTs on the
    // finalize transaction's own connection (the same app-clock LAST_HEARTBEAT basis the existing
    // liveness stack uses).
    static final String SELECT_PEER_LAST_HEARTBEAT =
            "SELECT LAST_HEARTBEAT FROM CLUSTER_NODE_STATUS_TABLE WHERE " + GROUP_ID + " = ? AND " + NODE_ID + " = ?";

    private static final String TASK_STATE_CONST =
            "( CASE " + TASK_STATE + " WHEN '" + CoordinatedTask.States.RUNNING + "' THEN '"
                    + CoordinatedTask.States.NONE + "' WHEN '" + CoordinatedTask.States.DEACTIVATED + "'THEN '"
                    + CoordinatedTask.States.PAUSED + "' ELSE " + TASK_STATE + " END )";

    static final String ADD_TASK =
            "INSERT INTO " + TABLE_NAME + " ( " + TASK_NAME + ", " + DESTINED_NODE_ID + ", " + TASK_STATE + ") "
                    + "VALUES (?,NULL,?)";

    static final String UPDATE_ASSIGNMENT_AND_STATE =
            "UPDATE  " + TABLE_NAME + " SET  " + DESTINED_NODE_ID + " = ? , " + TASK_STATE + " = " + TASK_STATE_CONST
                    + " WHERE " + TASK_NAME + " = ?";

    static final String UPDATE_TASK_STATUS_TO_DEACTIVATED =
            "UPDATE  " + TABLE_NAME + "  SET " + TASK_STATE + " = '" + CoordinatedTask.States.DEACTIVATED + "' "
                    + "WHERE " + TASK_NAME + " =? AND " + TASK_STATE + " !='" + CoordinatedTask.States.PAUSED + "'";

    static final String ACTIVATE_TASK =
            "UPDATE  " + TABLE_NAME + "  SET " + TASK_STATE + " = '" + CoordinatedTask.States.ACTIVATED + "' WHERE "
                    + TASK_NAME + " =? AND " + TASK_STATE + " !='" + CoordinatedTask.States.RUNNING + "'";

    static final String UPDATE_TASK_STATE =
            "UPDATE  " + TABLE_NAME + "  SET " + TASK_STATE + " = ? WHERE " + TASK_NAME + " =? ";

    static final String UPDATE_MP_STATE =
            "UPDATE " + MP_TABLE_NAME + " SET " + MP_STATE + " = ? WHERE " + MP_NAME + " =?";

    static final String INSERT_MP_STATE =
            "INSERT INTO " + MP_TABLE_NAME + " ( " + MP_NAME + ", " + MP_STATE + ") VALUES (?, ?)";

    static final String UPDATE_TASK_STATE_FOR_DESTINED_NODE =
            "UPDATE  " + TABLE_NAME + "  SET " + TASK_STATE + " = ? WHERE " + TASK_NAME + " =? AND " + DESTINED_NODE_ID
                    + " =?";

    // Fenced Writes: a state write issued on behalf of a running task carries ownership AND fence
    // currency — the destined-node predicate plus the full claim lifecycle tuple (OPEN + incarnation
    // included, so delete/re-create fencing is explicit). One-table correlated EXISTS, portable;
    // alias-free — TASK_NAME is a parameter INTO the subquery. A stale node's write matches zero rows.
    static final String UPDATE_TASK_STATE_FENCED =
            "UPDATE " + TABLE_NAME + " SET " + TASK_STATE + " = ? WHERE " + TASK_NAME + " = ? AND "
                    + DESTINED_NODE_ID + " = ? AND EXISTS (SELECT 1 FROM " + CLAIM_TABLE_NAME + " WHERE " + TASK_NAME
                    + " = ? AND " + STATUS + " = '" + CLAIM_STATUS_OPEN + "' AND " + INCARNATION + " = ? AND "
                    + OWNER_EPOCH + " = ? AND " + OWNER_BOOT_ID + " = ?)";

    static final String RETRIEVE_ALL_TASKS = "SELECT  " + TASK_NAME  + "," + DESTINED_NODE_ID + " FROM " + TABLE_NAME;

    // The resolver never assigns an in-wave row: the OPEN/FINALIZING/RETIRING wave exclusion replaces
    // the retired DELETE_PENDING state predicate's read-effect — a task whose absence is being decided
    // is never handed to any node, patched or not.
    static final String RETRIEVE_UNASSIGNED_NOT_COMPLETED_TASKS =
            "SELECT " + TASK_NAME + " FROM " + TABLE_NAME + " WHERE  " + DESTINED_NODE_ID + " IS NULL AND " + TASK_STATE
                    + " !='" + CoordinatedTask.States.COMPLETED + "' AND " + TASK_NAME + " NOT IN (SELECT " + TASK_NAME
                    + " FROM " + TASK_DELETE_BARRIER_TABLE + " WHERE " + STATUS + " IN ('" + BARRIER_STATUS_OPEN
                    + "','" + BARRIER_STATUS_FINALIZING + "','" + BARRIER_STATUS_RETIRING + "'))";

    static final String RETRIEVE_TASKS_OF_NODE =
            "SELECT " + TASK_NAME + " FROM " + TABLE_NAME + "  WHERE " + DESTINED_NODE_ID + " =? AND " + TASK_STATE
                    + " =?";

    static final String RETRIEVE_TASK_STATE =
            "SELECT " + TASK_STATE + " FROM " + TABLE_NAME + "  WHERE " + TASK_NAME + " =?";

    static final String RETRIEVE_MP_STATE =
            "SELECT " + MP_STATE + " FROM " + MP_TABLE_NAME + "  WHERE " + MP_NAME + " =?";

    static final String REMOVE_ASSIGNMENT_AND_UPDATE_STATE =
            "UPDATE " + TABLE_NAME + " SET " + DESTINED_NODE_ID + " = NULL , " + TASK_STATE + " = " + TASK_STATE_CONST
                    + " WHERE " + TASK_NAME + " =?";

    // The task-row write of the startup handback, conditioned on the destination the caller OBSERVED —
    // a resolver may have reassigned between the read and this write, and a stale cleanup must not
    // clear a NEW assignment or advance its epoch.
    static final String UNASSIGN_TASK_IF_DESTINED =
            "UPDATE " + TABLE_NAME + " SET " + DESTINED_NODE_ID + " = NULL , " + TASK_STATE + " = " + TASK_STATE_CONST
                    + " WHERE " + TASK_NAME + " = ? AND " + DESTINED_NODE_ID + " = ?";

    static final String REMOVE_TASKS_OF_NODE = "DELETE FROM " + TABLE_NAME + "  WHERE " + DESTINED_NODE_ID + " =? AND "
            + TASK_STATE + " NOT IN ('" + CoordinatedTask.States.COMPLETED + "', '" + CoordinatedTask.States.ACTIVATED
            + "', '" + CoordinatedTask.States.DEACTIVATED + "')";

    static final String DELETE_TASK = "DELETE FROM " + TABLE_NAME + " WHERE " + TASK_NAME + " =?";

    static final String CLEAN_TASKS_OF_NODE =
            "UPDATE " + TABLE_NAME + " SET " + DESTINED_NODE_ID + " = NULL , " + TASK_STATE + " = " + TASK_STATE_CONST
                    + " WHERE " + DESTINED_NODE_ID + " = ? AND " + TASK_STATE + " !='"
                    + CoordinatedTask.States.COMPLETED + "'";

    static final String GET_ALL_ASSIGNED_INCOMPLETE_TASKS =
            "SELECT * FROM " + TABLE_NAME + " WHERE " + DESTINED_NODE_ID + " IS NOT NULL AND " + TASK_STATE + " != '"
                    + CoordinatedTask.States.COMPLETED + "'";

    static final String INSERT_TASK_DELETE_GUARD =
            "INSERT INTO " + TASK_DELETE_GUARD_TABLE + " (" + TASK_NAME + ", " + GUARD_UUID + ", " + UPDATED_AT
                    + ") VALUES (?,?,?)";

    static final String UPDATE_TASK_DELETE_GUARD =
            "UPDATE " + TASK_DELETE_GUARD_TABLE + " SET " + GUARD_UUID + " = ?, " + UPDATED_AT + " = ? WHERE "
                    + TASK_NAME + " = ?";

    static final String UPDATE_TASK_DELETE_GUARD_IF_MATCH =
            "UPDATE " + TASK_DELETE_GUARD_TABLE + " SET " + GUARD_UUID + " = ?, " + UPDATED_AT + " = ? WHERE "
                    + TASK_NAME + " = ? AND " + GUARD_UUID + " = ?";

    static final String UPDATE_TASK_DELETE_GUARD_TIMESTAMP_IF_MATCH =
            "UPDATE " + TASK_DELETE_GUARD_TABLE + " SET " + UPDATED_AT + " = ? WHERE " + TASK_NAME
                    + " = ? AND " + GUARD_UUID + " = ?";

    static final String SELECT_TASK_DELETE_GUARD =
            "SELECT " + GUARD_UUID + " FROM " + TASK_DELETE_GUARD_TABLE + " WHERE " + TASK_NAME + " = ?";

    static final String SELECT_MAX_TASK_DELETE_GUARD_UPDATED_AT =
            "SELECT MAX(" + UPDATED_AT + ") FROM " + TASK_DELETE_GUARD_TABLE;

    static final String INSERT_TASK_DELETE_BARRIER =
            "INSERT INTO " + TASK_DELETE_BARRIER_TABLE + " (" + TASK_NAME + ", " + GUARD_UUID + ", "
                    + OWNER_NODE_ID + ", " + STATUS + ", " + DEADLINE_AT + ", " + UPDATED_AT
                    + ") VALUES (?,?,?,?,?,?)";

    static final String INSERT_TASK_DELETE_BARRIER_EXPECTED =
            "INSERT INTO " + TASK_DELETE_BARRIER_EXPECTED_TABLE + " (" + TASK_NAME + ", " + GUARD_UUID + ", "
                    + NODE_ID + ") VALUES (?,?,?)";

    static final String INSERT_TASK_DELETE_BARRIER_ACK =
            "INSERT INTO " + TASK_DELETE_BARRIER_ACK_TABLE + " (" + TASK_NAME + ", " + GUARD_UUID + ", " + NODE_ID
                    + ", " + ACKED_AT + ") VALUES (?,?,?,?)";

    static final String UPDATE_TASK_DELETE_BARRIER_ACK =
            "UPDATE " + TASK_DELETE_BARRIER_ACK_TABLE + " SET " + ACKED_AT + " = ? WHERE " + TASK_NAME + " = ? AND "
                    + GUARD_UUID + " = ? AND " + NODE_ID + " = ?";

    static final String UPDATE_TASK_DELETE_BARRIER_STATUS =
            "UPDATE " + TASK_DELETE_BARRIER_TABLE + " SET " + STATUS + " = ?, " + UPDATED_AT + " = ? WHERE "
                    + TASK_NAME + " = ? AND " + GUARD_UUID + " = ? AND " + STATUS + " = ?";

    static final String UPDATE_TASK_DELETE_BARRIER_TIMESTAMP =
            "UPDATE " + TASK_DELETE_BARRIER_TABLE + " SET " + UPDATED_AT + " = ? WHERE " + TASK_NAME + " = ? AND "
                    + GUARD_UUID + " = ?";

    static final String UPDATE_TASK_STATUS_TO_DELETE_PENDING =
            "UPDATE " + TABLE_NAME + " SET " + DESTINED_NODE_ID + " = NULL , " + TASK_STATE + " = '"
                    + TASK_DELETE_PENDING_STATE + "' WHERE " + TASK_NAME + " = ? AND " + TASK_STATE + " != '"
                    + CoordinatedTask.States.COMPLETED + "'";

    static final String DELETE_TASK_IF_STATE_MATCH =
            "DELETE FROM " + TABLE_NAME + " WHERE " + TASK_NAME + " = ? AND " + TASK_STATE + " = ?";

    static final String DELETE_TASK_IF_STATE_NOT_MATCH =
            "DELETE FROM " + TABLE_NAME + " WHERE " + TASK_NAME + " = ? AND " + TASK_STATE + " <> ?";

    static final String SELECT_OPEN_TASK_DELETE_BARRIER_BY_TASK_AND_GUARD =
            "SELECT " + TASK_NAME + ", " + GUARD_UUID + ", " + OWNER_NODE_ID + ", " + STATUS + ", " + DEADLINE_AT
                    + ", " + UPDATED_AT + " FROM " + TASK_DELETE_BARRIER_TABLE + " WHERE " + TASK_NAME + " = ? AND "
                    + GUARD_UUID + " = ? AND " + STATUS + " = ?";

    static final String SELECT_OPEN_TASK_DELETE_BARRIERS =
            "SELECT " + TASK_NAME + ", " + GUARD_UUID + ", " + OWNER_NODE_ID + ", " + STATUS + ", " + DEADLINE_AT
                    + ", " + UPDATED_AT + " FROM " + TASK_DELETE_BARRIER_TABLE + " WHERE " + STATUS + " = ?";

    static final String SELECT_TASK_DELETE_BARRIER =
            "SELECT " + TASK_NAME + ", " + GUARD_UUID + ", " + OWNER_NODE_ID + ", " + STATUS + ", " + DEADLINE_AT
                    + ", " + UPDATED_AT + " FROM " + TASK_DELETE_BARRIER_TABLE + " WHERE " + TASK_NAME + " = ? AND "
                    + GUARD_UUID + " = ?";

    static final String SELECT_TASK_DELETE_BARRIER_EXPECTED_NODES =
            "SELECT " + NODE_ID + " FROM " + TASK_DELETE_BARRIER_EXPECTED_TABLE + " WHERE " + TASK_NAME + " = ? AND "
                    + GUARD_UUID + " = ?";

    static final String SELECT_TASK_DELETE_BARRIER_ACK_NODES =
            "SELECT " + NODE_ID + " FROM " + TASK_DELETE_BARRIER_ACK_TABLE + " WHERE " + TASK_NAME + " = ? AND "
                    + GUARD_UUID + " = ?";

    static final String DELETE_TASK_DELETE_BARRIER_ACKS =
            "DELETE FROM " + TASK_DELETE_BARRIER_ACK_TABLE + " WHERE " + TASK_NAME + " = ? AND " + GUARD_UUID + " = ?";

    static final String DELETE_TASK_DELETE_BARRIER_EXPECTED =
            "DELETE FROM " + TASK_DELETE_BARRIER_EXPECTED_TABLE + " WHERE " + TASK_NAME + " = ? AND " + GUARD_UUID
                    + " = ?";

    static final String DELETE_TASK_DELETE_BARRIER =
            "DELETE FROM " + TASK_DELETE_BARRIER_TABLE + " WHERE " + TASK_NAME + " = ? AND " + GUARD_UUID + " = ?";

    private TaskQueryHelper() throws IllegalAccessException {
        throw new IllegalAccessException("This class not to be initialized.");
    }

}

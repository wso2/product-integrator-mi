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

package org.wso2.micro.integrator.ntask.core.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.JobDataMap;
import org.wso2.micro.integrator.ntask.coordination.task.ClaimOutcome;
import org.wso2.micro.integrator.ntask.core.CoordinationReadinessRegistry;
import org.wso2.micro.integrator.ntask.core.internal.TasksDSComponent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * The episode limiter: an in-memory map of (taskName, reason) to episode state. First occurrence of a
 * (task, reason) emits one WARN; while the episode stays open, one progress WARN per minute carrying
 * the exact count; when a subsequent evaluation of the same (task, reason) succeeds, one INFO
 * "cleared after N occurrences". Invariant-violation reasons additionally raise one immediate
 * readiness ERROR — the alarm is instant, the repetition is rate-limited. No raw log.warn anywhere
 * in the veto paths.
 */
public class EpisodeLog {

    private static final Log log = LogFactory.getLog(EpisodeLog.class);
    private static final long PROGRESS_WARN_INTERVAL_MILLIS = 60000L;
    private static final char KEY_SEPARATOR = '\u0000';
    private static final String ANTI_JOIN_INVARIANT_CONDITION = "anti-join-invariant";
    private static final String LAPSED_REASON = "LAPSED";
    private static final String NON_PROVEN_LEASE_STATE_CONDITION = "non-proven-lease-state";
    private static final String FINGERPRINT_CONFLICT_CONDITION = "fingerprint-conflict";
    private static final String ORPHAN_TASK_ROW_CONDITION = "orphan-task-row";

    private static final class Episode {

        private final long firstAt;
        private long count;
        private long lastEmitAt;
        private boolean open;

        private Episode(long firstAt) {
            this.firstAt = firstAt;
        }
    }

    private final ConcurrentHashMap<String, Episode> episodes = new ConcurrentHashMap<>();
    private final String localNodeId;

    // Counters are exposed on the readiness resource: every live limiter registers itself so the
    // resource can snapshot (task, reason) counters across all of them.
    private static final CopyOnWriteArrayList<EpisodeLog> INSTANCES = new CopyOnWriteArrayList<>();

    public EpisodeLog(String localNodeId) {

        this.localNodeId = localNodeId;
        INSTANCES.add(this);
    }

    /**
     * One episode counter, as exposed on the readiness resource.
     */
    public static final class CounterView {

        private final String taskName;
        private final String reason;
        private final long count;
        private final long firstAt;
        private final boolean open;

        private CounterView(String taskName, String reason, long count, long firstAt, boolean open) {
            this.taskName = taskName;
            this.reason = reason;
            this.count = count;
            this.firstAt = firstAt;
            this.open = open;
        }

        public String getTaskName() {
            return taskName;
        }

        public String getReason() {
            return reason;
        }

        public long getCount() {
            return count;
        }

        public long getFirstAt() {
            return firstAt;
        }

        public boolean isOpen() {
            return open;
        }
    }

    /**
     * Snapshot of every limiter's (task, reason) counters, for the readiness resource.
     */
    public static List<CounterView> snapshotCounters() {

        List<CounterView> counters = new ArrayList<>();
        for (EpisodeLog instance : INSTANCES) {
            for (Map.Entry<String, Episode> entry : instance.episodes.entrySet()) {
                int separator = entry.getKey().indexOf(KEY_SEPARATOR);
                String taskName = separator < 0 ? entry.getKey() : entry.getKey().substring(0, separator);
                String reason = separator < 0 ? "" : entry.getKey().substring(separator + 1);
                Episode episode = entry.getValue();
                counters.add(new CounterView(taskName, reason, episode.count, episode.firstAt, episode.open));
            }
        }
        return counters;
    }

    public void veto(String reason, JobDataMap data) {

        veto(reason, data, null);
    }

    public void veto(String reason, JobDataMap data, Throwable cause) {

        String taskName = String.valueOf(data.get(CoordinatedTaskTriggerListener.JOB_DATA_TASK_KEY));
        long now = System.currentTimeMillis();
        episodes.compute(taskName + KEY_SEPARATOR + reason, (key, episode) -> {
            if (episode == null || !episode.open) {
                Episode fresh = new Episode(now);
                fresh.count = 1;
                fresh.lastEmitAt = now;
                fresh.open = true;
                String message = "Coordinated fire vetoed for task [" + taskName + "] reason [" + reason
                        + "] on node [" + localNodeId + "]: " + fenceTuple(data);
                if (cause == null) {
                    log.warn(message);
                } else {
                    log.warn(message, cause);
                }
                if (ClaimOutcome.ClaimReason.NO_FENCE_ROW.name().equals(reason)) {
                    TasksDSComponent.getReadinessRegistry().raise(ANTI_JOIN_INVARIANT_CONDITION,
                            CoordinationReadinessRegistry.ConditionClass.CONTINUOUS,
                            "claim row missing at fire time for task [" + taskName
                                    + "] — the seeding anti-join invariant is broken");
                }
                if (LAPSED_REASON.equals(reason)) {
                    TasksDSComponent.getReadinessRegistry().raise(NON_PROVEN_LEASE_STATE_CONDITION,
                            CoordinationReadinessRegistry.ConditionClass.CONTINUOUS,
                            "coordinated fire vetoed in a non-PROVEN lease state for task [" + taskName + "]");
                }
                if (ClaimOutcome.ClaimReason.FP_MISMATCH.name().equals(reason)) {
                    TasksDSComponent.getReadinessRegistry().raise(FINGERPRINT_CONFLICT_CONDITION,
                            CoordinationReadinessRegistry.ConditionClass.CONTINUOUS,
                            "coordinated fire vetoed on a schedule-fingerprint conflict for task [" + taskName
                                    + "] — the job's stamped definition differs from the bound one");
                }
                if (ClaimOutcome.ClaimReason.CLOSED.name().equals(reason)) {
                    TasksDSComponent.getReadinessRegistry().raise(ORPHAN_TASK_ROW_CONDITION,
                            CoordinationReadinessRegistry.ConditionClass.GATE,
                            "coordinated fire vetoed against a CLOSED claim for task [" + taskName
                                    + "] while the task is still scheduled locally (CLOSED-over-live-task)");
                }
                return fresh;
            }
            episode.count++;
            if (now - episode.lastEmitAt >= PROGRESS_WARN_INTERVAL_MILLIS) {
                episode.lastEmitAt = now;
                log.warn("Coordinated fire still vetoed for task [" + taskName + "] reason [" + reason
                        + "] on node [" + localNodeId + "]: " + episode.count + " occurrences since "
                        + episode.firstAt + ".");
            }
            return episode;
        });
    }

    /**
     * The parking path's episode-limited WARN: first occurrence of a (task, reason) emits one WARN,
     * an open episode emits one progress WARN per minute carrying the exact count.
     */
    public void episodeWarn(String taskName, String reason, String message) {

        long now = System.currentTimeMillis();
        episodes.compute(taskName + KEY_SEPARATOR + reason, (key, episode) -> {
            if (episode == null || !episode.open) {
                Episode fresh = new Episode(now);
                fresh.count = 1;
                fresh.lastEmitAt = now;
                fresh.open = true;
                log.warn(message);
                return fresh;
            }
            episode.count++;
            if (now - episode.lastEmitAt >= PROGRESS_WARN_INTERVAL_MILLIS) {
                episode.lastEmitAt = now;
                log.warn(message + " (" + episode.count + " occurrences since " + episode.firstAt + ")");
            }
            return episode;
        });
    }

    public void cleared(String taskName) {

        String prefix = taskName + KEY_SEPARATOR;
        for (String key : episodes.keySet()) {
            if (key.startsWith(prefix)) {
                episodes.compute(key, (k, episode) -> {
                    if (episode != null && episode.open) {
                        episode.open = false;
                        log.info("Coordinated fire veto cleared for task [" + taskName + "] reason ["
                                + k.substring(prefix.length()) + "] after " + episode.count + " occurrences.");
                    }
                    return episode;
                });
            }
        }
    }

    private static String fenceTuple(JobDataMap data) {

        return "ownerEpoch=" + data.get(CoordinatedTaskTriggerListener.JOB_DATA_OWNER_EPOCH)
                + ", incarnation=" + data.get(CoordinatedTaskTriggerListener.JOB_DATA_INCARNATION)
                + ", ownerBootId=" + data.get(CoordinatedTaskTriggerListener.JOB_DATA_OWNER_BOOT_ID)
                + ", scheduleFingerprint=" + data.get(CoordinatedTaskTriggerListener.JOB_DATA_SCHEDULE_FP)
                + ", triggerFamily=" + data.get(CoordinatedTaskTriggerListener.JOB_DATA_TRIGGER_FAMILY);
    }

}

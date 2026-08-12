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

import org.quartz.CronTrigger;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.listeners.TriggerListenerSupport;
import org.wso2.micro.integrator.ntask.coordination.task.ClaimOutcome;
import org.wso2.micro.integrator.ntask.coordination.task.store.TaskStore;
import org.wso2.micro.integrator.ntask.core.BootLeaseController;

/**
 * Global Quartz listener that protects coordinated jobs immediately before body execution. It is registered
 * once for the scheduler and ignores jobs without the coordinated marker. A separate listener per task manager would
 * process the same multi-manager fire more than once and could veto a valid winner. Armed nodes register this listener;
 * stock nodes do not. For a coordinated job, every uncertain lease or claim result becomes a veto rather than an error
 * escaping into Quartz.
 */
public class CoordinatedTaskTriggerListener extends TriggerListenerSupport {

    public static final String JOB_DATA_COORDINATED = "coordinated";
    public static final String JOB_DATA_TASK_KEY = "taskKey";
    public static final String JOB_DATA_OWNER_EPOCH = "ownerEpoch";
    public static final String JOB_DATA_INCARNATION = "incarnation";
    public static final String JOB_DATA_OWNER_BOOT_ID = "ownerBootId";
    public static final String JOB_DATA_SCHEDULE_FP = "scheduleFp";
    public static final String JOB_DATA_TRIGGER_FAMILY = "triggerFamily";

    private static final String CLAIM_UNCONFIRMED = "CLAIM_UNCONFIRMED";
    private static final String LAPSED = "LAPSED";

    private final TaskStore taskStore;
    private final String localNodeId;
    private final BootLeaseController bootLeaseController;
    private final EpisodeLog episodeLog;

    public CoordinatedTaskTriggerListener(TaskStore taskStore, String localNodeId, BootLeaseController bootLeaseController) {

        this.taskStore = taskStore;
        this.localNodeId = localNodeId;
        this.bootLeaseController = bootLeaseController;
        this.episodeLog = new EpisodeLog(localNodeId);
    }

    @Override
    public String getName() {

        return CoordinatedTaskTriggerListener.class.getName();
    }

    @Override
    public boolean vetoJobExecution(Trigger trigger, JobExecutionContext executionContext) {

        JobDataMap jobDataMap = executionContext.getJobDetail().getJobDataMap();
        if (!jobDataMap.getBoolean(JOB_DATA_COORDINATED)) {
            return false;                     // local/pinned task — never veto
        }
        if (bootLeaseController.isDispatchBlocked()) {
            // This is the first and only lease gate used by dispatch. A node must be locally PROVEN and still
            // within its monotonic renewal margin. Computing freshness here protects either wake-up order: the Quartz
            // worker may run before or after the heartbeat thread notices a JVM pause.
            episodeLog.veto(LAPSED, jobDataMap);
            return true;
        }
        String taskName = jobDataMap.getString(JOB_DATA_TASK_KEY);
        try {
            long fireKey = computeFireKey(trigger, executionContext);
            ClaimOutcome claimOutcome = taskStore.claimFire(taskName, localNodeId, fireKey,
                    jobDataMap.getLong(JOB_DATA_OWNER_EPOCH), jobDataMap.getInt(JOB_DATA_INCARNATION),
                    jobDataMap.getString(JOB_DATA_OWNER_BOOT_ID), jobDataMap.getString(JOB_DATA_SCHEDULE_FP),
                    jobDataMap.getString(JOB_DATA_TRIGGER_FAMILY), System.currentTimeMillis());
            if (claimOutcome.state != ClaimOutcome.ClaimState.WON) {
                episodeLog.veto(claimOutcome.reason.name(), jobDataMap);
                return true;
            }
            episodeLog.cleared(taskName);
            return false;                     // Only a confirmed claim winner may enter the task body.
        } catch (Throwable claimFailure) {               // An uncertain claim must not become a retryable fire.
            episodeLog.veto(CLAIM_UNCONFIRMED, jobDataMap, claimFailure);
            return true;                      // Preserve at-most-once safety by vetoing on doubt.
        }
    }

    /**
     * Builds the cluster-wide occurrence identity on the firing thread. Cron tasks use Quartz scheduled fire
     * time. Repeating simple triggers use the start of the current UTC interval bucket. A one-shot uses zero and relies
     * on incarnation to distinguish an operator-authorized refire. No runtime clock adjustment is applied here; the
     * deployment clock-skew precondition keeps nodes in the same interval bucket.
     */
    private long computeFireKey(Trigger trigger, JobExecutionContext executionContext) {

        if (trigger instanceof CronTrigger) {
            return executionContext.getScheduledFireTime().getTime();
        }
        if (trigger instanceof SimpleTrigger) {
            long repeatIntervalMillis = ((SimpleTrigger) trigger).getRepeatInterval();
            if (repeatIntervalMillis > 0) {
                long currentTimeMillis = System.currentTimeMillis();
                return currentTimeMillis - Math.floorMod(currentTimeMillis, repeatIntervalMillis);
            }
        }
        return 0L;
    }

}

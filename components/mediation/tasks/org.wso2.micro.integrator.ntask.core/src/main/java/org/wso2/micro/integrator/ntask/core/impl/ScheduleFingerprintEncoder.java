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

import org.wso2.micro.integrator.ntask.common.TaskConstants;
import org.wso2.micro.integrator.ntask.core.TaskInfo;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;

/**
 * Produces the canonical, versioned schedule fingerprint stored in SCHEDULE_FP. Registration, boot repair,
 * ownership publication, Quartz fence stamping, and operator reconfiguration all use this single encoding. The
 * canonical input always contains the same ten fields in the same order, separated by LF with no trailing newline.
 * Values come from the authored TriggerInfo rather than computed next-fire times or runtime defaults; an unavailable
 * field is encoded as ABSENT. Exact comparison of the complete v1:sha256 value prevents mixed encodings from being
 * treated as the same task definition.
 */
public final class ScheduleFingerprintEncoder {

    private static final String ABSENT = "ABSENT";

    private ScheduleFingerprintEncoder() {
    }

    public static String encode(TaskInfo.TriggerInfo triggerInfo) {

        boolean cron = triggerInfo.getCronExpression() != null;
        StringBuilder canonical = new StringBuilder("enc=1");
        appendLine(canonical, "type", cron ? "CRON" : "SIMPLE");
        appendLine(canonical, "cron", cron ? triggerInfo.getCronExpression() : ABSENT);
        // The definition model declares no timezone field, so tz is ABSENT for every definition — safe
        // under the one-timezone-cluster-wide precondition carried by the config fingerprint.
        appendLine(canonical, "tz", ABSENT);
        appendLine(canonical, "interval", cron ? ABSENT : Long.toString(triggerInfo.getIntervalMillis()));
        appendLine(canonical, "count", cron ? ABSENT : Integer.toString(triggerInfo.getRepeatCount()));
        appendLine(canonical, "start", epochMillisOrAbsent(triggerInfo.getStartTime()));
        appendLine(canonical, "end", epochMillisOrAbsent(triggerInfo.getEndTime()));
        // Encode the declared policy name exactly as authored. Converting it to a resolved Quartz instruction
        // would fingerprint a runtime interpretation rather than the deployed definition.
        TaskConstants.TaskMisfirePolicy misfirePolicy = triggerInfo.getMisfirePolicy();
        appendLine(canonical, "misfire", misfirePolicy == null ? ABSENT : misfirePolicy.name());
        appendLine(canonical, "occv", "1");
        return "v1:sha256:" + lowercaseHex(sha256(canonical.toString()));
    }

    private static void appendLine(StringBuilder canonical, String field, String value) {

        if (value.indexOf('\n') >= 0) {
            throw new IllegalArgumentException(
                    "Declared value of schedule field [" + field + "] contains an LF and cannot be canonicalized.");
        }
        canonical.append('\n').append(field).append('=').append(value);
    }

    private static String epochMillisOrAbsent(Date declared) {

        return declared == null ? ABSENT : Long.toString(declared.getTime());
    }

    private static byte[] sha256(String canonicalString) {

        try {
            return MessageDigest.getInstance("SHA-256").digest(canonicalString.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable.", e);
        }
    }

    private static String lowercaseHex(byte[] digest) {

        StringBuilder hex = new StringBuilder(digest.length * 2);
        for (byte digestByte : digest) {
            hex.append(Character.forDigit((digestByte >> 4) & 0xF, 16)).append(Character.forDigit(digestByte & 0xF, 16));
        }
        return hex.toString();
    }

}

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
package org.wso2.micro.integrator.ntask.core.internal;

import org.wso2.micro.integrator.ntask.coordination.TaskCoordinationException;
import org.wso2.micro.integrator.ntask.core.CoordinationReadinessRegistry;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * The canonical config-fingerprint producer — built once at startup from the immutable
 * CoordinationHardeningConfig snapshot and stored in NODE_ADVERTISEMENT. A stable k=v string with,
 * today, exactly one field: tz=&lt;JVM zone id&gt; — the one correctness-bearing value that is
 * environmental and can genuinely differ between two correctly-configured nodes. The k=v envelope
 * and whole-string equality are kept deliberately: any future field addition still surfaces as a
 * whole-string mismatch against older binaries. Everything else earns its absence: barrier and
 * syncInject are locally mandated true by the hardened-profile validation; version/capability
 * constants cannot differ between same-patch nodes; the heartbeat window has its own always-WARN
 * comparison (Join Check).
 */
public final class ConfigFingerprint {

    // The stored bound: NODE_ADVERTISEMENT.CONFIG_FINGERPRINT VARCHAR(512).
    private static final int MAX_UTF8_BYTES = 512;
    private static final String CONFIG_FENCE_BYTE_LENGTH_CONDITION = "config-fence-byte-length";
    private static final String ENTRY_SEPARATOR = ";";

    private ConfigFingerprint() {

    }

    /**
     * Builds the canonical fingerprint from the startup snapshot. The UTF-8 byte-length check refuses
     * coordination (readiness ERROR) beyond the stored column bound — refusal, never truncation.
     */
    public static String build(CoordinationHardeningConfig config) throws TaskCoordinationException {

        String fingerprint = "tz=" + config.getZoneId().getId();
        int bytes = fingerprint.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > MAX_UTF8_BYTES) {
            String message = "The config fingerprint [" + fingerprint + "] is [" + bytes
                    + "] UTF-8 bytes — longer than the stored CONFIG_FINGERPRINT bound (VARCHAR(" + MAX_UTF8_BYTES
                    + ")). Refusing coordination — refusal, never truncation.";
            TasksDSComponent.getReadinessRegistry().raise(CONFIG_FENCE_BYTE_LENGTH_CONDITION,
                    CoordinationReadinessRegistry.ConditionClass.GATE, message);
            throw new TaskCoordinationException(message);
        }
        return fingerprint;
    }

    /**
     * Names the keys whose values differ between two canonical fingerprints (a key present on one side
     * only counts as differing). The comparison contract itself is whole-string equality; this only
     * names the keys for the refusal detail.
     */
    public static Set<String> differingKeys(String mine, String theirs) {

        Map<String, String> myEntries = parseEntries(mine);
        Map<String, String> theirEntries = parseEntries(theirs);
        Set<String> keys = new TreeSet<>();
        keys.addAll(myEntries.keySet());
        keys.addAll(theirEntries.keySet());
        Set<String> differing = new TreeSet<>();
        for (String key : keys) {
            if (!Objects.equals(myEntries.get(key), theirEntries.get(key))) {
                differing.add(key);
            }
        }
        return differing;
    }

    private static Map<String, String> parseEntries(String fingerprint) {

        Map<String, String> entries = new LinkedHashMap<>();
        for (String entry : fingerprint.split(ENTRY_SEPARATOR)) {
            int eq = entry.indexOf('=');
            if (eq < 0) {
                entries.put(entry, "");
            } else {
                entries.put(entry.substring(0, eq), entry.substring(eq + 1));
            }
        }
        return entries;
    }
}

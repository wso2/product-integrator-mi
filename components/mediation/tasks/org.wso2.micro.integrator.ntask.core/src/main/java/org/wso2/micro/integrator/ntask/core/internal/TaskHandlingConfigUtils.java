/*
 * Copyright (c) 2024, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.config.mapper.ConfigParser;

import java.util.Map;

/**
 * Utility methods for task handling configuration flags loaded from deployment.toml.
 */
public final class TaskHandlingConfigUtils {

    private static final Log LOG = LogFactory.getLog(TaskHandlingConfigUtils.class);
    private static final String TASK_HANDLING_CONFIG = "task_handling";
    public static final String TASK_DELETE_BARRIER_ENABLED_CONFIG =
            TASK_HANDLING_CONFIG + ".enable_task_delete_barrier";

    private TaskHandlingConfigUtils() {

    }

    /**
     * Returns whether clustered task delete barrier flow is enabled.
     * Defaults to true unless explicitly disabled via deployment.toml.
     *
     * @return true when barrier flow is enabled (default), false only when explicitly disabled in deployment.toml
     */
    public static boolean isTaskDeleteBarrierEnabled() {
        try {
            Map<String, Object> configs = ConfigParser.getParsedConfigs();
            if (configs == null) {
                return true;
            }
            Object value = configs.get(TASK_DELETE_BARRIER_ENABLED_CONFIG);
            if (value == null) {
                return true;
            }
            if (value instanceof Boolean) {
                return (Boolean) value;
            }
            return Boolean.parseBoolean(value.toString());
        } catch (Throwable throwable) {
            LOG.warn("Unable to read deployment.toml config [" + TASK_DELETE_BARRIER_ENABLED_CONFIG
                    + "]. Defaulting to enabled barrier flow.");
            return true;
        }
    }
}

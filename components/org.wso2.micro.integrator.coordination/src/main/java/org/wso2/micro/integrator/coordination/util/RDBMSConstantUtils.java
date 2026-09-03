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

package org.wso2.micro.integrator.coordination.util;

/**
 * Contains constant values needed for the coordination mechanism.
 */
public class RDBMSConstantUtils {

    // Cluster node status table columns
    public static final String GROUP_ID = "GROUP_ID";
    public static final String TASK_MARK_NODE_NOT_NEW = "marking node as not new";

    //columns for cluster membership communication
    public static final String MEMBERSHIP_CHANGE_TYPE = "CHANGE_TYPE";
    public static final String MEMBERSHIP_CHANGED_MEMBER_ID = "CHANGED_MEMBER_ID";

    public static final String TASK_ADD_MESSAGE_ID = "adding message id";
    public static final String TASK_CHECK_IS_COORDINATOR = "checking is coordinator";
    public static final String TASK_CHECK_COORDINATOR_VALIDITY = "checking coordinator validity";
    public static final String TASK_UPDATE_COORDINATOR_HEARTBEAT = "updating coordinator heartbeat";
    public static final String TASK_GET_COORDINATOR_INFORMATION = "reading coordinator information";
    public static final String TASK_REMOVE_COORDINATOR = "removing coordinator heartbeat";
    public static final String TASK_REMOVE_COORDINATOR_SELF = "removing own coordinator row";
    public static final String TASK_READ_NODE_ADVERTISEMENTS = "reading node advertisements";
    public static final String TASK_UPDATE_NODE_HEARTBEAT = "updating node heartbeat";
    public static final String TASK_CREATE_NODE_HEARTBEAT = "creating node heartbeat";
    public static final String TASK_GET_ALL_QUEUES = "getting all queues";
    public static final String MP_GET_ALL_STATES = "getting states of all message processors";
    public static final String TASK_REMOVE_NODE_HEARTBEAT = "removing node heartbeat entry";

    public static final String HEART_BEAT_INTERVAL = "heartBeatInterval";
    public static final String HEART_BEAT_MAX_RETRY = "heartbeatMaxRetry";
    public static final String LOCAL_GROUP_ID = "localGroupId";
    public static final String SCHEDULED_PERIOD = "scheduledPeriod";
    public static final String NODE_ID = "nodeId";
    public static final String NODE_ID_SYSTEM_PROPERTY = "node.id";
    public static final String CLUSTER_CONFIG = "cluster_config";
    public static final String NODE_ID_CONFIG_NAME = "node_id";

    public static final int DEFAULT_HEART_BEAT_INTERVAL = 5000;
    public static final int DEFAULT_HEART_BEAT_MAX_RETRY = 3;
    public static final String DEFAULT_LOCAL_GROUP_ID = "default";
    public static final int DEFAULT_SCHEDULED_PERIOD_INTERVAL = 1000;

    public static final String COORDINATION_DB_NAME = "WSO2_COORDINATION_DB";

}

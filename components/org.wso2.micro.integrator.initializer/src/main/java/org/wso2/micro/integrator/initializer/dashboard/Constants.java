package org.wso2.micro.integrator.initializer.dashboard;

public class Constants {

    private Constants() {

    }

    public static final String PRODUCT_MI = "mi";
    public static final String NODE_ID_SYSTEM_PROPERTY = "node.id";
    public static final String DASHBOARD_CONFIG_URL = "dashboard_config.dashboard_url";
    public static final String DASHBOARD_CONFIG_GROUP_ID = "dashboard_config.group_id";
    public static final String DASHBOARD_CONFIG_NODE_ID = "dashboard_config.node_id";
    public static final String DASHBOARD_CONFIG_HEARTBEAT_INTERVAL = "dashboard_config.heartbeat_interval";
    public static final String DASHBOARD_CONFIG_MANAGEMENT_HOSTNAME = "dashboard_config.management_hostname";
    public static final String DASHBOARD_CONFIG_MANAGEMENT_PORT = "dashboard_config.management_port";

    // New ICP Configuration
    public static final String ICP_API_DEFAULT_HOST = "localhost";
    public static final int ICP_API_DEFAULT_PORT = 9164;
    public static final String PORT_OFFSET = "server.offset";
    public static final String HOSTNAME = "server.hostname";
    public static final String ICP_CONFIG_URL = "icp_config.icp_url";
    public static final String ICP_CONFIG_ENVIRONMENT = "icp_config.environment";
    public static final String ICP_CONFIG_PROJECT = "icp_config.project";
    public static final String ICP_CONFIG_COMPONENT = "icp_config.integration";
    public static final String ICP_CONFIG_RUNTIME = "icp_config.runtime";
    public static final String ICP_CONFIG_ENABLED = "icp_config.enabled";
    public static final String  ICP_CONFIG_HEARTBEAT_INTERVAL = "icp_config.heartbeat_interval";
    public static final String ICP_CONFIG_SSL_VERIFY = "icp_config.ssl_verify";

    // JWT Configuration
    public static final String ICP_JWT_ISSUER = "icp_config.jwt_issuer";
    public static final String ICP_JWT_AUDIENCE = "icp_config.jwt_audience";
    public static final String ICP_JWT_SCOPE = "icp_config.jwt_scope";
    public static final String ICP_JWT_EXPIRY_SECONDS = "icp_config.jwt_expiry_seconds";
    public static final String ICP_SHARED_SECRET = "icp_config.secret";
    public static final String ICP_JWT_CLOCK_SKEW_TOLERANCE_MS = "icp_config.jwt_clock_skew_tolerance_ms";
    
    // Default ICP Configuration
    public static final String DEFAULT_ENVIRONMENT = "production";
    public static final String DEFAULT_PROJECT = "default";
    public static final String DEFAULT_COMPONENT = "default";
    public static final String DEFAULT_ICP_URL = "https://localhost:9445";

    public static final String DEFAULT_JWT_ISSUER = "icp-runtime-jwt-issuer";
    public static final String DEFAULT_JWT_AUDIENCE = "icp-server";
    public static final String DEFAULT_JWT_SCOPE = "runtime_agent";
    public static final long DEFAULT_JWT_EXPIRY_SECONDS = 3600;

    public static final long DEFAULT_JWT_CLOCK_SKEW_TOLERANCE_MS = 60_000L;
    public static final String RUNTIME_ID = "runtimeId";
    public static final String RUNTIME_TYPE_MI = "MI";
    public static final String RUNTIME_STATUS_RUNNING = "RUNNING";
    public static final String HEARTBEAT_VERSION = "v1.0";

    public static final String DEFAULT_GROUP_ID = "default";
    public static final long DEFAULT_HEARTBEAT_INTERVAL = 10;

    public static final String HEADER_VALUE_APPLICATION_JSON = "application/json";
    public static final String FORWARD_SLASH = "/";
    public static final String COLON = ":";
    public static final String HTTPS_PREFIX = "https://";
    public static final String MANAGEMENT = "management";
    
    // ICP Endpoints
    public static final String ICP_HEARTBEAT_ENDPOINT = "/icp/heartbeat";
    public static final String ICP_DELTA_HEARTBEAT_ENDPOINT = "/icp/deltaHeartbeat";

    public static final String ENDPOINT_TEMPLATE_TYPE = "endpoint";
    public static final String SEQUENCE_TEMPLATE_TYPE = "sequence";

    // JSON Response Field Names
    public static final String JSON_FIELD_BODY = "body";
    public static final String JSON_FIELD_MESSAGE = "message";

    // Heartbeat Payload Field Names
    public static final String FIELD_RUNTIME_HASH = "runtimeHash";
}

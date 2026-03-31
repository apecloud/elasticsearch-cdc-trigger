package com.clougence.cloudcanal.es6.sink;

public class CcEs6SinkConstant {

    public static final String SINK_STATE_IDX = "ape_es_trigger_sink_state";

    public static final String SINK_STATE_DOC_ID = "default";

    public static final String SINK_DOC_TYPE = "_doc";

    public static final String SRC_CONF_PREFIX = "node";

    public static final String SOURCE_TRIGGER_IDX_HOST_KEY = SRC_CONF_PREFIX + ".source_trigger_idx_host";

    public static final String SOURCE_TRIGGER_IDX_USER_KEY = SRC_CONF_PREFIX + ".source_trigger_idx_user";

    public static final String SOURCE_TRIGGER_IDX_PASSWD_KEY = SRC_CONF_PREFIX + ".source_trigger_idx_password";

    public static final String SOURCE_TRIGGER_IDX_NAME_KEY = SRC_CONF_PREFIX + ".source_trigger_idx_name";

    public static final String SOURCE_TRIGGER_START_TIME_KEY = SRC_CONF_PREFIX + ".source_trigger_start_time";

    public static final String SOURCE_TRIGGER_POLL_INTERVAL_MS_KEY = SRC_CONF_PREFIX + ".source_trigger_poll_interval_ms";

    public static final String SOURCE_TRIGGER_IDLE_POLL_INTERVAL_MS_KEY = SRC_CONF_PREFIX + ".source_trigger_idle_poll_interval_ms";

    public static final String SOURCE_TRIGGER_ERROR_BACKOFF_MS_KEY = SRC_CONF_PREFIX + ".source_trigger_error_backoff_ms";

    public static final String SOURCE_TRIGGER_BATCH_SIZE_KEY = SRC_CONF_PREFIX + ".source_trigger_batch_size";

    private CcEs6SinkConstant() {
    }
}

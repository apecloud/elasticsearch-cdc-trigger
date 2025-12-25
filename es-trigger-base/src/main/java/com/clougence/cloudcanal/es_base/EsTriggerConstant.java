package com.clougence.cloudcanal.es_base;

/**
 * @author bucketli 2024/7/30 14:16:15
 */
public class EsTriggerConstant {

    public static final String NODE_CONF_PREFIX = "node";

    public static final String TRIGGER_IDX_HOST_KEY = NODE_CONF_PREFIX + ".trigger_idx_host";

    public static final String TRIGGER_IDX_USER_KEY = NODE_CONF_PREFIX + ".trigger_idx_user";

    public static final String TRIGGER_IDX_PASSWD_KEY = NODE_CONF_PREFIX + ".trigger_idx_password";

    public static final String NODE_TRIGGER_IDXS = NODE_CONF_PREFIX + ".cdc_enable_idxs";

    public static final String ES_TRIGGER_IDX = "ape_es_trigger_idx";

    public static final String IDX_ENABLE_CDC_CONF_KEY = "index.cdc_enabled";

    public static final String TRIGGER_IDX_MAX_SCN_KEY = "index.cdc_max_scn";

}

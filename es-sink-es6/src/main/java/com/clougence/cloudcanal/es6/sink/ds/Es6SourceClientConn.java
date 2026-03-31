package com.clougence.cloudcanal.es6.sink.ds;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.ClusterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clougence.cloudcanal.es6.sink.CcEs6SinkConstant;
import com.clougence.cloudcanal.es6.sink.CcEs6SinkPlugin;
import com.clougence.cloudcanal.es_base.EsConnConfig;

import lombok.Getter;

public class Es6SourceClientConn {

    private static final Logger log = LoggerFactory.getLogger(Es6SourceClientConn.class);

    public static final Es6SourceClientConn instance = new Es6SourceClientConn();

    @Getter
    private RestHighLevelClient esClient;

    private static final Map<String, Object> configInMem = new ConcurrentHashMap<>();

    private Es6SourceClientConn() {
    }

    public void initFromSettings(ClusterSettings settings) {
        configInMem.put(CcEs6SinkConstant.SOURCE_TRIGGER_IDX_HOST_KEY, settings.get(CcEs6SinkPlugin.sourceTriggerIdxHost));
        configInMem.put(CcEs6SinkConstant.SOURCE_TRIGGER_IDX_USER_KEY, settings.get(CcEs6SinkPlugin.sourceTriggerIdxUser));
        configInMem.put(CcEs6SinkConstant.SOURCE_TRIGGER_IDX_PASSWD_KEY, settings.get(CcEs6SinkPlugin.sourceTriggerIdxPassword));

        refreshByConfig(configInMem);
    }

    public void refreshByConfig(Map<String, Object> config) {
        String hosts = (String) config.get(CcEs6SinkConstant.SOURCE_TRIGGER_IDX_HOST_KEY);
        if (StringUtils.isBlank(hosts)) {
            log.warn("{} is blank, skip source client create.", CcEs6SinkConstant.SOURCE_TRIGGER_IDX_HOST_KEY);
            return;
        }

        EsConnConfig connConfig = new EsConnConfig();
        connConfig.setHosts(hosts);
        connConfig.setUserName((String) config.get(CcEs6SinkConstant.SOURCE_TRIGGER_IDX_USER_KEY));
        connConfig.setPassword((String) config.get(CcEs6SinkConstant.SOURCE_TRIGGER_IDX_PASSWD_KEY));
        reCreateEsClient(connConfig);
    }

    protected synchronized void reCreateEsClient(EsConnConfig connConfig) {
        try {
            try {
                if (esClient != null) {
                    esClient.close();
                }
            } catch (Exception e) {
                log.warn("Close source client failed but ignore.msg:{}", ExceptionUtils.getRootCauseMessage(e));
            }

            esClient = Es6SourceClientHelper.generateEsClient(connConfig);
            log.info("Source es client create successfully.");
        } catch (Exception e) {
            log.error("Create source es client failed,msg:{}", ExceptionUtils.getRootCauseMessage(e), e);
        }
    }
}

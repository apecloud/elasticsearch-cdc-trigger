package com.clougence.cloudcanal.es6.sink;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clougence.cloudcanal.es6.sink.ds.Es6SourceClientConn;
import com.clougence.cloudcanal.es_base.EsTriggerConstant;

public class CcEs6SinkPlugin extends Plugin {

    private static final Logger log = LoggerFactory.getLogger(CcEs6SinkPlugin.class);

    public static final Setting<Boolean> triggerEnabled = Setting
            .boolSetting(CcEs6SinkConstant.SOURCE_TRIGGER_ENABLED_KEY, false, Property.NodeScope, Property.Dynamic);
    public static final Setting<String> sourceTriggerIdxHost = Setting.simpleString(
            CcEs6SinkConstant.SOURCE_TRIGGER_IDX_HOST_KEY, Property.NodeScope);
    public static final Setting<String> sourceTriggerIdxUser = Setting.simpleString(
            CcEs6SinkConstant.SOURCE_TRIGGER_IDX_USER_KEY, Property.NodeScope);
    public static final Setting<String> sourceTriggerIdxPassword = Setting.simpleString(
            CcEs6SinkConstant.SOURCE_TRIGGER_IDX_PASSWD_KEY, Property.NodeScope);
    public static final Setting<String> sourceTriggerIdxName = Setting.simpleString(
            CcEs6SinkConstant.SOURCE_TRIGGER_IDX_NAME_KEY, EsTriggerConstant.ES_TRIGGER_IDX, Property.NodeScope);
    public static final Setting<String> sourceTriggerStartTime = Setting.simpleString(
            CcEs6SinkConstant.SOURCE_TRIGGER_START_TIME_KEY, "", Property.NodeScope);
    public static final Setting<Integer> sourceTriggerPollIntervalMs = Setting.intSetting(
            CcEs6SinkConstant.SOURCE_TRIGGER_POLL_INTERVAL_MS_KEY, 1000, 1, Property.NodeScope);
    public static final Setting<Integer> sourceTriggerIdlePollIntervalMs = Setting.intSetting(
            CcEs6SinkConstant.SOURCE_TRIGGER_IDLE_POLL_INTERVAL_MS_KEY, 3000, 1000, Property.NodeScope);
    public static final Setting<Integer> sourceTriggerErrorBackoffMs = Setting.intSetting(
            CcEs6SinkConstant.SOURCE_TRIGGER_ERROR_BACKOFF_MS_KEY, 10000, 1, Property.NodeScope);
    public static final Setting<Integer> sourceTriggerBatchSize = Setting.intSetting(
            CcEs6SinkConstant.SOURCE_TRIGGER_BATCH_SIZE_KEY, 200, 1, Property.NodeScope);

    private final List<Setting<?>> settings = new ArrayList<>();

    private CcEs6SinkCoordinator sinkCoordinator;

    public CcEs6SinkPlugin() {
        settings.add(triggerEnabled);
        settings.add(sourceTriggerIdxHost);
        settings.add(sourceTriggerIdxUser);
        settings.add(sourceTriggerIdxPassword);
        settings.add(sourceTriggerIdxName);
        settings.add(sourceTriggerStartTime);
        settings.add(sourceTriggerPollIntervalMs);
        settings.add(sourceTriggerIdlePollIntervalMs);
        settings.add(sourceTriggerErrorBackoffMs);
        settings.add(sourceTriggerBatchSize);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return settings;
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService, ScriptService scriptService,
            NamedXContentRegistry xContentRegistry, Environment environment, NodeEnvironment nodeEnvironment,
            NamedWriteableRegistry namedWriteableRegistry) {
        try {
            Es6SourceClientConn.instance.initFromSettings(clusterService.getClusterSettings());
            CcEs6SinkConfig config = buildConfig(clusterService);
            log.info("{} createComponents", getClass().getSimpleName());
            sinkCoordinator = new CcEs6SinkCoordinator(clusterService, threadPool, client, config,
                    clusterService.getClusterSettings().get(triggerEnabled));
            clusterService.addLocalNodeMasterListener(sinkCoordinator);
            clusterService.getClusterSettings().addSettingsUpdateConsumer(triggerEnabled, sinkCoordinator::setEnabled);
            if (clusterService.state().nodes().isLocalNodeElectedMaster()) {
                sinkCoordinator.onMaster();
            }
        } catch (Exception e) {
            log.error("Create sink components failed but ignore.msg:{}", ExceptionUtils.getRootCauseMessage(e), e);
        }
        return super.createComponents(client, clusterService, threadPool, resourceWatcherService, scriptService,
                xContentRegistry, environment, nodeEnvironment, namedWriteableRegistry);
    }

    private CcEs6SinkConfig buildConfig(ClusterService clusterService) {
        CcEs6SinkConfig config = new CcEs6SinkConfig();
        config.setSourceHosts(clusterService.getClusterSettings().get(sourceTriggerIdxHost));
        config.setSourceUser(clusterService.getClusterSettings().get(sourceTriggerIdxUser));
        config.setSourcePassword(clusterService.getClusterSettings().get(sourceTriggerIdxPassword));
        config.setSourceIndexName(clusterService.getClusterSettings().get(sourceTriggerIdxName));
        config.setStartTime(clusterService.getClusterSettings().get(sourceTriggerStartTime));
        config.setPollIntervalMs(clusterService.getClusterSettings().get(sourceTriggerPollIntervalMs));
        config.setIdlePollIntervalMs(clusterService.getClusterSettings().get(sourceTriggerIdlePollIntervalMs));
        config.setErrorBackoffMs(clusterService.getClusterSettings().get(sourceTriggerErrorBackoffMs));
        config.setBatchSize(clusterService.getClusterSettings().get(sourceTriggerBatchSize));
        return config;
    }
}

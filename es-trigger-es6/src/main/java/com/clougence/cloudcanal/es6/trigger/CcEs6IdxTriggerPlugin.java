package com.clougence.cloudcanal.es6.trigger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clougence.cloudcanal.es6.trigger.ds.Es6ClientConn;
import com.clougence.cloudcanal.es6.trigger.writer.CcEs6TriggerIdxWriterImpl;
import com.clougence.cloudcanal.es_base.CcEsTriggerIdxWriter;
import com.clougence.cloudcanal.es_base.EsTriggerConstant;

/**
 * @author bucketli 2024/6/26 18:38:57
 */
public class CcEs6IdxTriggerPlugin extends Plugin {

    private static final Logger log = LoggerFactory.getLogger(CcEs6IdxTriggerPlugin.class);

    private final List<Setting<?>> settings = new ArrayList<>();

    public static final Setting<String> nodeTriggerIdxs = Setting.simpleString(EsTriggerConstant.NODE_TRIGGER_IDXS, Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<String> triggerIdxHost = Setting.simpleString(EsTriggerConstant.TRIGGER_IDX_HOST_KEY,
            Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<String> triggerIdxUser = Setting.simpleString(EsTriggerConstant.TRIGGER_IDX_USER_KEY,
            Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<String> triggerIdxPassword = Setting.simpleString(
            EsTriggerConstant.TRIGGER_IDX_PASSWD_KEY, Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<String> triggerIdxMaxScn = Setting.simpleString(
            EsTriggerConstant.TRIGGER_IDX_MAX_SCN_KEY, Setting.Property.IndexScope, Setting.Property.Dynamic);
    public final Setting<Boolean> cdcEnableSetting = Setting
            .boolSetting(EsTriggerConstant.IDX_ENABLE_CDC_CONF_KEY, false, Setting.Property.IndexScope,
                    Setting.Property.Dynamic);

    private final CcEs6IndexLifecycleRegistry lifecycleRegistry = new CcEs6IndexLifecycleRegistry();

    private ClusterService clusterService;

    private CcEsTriggerIdxWriter triggerIdxWriter;

    public CcEs6IdxTriggerPlugin() {
        settings.add(nodeTriggerIdxs);
        settings.add(cdcEnableSetting);
        settings.add(triggerIdxHost);
        settings.add(triggerIdxUser);
        settings.add(triggerIdxPassword);
        settings.add(triggerIdxMaxScn);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return settings;
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        if (indexModule.getIndex().getName().equals(EsTriggerConstant.ES_TRIGGER_IDX)) {
            log.debug("Not subscribe " + EsTriggerConstant.ES_TRIGGER_IDX);
            return;
        }

        log.info("Add index listener,index:" + indexModule.getIndex());

        final CcEs6IdxOpListener cdcListener = new CcEs6IdxOpListener(indexModule, triggerIdxWriter, clusterService,
                lifecycleRegistry, clusterService.getClusterSettings());
        cdcListener.start();

        indexModule.addSettingsUpdateConsumer(cdcEnableSetting, cdcListener);
        indexModule.addIndexOperationListener(cdcListener);
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService,
            ScriptService scriptService, NamedXContentRegistry xContentRegistry, Environment environment,
            NodeEnvironment nodeEnvironment,
            NamedWriteableRegistry namedWriteableRegistry) {
        log.info(this.getClass().getSimpleName() + " createComponents");
        try {
            this.clusterService = clusterService;
            Es6ClientConn.instance.addHostSettingConsumer(clusterService.getClusterSettings());
            initIdxWriter();
            ClusterState currentState = clusterService.state();
            lifecycleRegistry.bootstrap(currentState);
            boolean recoveryComplete = currentState != null
                    && currentState.blocks() != null
                    && !currentState.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK);
            clusterService.addListener(new CcEs6ClusterStateListener(triggerIdxWriter, lifecycleRegistry,
                    clusterService.getClusterSettings(), recoveryComplete));
        } catch (Exception e) {
            // not throw, or will make ElasticSearch node fail.
            log.error("Create components FAILED,but ignore.msg:" + ExceptionUtils.getRootCauseMessage(e), e);
        }

        return super.createComponents(client, clusterService, threadPool, resourceWatcherService, scriptService,
                xContentRegistry, environment, nodeEnvironment, namedWriteableRegistry);
    }

    protected synchronized void initIdxWriter() {
        if (triggerIdxWriter == null) {
            triggerIdxWriter = new CcEs6TriggerIdxWriterImpl();
            triggerIdxWriter.start();
        }
    }
}

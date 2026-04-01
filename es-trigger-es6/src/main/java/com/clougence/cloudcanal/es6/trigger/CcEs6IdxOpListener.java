package com.clougence.cloudcanal.es6.trigger;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.commons.lang3.StringUtils;
import com.clougence.cloudcanal.es_base.CcEsTriggerIdxWriter;
import com.clougence.cloudcanal.es_base.ComponentLifeCycle;
import com.clougence.cloudcanal.es_base.EsTriggerConstant;
import com.clougence.cloudcanal.es_base.TriggerEventType;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author bucketli 2024/6/26 17:48:55
 */
public class CcEs6IdxOpListener implements IndexingOperationListener, ComponentLifeCycle, Consumer<Boolean> {

    private static final Logger        log    = LoggerFactory.getLogger(CcEs6IdxOpListener.class);

    private static final AtomicBoolean inited = new AtomicBoolean(false);

    private final IndexModule          indexModule;

    private final CcEsTriggerIdxWriter ccEsTriggerIdxWriter;

    private final ClusterService clusterService;

    private final CcEs6IndexLifecycleRegistry lifecycleRegistry;

    private final ClusterSettings clusterSettings;

    private volatile String cachedNodeTriggerIdxs;

    public CcEs6IdxOpListener(IndexModule indexModule, CcEsTriggerIdxWriter ccEsTriggerIdxWriter,
            ClusterService clusterService, CcEs6IndexLifecycleRegistry lifecycleRegistry,
            ClusterSettings clusterSettings){
        this.indexModule = indexModule;
        this.ccEsTriggerIdxWriter = ccEsTriggerIdxWriter;
        this.clusterService = clusterService;
        this.lifecycleRegistry = lifecycleRegistry;
        this.clusterSettings = clusterSettings;
    }

    @Override
    public void accept(Boolean cdcEnabled) {
    }

    @Override
    public void start() {
        if (inited.compareAndSet(false, true)) {
            log.info("Component " + this.getClass().getSimpleName() + " start successfully.");
        }

        try {
            if (clusterSettings != null) {
                this.cachedNodeTriggerIdxs = clusterSettings.get(CcEs6IdxTriggerPlugin.nodeTriggerIdxs);
                clusterSettings.addSettingsUpdateConsumer(CcEs6IdxTriggerPlugin.nodeTriggerIdxs, newValue -> {
                    this.cachedNodeTriggerIdxs = newValue;
                    log.info("nodeTriggerIdxs updated for index [{}]: {}", indexModule.getIndex().getName(),
                            newValue);
                });
            }
        } catch (Exception e) {
            log.error("Add settings update consumer failed, but ignore. msg:{}",
                    ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    @Override
    public void stop() {
        if (inited.compareAndSet(true, false)) {
            log.info("Component " + this.getClass().getSimpleName() + " stop successfully.");
        }
    }

    @Override
    public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
        try {
            //            log.info("receive DELETE event.");
            if (delete.origin() != Engine.Operation.Origin.PRIMARY // not primary shard
                || !isIdxCdcEnabled(shardId.getIndex().getName()) // not enable cdc
                || result.getFailure() != null // failed operation
                || !result.isFound()) { // not found
                return;
            }

            String indexName = shardId.getIndex().getName();
            String delId = delete.id();
            ensureCreateBeforeDml(indexName);

            if (log.isDebugEnabled()) {
                log.info("[DELETE] " + indexName + " data,pk:" + delId);
            }

            ccEsTriggerIdxWriter.insertTriggerIdx(indexName, TriggerEventType.DELETE, delId, null, delete.type());
        } catch (Exception e) {
            log.error("Handle DELETE event error,but ignore.msg:" + ExceptionUtils.getRootCauseMessage(e));
        }
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
        try {
            //            log.info("receive INDEX event.");
            if (index.origin() != Engine.Operation.Origin.PRIMARY // not primary shard
                || !isIdxCdcEnabled(shardId.getIndex().getName()) // cdc not enabled
                || result.getFailure() != null // has failure
                || result.getResultType() != Engine.Result.Type.SUCCESS) { // not success
                return;
            }

            String indexName = shardId.getIndex().getName();
            ensureCreateBeforeDml(indexName);
            ParsedDocument doc = index.parsedDoc();

            String docJson = null;
            if (doc != null && doc.source() != null) {
                docJson = doc.source().utf8ToString();
            }

            if (result.isCreated()) {
                if (log.isDebugEnabled()) {
                    log.debug("[INSERT] " + indexName + " data,seq:" + index.getIfSeqNo() + ",ts:" + index.getAutoGeneratedIdTimestamp() + ",ptm:" + index.getIfPrimaryTerm()
                              + ",data:" + docJson);
                }

                ccEsTriggerIdxWriter.insertTriggerIdx(indexName, TriggerEventType.INSERT, index.id(), docJson, index.type());
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[UPDATE] " + indexName + " data,seq:" + index.getIfSeqNo() + ",ts:" + index.getAutoGeneratedIdTimestamp() + ",ptm:" + index.getIfPrimaryTerm()
                              + ",data:" + docJson);
                }

                ccEsTriggerIdxWriter.insertTriggerIdx(indexName, TriggerEventType.UPDATE, index.id(), docJson, index.type());
            }
        } catch (Exception e) {
            log.error("Handle INDEX event error,but ignore.msg:" + ExceptionUtils.getRootCauseMessage(e));
        }
    }

    private void ensureCreateBeforeDml(String indexName) throws Exception {
        if (clusterService == null || lifecycleRegistry == null) {
            return;
        }

        IndexMetaData indexMetaData = clusterService.state().metaData().index(indexName);
        lifecycleRegistry.ensureCreateRecorded(indexName, indexMetaData, ccEsTriggerIdxWriter);
    }

    private boolean isIdxCdcEnabled(String indexName) {
        Boolean indexCdcEnabled = indexModule.getSettings().getAsBoolean(EsTriggerConstant.IDX_ENABLE_CDC_CONF_KEY,
                null);
        if (indexCdcEnabled != null) {
            return indexCdcEnabled;
        }

        String nodeTriggerIdxs = this.cachedNodeTriggerIdxs;
        if (StringUtils.isBlank(nodeTriggerIdxs) || StringUtils.isBlank(indexName)) {
            return false;
        }

        String[] cdcIdxs = nodeTriggerIdxs.split(",");
        for (String cdcIdx : cdcIdxs) {
            String trimmedIdx = cdcIdx.trim();
            if (StringUtils.isBlank(trimmedIdx)) {
                continue;
            }
            if ("*".equals(trimmedIdx)) {
                return true;
            }
            try {
                if (indexName.matches(trimmedIdx)) {
                    return true;
                }
            } catch (Exception e) {
                if (indexName.equals(trimmedIdx)) {
                    return true;
                }
            }
        }
        return false;
    }
}

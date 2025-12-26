package com.clougence.cloudcanal.es8.trigger;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clougence.cloudcanal.es_base.CcEsTriggerIdxWriter;
import com.clougence.cloudcanal.es_base.ComponentLifeCycle;
import com.clougence.cloudcanal.es_base.EsTriggerConstant;
import com.clougence.cloudcanal.es_base.TriggerEventType;

/**
 * @author bucketli 2024/6/26 17:48:55
 */
public class CcEs8IdxOpListener implements IndexingOperationListener, ComponentLifeCycle, Consumer<Boolean> {

    private static final Logger log = LoggerFactory.getLogger(CcEs8IdxOpListener.class);

    private static final AtomicBoolean inited = new AtomicBoolean(false);

    private final IndexModule indexModule;

    private final CcEsTriggerIdxWriter ccEsTriggerIdxWriter;

    private final ClusterSettings clusterSettings;

    // Cache nodeTriggerIdxs value to avoid frequent clusterSettings.get() calls in
    // high-frequency postIndex/postDelete
    private volatile String cachedNodeTriggerIdxs;

    public CcEs8IdxOpListener(IndexModule indexModule, CcEsTriggerIdxWriter ccEsTriggerIdxWriter,
            ClusterSettings clusterSettings) {
        this.indexModule = indexModule;
        this.ccEsTriggerIdxWriter = ccEsTriggerIdxWriter;
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
            this.cachedNodeTriggerIdxs = clusterSettings.get(CcEs8IdxTriggerPlugin.nodeTriggerIdxs);

            clusterSettings.addSettingsUpdateConsumer(
                    CcEs8IdxTriggerPlugin.nodeTriggerIdxs,
                    newValue -> {
                        this.cachedNodeTriggerIdxs = newValue;
                        log.info("nodeTriggerIdxs updated for index [{}]: {}",
                                indexModule.getIndex().getName(), newValue);
                    });
        } catch (Exception e) {
            log.error("Add settings update consumer failed, but ignore. msg: "
                    + ExceptionUtils.getRootCauseMessage(e), e);
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
            // log.info("receive DELETE event.");
            String indexName = shardId.getIndex().getName();

            if (delete.origin() != Engine.Operation.Origin.PRIMARY // not primary shard
                    || !isIdxCdcEnabled(indexName) // index cdc is not enabled
                    || result.getFailure() != null // failed operation
                    || !result.isFound()) { // not found
                return;
            }
            String delId = delete.id();

            if (log.isDebugEnabled()) {
                log.info("[DELETE] " + indexName + " data,pk:" + delId);
            }

            ccEsTriggerIdxWriter.insertTriggerIdx(indexName, TriggerEventType.DELETE, delId, null);
        } catch (Exception e) {
            log.error("Handle DELETE event error,but ignore.msg:" + ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
        try {
            // log.info("receive INDEX event.");
            String indexName = shardId.getIndex().getName();

            if (index.origin() != Engine.Operation.Origin.PRIMARY // not primary shard
                    || !isIdxCdcEnabled(indexName) // index cdc is not enabled
                    || result.getFailure() != null // has failure
                    || result.getResultType() != Engine.Result.Type.SUCCESS) { // not success
                return;
            }
            ParsedDocument doc = index.parsedDoc();

            String docJson = null;
            if (doc != null && doc.source() != null) {
                docJson = doc.source().utf8ToString();
            }

            if (result.isCreated()) {
                if (log.isDebugEnabled()) {
                    log.debug("[INSERT] " + indexName + " data,seq:" + index.getIfSeqNo() + ",ts:"
                            + index.getAutoGeneratedIdTimestamp() + ",ptm:" + index.getIfPrimaryTerm()
                            + ",data:" + docJson);
                }

                ccEsTriggerIdxWriter.insertTriggerIdx(indexName, TriggerEventType.INSERT, index.id(), docJson);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[UPDATE] " + indexName + " data,seq:" + index.getIfSeqNo() + ",ts:"
                            + index.getAutoGeneratedIdTimestamp() + ",ptm:" + index.getIfPrimaryTerm()
                            + ",data:" + docJson);
                }

                ccEsTriggerIdxWriter.insertTriggerIdx(indexName, TriggerEventType.UPDATE, index.id(), docJson);
            }
        } catch (Exception e) {
            log.error("Handle INDEX event error,but ignore.msg:" + ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    /**
     * Check if CDC is enabled for the given index.
     * Priority:
     * 1. index.cdc_enabled=false -> return false (highest priority)
     * 2. index.cdc_enabled=true -> return true
     * 3. node.trigger_idxs contains current index -> return true
     * 4. Otherwise -> return false
     */
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

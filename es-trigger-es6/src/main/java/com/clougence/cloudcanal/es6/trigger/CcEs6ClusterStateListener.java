package com.clougence.cloudcanal.es6.trigger;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;
import com.clougence.cloudcanal.es_base.EsTriggerConstant;
import com.clougence.cloudcanal.es_base.CcEsTriggerIdxWriter;

/**
 * Records index create/delete events based on applied cluster state changes.
 * Uses GatewayService.STATE_NOT_RECOVERED_BLOCK as the gate to avoid replaying
 * historical events during cluster startup / recovery.
 */
public class CcEs6ClusterStateListener implements ClusterStateListener {

    private static final Logger log = LoggerFactory.getLogger(CcEs6ClusterStateListener.class);

    private final CcEsTriggerIdxWriter triggerIdxWriter;

    private final CcEs6IndexLifecycleRegistry lifecycleRegistry;
    private volatile String cachedNodeTriggerIdxs;
    private volatile boolean recoveryComplete;

    public CcEs6ClusterStateListener(CcEsTriggerIdxWriter triggerIdxWriter,
            CcEs6IndexLifecycleRegistry lifecycleRegistry, ClusterSettings clusterSettings,
            boolean recoveryComplete) {
        this.triggerIdxWriter = triggerIdxWriter;
        this.lifecycleRegistry = lifecycleRegistry;
        this.recoveryComplete = recoveryComplete;
        if (clusterSettings != null) {
            this.cachedNodeTriggerIdxs = clusterSettings.get(CcEs6IdxTriggerPlugin.nodeTriggerIdxs);
            clusterSettings.addSettingsUpdateConsumer(CcEs6IdxTriggerPlugin.nodeTriggerIdxs,
                    newValue -> this.cachedNodeTriggerIdxs = newValue);
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (!event.metaDataChanged()) {
            return;
        }

        boolean currentNotRecovered = event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK);

        if (currentNotRecovered) {
            bootstrapKnownIndices(event, "recovery in progress");
            return;
        }

        if (!recoveryComplete) {
            boolean previousNotRecovered = event.previousState().blocks()
                    .hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK);
            if (previousNotRecovered) {
                bootstrapKnownIndices(event, "recovery just completed");
                recoveryComplete = true;
                return;
            }

            bootstrapKnownIndices(event, "listener registered after recovery");
            recoveryComplete = true;
            return;
        }

        processClusterStateChange(event);
    }

    private void bootstrapKnownIndices(ClusterChangedEvent event, String reason) {
        int count = 0;
        for (IndexMetaData indexMetaData : event.state().metaData()) {
            String indexName = indexMetaData.getIndex().getName();
            if (shouldCaptureIndex(indexName)) {
                lifecycleRegistry.markKnown(indexName);
                count++;
            }
        }
        log.info("Bootstrap knownIndices from cluster state ({}), marked {} indices.", reason, count);
    }

    private void processClusterStateChange(ClusterChangedEvent event) {
        try {
            boolean isMaster = event.localNodeMaster();

            for (String indexName : event.indicesCreated()) {
                if (isMaster && shouldCaptureIndex(indexName)) {
                    IndexMetaData indexMetaData = event.state().metaData().index(indexName);
                    lifecycleRegistry.ensureCreateRecorded(indexName, indexMetaData, triggerIdxWriter);
                }
            }

            for (Index index : event.indicesDeleted()) {
                lifecycleRegistry.markUnknown(index.getName());
                if (isMaster && shouldCaptureIndex(index.getName())) {
                    lifecycleRegistry.writeDeleteEvent(index.getName(), triggerIdxWriter);
                }
            }

            if (!isMaster) {
                return;
            }

            for (IndexMetaData currentIndexMetaData : event.state().metaData()) {
                String indexName = currentIndexMetaData.getIndex().getName();
                if (event.indicesCreated().contains(indexName) || !shouldCaptureIndex(indexName)) {
                    continue;
                }

                IndexMetaData previousIndexMetaData = event.previousState().metaData().index(indexName);
                if (previousIndexMetaData == null) {
                    continue;
                }

                if (lifecycleRegistry.hasMappingsChanged(previousIndexMetaData, currentIndexMetaData)) {
                    lifecycleRegistry.writeMappingEvent(currentIndexMetaData, triggerIdxWriter);
                }
                if (lifecycleRegistry.hasAliasesChanged(previousIndexMetaData, currentIndexMetaData)) {
                    lifecycleRegistry.writeAliasesEvent(currentIndexMetaData, triggerIdxWriter);
                }
                // TODO: support update settings
                // if (lifecycleRegistry.hasSettingsChanged(previousIndexMetaData, currentIndexMetaData)) {
                //     lifecycleRegistry.writeSettingsEvent(currentIndexMetaData, triggerIdxWriter);
                // }
            }
        } catch (Exception e) {
            log.error("Handle cluster state change failed,but ignore.msg:" + ExceptionUtils.getRootCauseMessage(e),
                    e);
        }
    }

    private boolean shouldCaptureIndex(String indexName) {
        if (StringUtils.isBlank(indexName) || EsTriggerConstant.ES_TRIGGER_IDX.equals(indexName)) {
            return false;
        }

        String nodeTriggerIdxs = this.cachedNodeTriggerIdxs;
        if (StringUtils.isBlank(nodeTriggerIdxs)) {
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

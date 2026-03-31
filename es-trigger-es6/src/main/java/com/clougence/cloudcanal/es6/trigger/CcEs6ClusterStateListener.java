package com.clougence.cloudcanal.es6.trigger;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.index.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clougence.cloudcanal.es_base.CcEsTriggerIdxWriter;

/**
 * Records index create/delete events based on applied cluster state changes.
 */
public class CcEs6ClusterStateListener implements ClusterStateListener {

    private static final Logger log = LoggerFactory.getLogger(CcEs6ClusterStateListener.class);

    private final CcEsTriggerIdxWriter triggerIdxWriter;

    private final CcEs6IndexLifecycleRegistry lifecycleRegistry;

    public CcEs6ClusterStateListener(CcEsTriggerIdxWriter triggerIdxWriter,
            CcEs6IndexLifecycleRegistry lifecycleRegistry) {
        this.triggerIdxWriter = triggerIdxWriter;
        this.lifecycleRegistry = lifecycleRegistry;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (!event.metaDataChanged()) {
            return;
        }

        try {
            boolean isMaster = event.localNodeMaster();

            for (String indexName : event.indicesCreated()) {
                if (isMaster) {
                    IndexMetaData indexMetaData = event.state().metaData().index(indexName);
                    lifecycleRegistry.ensureCreateRecorded(indexName, indexMetaData, triggerIdxWriter);
                }
            }

            for (Index index : event.indicesDeleted()) {
                lifecycleRegistry.markUnknown(index.getName());
                if (isMaster) {
                    lifecycleRegistry.writeDeleteEvent(index.getName(), triggerIdxWriter);
                }
            }

            if (!isMaster) {
                return;
            }

            for (IndexMetaData currentIndexMetaData : event.state().metaData()) {
                String indexName = currentIndexMetaData.getIndex().getName();
                if (event.indicesCreated().contains(indexName)) {
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
}

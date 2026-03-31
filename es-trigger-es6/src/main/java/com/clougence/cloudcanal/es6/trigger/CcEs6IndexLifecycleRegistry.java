package com.clougence.cloudcanal.es6.trigger;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.clougence.cloudcanal.es_base.CcEsTriggerIdxWriter;
import com.clougence.cloudcanal.es_base.EsTriggerConstant;
import com.clougence.cloudcanal.es_base.TriggerEventType;

/**
 * Tracks known indices so DDL events can be deduplicated and CREATE_INDEX can be
 * enqueued before the first DML event of a new index.
 */
public class CcEs6IndexLifecycleRegistry {

    private static final Logger log = LoggerFactory.getLogger(CcEs6IndexLifecycleRegistry.class);

    private final Set<String> knownIndices = ConcurrentHashMap.newKeySet();

    public void bootstrap(ClusterState clusterState) {
        if (clusterState == null || clusterState.metaData() == null) {
            return;
        }

        for (IndexMetaData indexMetaData : clusterState.metaData()) {
            String indexName = indexMetaData.getIndex().getName();
            if (shouldIgnore(indexName)) {
                continue;
            }
            knownIndices.add(indexName);
        }
    }

    public void markKnown(String indexName) {
        if (!shouldIgnore(indexName)) {
            knownIndices.add(indexName);
        }
    }

    public void markUnknown(String indexName) {
        if (!shouldIgnore(indexName)) {
            knownIndices.remove(indexName);
        }
    }

    public synchronized void ensureCreateRecorded(String indexName, IndexMetaData indexMetaData,
            CcEsTriggerIdxWriter writer) {
        if (shouldIgnore(indexName) || writer == null || knownIndices.contains(indexName)) {
            return;
        }

        if (indexMetaData == null) {
            log.warn("Skip CREATE_INDEX event because metadata is null, idx_name:{}", indexName);
            return;
        }

        try {
            writer.insertTriggerIdx(indexName, TriggerEventType.CREATE_INDEX, indexName,
                    buildCreateIndexPayload(indexMetaData));
            knownIndices.add(indexName);
        } catch (Exception e) {
            log.warn("Write CREATE_INDEX event failed but ignore, idx_name:{}, msg:{}", indexName,
                    ExceptionUtils.getRootCauseMessage(e));
        }
    }

    public void writeDeleteEvent(String indexName, CcEsTriggerIdxWriter writer) {
        if (shouldIgnore(indexName) || writer == null) {
            return;
        }

        try {
            writer.insertTriggerIdx(indexName, TriggerEventType.DELETE_INDEX, indexName, null);
        } catch (Exception e) {
            log.warn("Write DELETE_INDEX event failed but ignore, idx_name:{}, msg:{}", indexName,
                    ExceptionUtils.getRootCauseMessage(e));
        }
    }

    public void writeMappingEvent(IndexMetaData indexMetaData, CcEsTriggerIdxWriter writer) {
        writeMetadataEvent(indexMetaData, writer, TriggerEventType.UPDATE_MAPPING, buildUpdateMappingPayload(indexMetaData));
    }

    public void writeSettingsEvent(IndexMetaData indexMetaData, CcEsTriggerIdxWriter writer) {
        writeMetadataEvent(indexMetaData, writer, TriggerEventType.UPDATE_SETTINGS, buildUpdateSettingsPayload(indexMetaData));
    }

    public void writeAliasesEvent(IndexMetaData indexMetaData, CcEsTriggerIdxWriter writer) {
        writeMetadataEvent(indexMetaData, writer, TriggerEventType.UPDATE_ALIASES, buildUpdateAliasesPayload(indexMetaData));
    }

    public boolean hasMappingsChanged(IndexMetaData previous, IndexMetaData current) {
        return !normalizeMappings(previous).equals(normalizeMappings(current));
    }

    public boolean hasSettingsChanged(IndexMetaData previous, IndexMetaData current) {
        return !normalizeSettings(previous).equals(normalizeSettings(current));
    }

    public boolean hasAliasesChanged(IndexMetaData previous, IndexMetaData current) {
        return !normalizeAliases(previous).equals(normalizeAliases(current));
    }

    private void writeMetadataEvent(IndexMetaData indexMetaData, CcEsTriggerIdxWriter writer, TriggerEventType eventType,
            String payload) {
        if (indexMetaData == null || writer == null || shouldIgnore(indexMetaData.getIndex().getName())) {
            return;
        }

        try {
            writer.insertTriggerIdx(indexMetaData.getIndex().getName(), eventType, indexMetaData.getIndex().getName(),
                    payload);
        } catch (Exception e) {
            log.warn("Write {} event failed but ignore, idx_name:{}, msg:{}", eventType,
                    indexMetaData.getIndex().getName(), ExceptionUtils.getRootCauseMessage(e));
        }
    }

    private boolean shouldIgnore(String indexName) {
        return StringUtils.isBlank(indexName) || EsTriggerConstant.ES_TRIGGER_IDX.equals(indexName);
    }

    private String buildCreateIndexPayload(IndexMetaData indexMetaData) {
        return buildIndexPayload(indexMetaData, true, true, true);
    }

    private String buildUpdateMappingPayload(IndexMetaData indexMetaData) {
        return buildIndexPayload(indexMetaData, false, true, false);
    }

    private String buildUpdateSettingsPayload(IndexMetaData indexMetaData) {
        return buildIndexPayload(indexMetaData, true, false, false);
    }

    private String buildUpdateAliasesPayload(IndexMetaData indexMetaData) {
        return buildIndexPayload(indexMetaData, false, false, true);
    }

    private String buildIndexPayload(IndexMetaData indexMetaData, boolean includeSettings, boolean includeMappings,
            boolean includeAliases) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.field("index", indexMetaData.getIndex().getName());
            if (includeSettings) {
                builder.field("settings", normalizeSettings(indexMetaData));
            }
            if (includeMappings) {
                builder.field("mappings", normalizeMappings(indexMetaData));
            }
            if (includeAliases) {
                builder.field("aliases", normalizeAliases(indexMetaData));
            }
            builder.endObject();
            return Strings.toString(builder);
        } catch (Exception e) {
            String msg = "Serialize index metadata failed,msg:" + ExceptionUtils.getRootCauseMessage(e);
            throw new RuntimeException(msg, e);
        }
    }

    Map<String, Object> normalizeSettings(IndexMetaData indexMetaData) {
        Map<String, Object> normalized = new TreeMap<>();
        if (indexMetaData == null || indexMetaData.getSettings() == null) {
            return normalized;
        }

        for (String key : indexMetaData.getSettings().keySet()) {
            if (isIgnoredSetting(key)) {
                continue;
            }
            normalized.put(key, String.valueOf(indexMetaData.getSettings().get(key)));
        }
        return normalized;
    }

    Map<String, Object> normalizeMappings(IndexMetaData indexMetaData) {
        Map<String, Object> normalized = new TreeMap<>();
        if (indexMetaData == null) {
            return normalized;
        }

        for (ObjectObjectCursor<String, MappingMetaData> cursor : indexMetaData.getMappings()) {
            normalized.put(cursor.key, normalizeMap(cursor.value.getSourceAsMap()));
        }
        return normalized;
    }

    Map<String, Object> normalizeAliases(IndexMetaData indexMetaData) {
        Map<String, Object> normalized = new TreeMap<>();
        if (indexMetaData == null) {
            return normalized;
        }

        for (ObjectObjectCursor<String, AliasMetaData> cursor : indexMetaData.getAliases()) {
            AliasMetaData aliasMetaData = cursor.value;
            Map<String, Object> aliasMap = new LinkedHashMap<>();
            if (aliasMetaData.filteringRequired() && aliasMetaData.filter() != null) {
                aliasMap.put("filter", normalizeFilter(aliasMetaData.filter()));
            }
            if (StringUtils.isNotBlank(aliasMetaData.indexRouting())) {
                aliasMap.put("index_routing", aliasMetaData.indexRouting());
            }
            if (StringUtils.isNotBlank(aliasMetaData.searchRouting())) {
                aliasMap.put("search_routing", aliasMetaData.searchRouting());
            }
            if (StringUtils.isNotBlank(aliasMetaData.indexRouting())
                    && StringUtils.equals(aliasMetaData.indexRouting(), aliasMetaData.searchRouting())) {
                aliasMap.put("routing", aliasMetaData.indexRouting());
            }
            if (aliasMetaData.writeIndex() != null) {
                aliasMap.put("is_write_index", aliasMetaData.writeIndex());
            }
            normalized.put(cursor.key, normalizeMap(aliasMap));
        }
        return normalized;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> normalizeFilter(CompressedXContent filter) {
        try {
            Tuple<XContentType, Map<String, Object>> tuple = XContentHelper.convertToMap(
                    new BytesArray(filter.uncompressed()), false, XContentType.JSON);
            return normalizeMap(tuple.v2());
        } catch (Exception e) {
            throw new RuntimeException("Parse alias filter failed,msg:" + ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> normalizeMap(Map<String, Object> source) {
        Map<String, Object> normalized = new TreeMap<>();
        if (source == null) {
            return normalized;
        }

        for (Map.Entry<String, Object> entry : source.entrySet()) {
            if ("_meta".equals(entry.getKey())) {
                continue;
            }

            Object value = entry.getValue();
            if (value instanceof Map) {
                normalized.put(entry.getKey(), normalizeMap((Map<String, Object>) value));
            } else {
                normalized.put(entry.getKey(), value);
            }
        }
        return normalized;
    }

    private boolean isIgnoredSetting(String key) {
        return key.startsWith("index.uuid")
                || key.startsWith("index.version")
                || key.startsWith("index.creation_date")
                || key.startsWith("index.provided_name")
                || key.startsWith("index.routing.allocation")
                || EsTriggerConstant.IDX_ENABLE_CDC_CONF_KEY.equals(key)
                || EsTriggerConstant.TRIGGER_IDX_MAX_SCN_KEY.equals(key);
    }
}

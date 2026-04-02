package com.clougence.cloudcanal.es6.sink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.clougence.cloudcanal.es_base.EsTriggerConstant;
import com.clougence.cloudcanal.es_base.TriggerEventType;

public class CcEs6ReplayService {

    private static final Logger log = LoggerFactory.getLogger(CcEs6ReplayService.class);

    private final Client client;

    public CcEs6ReplayService(Client client) {
        this.client = client;
    }

    public CcEs6ReplayResult replay(CcEs6TriggerEvent event) throws Exception {
        switch (event.getEventType()) {
            case INSERT:
            case UPDATE:
                replayUpsert(event);
                return CcEs6ReplayResult.applied();
            case DELETE:
                replayDelete(event);
                return CcEs6ReplayResult.applied();
            case CREATE_INDEX:
                return replayCreateIndex(event);
            case DELETE_INDEX:
                return replayDeleteIndex(event);
            case UPDATE_MAPPING:
                return replayUpdateMapping(event);
            case UPDATE_SETTINGS:
                return replayUpdateSettings(event);
            case UPDATE_ALIASES:
                return replayUpdateAliases(event);
            default:
                throw new IllegalStateException("Unsupported event type:" + event.getEventType());
        }
    }

    private void replayUpsert(CcEs6TriggerEvent event) {
        ensurePayloadPresent(event);
        String type = resolveDocType(event);
        IndexRequest request = new IndexRequest(event.getIndexName(), type, event.getPk());
        request.source(event.getRowData(), XContentType.JSON);
        client.index(request).actionGet();
    }

    private void replayDelete(CcEs6TriggerEvent event) {
        String type = resolveDocType(event);
        DeleteRequest request = new DeleteRequest(event.getIndexName(), type, event.getPk());
        client.delete(request).actionGet();
    }

    private String resolveDocType(CcEs6TriggerEvent event) {
        return StringUtils.isNotBlank(event.getDocType()) ? event.getDocType() : EsTriggerConstant.ES6_DOC_TYPE;
    }

    @SuppressWarnings("unchecked")
    private CcEs6ReplayResult replayCreateIndex(CcEs6TriggerEvent event) throws Exception {
        ensurePayloadPresent(event);
        Map<String, Object> payload = parsePayload(event);

        String indexName = event.getIndexName();
        Map<String, Object> settings = (Map<String, Object>) payload.get("settings");
        Map<String, Object> mappings = (Map<String, Object>) payload.get("mappings");
        Map<String, Object> aliases = (Map<String, Object>) payload.get("aliases");

        boolean exists = client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists();
        if (!exists) {
            CreateIndexRequest request = new CreateIndexRequest(indexName);
            if (settings != null) {
                request.settings(settings);
            }
            if (mappings != null) {
                for (Map.Entry<String, Object> entry : mappings.entrySet()) {
                    request.mapping(entry.getKey(), (Map<String, Object>) entry.getValue());
                }
            }
            if (aliases != null && !aliases.isEmpty()) {
                request.aliases(buildCreateIndexAliasesSource(aliases));
            }
            client.admin().indices().create(request).actionGet();
            return CcEs6ReplayResult.applied();
        }

        if (!isIndexStateCompatible(indexName, settings, mappings, aliases)) {
            return skip("Target index exists but incompatible, idx_name:" + indexName);
        }

        return CcEs6ReplayResult.applied();
    }

    private CcEs6ReplayResult replayDeleteIndex(CcEs6TriggerEvent event) {
        String indexName = event.getIndexName();
        boolean exists = client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists();
        if (!exists) {
            return CcEs6ReplayResult.applied();
        }
        client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
        return CcEs6ReplayResult.applied();
    }

    @SuppressWarnings("unchecked")
    private CcEs6ReplayResult replayUpdateMapping(CcEs6TriggerEvent event) throws Exception {
        ensurePayloadPresent(event);
        if (!indexExists(event.getIndexName())) {
            return skip("Target index missing for UPDATE_MAPPING, idx_name:" + event.getIndexName());
        }

        Map<String, Object> payload = parsePayload(event);
        Map<String, Object> sourceMappings = normalizeMappings((Map<String, Object>) payload.get("mappings"));
        if (Objects.equals(sourceMappings, fetchMappings(event.getIndexName()))) {
            return CcEs6ReplayResult.applied();
        }

        try {
            for (Map.Entry<String, Object> entry : sourceMappings.entrySet()) {
                PutMappingRequest request = new PutMappingRequest(event.getIndexName()).type(entry.getKey())
                        .source((Map<String, Object>) entry.getValue());
                AcknowledgedResponse response = client.admin().indices().putMapping(request).actionGet();
                if (!response.isAcknowledged()) {
                    return skip("Put mapping not acknowledged, idx_name:" + event.getIndexName());
                }
            }
        } catch (Exception e) {
            return skip("Put mapping failed, idx_name:" + event.getIndexName() + ", msg:" + rootMsg(e));
        }

        if (!Objects.equals(sourceMappings, fetchMappings(event.getIndexName()))) {
            return skip("Target mapping still incompatible after update, idx_name:" + event.getIndexName());
        }
        return CcEs6ReplayResult.applied();
    }

    @SuppressWarnings("unchecked")
    private CcEs6ReplayResult replayUpdateSettings(CcEs6TriggerEvent event) throws Exception {
        ensurePayloadPresent(event);
        if (!indexExists(event.getIndexName())) {
            return skip("Target index missing for UPDATE_SETTINGS, idx_name:" + event.getIndexName());
        }

        Map<String, Object> payload = parsePayload(event);
        Map<String, Object> sourceSettings = normalizeSettings((Map<String, Object>) payload.get("settings"));
        Map<String, Object> targetSettings = fetchSettings(event.getIndexName());
        Map<String, Object> changed = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : sourceSettings.entrySet()) {
            if (!Objects.equals(entry.getValue(), targetSettings.get(entry.getKey()))) {
                changed.put(entry.getKey(), entry.getValue());
            }
        }

        if (changed.isEmpty()) {
            return CcEs6ReplayResult.applied();
        }

        try {
            Settings.Builder builder = Settings.builder();
            for (Map.Entry<String, Object> entry : changed.entrySet()) {
                builder.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
            UpdateSettingsRequest request = new UpdateSettingsRequest(event.getIndexName()).settings(builder);
            AcknowledgedResponse response = client.admin().indices().updateSettings(request).actionGet();
            if (!response.isAcknowledged()) {
                return skip("Update settings not acknowledged, idx_name:" + event.getIndexName());
            }
        } catch (Exception e) {
            return skip("Update settings failed, idx_name:" + event.getIndexName() + ", msg:" + rootMsg(e));
        }

        if (!Objects.equals(sourceSettings, fetchSettings(event.getIndexName()))) {
            return skip("Target settings still incompatible after update, idx_name:" + event.getIndexName());
        }
        return CcEs6ReplayResult.applied();
    }

    @SuppressWarnings("unchecked")
    private CcEs6ReplayResult replayUpdateAliases(CcEs6TriggerEvent event) throws Exception {
        ensurePayloadPresent(event);
        if (!indexExists(event.getIndexName())) {
            return skip("Target index missing for UPDATE_ALIASES, idx_name:" + event.getIndexName());
        }

        Map<String, Object> payload = parsePayload(event);
        Map<String, Object> sourceAliases = normalizeAliases((Map<String, Object>) payload.get("aliases"));
        Map<String, Object> targetAliases = fetchAliases(event.getIndexName());
        if (Objects.equals(sourceAliases, targetAliases)) {
            return CcEs6ReplayResult.applied();
        }

        IndicesAliasesRequest request = new IndicesAliasesRequest();
        for (String aliasName : targetAliases.keySet()) {
            if (!sourceAliases.containsKey(aliasName)) {
                request.addAliasAction(IndicesAliasesRequest.AliasActions.remove().index(event.getIndexName())
                        .alias(aliasName));
            }
        }
        for (Map.Entry<String, Object> entry : sourceAliases.entrySet()) {
            if (!Objects.equals(entry.getValue(), targetAliases.get(entry.getKey()))) {
                if (targetAliases.containsKey(entry.getKey())) {
                    request.addAliasAction(IndicesAliasesRequest.AliasActions.remove().index(event.getIndexName())
                            .alias(entry.getKey()));
                }
                request.addAliasAction(buildAddAliasAction(event.getIndexName(), entry.getKey(),
                        castMap(entry.getValue())));
            }
        }

        if (!request.getAliasActions().isEmpty()) {
            try {
                AcknowledgedResponse response = client.admin().indices().aliases(request).actionGet();
                if (!response.isAcknowledged()) {
                    return skip("Update aliases not acknowledged, idx_name:" + event.getIndexName());
                }
            } catch (Exception e) {
                return skip("Update aliases failed, idx_name:" + event.getIndexName() + ", msg:" + rootMsg(e));
            }
        }

        if (!Objects.equals(sourceAliases, fetchAliases(event.getIndexName()))) {
            return skip("Target aliases still incompatible after update, idx_name:" + event.getIndexName());
        }
        return CcEs6ReplayResult.applied();
    }

    private boolean isIndexStateCompatible(String indexName, Map<String, Object> sourceSettings,
            Map<String, Object> sourceMappings, Map<String, Object> sourceAliases) throws IOException {
        return Objects.equals(normalizeSettings(sourceSettings), fetchSettings(indexName))
                && Objects.equals(normalizeMappings(sourceMappings), fetchMappings(indexName))
                && Objects.equals(normalizeAliases(sourceAliases), fetchAliases(indexName));
    }

    private boolean indexExists(String indexName) {
        return client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists();
    }

    private Map<String, Object> parsePayload(CcEs6TriggerEvent event) throws IOException {
        Tuple<XContentType, Map<String, Object>> payloadTuple = XContentHelper.convertToMap(
                new BytesArray(event.getRowData()), false, XContentType.JSON);
        return payloadTuple.v2();
    }

    private Map<String, Object> fetchSettings(String indexName) {
        GetIndexResponse response = client.admin().indices().getIndex(new GetIndexRequest().indices(indexName))
                .actionGet();
        return normalizeSettings(toSettingsMap(response.getSettings().get(indexName)));
    }

    private Map<String, Object> fetchMappings(String indexName) {
        GetIndexResponse response = client.admin().indices().getIndex(new GetIndexRequest().indices(indexName))
                .actionGet();
        Map<String, Object> targetMappingsMap = new HashMap<>();
        if (response.getMappings().containsKey(indexName)) {
            for (ObjectObjectCursor<String, org.elasticsearch.cluster.metadata.MappingMetaData> cursor : response
                    .getMappings().get(indexName)) {
                targetMappingsMap.put(cursor.key, cursor.value.getSourceAsMap());
            }
        }
        return normalizeMappings(targetMappingsMap);
    }

    private Map<String, Object> fetchAliases(String indexName) {
        GetAliasesRequest request = new GetAliasesRequest().indices(indexName);
        GetAliasesResponse response = client.admin().indices().getAliases(request).actionGet();
        Map<String, Object> aliases = new HashMap<>();
        List<org.elasticsearch.cluster.metadata.AliasMetaData> aliasMetaDataList = response.getAliases().get(indexName);
        if (aliasMetaDataList != null) {
            for (org.elasticsearch.cluster.metadata.AliasMetaData aliasMetaData : aliasMetaDataList) {
                Map<String, Object> aliasMap = new LinkedHashMap<>();
                if (aliasMetaData.filteringRequired() && aliasMetaData.filter() != null) {
                    aliasMap.put("filter", parseJson(aliasMetaData.filter().string()));
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
                aliases.put(aliasMetaData.alias(), aliasMap);
            }
        }
        return normalizeAliases(aliases);
    }

    private Map<String, Object> toSettingsMap(Settings settings) {
        Map<String, Object> result = new HashMap<>();
        if (settings == null) {
            return result;
        }

        for (String key : settings.keySet()) {
            result.put(key, settings.get(key));
        }
        return result;
    }

    private IndicesAliasesRequest.AliasActions buildAddAliasAction(String indexName, String aliasName,
            Map<String, Object> aliasConfig) {
        IndicesAliasesRequest.AliasActions action = IndicesAliasesRequest.AliasActions.add().index(indexName)
                .alias(aliasName);
        boolean hasSpecificRouting = aliasConfig.containsKey("index_routing")
                || aliasConfig.containsKey("search_routing");
        if (!hasSpecificRouting && aliasConfig.containsKey("routing")) {
            action.routing(String.valueOf(aliasConfig.get("routing")));
        }
        if (aliasConfig.containsKey("index_routing")) {
            action.indexRouting(String.valueOf(aliasConfig.get("index_routing")));
        }
        if (aliasConfig.containsKey("search_routing")) {
            action.searchRouting(String.valueOf(aliasConfig.get("search_routing")));
        }
        if (aliasConfig.containsKey("filter")) {
            action.filter(castMap(aliasConfig.get("filter")));
        }
        if (aliasConfig.containsKey("is_write_index")) {
            action.writeIndex((Boolean) aliasConfig.get("is_write_index"));
        }
        return action;
    }

    private String buildCreateIndexAliasesSource(Map<String, Object> aliases) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            for (Map.Entry<String, Object> entry : aliases.entrySet()) {
                builder.startObject(entry.getKey());
                Map<String, Object> aliasConfig = castMap(entry.getValue());
                for (Map.Entry<String, Object> aliasEntry : aliasConfig.entrySet()) {
                    builder.field(aliasEntry.getKey(), aliasEntry.getValue());
                }
                builder.endObject();
            }
            builder.endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> castMap(Object value) {
        return value == null ? new HashMap<String, Object>() : (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseJson(String json) {
        return normalizeMap(XContentHelper.convertToMap(new BytesArray(json), false, XContentType.JSON).v2());
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> normalizeMappings(Map<String, Object> mappings) {
        Map<String, Object> normalized = new TreeMap<>();
        if (mappings == null) {
            return normalized;
        }

        for (Map.Entry<String, Object> entry : mappings.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Map) {
                normalized.put(entry.getKey(), normalizeMap((Map<String, Object>) value));
            } else {
                normalized.put(entry.getKey(), value);
            }
        }
        return normalized;
    }

    Map<String, Object> normalizeSettings(Map<String, Object> settings) {
        Map<String, Object> comparable = new TreeMap<>();
        if (settings == null) {
            return comparable;
        }

        for (Map.Entry<String, Object> entry : settings.entrySet()) {
            String key = entry.getKey();
            if (StringUtils.startsWith(key, "index.uuid")
                    || StringUtils.startsWith(key, "index.version")
                    || StringUtils.startsWith(key, "index.creation_date")
                    || StringUtils.startsWith(key, "index.provided_name")
                    || StringUtils.equals(key, EsTriggerConstant.IDX_ENABLE_CDC_CONF_KEY)
                    || StringUtils.equals(key, EsTriggerConstant.TRIGGER_IDX_MAX_SCN_KEY)) {
                continue;
            }
            comparable.put(key, String.valueOf(entry.getValue()));
        }
        return comparable;
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> normalizeAliases(Map<String, Object> aliases) {
        Map<String, Object> normalized = new TreeMap<>();
        if (aliases == null) {
            return normalized;
        }

        for (Map.Entry<String, Object> entry : aliases.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Map) {
                normalized.put(entry.getKey(), normalizeMap((Map<String, Object>) value));
            } else {
                normalized.put(entry.getKey(), value);
            }
        }
        return normalized;
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

    private void ensurePayloadPresent(CcEs6TriggerEvent event) {
        if (StringUtils.isBlank(event.getRowData())) {
            throw new IllegalStateException("row_data is blank for event:" + event.getEventType() + ", idx_name:"
                    + event.getIndexName() + ", scn:" + event.getScn());
        }
    }

    private CcEs6ReplayResult skip(String message) {
        log.warn(message);
        return CcEs6ReplayResult.skipped(message);
    }

    private String rootMsg(Exception e) {
        Throwable root = e;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        return root.getMessage();
    }
}

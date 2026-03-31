package com.clougence.cloudcanal.es6.sink;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CcEs6SinkCheckpointStore {

    private static final Logger log = LoggerFactory.getLogger(CcEs6SinkCheckpointStore.class);

    private final Client client;

    public CcEs6SinkCheckpointStore(Client client) {
        this.client = client;
    }

    public synchronized CcEs6SinkState loadOrInit(CcEs6SinkConfig config) {
        try {
            ensureStateIndex();
            GetRequest getRequest = new GetRequest(CcEs6SinkConstant.SINK_STATE_IDX, CcEs6SinkConstant.SINK_DOC_TYPE,
                    CcEs6SinkConstant.SINK_STATE_DOC_ID);
            GetResponse response = client.get(getRequest).actionGet();
            if (response.isExists()) {
                return CcEs6SinkState.fromMap(response.getSourceAsMap());
            }

            CcEs6SinkState state = CcEs6SinkState.initial(config);
            save(state);
            return state;
        } catch (Exception e) {
            String msg = "Load sink state failed.msg:" + ExceptionUtils.getRootCauseMessage(e);
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    public synchronized void save(CcEs6SinkState state) {
        try {
            IndexRequest request = new IndexRequest(CcEs6SinkConstant.SINK_STATE_IDX, CcEs6SinkConstant.SINK_DOC_TYPE,
                    CcEs6SinkConstant.SINK_STATE_DOC_ID);
            request.source(state.toMap(), XContentType.JSON);
            client.index(request).actionGet();
        } catch (Exception e) {
            String msg = "Save sink state failed.msg:" + ExceptionUtils.getRootCauseMessage(e);
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    private void ensureStateIndex() throws IOException {
        IndicesExistsRequest existsRequest = new IndicesExistsRequest(CcEs6SinkConstant.SINK_STATE_IDX);
        boolean exists = client.admin().indices().exists(existsRequest).actionGet().isExists();
        if (exists) {
            return;
        }

        CreateIndexRequest request = new CreateIndexRequest(CcEs6SinkConstant.SINK_STATE_IDX);
        request.settings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0));
        request.source(buildStateIndexSource(), XContentType.JSON);
        CreateIndexResponse response = client.admin().indices().create(request).actionGet();
        if (!response.isAcknowledged()) {
            throw new RuntimeException("Create sink state index failed, acknowledged is false.");
        }
    }

    private String buildStateIndexSource() throws IOException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("sink_id", field("keyword"));
        properties.put("source_host", field("keyword"));
        properties.put("source_index", field("keyword"));
        properties.put("start_time", field("keyword"));
        properties.put("checkpoint_scn", field("long"));
        properties.put("last_processed_scn", field("long"));
        properties.put("last_skipped_scn", field("long"));
        properties.put("last_skipped_event_type", field("keyword"));
        properties.put("last_skipped_reason", textField(false));
        properties.put("last_skipped_time", dateField());
        properties.put("status", field("keyword"));
        properties.put("last_poll_time", dateField());
        properties.put("last_success_time", dateField());
        properties.put("last_error", textField(false));
        properties.put("last_error_time", dateField());

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startObject("mappings");
        builder.startObject(CcEs6SinkConstant.SINK_DOC_TYPE);
        builder.startObject("properties");
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();
        builder.endObject();
        builder.endObject();
        builder.endObject();
        return Strings.toString(builder);
    }

    private Map<String, Object> field(String type) {
        Map<String, Object> map = new HashMap<>();
        map.put("type", type);
        return map;
    }

    private Map<String, Object> textField(boolean indexed) {
        Map<String, Object> map = field("text");
        map.put("index", indexed);
        return map;
    }

    private Map<String, Object> dateField() {
        Map<String, Object> map = field("date");
        map.put("format", "yyyy-MM-dd'T'HH:mm:ssSSS");
        return map;
    }
}

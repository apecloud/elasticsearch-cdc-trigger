package com.clougence.cloudcanal.es6.trigger.writer;

import static com.clougence.cloudcanal.es_base.EsTriggerConstant.TRIGGER_IDX_MAX_SCN_KEY;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clougence.cloudcanal.es6.trigger.ds.Es6ClientConn;
import com.clougence.cloudcanal.es_base.AbstractCcEsTriggerIdxWriter;
import com.clougence.cloudcanal.es_base.EsTriggerConstant;
import com.clougence.cloudcanal.es_base.TriggerWriteEvent;
import com.clougence.cloudcanal.es_base.TriggerEventType;

/**
 * @author bucketli 2024/7/30 16:06:32
 */
public class CcEs6TriggerIdxWriterImpl extends AbstractCcEsTriggerIdxWriter {

    private static final Logger log = LoggerFactory.getLogger(CcEs6TriggerIdxWriterImpl.class);

    protected BlockingQueue<TriggerWriteEvent> cache = new ArrayBlockingQueue<>(cacheSize);

    private final AtomicLong droppedEventCount = new AtomicLong();

    private final AtomicLong lastDroppedAt = new AtomicLong();

    protected boolean isClientInited() {
        return Es6ClientConn.instance.getEsClient() != null;
    }

    protected boolean initTriggerIdx() {
        try {
            Request existsRequest = new Request("HEAD", "/" + EsTriggerConstant.ES_TRIGGER_IDX);
            int statusCode = Es6ClientConn.instance.getEsClient().getLowLevelClient().performRequest(existsRequest)
                    .getStatusLine().getStatusCode();
            if (statusCode == 200) {
                return true;
            }
        } catch (Exception e) {
            if (!ExceptionUtils.getRootCauseMessage(e).contains("404")) {
                log.warn("Check trigger index exists failed,msg:" + ExceptionUtils.getRootCauseMessage(e));
                return false;
            }
        }

        try {
            Request createRequest = new Request("PUT", "/" + EsTriggerConstant.ES_TRIGGER_IDX);
            createRequest.setJsonEntity(buildCreateIndexBody());
            Es6ClientConn.instance.getEsClient().getLowLevelClient().performRequest(createRequest);
            return true;
        } catch (Exception e) {
            if (ExceptionUtils.getRootCauseMessage(e).contains("resource_already_exists_exception")) {
                return true;
            }
            log.error("Init trigger index failed,msg:" + ExceptionUtils.getRootCauseMessage(e), e);
            return false;
        }
    }

    protected String fetchScnCurrVal() {
        try {
            GetSettingsRequest req = new GetSettingsRequest().indices(EsTriggerConstant.ES_TRIGGER_IDX);
            GetSettingsResponse res = Es6ClientConn.instance.getEsClient().indices().getSettings(req,
                    RequestOptions.DEFAULT);
            return res.getSetting(EsTriggerConstant.ES_TRIGGER_IDX, TRIGGER_IDX_MAX_SCN_KEY);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void updateIncreIdToNextStep(long nextStart) {
        try {
            Settings.Builder sb = Settings.builder().put(TRIGGER_IDX_MAX_SCN_KEY, nextStart);
            UpdateSettingsRequest req = new UpdateSettingsRequest().indices(EsTriggerConstant.ES_TRIGGER_IDX)
                    .settings(sb);

            AcknowledgedResponse res = Es6ClientConn.instance.getEsClient().indices().putSettings(req,
                    RequestOptions.DEFAULT);
            if (!res.isAcknowledged()) {
                throw new RuntimeException("Update trigger index settings failed, acknowledged is false.");
            }

            log.info("Updated " + TRIGGER_IDX_MAX_SCN_KEY + " to " + nextStart);
        } catch (Exception e) {
            String msg = "Update trigger index settings failed.msg:" + ExceptionUtils.getRootCauseMessage(e);
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    @Override
    protected void insertInner(TriggerWriteEvent event) {
        try {
            boolean offered = this.cache.offer(event, 2, TimeUnit.SECONDS);
            if (!offered) {
                long dropped = droppedEventCount.incrementAndGet();
                lastDroppedAt.set(System.currentTimeMillis());
                log.warn("Offer to write cache timeout cause no space left,just skip and record here,idx_name:{}"
                                + ",_id:{},event_type:{},dropped_total:{}",
                        event.getIdxName(), event.getId(), event.getEventType(), dropped);
            }
        } catch (InterruptedException e) {
            long dropped = droppedEventCount.incrementAndGet();
            lastDroppedAt.set(System.currentTimeMillis());
            log.warn("Offer to cache interruppted,but skip,idx_name:{},_id:{},event_type:{},dropped_total:{}",
                    event.getIdxName(), event.getId(), event.getEventType(), dropped);
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                if (cache.isEmpty()) {
                    Thread.sleep(1000);
                    continue;
                }

                if (!isWriterStateReady()) {
                    initializeWriterState();
                    if (!isWriterStateReady()) {
                        Thread.sleep(errorBackoffMs);
                        continue;
                    }
                }

                List<TriggerWriteEvent> events = new ArrayList<>();
                int real = cache.drainTo(events, batchSize);
                // log.info("Drain " + real + " documents from cache");
                if (real > 0) {
                    BulkRequest reqs = new BulkRequest();
                    for (TriggerWriteEvent event : events) {
                        reqs.add(toIndexRequest(event));
                    }

                    WriteRequest.RefreshPolicy refreshPolicy = WriteRequest.RefreshPolicy.NONE;
                    reqs.setRefreshPolicy(refreshPolicy);

                    log.info("Try to bulk documents,real:" + real);

                    BulkResponse bulkResponse = Es6ClientConn.instance.getEsClient().bulk(reqs, RequestOptions.DEFAULT);

                    for (BulkItemResponse response : bulkResponse) {
                        if (response.isFailed()) {
                            String errMsg = "bulk put FAILED!msg:" + response.getFailureMessage() + ",action id: "
                                    + response.getId();
                            log.error(errMsg);
                            throw new RuntimeException(errMsg);
                        }
                    }

                    log.info("Bulk documents success,real:" + real);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.info("Trigger idx writer interrupted, stop consume loop.");
            } catch (Exception e) {
                log.error("Consume request from queue failed.msg:" + ExceptionUtils.getRootCauseMessage(e), e);
                try {
                    Thread.sleep(errorBackoffMs);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    log.info("Trigger idx writer interrupted during backoff, stop consume loop.");
                }
            }
        }
    }

    private IndexRequest toIndexRequest(TriggerWriteEvent event) {
        Map<String, Object> doc = new HashMap<>();
        doc.put("scn", nextId());
        doc.put("idx_name", event.getIdxName());
        doc.put("event_type", event.getEventType().getCode());
        doc.put("pk", event.getId());
        if (event.getDocJson() != null) {
            doc.put("row_data", event.getDocJson());
        }
        if (event.getDocType() != null) {
            doc.put("doc_type", event.getDocType());
        }
        doc.put("create_time", event.getCreateTime());
        return new IndexRequest().index(EsTriggerConstant.ES_TRIGGER_IDX)
                .type(EsTriggerConstant.ES6_DOC_TYPE).source(doc);
    }

    private String buildCreateIndexBody() {
        return "{"
                + "\"settings\":{\"index.number_of_shards\":10,\"index.number_of_replicas\":0},"
                + "\"mappings\":{"
                + "\"" + EsTriggerConstant.ES6_DOC_TYPE + "\":{"
                + "\"properties\":{"
                + "\"scn\":{\"type\":\"long\"},"
                + "\"idx_name\":{\"type\":\"text\",\"index\":true,\"analyzer\":\"standard\"},"
                + "\"event_type\":{\"type\":\"text\",\"index\":true,\"analyzer\":\"standard\"},"
                + "\"pk\":{\"type\":\"text\",\"index\":true,\"analyzer\":\"standard\"},"
                + "\"row_data\":{\"type\":\"text\",\"index\":false},"
                + "\"doc_type\":{\"type\":\"keyword\"},"
                + "\"create_time\":{\"type\":\"date\",\"index\":true,\"format\":\"yyyy-MM-dd'T'HH:mm:ssSSS\"}"
                + "}}}"
                + "}";
    }
}

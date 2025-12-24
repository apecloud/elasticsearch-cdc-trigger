package com.clougence.cloudcanal.es8.trigger.writer;

import static com.clougence.cloudcanal.es_base.EsTriggerConstant.TRIGGER_IDX_MAX_SCN_KEY;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clougence.cloudcanal.es8.trigger.ds.Es8ClientConn;
import com.clougence.cloudcanal.es8.trigger.meta.CcEs8MetaServiceImpl;
import com.clougence.cloudcanal.es_base.AbstractCcEsTriggerIdxWriter;
import com.clougence.cloudcanal.es_base.EsTriggerConstant;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.elasticsearch.indices.*;
import co.elastic.clients.json.JsonData;

/**
 * @author bucketli 2024/7/30 16:06:32
 */
public class CcEs8TriggerIdxWriterImpl extends AbstractCcEsTriggerIdxWriter {

    private static final Logger log = LoggerFactory.getLogger(CcEs8TriggerIdxWriterImpl.class);

    protected BlockingQueue<BulkOperation> cache = new ArrayBlockingQueue<>(cacheSize);

    protected boolean isClientInited() {
        return Es8ClientConn.instance.getEsClient() != null;
    }

    @Override
    public void initTriggerIdx() {
        try {
            if (!isClientInited()) {
                log.debug("ES client is not initialized, skip init trigger index.");
                return;
            }

            ElasticsearchClient esClient = Es8ClientConn.instance.getEsClient();

            ExistsRequest existsRequest = new ExistsRequest.Builder()
                    .index(EsTriggerConstant.ES_TRIGGER_IDX).build();
            boolean indexExists = esClient.indices().exists(existsRequest).value();

            if (!indexExists) {
                log.debug("Trigger index " + EsTriggerConstant.ES_TRIGGER_IDX + " does not exist, creating it...");
                try {
                    CcEs8MetaServiceImpl metaService = new CcEs8MetaServiceImpl();
                    TypeMapping typeMapping = metaService.buildTriggerIdxMapping();
                    CreateIndexRequest createRequest = new CreateIndexRequest.Builder()
                            .index(EsTriggerConstant.ES_TRIGGER_IDX)
                            .mappings(typeMapping).build();
                    CreateIndexResponse createResponse = esClient.indices().create(createRequest);
                    if (!createResponse.acknowledged()) {
                        throw new RuntimeException(
                                "Create trigger index " + EsTriggerConstant.ES_TRIGGER_IDX
                                        + " failed, acknowledged is false.");
                    }
                    log.debug("Trigger index " + EsTriggerConstant.ES_TRIGGER_IDX + " created successfully.");
                } catch (ElasticsearchException e) {
                    if (e.getMessage() != null && e.getMessage().contains("resource_already_exists_exception")) {
                        log.info("Trigger index " + EsTriggerConstant.ES_TRIGGER_IDX
                                + " already exists (created by another node), skip creation.");
                    } else {
                        throw e;
                    }
                }
            } else {
                log.debug("Trigger index " + EsTriggerConstant.ES_TRIGGER_IDX + " already exists, skip creation.");
            }
        } catch (Exception e) {
            log.error("Init trigger index failed, msg: " + ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    protected String fetchScnCurrVal() {
        Map<String, Object> re = queryIndexCcSettings(Es8ClientConn.instance.getEsClient(),
                EsTriggerConstant.ES_TRIGGER_IDX, Collections
                        .singletonList(EsTriggerConstant.TRIGGER_IDX_MAX_SCN_KEY));
        Object s = re.get(EsTriggerConstant.TRIGGER_IDX_MAX_SCN_KEY);

        if (s != null) {
            return String.valueOf(s);
        } else {
            return null;
        }
    }

    protected void updateIncreIdToNextStep(long nextStart) {
        try {
            PutIndicesSettingsRequest req = new PutIndicesSettingsRequest.Builder()
                    .index(EsTriggerConstant.ES_TRIGGER_IDX)
                    .settings(t -> t.otherSettings(EsTriggerConstant.TRIGGER_IDX_MAX_SCN_KEY, JsonData.of(nextStart)))
                    .build();
            PutIndicesSettingsResponse res = Es8ClientConn.instance.getEsClient().indices().putSettings(req);
            if (!res.acknowledged()) {
                throw new RuntimeException("Update trigger index settings failed, acknowledged is false.");
            }

            log.info("Updated " + TRIGGER_IDX_MAX_SCN_KEY + " to " + nextStart);
        } catch (Exception e) {
            String msg = "Update trigger index settings failed.msg:" + ExceptionUtils.getRootCauseMessage(e);
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    protected Map<String, Object> queryIndexCcSettings(ElasticsearchClient esClient, String idxName,
            List<String> keys) {
        try {
            GetIndicesSettingsRequest req = new GetIndicesSettingsRequest.Builder().index(idxName).name(keys).build();
            GetIndicesSettingsResponse res = esClient.indices().getSettings(req);
            Map<String, Object> re = new HashMap<>();

            if (res.result().get(idxName) == null) {
                return re;
            }

            IndexSettings settings = res.result().get(idxName).settings();

            if (settings != null && settings.index() != null) {
                Map<String, JsonData> x = settings.index().otherSettings();
                if (x != null) {
                    for (String key : keys) {
                        String realKey = key.substring(key.lastIndexOf(".") + 1);
                        JsonData d = x.get(realKey);
                        if (d != null) {
                            re.put(key, d.to(String.class));
                        }
                    }
                }
            }

            return re;
        } catch (Exception e) {
            String errMsg = "Query settings failed,msg:" + ExceptionUtils.getRootCauseMessage(e);
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    protected void insertInner(Map<String, Object> doc, String srcIdx, String srcId) {
        BulkOperation ir = new BulkOperation.Builder().index(new IndexOperation.Builder<>().document(doc).build())
                .build();

        try {
            boolean offered = this.cache.offer(ir, 2, TimeUnit.SECONDS);
            if (!offered) {
                log.warn("Offer to write cache timeout cause no space left,just skip and record here,idx_name:" + srcIdx
                        + ",_id:" + srcId);
            }
        } catch (InterruptedException e) {
            log.warn("Offer to cache interruppted,but skip,idx_name:" + srcIdx + ",_id:" + srcId);
        }
    }

    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                List<BulkOperation> irs = new ArrayList<>();
                int real = cache.drainTo(irs, batchSize);

                if (real > 0) {
                    log.info("Try to bulk documents,real:" + real);

                    BulkResponse response = Es8ClientConn.instance.getEsClient()
                            .bulk(b -> b.index(EsTriggerConstant.ES_TRIGGER_IDX).operations(irs));

                    if (response.errors()) {
                        for (BulkResponseItem item : response.items()) {
                            if (item.error() != null) {
                                String errMsg = "bulk put FAILED!msg:" + item.error();
                                log.error(errMsg);
                                throw new RuntimeException(errMsg);
                            }
                        }
                    }

                    log.info("Bulk documents success,real:" + real);
                }

                // no need to sleep
                if (real < batchSize) {
                    Thread.sleep(1000);
                }
            }
        } catch (Exception e) {
            log.error("Consume request from queue failed but ignore.msg:" + ExceptionUtils.getRootCauseMessage(e), e);
        }
    }
}

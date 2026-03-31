package com.clougence.cloudcanal.es6.sink;

import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clougence.cloudcanal.es6.sink.ds.Es6SourceClientConn;

public class CcEs6SinkPoller implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(CcEs6SinkPoller.class);

    private final CcEs6SinkCoordinator coordinator;

    private final CcEs6SinkCheckpointStore checkpointStore;

    private final CcEs6ReplayService replayService;

    private final CcEs6SinkConfig config;

    public CcEs6SinkPoller(CcEs6SinkCoordinator coordinator, CcEs6SinkCheckpointStore checkpointStore,
            CcEs6ReplayService replayService, CcEs6SinkConfig config) {
        this.coordinator = coordinator;
        this.checkpointStore = checkpointStore;
        this.replayService = replayService;
        this.config = config;
    }

    @Override
    public void run() {
        long nextDelay = config.getPollIntervalMs();
        CcEs6SinkState state = checkpointStore.loadOrInit(config);
        Long latestScn = state.getCheckpointScn();
        try {
            if (!coordinator.isActiveMaster()) {
                return;
            }

            if (Es6SourceClientConn.instance.getEsClient() == null) {
                throw new IllegalStateException("Source es client is not initialized.");
            }

            if (state.getCheckpointScn() == null && StringUtils.isBlank(state.getStartTime())) {
                throw new IllegalStateException("source_trigger_start_time is blank for initial load.");
            }

            state.markRunning();
            checkpointStore.save(state);

            List<CcEs6TriggerEvent> events = fetchEvents(state);
            if (events.isEmpty()) {
                state.markSuccess(state.getCheckpointScn());
                checkpointStore.save(state);
                nextDelay = config.getIdlePollIntervalMs();
                return;
            }

            for (CcEs6TriggerEvent event : events) {
                CcEs6ReplayResult replayResult = replayService.replay(event);
                latestScn = event.getScn();
                state.markEventProcessed(latestScn);
                if (replayResult.isSkipped()) {
                    state.markSkipped(event, replayResult.getMessage());
                }
            }

            state.markSuccess(latestScn);
            checkpointStore.save(state);
            nextDelay = events.size() >= config.getBatchSize() ? 1L : config.getPollIntervalMs();
        } catch (Exception e) {
            String msg = ExceptionUtils.getRootCauseMessage(e);
            log.error("Poll source trigger index failed,msg:{}", msg, e);
            if (latestScn != null && !latestScn.equals(state.getCheckpointScn())) {
                state.markSuccess(latestScn);
            }
            state.markError(msg);
            checkpointStore.save(state);
            nextDelay = config.getErrorBackoffMs();
        } finally {
            coordinator.scheduleNext(nextDelay);
        }
    }

    private List<CcEs6TriggerEvent> fetchEvents(CcEs6SinkState state) throws Exception {
        RestHighLevelClient sourceClient = Es6SourceClientConn.instance.getEsClient();
        SearchRequest request = new SearchRequest(config.getSourceIndexName());
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource().size(config.getBatchSize())
                .sort("scn", SortOrder.ASC).trackTotalHits(false).timeout(TimeValue.timeValueSeconds(10));

        if (state.getCheckpointScn() != null) {
            sourceBuilder.query(rangeQuery("scn").gt(state.getCheckpointScn()));
        } else {
            sourceBuilder.query(rangeQuery("create_time").gte(state.getStartTime()));
        }

        request.source(sourceBuilder);
        SearchResponse response = sourceClient.search(request, RequestOptions.DEFAULT);

        List<CcEs6TriggerEvent> events = new ArrayList<>();
        for (SearchHit hit : response.getHits().getHits()) {
            events.add(CcEs6TriggerEvent.fromSource(hit.getSourceAsMap()));
        }
        return events;
    }
}

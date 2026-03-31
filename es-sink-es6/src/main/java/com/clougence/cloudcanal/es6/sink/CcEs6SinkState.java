package com.clougence.cloudcanal.es6.sink;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CcEs6SinkState {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssSSS");

    private String sinkId = CcEs6SinkConstant.SINK_STATE_DOC_ID;

    private String sourceHost;

    private String sourceIndex;

    private String startTime;

    private Long checkpointScn;

    private Long lastProcessedScn;

    private Long lastSkippedScn;

    private String lastSkippedEventType;

    private String lastSkippedReason;

    private String lastSkippedTime;

    private String status;

    private String lastPollTime;

    private String lastSuccessTime;

    private String lastError;

    private String lastErrorTime;

    public static CcEs6SinkState initial(CcEs6SinkConfig config) {
        CcEs6SinkState state = new CcEs6SinkState();
        state.setSourceHost(config.getSourceHosts());
        state.setSourceIndex(config.getSourceIndexName());
        state.setStartTime(config.getStartTime());
        state.setStatus("INIT");
        return state;
    }

    public static CcEs6SinkState fromMap(Map<String, Object> source) {
        CcEs6SinkState state = new CcEs6SinkState();
        state.setSinkId(stringVal(source.get("sink_id"), CcEs6SinkConstant.SINK_STATE_DOC_ID));
        state.setSourceHost(stringVal(source.get("source_host"), null));
        state.setSourceIndex(stringVal(source.get("source_index"), null));
        state.setStartTime(stringVal(source.get("start_time"), null));
        Object checkpoint = source.get("checkpoint_scn");
        state.setCheckpointScn(checkpoint == null ? null : Long.valueOf(String.valueOf(checkpoint)));
        Object lastProcessed = source.get("last_processed_scn");
        state.setLastProcessedScn(lastProcessed == null ? null : Long.valueOf(String.valueOf(lastProcessed)));
        Object lastSkipped = source.get("last_skipped_scn");
        state.setLastSkippedScn(lastSkipped == null ? null : Long.valueOf(String.valueOf(lastSkipped)));
        state.setLastSkippedEventType(stringVal(source.get("last_skipped_event_type"), null));
        state.setLastSkippedReason(stringVal(source.get("last_skipped_reason"), null));
        state.setLastSkippedTime(stringVal(source.get("last_skipped_time"), null));
        state.setStatus(stringVal(source.get("status"), null));
        state.setLastPollTime(stringVal(source.get("last_poll_time"), null));
        state.setLastSuccessTime(stringVal(source.get("last_success_time"), null));
        state.setLastError(stringVal(source.get("last_error"), null));
        state.setLastErrorTime(stringVal(source.get("last_error_time"), null));
        return state;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> doc = new HashMap<>();
        doc.put("sink_id", sinkId);
        doc.put("source_host", sourceHost);
        doc.put("source_index", sourceIndex);
        doc.put("start_time", startTime);
        if (checkpointScn != null) {
            doc.put("checkpoint_scn", checkpointScn);
        }
        if (lastProcessedScn != null) {
            doc.put("last_processed_scn", lastProcessedScn);
        }
        if (lastSkippedScn != null) {
            doc.put("last_skipped_scn", lastSkippedScn);
        }
        doc.put("last_skipped_event_type", lastSkippedEventType);
        doc.put("last_skipped_reason", lastSkippedReason);
        doc.put("last_skipped_time", lastSkippedTime);
        doc.put("status", status);
        doc.put("last_poll_time", lastPollTime);
        doc.put("last_success_time", lastSuccessTime);
        doc.put("last_error", lastError);
        doc.put("last_error_time", lastErrorTime);
        return doc;
    }

    public void markRunning() {
        this.status = "RUNNING";
        this.lastPollTime = now();
        this.lastError = null;
    }

    public void markSuccess(Long checkpointScn) {
        this.status = "RUNNING";
        this.lastPollTime = now();
        this.lastSuccessTime = now();
        this.lastError = null;
        this.lastErrorTime = null;
        if (checkpointScn != null) {
            this.checkpointScn = checkpointScn;
        }
    }

    public void markEventProcessed(Long scn) {
        this.lastProcessedScn = scn;
    }

    public void markSkipped(CcEs6TriggerEvent event, String reason) {
        this.lastSkippedScn = event.getScn();
        this.lastSkippedEventType = event.getEventType() == null ? null : event.getEventType().name();
        this.lastSkippedReason = reason;
        this.lastSkippedTime = now();
        this.lastProcessedScn = event.getScn();
    }

    public void markError(String error) {
        this.status = "ERROR";
        this.lastPollTime = now();
        this.lastError = error;
        this.lastErrorTime = now();
    }

    private static String stringVal(Object value, String defaultValue) {
        return value == null ? defaultValue : String.valueOf(value);
    }

    private static String now() {
        return LocalDateTime.now().format(FORMATTER);
    }
}

package com.clougence.cloudcanal.es6.sink;

import java.util.Map;

import com.clougence.cloudcanal.es_base.TriggerEventType;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CcEs6TriggerEvent {

    private long scn;

    private String indexName;

    private TriggerEventType eventType;

    private String pk;

    private String rowData;

    private String docType;

    private String createTime;

    public static CcEs6TriggerEvent fromSource(Map<String, Object> source) {
        CcEs6TriggerEvent event = new CcEs6TriggerEvent();
        event.setScn(Long.parseLong(String.valueOf(source.get("scn"))));
        event.setIndexName(String.valueOf(source.get("idx_name")));
        event.setEventType(TriggerEventType.getEventType(String.valueOf(source.get("event_type"))));
        Object pk = source.get("pk");
        event.setPk(pk == null ? null : String.valueOf(pk));
        Object rowData = source.get("row_data");
        event.setRowData(rowData == null ? null : String.valueOf(rowData));
        Object docType = source.get("doc_type");
        event.setDocType(docType == null ? null : String.valueOf(docType));
        Object createTime = source.get("create_time");
        event.setCreateTime(createTime == null ? null : String.valueOf(createTime));
        return event;
    }
}

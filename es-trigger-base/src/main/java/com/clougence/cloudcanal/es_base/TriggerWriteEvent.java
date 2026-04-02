package com.clougence.cloudcanal.es_base;

public class TriggerWriteEvent {

    private final String idxName;
    private final TriggerEventType eventType;
    private final String id;
    private final String docJson;
    private final String docType;
    private final String createTime;

    public TriggerWriteEvent(String idxName, TriggerEventType eventType, String id, String docJson, String docType,
            String createTime) {
        this.idxName = idxName;
        this.eventType = eventType;
        this.id = id;
        this.docJson = docJson;
        this.docType = docType;
        this.createTime = createTime;
    }

    public String getIdxName() {
        return idxName;
    }

    public TriggerEventType getEventType() {
        return eventType;
    }

    public String getId() {
        return id;
    }

    public String getDocJson() {
        return docJson;
    }

    public String getDocType() {
        return docType;
    }

    public String getCreateTime() {
        return createTime;
    }
}

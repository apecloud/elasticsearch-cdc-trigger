package com.clougence.cloudcanal.es_base;

public enum TriggerEventType {

    /**
     * Insert DML type.
     */
    INSERT("I"),
    /**
     * Update DML type.
     */
    UPDATE("U"),
    /**
     * Delete DML type.
     */
    DELETE("D"),
    /**
     * Create index DDL type.
     */
    CREATE_INDEX("CI"),
    /**
     * Delete index DDL type.
     */
    DELETE_INDEX("DI"),
    /**
     * Update mapping DDL type.
     */
    UPDATE_MAPPING("UM"),
    /**
     * Update settings DDL type.
     */
    UPDATE_SETTINGS("US"),
    /**
     * Update aliases DDL type.
     */
    UPDATE_ALIASES("UA");

    private final String code;

    TriggerEventType(String code){
        this.code = code;
    }

    public boolean isDml() { return this == INSERT || this == DELETE || this == UPDATE; }

    public boolean isDdl() {
        return this == CREATE_INDEX || this == DELETE_INDEX || this == UPDATE_MAPPING || this == UPDATE_SETTINGS
                || this == UPDATE_ALIASES;
    }

    public String getCode() { return this.code; }

    public static TriggerEventType getEventType(String s) {
        for (TriggerEventType e : values()) {
            if (e.getCode().equals(s)) {
                return e;
            }
        }
        throw new IllegalStateException(String.format("Invalid data event type of %s", s));
    }
}

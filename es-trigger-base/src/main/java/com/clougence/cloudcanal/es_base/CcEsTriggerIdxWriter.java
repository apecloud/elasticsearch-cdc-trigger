package com.clougence.cloudcanal.es_base;

import java.io.IOException;

/**
 * @author bucketli 2024/7/30 16:01:07
 */
public interface CcEsTriggerIdxWriter extends ComponentLifeCycle {

    default void insertTriggerIdx(String idxName, TriggerEventType dataOp, String id, String docJson)
            throws IOException {
        insertTriggerIdx(idxName, dataOp, id, docJson, null);
    }

    void insertTriggerIdx(String idxName, TriggerEventType dataOp, String id, String docJson, String docType)
            throws IOException;

}

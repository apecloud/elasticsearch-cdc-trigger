package com.clougence.cloudcanal.es_base;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author bucketli 2024/8/27 10:12:03
 */
public abstract class AbstractCcEsTriggerIdxWriter implements Runnable, CcEsTriggerIdxWriter {

    private static final Logger log = LoggerFactory.getLogger(AbstractCcEsTriggerIdxWriter.class);

    private static final AtomicBoolean inited = new AtomicBoolean(false);

    private static final AtomicBoolean triggerIdxIdInited = new AtomicBoolean(false);

    private final AtomicLong incrementId = new AtomicLong(0);

    private static final int scnStep = 100000;

    private long currentStepMaxVal = 0;

    private ExecutorService executor;

    protected static final int cacheSize = 65535;

    protected static final int batchSize = 1024;

    protected static final long errorBackoffMs = 1000L;

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssSSS");

    private static final AtomicBoolean triggerIdxInitialized = new AtomicBoolean(false);
    private static final AtomicBoolean writerStateReady = new AtomicBoolean(false);

    @Override
    public void start() {
        if (inited.compareAndSet(false, true)) {
            log.info(this.getClass().getSimpleName() + " begin to start.");
            initWriterThread();
            log.info(this.getClass().getSimpleName() + " start successfully.");
        }
    }

    protected abstract String fetchScnCurrVal();

    protected abstract boolean isClientInited();

    protected abstract boolean initTriggerIdx();

    protected abstract void updateIncreIdToNextStep(long nextStart);

    protected abstract void insertInner(TriggerWriteEvent event);

    private synchronized void initTriggerIdxId() {
        try {
            if (!isClientInited()) {
                triggerIdxIdInited.compareAndSet(true, false);
                return;
            }

            String s = fetchScnCurrVal();
            long currVal;
            if (StringUtils.isBlank(s)) {
                currVal = 0;
            } else {
                currVal = Long.parseLong(s);
            }

            long nextStart = currVal + scnStep;
            updateIncreIdToNextStep(nextStart);

            currentStepMaxVal = nextStart;

            incrementId.set(currVal);

            writerStateReady.set(triggerIdxInitialized.get());
            triggerIdxIdInited.compareAndSet(false, true);
        } catch (Exception e) {
            writerStateReady.set(false);
            triggerIdxIdInited.compareAndSet(true, false);
            String msg = "Init trigger index settings failed,init later.msg:" + ExceptionUtils.getRootCauseMessage(e);
            log.error(msg, e);
        }
    }

    private void initWriterThread() {
        executor = Executors.newFixedThreadPool(1);
        executor.execute(this);
    }

    @Override
    public synchronized void initializeWriterState() {
        if (!isClientInited()) {
            writerStateReady.set(false);
            triggerIdxInitialized.set(false);
            triggerIdxIdInited.set(false);
            return;
        }

        if (!initTriggerIdx()) {
            writerStateReady.set(false);
            triggerIdxInitialized.set(false);
            triggerIdxIdInited.set(false);
            return;
        }

        triggerIdxInitialized.set(true);
        initTriggerIdxId();
        if (triggerIdxIdInited.get()) {
            writerStateReady.set(true);
        }
    }

    protected boolean isWriterStateReady() {
        return writerStateReady.get();
    }

    @Override
    public void insertTriggerIdx(String idxName, TriggerEventType dataOp, String id, String docJson, String docType)
            throws IOException {
        try {
            insertInner(new TriggerWriteEvent(idxName, dataOp, id, docJson, docType,
                    LocalDateTime.now().format(formatter)));
        } catch (Exception e) {
            log.warn("Insert trigger event failed but ignore, idx_name:{}, event_type:{}, _id:{}, msg:{}", idxName,
                    dataOp, id, ExceptionUtils.getRootCauseMessage(e));
        }
    }

    protected synchronized long nextId() {
        if (!writerStateReady.get()) {
            throw new IllegalArgumentException("Trigger idx writer is not ready.");
        }

        if (!triggerIdxIdInited.get()) {
            initTriggerIdxId();

            if (!triggerIdxIdInited.get()) {
                throw new IllegalArgumentException("Trigger idx id can not be inited,maybe datasource not ready.");
            }
        }

        if (incrementId.get() >= currentStepMaxVal) {
            initTriggerIdxId();
            if (!triggerIdxIdInited.get()) {
                throw new IllegalArgumentException("Trigger idx id step can not be refreshed.");
            }
        }

        return incrementId.incrementAndGet();
    }

    @Override
    public void stop() {
        if (inited.compareAndSet(true, false)) {
            try {
                log.info(this.getClass().getSimpleName() + " try to stop...");

                if (this.executor != null) {
                    this.executor.shutdown();
                }

                log.info(this.getClass().getSimpleName() + " stop successfully.");
            } catch (Exception e) {
                log.warn(this.getClass().getSimpleName() + " stop failed,but ignore.msg:"
                        + ExceptionUtils.getRootCauseMessage(e));
            }
        }
    }
}

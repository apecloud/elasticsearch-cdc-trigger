package com.clougence.cloudcanal.es6.sink;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CcEs6SinkCoordinator implements LocalNodeMasterListener, Closeable {

    private static final Logger log = LoggerFactory.getLogger(CcEs6SinkCoordinator.class);

    private final ClusterService clusterService;

    private final ThreadPool threadPool;

    private final CcEs6SinkPoller poller;

    private final AtomicBoolean active = new AtomicBoolean(false);

    private volatile Scheduler.ScheduledCancellable scheduledTask;

    public CcEs6SinkCoordinator(ClusterService clusterService, ThreadPool threadPool, Client client,
            CcEs6SinkConfig config) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.poller = new CcEs6SinkPoller(this, new CcEs6SinkCheckpointStore(client), new CcEs6ReplayService(client),
                config);
    }

    @Override
    public void onMaster() {
        if (active.compareAndSet(false, true)) {
            log.info("Local node became master, start sink poller.");
            scheduleNext(1L);
        }
    }

    @Override
    public void offMaster() {
        if (active.compareAndSet(true, false)) {
            log.info("Local node lost master role, stop sink poller.");
            cancelScheduled();
        }
    }

    @Override
    public String executorName() {
        return ThreadPool.Names.GENERIC;
    }

    public boolean isActiveMaster() {
        return active.get() && clusterService.state().nodes().isLocalNodeElectedMaster();
    }

    public synchronized void scheduleNext(long delayMs) {
        if (!isActiveMaster()) {
            return;
        }
        cancelScheduled();
        scheduledTask = threadPool.schedule(threadPool.preserveContext(poller), TimeValue.timeValueMillis(delayMs),
                ThreadPool.Names.GENERIC);
    }

    private synchronized void cancelScheduled() {
        if (scheduledTask != null) {
            scheduledTask.cancel();
            scheduledTask = null;
        }
    }

    @Override
    public void close() {
        active.set(false);
        cancelScheduled();
    }
}

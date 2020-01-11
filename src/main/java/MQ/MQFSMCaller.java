package MQ;

import DBExceptions.TxMQError;
import DBExceptions.TxMQException;
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ClosureQueue;
import com.alipay.sofa.jraft.closure.TaskClosure;
import com.alipay.sofa.jraft.core.NodeMetrics;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.util.*;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import options.MQFSMCallerOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author :jinkai
 * @date :Created in 2020/1/3 16:22
 * @description：
 * @modified By:
 * @version:
 */

public class MQFSMCaller implements Lifecycle<MQFSMCallerOptions>, Describer {

    private static final Logger LOG = LoggerFactory.getLogger(MQFSMCaller.class);

    private volatile TaskType               currTask;
    private volatile TxMQException          error;
    private final AtomicLong                lastAppliedIndex;
    private ClosureQueue                    closureQueue;
    private MQStateMachine                  fsm;
    private final AtomicLong                oldCommitingIndex;
    private MQLogManager                    logManager;
    private NodeMetrics                     nodeMetrics;
    private DTGMQ                           mqImpl;
    private Closure                         afterShutdown;
    private Disruptor<ApplyTask>            disruptor;
    private RingBuffer<ApplyTask>           taskQueue;
    private volatile CountDownLatch         shutdownLatch;

    public MQFSMCaller(){
        super();
        this.currTask = TaskType.IDLE;
        this.lastAppliedIndex = new AtomicLong(0);
        this.oldCommitingIndex = new AtomicLong(0);
    }

    private enum TaskType {
        IDLE, //
        COMMITTED, //
        SNAPSHOT_SAVE, //
        SNAPSHOT_LOAD, //
        SHUTDOWN, //
        FLUSH, //
        ERROR;

        private String metricName;

        public String metricName() {
            if (this.metricName == null) {
                this.metricName = "MQfsm-" + name().toLowerCase().replaceAll("_", "-");
            }
            return this.metricName;
        }
    }

    private static class ApplyTask {
        TaskType            type;
        // union fields
        long                committedIndex;//commit in this class represent apply
        Status              status;
        Closure             done;
        CountDownLatch      shutdownLatch;

        public void reset() {
            this.type = null;
            this.committedIndex = 0;
            this.status = null;
            this.done = null;
            this.shutdownLatch = null;
        }
    }

    public class OnErrorClosure implements Closure {
        private TxMQException error;

        public OnErrorClosure(final TxMQException error) {
            super();
            this.error = error;
        }

        public TxMQException getError() {
            return this.error;
        }

        public void setError(final TxMQException error) {
            this.error = error;
        }

        @Override
        public void run(final Status st) {
        }
    }

    private static class ApplyTaskFactory implements EventFactory<ApplyTask> {

        @Override
        public ApplyTask newInstance() {
            return new ApplyTask();
        }
    }

    private class ApplyTaskHandler implements EventHandler<ApplyTask> {
        // max committed index in current batch, reset to -1 every batch
        //private long maxCommittedIndex = -1;

        @Override
        public void onEvent(final ApplyTask event, final long sequence, final boolean endOfBatch) throws Exception {//System.out.println("ApplyTaskHandler：");
            //this.maxCommittedIndex = logManager.getLastLogIndex() - 1;
            runApplyTask(event, event.committedIndex, endOfBatch);
        }
    }



    @Override
    public boolean init(MQFSMCallerOptions opts) {
        this.logManager = opts.getLogManager();
        this.fsm = opts.getFsm();
        this.closureQueue = opts.getClosureQueue();
        this.afterShutdown = opts.getAfterShutdown();
        this.mqImpl = opts.getNode();
        this.nodeMetrics = this.mqImpl.getMetrics();
        this.lastAppliedIndex.set(this.logManager.getLastLogIndex());
        //notifyLastAppliedIndexUpdated(this.lastAppliedIndex.get());
        this.oldCommitingIndex.set(this.logManager.geLastCommitIndex());
        this.closureQueue.resetFirstIndex(this.oldCommitingIndex.get() + 1);
        this.disruptor = DisruptorBuilder.<ApplyTask> newInstance() //
                .setEventFactory(new ApplyTaskFactory()) //
                .setRingBufferSize(opts.getDisruptorBufferSize()) //
                .setThreadFactory(new NamedThreadFactory("MQ-FSMCaller-Disruptor-", true)) //
                .setProducerType(ProducerType.MULTI) //
                .setWaitStrategy(new BlockingWaitStrategy()) //
                .build();
        this.disruptor.handleEventsWith(new ApplyTaskHandler());
        this.disruptor.setDefaultExceptionHandler(new LogExceptionHandler<Object>(getClass().getSimpleName()));
        this.taskQueue = this.disruptor.start();
        if (this.nodeMetrics.getMetricRegistry() != null) {
            this.nodeMetrics.getMetricRegistry().register("jraft-fsm-caller-disruptor",
                    new DisruptorMetricSet(this.taskQueue));
        }
        this.error = new TxMQException(EnumOutter.ErrorType.ERROR_TYPE_NONE);
        LOG.info("Starts FSMCaller successfully.");
        return true;
    }


    @Override
    public void shutdown() {
        if (this.shutdownLatch != null) {
            return;
        }
        LOG.info("Shutting down FSMCaller...");

        if (this.taskQueue != null) {
            final CountDownLatch latch = new CountDownLatch(1);
            this.shutdownLatch = latch;
            Utils.runInThread(() -> this.taskQueue.publishEvent((task, sequence) -> {
                task.reset();
                task.type = TaskType.SHUTDOWN;
                task.shutdownLatch = latch;
            }));
        }
        doShutdown();
    }

    private void doShutdown() {
        if (this.mqImpl != null) {
            this.mqImpl = null;
        }
        if (this.fsm != null) {
            this.fsm.onShutdown();
        }
    }

    private boolean enqueueTask(final EventTranslator<ApplyTask> tpl) {
        if (this.shutdownLatch != null) {
            // Shutting down
            LOG.warn("FSMCaller is stopped, can not apply new task.");
            return false;
        }
        if (!this.taskQueue.tryPublishEvent(tpl)) {
            onError(new TxMQException(EnumOutter.ErrorType.ERROR_TYPE_STATE_MACHINE, new Status(TxMQError.EBUSY.getNumber(),
                    "FSMCaller is overload.")));
            return false;
        }
        return true;
    }

    @Override
    public void describe(Printer out) {
        out.print("  ") //
                .println(toString());
    }

    /**
     * Listen on lastAppliedLogIndex update events.
     *
     * @author dennis
     */
    interface LastAppliedLogIndexListener {

        /**
         * Called when lastAppliedLogIndex updated.
         *
         * @param lastAppliedLogIndex the log index of last applied
         */
        void onApplied(final long lastAppliedLogIndex);
    }

    private long runApplyTask(final ApplyTask task, long maxCommittedIndex, final boolean endOfBatch) {
        //System.out.println("committedIndex:" + this.oldCommitingIndex + ", maxCommittedIndex:" + maxCommittedIndex);
        long oldCommit = this.oldCommitingIndex.get();
        CountDownLatch shutdown = null;
        if (task.type == TaskType.COMMITTED) {
            if (oldCommit > maxCommittedIndex) {
                maxCommittedIndex = oldCommit;
            }
        } else {
            if (maxCommittedIndex >= 0) {
                this.currTask = TaskType.COMMITTED;
                doCommitted(maxCommittedIndex, oldCommit);
                maxCommittedIndex = -1L; // reset maxCommittedIndex
            }
            final long startMs = Utils.monotonicMs();
            try {
                switch (task.type) {
                    case COMMITTED:
                        Requires.requireTrue(false, "Impossible");
                        break;
                    case ERROR:
                        this.currTask = TaskType.ERROR;
                        doOnError((OnErrorClosure) task.done);
                        break;
                    case IDLE:
                        Requires.requireTrue(false, "Can't reach here");
                        break;
                    case SHUTDOWN:
                        this.currTask = TaskType.SHUTDOWN;
                        shutdown = task.shutdownLatch;
                        break;
                    case FLUSH:
                        this.currTask = TaskType.FLUSH;
                        shutdown = task.shutdownLatch;
                        break;
                }
            } finally {
                this.nodeMetrics.recordLatency(task.type.metricName(), Utils.monotonicMs() - startMs);
            }
        }
        try {
            if (endOfBatch && maxCommittedIndex >= 0) {
                this.currTask = TaskType.COMMITTED;
                doCommitted(maxCommittedIndex, oldCommit);
                maxCommittedIndex = -1L; // reset maxCommittedIndex
            }
            this.currTask = TaskType.IDLE;
            return maxCommittedIndex;
        } finally {
            if (shutdown != null) {
                shutdown.countDown();
            }
        }
    }

    private void doCommitted(final long NeedCommittedToIndex, final long committedIndex) {
        //System.out.println("NeedCommittedToIndex: " + NeedCommittedToIndex + "， committedIndex" + committedIndex + ", lastAppliedIndex" + lastAppliedIndex.get());
        if (!this.error.getStatus().isOk()) {
            return;
        }
        // We can tolerate the disorder of committed_index
        if (committedIndex >= NeedCommittedToIndex) {
            return;
        }
        final long startMs = Utils.monotonicMs();
        try {
            final List<Closure> closures = new ArrayList<>();
            final List<TaskClosure> taskClosures = new ArrayList<>();
            final long firstClosureIndex = this.closureQueue.popClosureUntil(NeedCommittedToIndex, closures, taskClosures);

            // Calls TaskClosure#onCommitted if necessary
            onTaskCommitted(taskClosures);
            //System.out.println("index = " + firstClosureIndex);
            Requires.requireTrue(firstClosureIndex >= 0, "Invalid firstClosureIndex");
            final MQIteratorImpl iterImpl = new MQIteratorImpl(this.fsm, this.logManager, closures, firstClosureIndex,
                    new AtomicLong(committedIndex), NeedCommittedToIndex);
            while (iterImpl.isGood()) {
                final TransactionLogEntry logEntry = iterImpl.entry();
                if (logEntry.getType() != EnumOutter.EntryType.ENTRY_TYPE_DATA) {
                    if (iterImpl.done() != null) {
                        // For other entries, we have nothing to do besides flush the
                        // pending tasks and run this closure to notify the caller that the
                        // entries before this one were successfully committed and applied.
                        iterImpl.done().run(Status.OK());
                    }
                    iterImpl.next();
                    continue;
                }

                // Apply data task to user state machine
                doApplyTasks(iterImpl);
            }

            if (iterImpl.hasError()) {
                setError(iterImpl.getError());
                iterImpl.runTheRestClosureWithError();
            }
            final long lastIndex = iterImpl.getIndex() - 1;
            final MQLogId lastAppliedId = new MQLogId(lastIndex);
            //this.lastCommitIndex.set(lastIndex);
            //this.logManager.setCommitIndex(lastIndex);
            this.lastAppliedIndex.set(lastIndex);
            this.oldCommitingIndex.set(lastIndex);
            //this.logManager.setAppliedId(lastAppliedId);
            //notifyLastAppliedIndexUpdated(lastIndex);
        } finally {
            this.nodeMetrics.recordLatency("fsm-commit", Utils.monotonicMs() - startMs);
        }
    }

    private void doOnError(final OnErrorClosure done) {
        setError(done.getError());
    }

//    private void notifyLastAppliedIndexUpdated(final long lastAppliedIndex) {
//        for (final LastAppliedLogIndexListener listener : this.lastAppliedLogIndexListeners) {
//            listener.onApplied(lastAppliedIndex);
//        }
//    }

    private void doApplyTasks(final MQIteratorImpl iterImpl) {
        final long startApplyMs = Utils.monotonicMs();
        final long startIndex = iterImpl.getIndex();
        try {
            this.fsm.onApply(iterImpl);
        } finally {
            this.nodeMetrics.recordLatency("fsm-apply-tasks", Utils.monotonicMs() - startApplyMs);
            this.nodeMetrics.recordSize("fsm-apply-tasks-count", iterImpl.getIndex() - startIndex);
        }
        if (iterImpl.hasNext()) {
            LOG.error("Iterator is still valid, did you return before iterator reached the end?");
        }
        // Try move to next in case that we pass the same log twice.
        iterImpl.next();
    }

    private void onTaskCommitted(final List<TaskClosure> closures) {
        for (int i = 0, size = closures.size(); i < size; i++) {
            final TaskClosure done = closures.get(i);
            done.onCommitted();
        }
    }

    private void setError(final TxMQException e) {
        if (this.error.getType() != EnumOutter.ErrorType.ERROR_TYPE_NONE) {
            // already report
            return;
        }
        this.error = e;
        if (this.fsm != null) {
            this.fsm.onError(e);
        }
        if (this.mqImpl != null) {
            this.mqImpl.onError(e);
        }
    }

//    /**
//     * Adds a LastAppliedLogIndexListener.
//     */
//    public void addLastAppliedLogIndexListener(final LastAppliedLogIndexListener listener){
//        this.lastAppliedLogIndexListeners.add(listener);
//    }

    /**
     * Called when log entry committed
     *
     * @param committedIndex committed log indexx
     */
    public boolean onCommitted(final long committedIndex){
        //System.out.println("committedIndex: " + committedIndex);
        return enqueueTask((task, sequence) -> {
            task.type = TaskType.COMMITTED;
            task.committedIndex = committedIndex;
        });
    }

    @OnlyForTest
    void flush() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        enqueueTask((task, sequence) -> {
            task.type = TaskType.FLUSH;
            task.shutdownLatch = latch;
        });
        latch.await();
    }

    @OnlyForTest
    TxMQException getError() {
        return this.error;
    }

    private boolean passByStatus(final Closure done) {
        final Status status = this.error.getStatus();
        if (!status.isOk()) {
            if (done != null) {
                done.run(new Status(TxMQError.EINVAL.getNumber(), "FSMCaller is in bad status=`%s`", status));
                return false;
            }
        }
        return true;
    }

    /**
     * Called after loading snapshot.
     *
     * @param done callback
     */
    //boolean onSnapshotLoad(final LoadSnapshotClosure done);

    /**
     * Called after saving snapshot.
     *
     * @param done callback
     */
    //boolean onSnapshotSave(final SaveSnapshotClosure done);

    /**
     * Called when error happens.
     *
     * @param error error info
     */
    public boolean onError(final TxMQException error){
        return false;
    }

    /**
     * Returns the last log entry index to apply state machine.
     */
    public long getLastAppliedIndex(){
        return this.lastAppliedIndex.get();
    }

    /**
     * Called after shutdown to wait it terminates.
     *
     * @throws InterruptedException if the current thread is interrupted
     *         while waiting
     */
    public void join() throws InterruptedException{
        if (this.shutdownLatch != null) {
            this.shutdownLatch.await();
            this.disruptor.shutdown();
            if (this.afterShutdown != null) {
                this.afterShutdown.run(Status.OK());
                this.afterShutdown = null;
            }
            this.shutdownLatch = null;
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StateMachine [");
        switch (this.currTask) {
            case IDLE:
                sb.append("Idle");
                break;
            case COMMITTED:
                sb.append("Applying logIndex=").append(this.lastAppliedIndex);
                break;
            case SNAPSHOT_SAVE:
                sb.append("Saving snapshot");
                break;
            case SNAPSHOT_LOAD:
                sb.append("Loading snapshot");
                break;
            case ERROR:
                sb.append("Notifying error");
                break;
            case SHUTDOWN:
                sb.append("Shutting down");
                break;
            default:
                break;
        }
        return sb.append(']').toString();
    }

}

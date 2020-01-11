package MQ;

import DBExceptions.TxMQError;
import DBExceptions.TxMQException;
import MQ.Test.TestClosure;
import MQ.codec.v2.MQV2LogEntryCodecFactory;
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ClosureQueue;
import com.alipay.sofa.jraft.closure.ClosureQueueImpl;
import com.alipay.sofa.jraft.core.NodeMetrics;
import com.alipay.sofa.jraft.core.State;
import com.alipay.sofa.jraft.core.TimerManager;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.entity.UserLog;
import com.alipay.sofa.jraft.error.LogIndexOutOfBoundsException;
import com.alipay.sofa.jraft.error.LogNotFoundException;
import com.alipay.sofa.jraft.rhea.serialization.Serializers;
import com.alipay.sofa.jraft.util.*;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import config.DTGConstants;
import options.MQBallotBoxOptions;
import options.MQFSMCallerOptions;
import options.MQLogManagerOptions;
import options.MQOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tool.ObjectAndByte;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author :jinkai
 * @date :Created in 2019/12/28 20:36
 * @description：
 * @modified By:
 * @version:
 */

public class DTGMQ implements Lifecycle<MQOptions> {

    private static final Logger LOG                      = LoggerFactory
            .getLogger(DTGMQ.class);

    private MQOptions                                 opts;

    private Disruptor<TransactionLogEntryAndClosure>  applyDisruptor;
    private RingBuffer<TransactionLogEntryAndClosure> applyQueue;
    private MQFSMCaller                               fsmCaller;
    private MQLogManager                              logManager;
    private MQLogStorage                              logStorage;
    private NodeMetrics                               metrics;
    private ClosureQueue                              closureQueue;
    private volatile State                            state;
    private MQBallotBox                               ballotBox;
    private volatile CountDownLatch                   shutdownLatch;

    private final ReadWriteLock                       readWriteLock            = new ReentrantReadWriteLock();
    protected final Lock                              writeLock                = this.readWriteLock.writeLock();
    protected final Lock                              readLock                 = this.readWriteLock.readLock();
    private final List<Closure>                       shutdownContinuations    = new ArrayList<>();
    public static final AtomicInteger                 GLOBAL_NUM_NODES         = new AtomicInteger(0);
    private static final int                          MAX_APPLY_RETRY_TIMES    = 3;
    public static AtomicLong                          TX_LOG_INDEX             = new AtomicLong(0);

    @Override
    public boolean init(MQOptions opts) {
        Requires.requireNonNull(opts, "Null node options");
        this.opts = opts;
        this.metrics = new NodeMetrics(opts.isEnableMetrics());
        opts.setFsm(new MQStateMachine());
        this.applyDisruptor = DisruptorBuilder.<TransactionLogEntryAndClosure> newInstance() //
                .setRingBufferSize(DTGConstants.disruptorBufferSize) //
                .setEventFactory(new LogEntryAndClosureFactory()) //
                .setThreadFactory(new NamedThreadFactory("DTG-MQ-Disruptor-", true)) //
                .setProducerType(ProducerType.MULTI) //
                .setWaitStrategy(new BlockingWaitStrategy()) //
                .build();
        this.applyDisruptor.handleEventsWith(new LogEntryAndClosureHandler());
        this.applyDisruptor.setDefaultExceptionHandler(new LogExceptionHandler<Object>(getClass().getSimpleName()));
        this.applyQueue = this.applyDisruptor.start();
        if (this.metrics.getMetricRegistry() != null) {
            this.metrics.getMetricRegistry().register("jraft-node-impl-disruptor",
                    new DisruptorMetricSet(this.applyQueue));
        }
        this.fsmCaller = new MQFSMCaller();
        if (!initLogStorage()) {
            LOG.error("MQ initLogStorage failed.");
            return false;
        }
        if (!initFSMCaller(new MQLogId(0, 0))) {
            LOG.error("MQ initFSMCaller failed.");
            return false;
        }
        this.ballotBox = new MQBallotBox();
        final MQBallotBoxOptions ballotBoxOpts = new MQBallotBoxOptions();
        ballotBoxOpts.setWaiter(this.fsmCaller);
        ballotBoxOpts.setClosureQueue(this.closureQueue);
        if (!this.ballotBox.init(ballotBoxOpts)) {
            LOG.error("Node init ballotBox failed.");
            return false;
        }
        //final Status st = this.logManager.checkConsistency();
        this.TX_LOG_INDEX = new AtomicLong(logManager.getLastLogIndex());
        getUncommitLog();
//        if (!st.isOk()) {
//            LOG.error("MQ is initialized with inconsistent log, status={}.", st);
//            return false;
//        }
        return true;
    }

    private boolean initLogStorage() {
        Requires.requireNonNull(this.fsmCaller, "Null fsm caller");
        this.logStorage = new MQRocksDBLogStorage(opts.getRockDBPath());
        this.logManager = new MQLogManagerImpl();
        final MQLogManagerOptions opts = new MQLogManagerOptions();
        opts.setLogEntryCodecFactory(MQV2LogEntryCodecFactory.getInstance());
        opts.setLogStorage(this.logStorage);
        opts.setFsmCaller(this.fsmCaller);
        opts.setNodeMetrics(this.metrics);
        opts.setDisruptorBufferSize(DTGConstants.disruptorBufferSize);
        return this.logManager.init(opts);
    }

    private boolean initFSMCaller(final MQLogId bootstrapId) {
        if (this.fsmCaller == null) {
            LOG.error("Fail to init fsm caller, null instance, bootstrapId={}.", bootstrapId);
            return false;
        }
        this.closureQueue = new ClosureQueueImpl();
        final MQFSMCallerOptions opts = new MQFSMCallerOptions();
        opts.setAfterShutdown(status -> afterShutdown());
        opts.setLogManager(this.logManager);
        opts.setFsm(this.opts.getFsm());
        opts.setClosureQueue(this.closureQueue);
        opts.setNode(this);
        opts.setBootstrapId(bootstrapId);
        opts.setDisruptorBufferSize(DTGConstants.disruptorBufferSize);
        return this.fsmCaller.init(opts);
    }

    public void apply(final ByteTask task) {
        if (this.shutdownLatch != null) {
            Utils.runClosureInThread(task.getDone(), new Status( TxMQError.ENODESHUTDOWN.getNumber(), "Node is shutting down."));
            throw new IllegalStateException("Node is shutting down");
        }
        Requires.requireNonNull(task, "Null task");

        long index = getLogIndex();
        final TransactionLogEntry entry = new TransactionLogEntry(index);
        //entry.setType();
        entry.setData(task.getByteData());


        //onlyfortest
        TestClosure closure = ((TestClosure)task.getDone());
        closure.setSelfIndex(index);
        closure.setLogManager(logManager);



        int retryTimes = 0;
        try {
            final EventTranslator<TransactionLogEntryAndClosure> translator = (event, sequence) -> {
                event.reset();
                event.done = task.getDone();
                event.entry = entry;
            };
            while (true) {
                if (this.applyQueue.tryPublishEvent(translator)) {
                    break;
                } else {
                    retryTimes++;
                    if (retryTimes > MAX_APPLY_RETRY_TIMES) {
                        Utils.runClosureInThread(task.getDone(),
                                new Status(TxMQError.EBUSY.getNumber(), "Node is busy, has too many tasks."));
                        LOG.warn("MQ applyQueue is overload.");
                        this.metrics.recordTimes("apply-task-overload-times", 1);
                        return;
                    }
                    ThreadHelper.onSpinWait();
                }
            }

        } catch (final Exception e) {
            LOG.error("Fail to apply task.", e);
            Utils.runClosureInThread(task.getDone(), new Status(TxMQError.EPERM.getNumber(), "Node is down."));
        }
    }

    private void afterShutdown() {
        List<Closure> savedDoneList = null;
        this.writeLock.lock();
        try {
            if (!this.shutdownContinuations.isEmpty()) {
                savedDoneList = new ArrayList<>(this.shutdownContinuations);
            }
            if (this.logStorage != null) {
                this.logStorage.shutdown();
            }
            this.state = State.STATE_SHUTDOWN;
        } finally {
            this.writeLock.unlock();
        }
        if (savedDoneList != null) {
            for (final Closure closure : savedDoneList) {
                Utils.runClosureInThread(closure);
            }
        }
    }


    public NodeMetrics getMetrics() {
        return metrics;
    }

    public static class TransactionLogEntryAndClosure {
        TransactionLogEntry       entry;
        Closure                   done;
        long                      expectedTerm;
        CountDownLatch            shutdownLatch;

        public void reset() {
            this.entry = null;
            this.done = null;
            this.expectedTerm = 0;
            this.shutdownLatch = null;
        }
    }

    class LeaderStableClosure extends MQLogManager.StableClosure {

        public LeaderStableClosure(final List<TransactionLogEntry> entries) {
            super(entries);
        }

        @Override
        public void run(final Status status) {
            if (status.isOk()) {
                DTGMQ.this.ballotBox.commitAt(this.firstLogIndex + this.nEntries - 1);
            } else {
                LOG.error("MQ entries append [{}, {}] failed, status={}.", this.firstLogIndex,
                        this.firstLogIndex + this.nEntries - 1, status);
            }
        }
    }

    private static class LogEntryAndClosureFactory implements EventFactory<TransactionLogEntryAndClosure> {

        @Override
        public TransactionLogEntryAndClosure newInstance() {
            return new TransactionLogEntryAndClosure();
        }
    }

    private class LogEntryAndClosureHandler implements EventHandler<TransactionLogEntryAndClosure> {
        // task list for batch
        private final List<TransactionLogEntryAndClosure> tasks = new ArrayList<>(DTGConstants.applyBatch);

        @Override
        public void onEvent(final TransactionLogEntryAndClosure event, final long sequence, final boolean endOfBatch)
                throws Exception {
            if (event.shutdownLatch != null) {
                if (!this.tasks.isEmpty()) {
                    executeApplyingTasks(this.tasks);
                }
                final int num = GLOBAL_NUM_NODES.decrementAndGet();
                LOG.info("The number of active nodes decrement to {}.", num);
                event.shutdownLatch.countDown();
                return;
            }
            this.tasks.add(event);
            if (this.tasks.size() >= DTGConstants.applyBatch || endOfBatch) {
                executeApplyingTasks(this.tasks);
                this.tasks.clear();
            }
        }
    }

    private void executeApplyingTasks(final List<TransactionLogEntryAndClosure> tasks) {
        this.writeLock.lock();
        try {
            final int size = tasks.size();
            final List<TransactionLogEntry> entries = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                final TransactionLogEntryAndClosure task = tasks.get(i);
                if (!this.ballotBox.appendPendingTask(task.done, task.entry.getId().getIndex())) {
                    Utils.runClosureInThread(task.done, new Status(TxMQError.EINTERNAL.getNumber(), "Fail to append task."));
                    continue;
                }
                // set task entry info before adding to list.
                task.entry.setType(EnumOutter.EntryType.ENTRY_TYPE_DATA);
                entries.add(task.entry);
            }
            this.logManager.appendEntries(entries, new LeaderStableClosure(entries));
        } finally {
            this.writeLock.unlock();
        }
    }

    public void onError(final TxMQException error) {
        LOG.warn("Node got error: {}.", error);
        if (this.fsmCaller != null) {
            // onError of fsmCaller is guaranteed to be executed once.
            this.fsmCaller.onError(error);
        }
    }

    public NodeMetrics getNodeMetrics() {
        return this.metrics;
    }

    @Override
    public void shutdown() {
        List<RepeatedTimer> timers = null;
        this.writeLock.lock();
        try {
            LOG.info("MQ shutdown, state={}.", this.state);
            if (this.state.compareTo(State.STATE_SHUTTING) < 0) {
                if (this.logManager != null) {
                    this.logManager.shutdown();
                }
                if (this.fsmCaller != null) {
                    this.fsmCaller.shutdown();
                }
                if (this.applyQueue != null) {
                    final CountDownLatch latch = new CountDownLatch(1);
                    this.shutdownLatch = latch;
                    Utils.runInThread(
                            () -> this.applyQueue.publishEvent((event, sequence) -> event.shutdownLatch = latch));
                } else {
                    final int num = GLOBAL_NUM_NODES.decrementAndGet();
                    LOG.info("The number of active nodes decrement to {}.", num);
                }
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    private synchronized long getLogIndex(){
        return this.TX_LOG_INDEX.incrementAndGet();
    }

    public synchronized void join() throws InterruptedException {
        if (this.shutdownLatch != null) {
            if (this.logManager != null) {
                this.logManager.join();
            }
            this.shutdownLatch.await();
            this.applyDisruptor.shutdown();
            this.shutdownLatch = null;
        }
        if (this.fsmCaller != null) {
            this.fsmCaller.join();
        }
    }

    public UserLog readCommittedUserLog(final long index) {
        if (index <= 0) {
            throw new LogIndexOutOfBoundsException("Request index is invalid: " + index);
        }

        final long savedLastAppliedIndex = this.fsmCaller.getLastAppliedIndex();

        if (index > savedLastAppliedIndex) {
            throw new LogIndexOutOfBoundsException("Request index " + index + " is greater than lastAppliedIndex: "
                    + savedLastAppliedIndex);
        }

        long curIndex = index;
        TransactionLogEntry entry = this.logManager.getEntry(curIndex);
        if (entry == null) {
            throw new LogNotFoundException("User log is deleted at index: " + index);
        }

        do {
            if (entry.getType() == EnumOutter.EntryType.ENTRY_TYPE_DATA) {
                return new UserLog(curIndex, entry.getData());
            } else {
                curIndex++;
            }
            if (curIndex > savedLastAppliedIndex) {
                throw new IllegalStateException("No user log between index:" + index + " and last_applied_index:"
                        + savedLastAppliedIndex);
            }
            entry = this.logManager.getEntry(curIndex);
        } while (entry != null);

        throw new LogNotFoundException("User log is deleted at index: " + curIndex);
    }

    public void describe(final Describer.Printer out) {
        // node
        final String _nodeId;
        final String _state;
        final String _leaderId;
        final long _currTerm;
        final String _conf;
        final int _targetPriority;
        this.readLock.lock();
        try {
            _state = String.valueOf(this.state);
        } finally {
            this.readLock.unlock();
        }

        // logManager
        out.println("logManager: ");
        this.logManager.describe(out);

        // fsmCaller
        out.println("fsmCaller: ");
        this.fsmCaller.describe(out);

        // ballotBox
        out.println("ballotBox: ");
        this.ballotBox.describe(out);

        // log storage
        if (this.logStorage instanceof Describer) {
            out.println("logStorage: ");
            ((Describer) this.logStorage).describe(out);
        }
    }

    @Override
    public String toString() {
        return "MQ";
    }

    private void getUncommitLog(){
        long commitedIndex = this.logManager.geLastCommitIndex();
        long endIndex = this.logManager.getLastLogIndex();
        if(commitedIndex == endIndex){
            return;
        }
        System.out.println("commitedIndex : " + commitedIndex + ",  endIndex : " + endIndex);
        List<TransactionLogEntry> entries = logManager.getEntries(commitedIndex + 1 , endIndex);
        for(TransactionLogEntry entry : entries){


            //onlyfortest
            TestClosure closure = new TestClosure();
            closure.setLogManager(this.logManager);
            long index = entry.getId().getIndex();
            closure.setSelfIndex(index);
            closure.setId((String) ObjectAndByte.toObject(entry.getData().array()));



            if (!this.ballotBox.appendPendingTask(closure, index)) {
                Utils.runClosureInThread(closure, new Status(TxMQError.EINTERNAL.getNumber(), "Fail to append task."));
                continue;
            }
        }
        System.out.println("commitedIndex : " + commitedIndex + "endIndex : " + endIndex);
    }
}

package MQ;

import DBExceptions.TxMQError;
import DBExceptions.TxMQException;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.NodeMetrics;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.error.LogEntryCorruptedException;
import com.alipay.sofa.jraft.error.LogIndexOutOfBoundsException;
import com.alipay.sofa.jraft.util.*;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import config.DTGConstants;
import options.MQLogManagerOptions;
import options.MQLogStorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * @author :jinkai
 * @date :Created in 2019/12/29 20:45
 * @description：
 * @modified By:
 * @version:
 */

public class MQLogManagerImpl implements MQLogManager {

    private static final int                                 APPEND_LOG_RETRY_TIMES = 50;

    private static final Logger                              LOG                    = LoggerFactory
            .getLogger(MQLogManagerImpl.class);
    private MQLogStorage                                     logStorage;
    private final ReadWriteLock                              lock                   = new ReentrantReadWriteLock();
    private final Lock                                       writeLock              = this.lock.writeLock();
    private final Lock                                       readLock               = this.lock.readLock();
    private volatile boolean                                 stopped;
    private volatile boolean                                 hasError;
    private long                                             nextWaitId;
    //private MQLogId                                          diskId;//last append index in disk
    private MQLogId                                          commitId;//last commit index
    private ArrayDeque<TransactionLogEntry>                  logsInMemory           = new ArrayDeque<>();
    private volatile long                                    firstLogIndex;//first index
    private volatile long                                    lastLogIndex;//last index

    private final Map<Long, WaitMeta>                        waitMap                = new HashMap<>();
    private Disruptor<StableClosureEvent>                    disruptor;
    private RingBuffer<StableClosureEvent>                   diskQueue;
    private volatile CountDownLatch                          shutDownLatch;
    private NodeMetrics                                      nodeMetrics;
    private final CopyOnWriteArrayList<LastLogIndexListener> lastLogIndexListeners  = new CopyOnWriteArrayList<>();
    private MQFSMCaller                                      fsmCaller;
    private List<Long>                                       waitCommitList;

    private enum EventType {
        OTHER, // other event type.
        RESET, // reset
        TRUNCATE_PREFIX, // truncate log from prefix
        TRUNCATE_SUFFIX, // truncate log from suffix
        SHUTDOWN, //
        LAST_LOG_ID // get last log id
    }

    private static class StableClosureEvent {
        StableClosure done;
        EventType     type;

        void reset() {
            this.done = null;
            this.type = null;
        }
    }

    private static class StableClosureEventFactory implements EventFactory<StableClosureEvent> {

        @Override
        public StableClosureEvent newInstance() {
            return new StableClosureEvent();
        }
    }

    @Override
    public boolean init(MQLogManagerOptions opts) {
        this.writeLock.lock();
        try {
            if (opts.getLogStorage() == null) {
                LOG.error("Fail to init MQlog manager, log storage is null");
                return false;
            }
            this.nodeMetrics = opts.getNodeMetrics();
            this.logStorage = opts.getLogStorage();
            //this.configManager = opts.getConfigurationManager();

            MQLogStorageOptions lsOpts = new MQLogStorageOptions();
            //lsOpts.setConfigurationManager(this.configManager);
            lsOpts.setLogEntryCodecFactory(opts.getLogEntryCodecFactory());

            if (!this.logStorage.init(lsOpts)) {
                LOG.error("Fail to init MQlogStorage");
                return false;
            }
            this.firstLogIndex = this.logStorage.getFirstLogIndex();
            this.lastLogIndex = this.logStorage.getLastLogIndex();
            //this.diskId = new MQLogId(this.lastLogIndex);
            this.commitId = new MQLogId(this.logStorage.getCommitLogIndex(false));
            this.fsmCaller = opts.getFsmCaller();
            this.disruptor = DisruptorBuilder.<StableClosureEvent> newInstance() //
                    .setEventFactory(new StableClosureEventFactory()) //
                    .setRingBufferSize(opts.getDisruptorBufferSize()) //
                    .setThreadFactory(new NamedThreadFactory("JRaft-LogManager-Disruptor-", true)) //
                    .setProducerType(ProducerType.MULTI) //
                    /*
                     *  Use timeout strategy in log manager. If timeout happens, it will called reportError to halt the node.
                     */
                    .setWaitStrategy(new TimeoutBlockingWaitStrategy(
                            DTGConstants.DisruptorPublishEventWaitTimeoutSecs, TimeUnit.SECONDS)) //
                    .build();
            this.disruptor.handleEventsWith(new StableClosureEventHandler());
            this.disruptor.setDefaultExceptionHandler(new LogExceptionHandler<Object>(this.getClass().getSimpleName(),
                    (event, ex) -> reportError(-1, "MQLogManager handle event error")));
            this.diskQueue = this.disruptor.start();
            if (this.nodeMetrics.getMetricRegistry() != null) {
                this.nodeMetrics.getMetricRegistry().register("jraft-log-manager-disruptor",
                        new DisruptorMetricSet(this.diskQueue));
            }
            this.waitCommitList = new ArrayList<>();
        } finally {
            this.writeLock.unlock();
        }
        return true;
    }

    private void stopDiskThread() {
        this.shutDownLatch = new CountDownLatch(1);
        Utils.runInThread(() -> this.diskQueue.publishEvent((event, sequence) -> {
            event.reset();
            event.type = EventType.SHUTDOWN;
        }));
    }

    @Override
    public void shutdown() {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            if (this.stopped) {
                return;
            }
            this.stopped = true;
            doUnlock = false;
            wakeupAllWaiter(this.writeLock);
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
        stopDiskThread();
    }

    private boolean wakeupAllWaiter(final Lock lock) {
        if (this.waitMap.isEmpty()) {
            lock.unlock();
            return false;
        }
        final List<WaitMeta> wms = new ArrayList<>(this.waitMap.values());
        final int errCode = this.stopped ? TxMQError.ESTOP.getNumber() : TxMQError.SUCCESS.getNumber();
        this.waitMap.clear();
        lock.unlock();

        final int waiterCount = wms.size();
        for (int i = 0; i < waiterCount; i++) {
            final WaitMeta wm = wms.get(i);
            wm.errorCode = errCode;
            Utils.runInThread(() -> runOnNewLog(wm));
        }
        return true;
    }

    void runOnNewLog(final WaitMeta wm) {
        wm.onNewLog.onNewLog(wm.arg, wm.errorCode);
    }

//    @Override
//    public void addLastLogIndexListener(LastLogIndexListener listener) {
//        this.lastLogIndexListeners.add(listener);
//    }
//
//    @Override
//    public void removeLastLogIndexListener(LastLogIndexListener listener) {
//        this.lastLogIndexListeners.remove(listener);
//    }

    @Override
    public void join() throws InterruptedException {
        if (this.shutDownLatch == null) {
            return;
        }
        this.shutDownLatch.await();
        this.disruptor.shutdown();
    }

    @Override
    public void appendEntries(List<TransactionLogEntry> entries, StableClosure done) {
        Requires.requireNonNull(done, "done");
        if (this.hasError) {System.out.println("appendEntries error");
            entries.clear();
            Utils.runClosureInThread(done, new Status(TxMQError.EIO.getNumber(), "Corrupted MQLogStorage"));
            return;
        }
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            if (!entries.isEmpty() && !checkAndResolveConflict(entries, done)) {
                entries.clear();
                Utils.runClosureInThread(done, new Status(TxMQError.EINTERNAL.getNumber(), "Fail to checkAndResolveConflict."));
                return;
            }
            for (int i = 0; i < entries.size(); i++) {
                final TransactionLogEntry entry = entries.get(i);
                // Set checksum after checkAndResolveConflict
                if(DTGConstants.TX_LOG_SAVE_LEVEL){
                    entry.setChecksum(entry.checksum());
                }
            }
            if (!entries.isEmpty()) {
                done.setFirstLogIndex(entries.get(0).getId().getIndex());
                this.logsInMemory.addAll(entries);
            }
            done.setEntries(entries);

            int retryTimes = 0;
            final EventTranslator<StableClosureEvent> translator = (event, sequence) -> {
                event.reset();
                event.type = EventType.OTHER;
                event.done = done;
            };
            while (true) {
                if (tryOfferEvent(done, translator)) {
                    break;
                } else {
                    retryTimes++;
                    if (retryTimes > APPEND_LOG_RETRY_TIMES) {
                        reportError(TxMQError.EBUSY.getNumber(), "MQLogManager is busy, disk queue overload.");
                        return;
                    }
                    ThreadHelper.onSpinWait();
                }
            }
            doUnlock = false;
            if (!wakeupAllWaiter(this.writeLock)) {
                //notifyLastLogIndexListeners();
            }
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    private boolean tryOfferEvent(final StableClosure done, final EventTranslator<StableClosureEvent> translator) {
        if (this.stopped) {
            Utils.runClosureInThread(done, new Status(TxMQError.ESTOP.getNumber(), "MQLog manager is stopped."));
            return true;
        }
        return this.diskQueue.tryPublishEvent(translator);
    }

//    private void notifyLastLogIndexListeners() {
//        for (int i = 0; i < this.lastLogIndexListeners.size(); i++) {
//            final LastLogIndexListener listener = this.lastLogIndexListeners.get(i);
//            if (listener != null) {
//                try {
//                    listener.onLastLogIndexChanged(this.lastLogIndex);
//                } catch (final Exception e) {
//                    LOG.error("Fail to notify LastLogIndexListener, listener={}, index={}", listener, this.lastLogIndex);
//                }
//            }
//        }
//    }

    private boolean checkAndResolveConflict(final List<TransactionLogEntry> entries, final StableClosure done) {
        final TransactionLogEntry firstLogEntry = ArrayDeque.peekFirst(entries);
        if (firstLogEntry.getId().getIndex() <= lastLogIndex) {
            for (int i = 0; i < entries.size(); i++) {
                entries.get(i).getId().setIndex(++this.lastLogIndex);
            }
            return true;
        } else {
            if (firstLogEntry.getId().getIndex() > this.lastLogIndex + 1) {
                Utils.runClosureInThread(done, new Status(TxMQError.EINVAL.getNumber(),
                        "There's gap between first_index=%d and last_log_index=%d", firstLogEntry.getId().getIndex(),
                        this.lastLogIndex));
                return false;
            }
            final TransactionLogEntry lastLogEntry = ArrayDeque.peekLast(entries);
            this.lastLogIndex = lastLogEntry.getId().getIndex();
            return true;
        }
    }

    private void offerEvent(final StableClosure done, final EventType type) {
        if (this.stopped) {
            Utils.runClosureInThread(done, new Status(TxMQError.ESTOP.getNumber(), "MQLog manager is stopped."));
            return;
        }
        if (!this.diskQueue.tryPublishEvent((event, sequence) -> {
            event.reset();
            event.type = type;
            event.done = done;
        })) {
            reportError(TxMQError.EBUSY.getNumber(), "MQLog manager is overload.");
            Utils.runClosureInThread(done, new Status(TxMQError.EBUSY.getNumber(), "MQLog manager is overload."));
        }
    }



    @Override
    public void clearBufferedLogs() {
        this.writeLock.lock();
        try {
            if (this.commitId.getIndex() != 0) {
                truncatePrefix(this.commitId.getIndex());
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    private boolean truncatePrefix(final long firstIndexKept) {
        int index = 0;
        for (final int size = this.logsInMemory.size(); index < size; index++) {
            final TransactionLogEntry entry = this.logsInMemory.get(index);
            if (entry.getId().getIndex() > firstIndexKept) {
                break;
            }
        }
        if (index > 0) {
            this.logsInMemory.removeRange(0, index);
        }

        // TODO  maybe it's fine here
        Requires.requireTrue(firstIndexKept >= this.firstLogIndex,
                "Try to truncate logs before %d, but the firstLogIndex is %d", firstIndexKept, this.firstLogIndex);

        this.firstLogIndex = firstIndexKept;
        if (firstIndexKept > this.lastLogIndex) {
            // The entry log is dropped
            this.lastLogIndex = firstIndexKept;
        }
        LOG.debug("Truncate prefix, firstIndexKept is :{}", firstIndexKept);
        //this.configManager.truncatePrefix(firstIndexKept);
        final TruncatePrefixClosure c = new TruncatePrefixClosure(firstIndexKept);
        offerEvent(c, EventType.TRUNCATE_PREFIX);
        return true;
    }

    private String descLogsInMemory() {
        final StringBuilder sb = new StringBuilder();
        boolean wasFirst = true;
        for (final TransactionLogEntry logEntry : this.logsInMemory) {
            if (!wasFirst) {
                sb.append(",");
            } else {
                wasFirst = false;
            }
            sb.append("<id:(").append(logEntry.getId().getTransactionId()).append(",").append(logEntry.getId().getIndex())
                    .append("),type:").append(logEntry.getType()).append(">");
        }
        return sb.toString();
    }

    protected TransactionLogEntry getEntryFromMemory(final long index) {
        TransactionLogEntry entry = null;
        if (!this.logsInMemory.isEmpty()) {
            final long firstIndex = this.logsInMemory.peekFirst().getId().getIndex();
            final long lastIndex = this.logsInMemory.peekLast().getId().getIndex();
            if (lastIndex - firstIndex + 1 != this.logsInMemory.size()) {
                throw new IllegalStateException(String.format("lastIndex=%d,firstIndex=%d,logsInMemory=[%s]",
                        lastIndex, firstIndex, descLogsInMemory()));
            }
            if (index >= firstIndex && index <= lastIndex) {
                entry = this.logsInMemory.get((int) (index - firstIndex));
            }
        }
        return entry;
    }

    @Override
    public TransactionLogEntry getEntry(long index) {
        this.readLock.lock();
        try {
            if (index > this.lastLogIndex || index < this.firstLogIndex) {
                return null;
            }
            final TransactionLogEntry entry = getEntryFromMemory(index);
            if (entry != null) {
                return entry;
            }
        } finally {
            this.readLock.unlock();
        }
        final TransactionLogEntry entry = this.logStorage.getEntry(index);
        if (entry == null) {
            reportError(TxMQError.EIO.getNumber(), "Corrupted entry at index=%d, not found", index);
        }
        // Validate checksum
        if (entry != null && DTGConstants.TX_LOG_SAVE_LEVEL && entry.isCorrupted()) {
            String msg = String.format("MQ Corrupted entry at index=%d, term=%d, expectedChecksum=%d, realChecksum=%d",
                    index, entry.getId().getTransactionId(), entry.getChecksum(), entry.checksum());
            // Report error to node and throw exception.
            reportError(TxMQError.EIO.getNumber(), msg);
            throw new LogEntryCorruptedException(msg);
        }
        return entry;
    }

    @Override
    public long getFirstLogIndex() {
        this.readLock.lock();
        try {
            return this.firstLogIndex;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public long getLastLogIndex() {
        return getLastLogIndex(false);
    }

    @Override
    public void setCommitIndex(long index) {
        this.commitId = new MQLogId(index);
        this.logStorage.setCommitLogIndex(index);
    }

    @Override
    public long getLastLogIndex(boolean isFlush) {
        LastLogIdClosure c;
        this.readLock.lock();
        try {
            if (!isFlush) {
                return this.lastLogIndex;
            } else {
                if (this.lastLogIndex == this.commitId.getIndex()) {
                    return this.lastLogIndex;
                }
                c = new LastLogIdClosure();
                offerEvent(c, EventType.LAST_LOG_ID);
            }
        } finally {
            this.readLock.unlock();
        }
        try {
            c.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        return c.lastLogId.getIndex();
    }

    @Override
    public MQLogId getLastLogId(boolean isFlush) {
        LastLogIdClosure c;
        this.readLock.lock();
        try {
            if (!isFlush) {
                if (this.lastLogIndex >= this.firstLogIndex) {
                    return new MQLogId(this.lastLogIndex);
                }
                else {
                    throw new LogIndexOutOfBoundsException("last index is invalid");
                }
            } else {
                c = new LastLogIdClosure();
                offerEvent(c, EventType.LAST_LOG_ID);
            }
        } finally {
            this.readLock.unlock();
        }
        try {
            c.await();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
        return c.lastLogId;
    }

    @Override
    public long wait(long expectedLastLogIndex, NewLogCallback cb, Object arg) {
        final WaitMeta wm = new WaitMeta(cb, arg, 0);
        return notifyOnNewLog(expectedLastLogIndex, wm);
    }

    private long notifyOnNewLog(final long expectedLastLogIndex, final WaitMeta wm) {
        this.writeLock.lock();
        try {
            if (expectedLastLogIndex != this.lastLogIndex || this.stopped) {
                wm.errorCode = this.stopped ? TxMQError.ESTOP.getNumber() : 0;
                Utils.runInThread(() -> runOnNewLog(wm));
                return 0L;
            }
            if (this.nextWaitId == 0) { //skip 0
                ++this.nextWaitId;
            }
            final long waitId = this.nextWaitId++;
            this.waitMap.put(waitId, wm);
            return waitId;
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public boolean removeWaiter(long id) {
        this.writeLock.lock();
        try {
            return this.waitMap.remove(id) != null;
        } finally {
            this.writeLock.unlock();
        }
    }

//    @Override
//    public void setAppliedId(MQLogId appliedId) {
//        MQLogId clearId;
//        this.writeLock.lock();
//        try {
//            if (appliedId.compareTo(this.commitId) < 0) {
//                return;
//            }
//            this.commitId = appliedId.copy();
//            clearId = this.diskId.compareTo(this.commitId) <= 0 ? this.diskId : this.commitId;
//        } finally {
//            this.writeLock.unlock();
//        }
//        if (clearId != null) {
//            clearMemoryLogs(clearId);
//        }
//    }

    @Override
    public long geLastCommitIndex() {
        return this.logStorage.getCommitLogIndex(true);
    }

    @Override
    public List<TransactionLogEntry> getEntries(long start, long end) {
        this.readLock.lock();
        try{
            List<TransactionLogEntry> list = this.logStorage.getEntries(start, end);
            if(list == null || list.size() == 0){
                reportError(TxMQError.EIO.getNumber(), "Corrupted entry between start=%d, end=%d not found", start, end);
            }
            for(TransactionLogEntry entry : list){
                if (entry != null && DTGConstants.TX_LOG_SAVE_LEVEL && entry.isCorrupted()) {
                    String msg = String.format("MQ Corrupted entry at index=%d, term=%d, expectedChecksum=%d, realChecksum=%d",
                            entry.getId().getIndex(), entry.getId().getTransactionId(), entry.getChecksum(), entry.checksum());
                    // Report error to node and throw exception.
                    reportError(TxMQError.EIO.getNumber(), msg);
                    throw new LogEntryCorruptedException(msg);
                }
            }
            return list;
        }finally {
            this.readLock.unlock();
        }
    }

//    @Override
//    public Status checkConsistency() {
//        this.readLock.lock();
//        try {
//            Requires.requireTrue(this.firstLogIndex >= 0);
//            Requires.requireTrue(this.lastLogIndex > 0);
//
//        } finally {
//            this.readLock.unlock();
//        }
//    }

    @Override
    public void describe(Printer out) {
        final long _firstLogIndex;
        final long _lastLogIndex;
        final String _commitId;
        this.readLock.lock();
        try {
            _firstLogIndex = this.firstLogIndex;
            _lastLogIndex = this.lastLogIndex;
            _commitId = String.valueOf(this.commitId);
        } finally {
            this.readLock.unlock();
        }
        out.print("  storage: [") //
                .print(_firstLogIndex) //
                .print(", ") //
                .print(_lastLogIndex) //
                .println(']');
        out.print("  appliedId: ") //
                .println(_commitId);
    }

    /**
     * Waiter metadata
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-04 5:05:04 PM
     */
    private static class WaitMeta {
        /** callback when new log come in*/
        NewLogCallback onNewLog;
        /** callback error code*/
        int            errorCode;
        /** the waiter pass-in argument */
        Object         arg;

        public WaitMeta(final NewLogCallback onNewLog, final Object arg, final int errorCode) {
            super();
            this.onNewLog = onNewLog;
            this.arg = arg;
            this.errorCode = errorCode;
        }

    }

    private class StableClosureEventHandler implements EventHandler<StableClosureEvent> {
        MQLogId             lastId  = new MQLogId(MQLogManagerImpl.this.lastLogIndex);
        List<StableClosure> storage = new ArrayList<>(256);
        AppendBatcher       ab      = new AppendBatcher(this.storage, 256, new ArrayList<>(),
                lastId);

        @Override
        public void onEvent(final StableClosureEvent event, final long sequence, final boolean endOfBatch)
                throws Exception {
            if (event.type == EventType.SHUTDOWN) {
                this.lastId = this.ab.flush();
                //setDiskId(this.lastId);
                MQLogManagerImpl.this.shutDownLatch.countDown();
                return;
            }
            final StableClosure done = event.done;

            if (done.getEntries() != null && !done.getEntries().isEmpty()) {
                this.ab.append(done);
            } else {
                this.lastId = this.ab.flush();
                boolean ret = true;
                switch (event.type) {
                    case LAST_LOG_ID:
                        ((LastLogIdClosure) done).setLastLogId(this.lastId.copy());
                        break;
                    case TRUNCATE_PREFIX:
                        long startMs = Utils.monotonicMs();
                        try {
                            final TruncatePrefixClosure tpc = (TruncatePrefixClosure) done;
                            LOG.debug("Truncating storage to firstIndexKept={}", tpc.firstIndexKept);
                            ret = MQLogManagerImpl.this.logStorage.truncatePrefix(tpc.firstIndexKept);
                        } finally {
                            MQLogManagerImpl.this.nodeMetrics.recordLatency("truncate-log-prefix", Utils.monotonicMs()
                                    - startMs);
                        }
                        break;
                    case TRUNCATE_SUFFIX:
                        startMs = Utils.monotonicMs();
                        try {
                            final TruncateSuffixClosure tsc = (TruncateSuffixClosure) done;
                            LOG.warn("Truncating storage to lastIndexKept={}", tsc.lastIndexKept);
                            ret = MQLogManagerImpl.this.logStorage.truncateSuffix(tsc.lastIndexKept);
                            if (ret) {
                                this.lastId.setIndex(tsc.lastIndexKept);
                                this.lastId.setTransactionId(tsc.txId);
                                Requires.requireTrue(this.lastId.getIndex() == 0 || this.lastId.getTransactionId() >= 0);
                            }
                        } finally {
                            MQLogManagerImpl.this.nodeMetrics.recordLatency("truncate-log-suffix", Utils.monotonicMs()
                                    - startMs);
                        }
                        break;
                    case RESET:
                        final ResetClosure rc = (ResetClosure) done;
                        LOG.info("Reseting storage to nextLogIndex={}", rc.nextLogIndex);
                        ret = MQLogManagerImpl.this.logStorage.reset(rc.nextLogIndex);
                        break;
                    default:
                        break;
                }

                if (!ret) {
                    reportError(TxMQError.EIO.getNumber(), "Failed operation in MQLogStorage");
                } else {
                    done.run(Status.OK());
                }
            }
            if (endOfBatch) {
                this.lastId = this.ab.flush();
                //setDiskId(this.lastId);
            }
        }

    }

    private static class LastLogIdClosure extends StableClosure {

        public LastLogIdClosure() {
            super(null);
        }

        private MQLogId lastLogId;

        void setLastLogId(final MQLogId logId) {
            Requires.requireTrue(logId.getIndex() == 0 || logId.getTransactionId() >= 0);
            this.lastLogId = logId;
        }

        private final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void run(final Status status) {
            this.latch.countDown();
        }

        void await() throws InterruptedException {
            this.latch.await();
        }

    }

    private static class TruncatePrefixClosure extends StableClosure {
        long firstIndexKept;

        public TruncatePrefixClosure(final long firstIndexKept) {
            super(null);
            this.firstIndexKept = firstIndexKept;
        }

        @Override
        public void run(final Status status) {

        }

    }

    private static class ResetClosure extends StableClosure {
        long nextLogIndex;

        public ResetClosure(final long nextLogIndex) {
            super(null);
            this.nextLogIndex = nextLogIndex;
        }

        @Override
        public void run(final Status status) {

        }
    }

    private static class TruncateSuffixClosure extends StableClosure {
        long lastIndexKept;
        long txId;

        public TruncateSuffixClosure(final long lastIndexKept, final long txId) {
            super(null);
            this.lastIndexKept = lastIndexKept;
            this.txId = txId;
        }

        @Override
        public void run(final Status status) {

        }

    }

    private class AppendBatcher {
        List<StableClosure>            storage;
        int                            cap;
        int                            size;
        int                            bufferSize;
        List<TransactionLogEntry>      toAppend;
        MQLogId                        lastId;

        public AppendBatcher(final List<StableClosure> storage, final int cap, final List<TransactionLogEntry> toAppend,
                             final MQLogId lastId) {
            super();
            this.storage = storage;
            this.cap = cap;
            this.toAppend = toAppend;
            this.lastId = lastId;
        }

        MQLogId flush() {
            if (this.size > 0) {
                this.lastId = appendToStorage(this.toAppend);
                for (int i = 0; i < this.size; i++) {
                    if (MQLogManagerImpl.this.hasError) {
                        this.storage.get(i).run(new Status(TxMQError.EIO.getNumber(), "Corrupted MQLogStorage"));
                    } else {
                        this.storage.get(i).run(Status.OK());
                    }

                }
                this.toAppend.clear();
                this.storage.clear();
            }
            this.size = 0;
            this.bufferSize = 0;
            return this.lastId;
        }

        void append(final StableClosure done) {
//            if (this.size == this.cap || this.bufferSize >= DTGConstants.maxAppendBufferSize) {
//                flush();
//            }
            this.storage.add(done);
            this.size++;
            this.toAppend.addAll(done.getEntries());
            for (final TransactionLogEntry entry : done.getEntries()) {
                this.bufferSize += entry.getData() != null ? entry.getData().remaining() : 0;
            }
            flush();
        }
    }

    private MQLogId appendToStorage(final List<TransactionLogEntry> toAppend) {
        MQLogId lastId = null;
        if (!this.hasError) {
            final long startMs = Utils.monotonicMs();
            final int entriesCount = toAppend.size();
            this.nodeMetrics.recordSize("append-logs-count", entriesCount);
            try {
                int writtenSize = 0;
                for (int i = 0; i < entriesCount; i++) {
                    final TransactionLogEntry entry = toAppend.get(i);
                    writtenSize += entry.getData() != null ? entry.getData().remaining() : 0;
                }
                this.nodeMetrics.recordSize("append-logs-bytes", writtenSize);
                final int nAppent = this.logStorage.appendEntries(toAppend);
                if (nAppent != entriesCount) {
                    LOG.error("**Critical error**, fail to appendEntries, nAppent={}, toAppend={}", nAppent,
                            toAppend.size());
                    reportError(TxMQError.EIO.getNumber(), "Fail to append log entries");
                }
                if (nAppent > 0) {
                    lastId = toAppend.get(nAppent - 1).getId();
                }
                toAppend.clear();
            } finally {
                this.nodeMetrics.recordLatency("append-logs", Utils.monotonicMs() - startMs);
            }
        }
        return lastId;
    }

    private void reportError(final int code, final String fmt, final Object... args) {
        this.hasError = true;
        final TxMQException error = new TxMQException(EnumOutter.ErrorType.ERROR_TYPE_LOG);
        error.setStatus(new Status(code, fmt, args));
        this.fsmCaller.onError(error);
    }

//    private void setDiskId(final MQLogId id) {
//        if (id == null) {
//            return;
//        }
//        MQLogId clearId;
//        this.writeLock.lock();
//        try {
//            if (id.compareTo(this.diskId) < 0) {
//                return;
//            }
//            this.diskId = id;
//            clearId = this.diskId.compareTo(this.commitId) <= 0 ? this.diskId : this.commitId;
//        } finally {
//            this.writeLock.unlock();
//        }
//        if (clearId != null) {
//            clearMemoryLogs(clearId);
//        }
//    }

    private void clearMemoryLogs(final MQLogId id) {
        this.writeLock.lock();
        try {
            int index = 0;
            for (final int size = this.logsInMemory.size(); index < size; index++) {
                final TransactionLogEntry entry = this.logsInMemory.get(index);
                if (entry.getId().compareTo(id) > 0) {
                    break;
                }
            }
            if (index > 0) {
                this.logsInMemory.removeRange(0, index);
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    private boolean reset(final long nextLogIndex) {
        this.writeLock.lock();
        try {
            this.logsInMemory = new ArrayDeque<>();
            this.firstLogIndex = nextLogIndex;
            this.lastLogIndex = nextLogIndex;
            //this.configManager.truncatePrefix(this.firstLogIndex);
            //this.configManager.truncateSuffix(this.lastLogIndex);
            final ResetClosure c = new ResetClosure(nextLogIndex);
            offerEvent(c, EventType.RESET);
            return true;
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void addToWaitCommit(long commitIndex){
        long saveCommitIndex = this.commitId.getIndex();
        if(commitIndex == 3){
            int a= 1;
        }
        int i = 0;
        for(long index : waitCommitList){
            if(index == saveCommitIndex + 1){
                saveCommitIndex = index;
            }
            if(index > commitIndex){
                break;
            }
            i++;
        }
        waitCommitList.add(i, commitIndex);
        for(; i < waitCommitList.size(); i++){
            long index = waitCommitList.get(i);
            if(index == saveCommitIndex + 1){
                saveCommitIndex = index;
            }else {
                break;
            }
        }
        if(saveCommitIndex != this.commitId.getIndex()){//System.out.println("commit : " + saveCommitIndex);
            this.setCommitIndex(saveCommitIndex);
        }
    }

//    private void unsafeTruncateSuffix(final long lastIndexKept) {
//        if (lastIndexKept < this.commitId.getIndex()) {
//            LOG.error("FATAL ERROR: Can't truncate logs before commitId={}, lastIndexKept={}", this.commitId,
//                    lastIndexKept);
//            return;
//        }
//        while (!this.logsInMemory.isEmpty()) {
//            final TransactionLogEntry entry = this.logsInMemory.peekLast();
//            if (entry.getId().getIndex() > lastIndexKept) {
//                this.logsInMemory.pollLast();
//            } else {
//                break;
//            }
//        }
//        this.lastLogIndex = lastIndexKept;
//        //final long lastTermKept = unsafeGetTerm(lastIndexKept);
//        Requires.requireTrue(this.lastLogIndex == 0);
//        LOG.debug("Truncate suffix :{}", lastIndexKept);
//       // this.configManager.truncateSuffix(lastIndexKept);
//        final TruncateSuffixClosure c = new TruncateSuffixClosure(lastIndexKept, commitId.getIndex());
//        offerEvent(c, EventType.TRUNCATE_SUFFIX);
//    }
}

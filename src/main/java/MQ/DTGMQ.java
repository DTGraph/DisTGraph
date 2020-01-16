package MQ;

import DBExceptions.TxMQError;
import MQ.codec.v2.MQV2LogEntryCodecFactory;
import Region.DTGRegion;
import UserClient.Transaction.TransactionLog;
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ClosureQueue;
import com.alipay.sofa.jraft.closure.ClosureQueueImpl;
import com.alipay.sofa.jraft.util.*;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import config.DTGConstants;
import options.MQLogStorageOptions;
import options.MQOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author :jinkai
 * @date :Created in 2020/1/12 14:22
 * @description：
 * @modified By:
 * @version:
 */

public class DTGMQ implements Lifecycle<MQOptions> {

    private static AtomicLong TX_LOG_INDEX          = new AtomicLong(0);
    private static final int  MAX_APPLY_RETRY_TIMES = 3;

    private MQLogStorage                              logStorage;
    private Disruptor<TransactionLogEntryAndClosure>  applyDisruptor;
    private RingBuffer<TransactionLogEntryAndClosure> applyQueue;

    private Disruptor<RunClosure>                     commitDisruptor;
    private RingBuffer<RunClosure>                    commitQueue;

    private volatile CountDownLatch                   shutdownLatch;
    private ClosureQueue                              closureQueue;
    private final MQStateMachine                      stateMachine;
    private List<Long>                                waitCommitList;
    private long                                      commitIndex;

    public DTGMQ(){
        this.stateMachine = new MQStateMachine();
    }

    private static class LogEntryAndClosureFactory implements EventFactory<TransactionLogEntryAndClosure> {

        @Override
        public TransactionLogEntryAndClosure newInstance() {
            return new TransactionLogEntryAndClosure();
        }
    }

    private static class RunClosureFactory implements EventFactory<RunClosure> {

        @Override
        public RunClosure newInstance() {
            return new RunClosure();
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

    public static class RunClosure implements Closure{

        Closure done;
        CountDownLatch     shutdownLatch;

        public void setDone(Closure done) {
            this.done = done;
        }

        public Closure getDone() {
            return done;
        }

        @Override
        public void run(Status status) {
            if(this.done != null){
                this.done.run(status);
            }
        }

        public void reset(){
            this.done = null;
            this.shutdownLatch = null;
        }
    }

    public class InternalRunClosure implements Closure{
        TransactionLogEntry log;

        public void setLog(TransactionLogEntry log) {
            this.log = log;
        }

        public TransactionLogEntry getLog() {
            return log;
        }

        @Override
        public void run(Status status) {
            addToWaitCommit(this.log.getId().getIndex());
            System.out.println("success : " + log.getId() + ", time : " + System.currentTimeMillis() );
        }
    }

    private class RunClosureHandler implements EventHandler<RunClosure> {
        // task list for batch
        private final List<RunClosure> tasks = new ArrayList<>(DTGConstants.applyBatch);

        @Override
        public void onEvent(final RunClosure event, final long sequence, final boolean endOfBatch)
                throws Exception {

            if (event.shutdownLatch != null) {
                if (!this.tasks.isEmpty()) {
                    stateMachine.onApply(tasks);
                }
                event.shutdownLatch.countDown();
                return;
            }
            this.tasks.add(event);
            if (this.tasks.size() >= DTGConstants.applyBatch || endOfBatch) {
                stateMachine.onApply(tasks);
                this.tasks.clear();
            }
        }
    }

    public void executeApplyingTasks(final List<TransactionLogEntryAndClosure> tasks){
        List<TransactionLogEntry> logEntries = new ArrayList<>();
        for(TransactionLogEntryAndClosure closure : tasks){
            logEntries.add(closure.getEntry());
        }
        int appiled = logStorage.appendEntries(logEntries);
        if(appiled == tasks.size()){
            for(TransactionLogEntryAndClosure closure : tasks){
                closure.run(Status.OK());
                InternalRunClosure internalRunClosure = new InternalRunClosure();
                internalRunClosure.setLog(closure.getEntry());
                publishCommit(internalRunClosure);
            }
        }else {
            for(TransactionLogEntryAndClosure closure : tasks){
                Status status = new Status(TxMQError.SAVEFAILED.getNumber(), "Can not save TransactionLogEntry");
                closure.run(status);
            }
        }
    }


    @Override
    public boolean init(MQOptions opts) {
        this.logStorage = new MQRocksDBLogStorage(opts.getRockDBPath());
        MQLogStorageOptions lsOpts = new MQLogStorageOptions();
        lsOpts.setLogEntryCodecFactory(MQV2LogEntryCodecFactory.getInstance());
        this.logStorage.init(lsOpts);
        this.TX_LOG_INDEX = new AtomicLong(logStorage.getLastLogIndex());
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

        this.commitDisruptor = DisruptorBuilder.<RunClosure> newInstance() //
                .setRingBufferSize(DTGConstants.disruptorBufferSize) //
                .setEventFactory(new RunClosureFactory()) //
                .setThreadFactory(new NamedThreadFactory("DTG-MQ-Disruptor-", true)) //
                .setProducerType(ProducerType.MULTI) //
                .setWaitStrategy(new BlockingWaitStrategy()) //
                .build();
        this.commitDisruptor.handleEventsWith(new RunClosureHandler());
        this.commitDisruptor.setDefaultExceptionHandler(new LogExceptionHandler<Object>(getClass().getSimpleName()));
        this.commitQueue = this.commitDisruptor.start();
        this.closureQueue = new ClosureQueueImpl();
        this.commitIndex = this.logStorage.getCommitLogIndex(true);
        this.waitCommitList = new ArrayList<>();
        if(!this.stateMachine.init(opts.getSaveStore())){
            return false;
        }
        InitUncommitLog();
        return true;
    }

    @Override
    public void shutdown() {

    }

    public void apply(final ByteTask task){
        if (this.shutdownLatch != null) {
            Utils.runClosureInThread(task.getDone(), new Status( TxMQError.ENODESHUTDOWN.getNumber(), "MQ is shutting down."));
            throw new IllegalStateException("Node is shutting down");
        }
        Requires.requireNonNull(task, "Null task");

        long index = getTxLogId();
        final TransactionLogEntry entry = new TransactionLogEntry(index);
        entry.setData(task.getByteData());

        int retryTimes = 0;
        try {
            final EventTranslator<TransactionLogEntryAndClosure> translator = (event, sequence) -> {
                event.reset();
                event.setDone(task.getDone());
                event.setEntry(entry);
            };
            while (true) {
                if (this.applyQueue.tryPublishEvent(translator)) {
                    break;
                } else {
                    retryTimes++;
                    if (retryTimes > MAX_APPLY_RETRY_TIMES) {
                        Utils.runClosureInThread(task.getDone(),
                                new Status(TxMQError.EBUSY.getNumber(), "MQ is busy, has too many tasks."));
                        return;
                    }
                    ThreadHelper.onSpinWait();
                }
            }
        } catch (final Exception e) {
            Utils.runClosureInThread(task.getDone(), new Status(TxMQError.EPERM.getNumber(), "MQ is down."));
        }
    }

    private void publishCommit(InternalRunClosure closure){
        int retryTimes = 0;
        final EventTranslator<RunClosure> translator = (event, sequence) -> {
            event.reset();
            event.setDone(closure);
        };
        while (true) {
            if (this.commitQueue.tryPublishEvent(translator)) {
                break;
            } else {
                retryTimes++;
                if (retryTimes > MAX_APPLY_RETRY_TIMES) {
                    Utils.runClosureInThread(closure,
                            new Status(TxMQError.COMMITERROR.getNumber(), "MQ commit failed!."));
                    return;
                }
                ThreadHelper.onSpinWait();
            }
        }
    }

    public void addToWaitCommit(long commitIndex){
        long saveCommitIndex = this.commitIndex;
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
        if(saveCommitIndex != this.commitIndex){//System.out.println("commit : " + saveCommitIndex);
            this.logStorage.setCommitLogIndex(saveCommitIndex);
        }
    }

    private long getTxLogId(){
        return this.TX_LOG_INDEX.incrementAndGet();
    }

    private void InitUncommitLog(){
        long commitedIndex = this.logStorage.getCommitLogIndex(true);
        long endIndex = this.logStorage.getLastLogIndex();
        if(commitedIndex == endIndex){
            return;
        }
        List<TransactionLogEntry> list = this.logStorage.getEntries(commitedIndex, endIndex);
        for(TransactionLogEntry entry : list){
            InternalRunClosure internalRunClosure = new InternalRunClosure();
            internalRunClosure.setLog(entry);
            publishCommit(internalRunClosure);
        }
}

}

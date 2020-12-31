package MQ;

import DBExceptions.TxMQError;
import Element.EntityEntry;
import LocalDBMachine.LocalDB;
import MQ.codec.v2.MQV2LogEntryCodecFactory;
import Region.DTGRegion;
import Region.DTGRegionEngine;
import UserClient.DTGSaveStore;
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
import tool.ObjectAndByte;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static com.alipay.sofa.jraft.entity.EnumOutter.EntryType.ENTRY_TYPE_DATA;
import static com.alipay.sofa.jraft.entity.EnumOutter.EntryType.ENTRY_TYPE_NO_OP;

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

//    private Disruptor<RunClosure>                     commitDisruptor;
//    private RingBuffer<RunClosure>                    commitQueue;

    private volatile CountDownLatch                   shutdownLatch;

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

    public void executeApplyingTasks(final List<TransactionLogEntryAndClosure> tasks){
        List<TransactionLogEntry> logEntries = new ArrayList<>();
        for(TransactionLogEntryAndClosure closure : tasks){
            logEntries.add(closure.getEntry());
        }
        int appiled = logStorage.appendEntries(logEntries);
        if(appiled == tasks.size()){
            for(TransactionLogEntryAndClosure closure : tasks){
                closure.run(Status.OK());
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

        if(opts.getSaveStore() != null){
            List<DTGRegionEngine> list = opts.getSaveStore().getStoreEngine().getAllRegionEngines();
            List<Long> regionIds = new ArrayList<>() ;
            for(DTGRegionEngine r : list){
                regionIds.add(r.getRegion().getId());
            }
            //reRunUncommitLog(opts.getSaveStore());
        }
//        else if(opts.getLocalDB() != null){
//            reRunUncommitLog(opts.getLocalDB());
//        }
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

        //long index = getTxLogId();
        final TransactionLogEntry entry = new TransactionLogEntry(task.getVersion());
        if(task.getByteData() != null){
            entry.setData(task.getByteData());
            entry.setType(ENTRY_TYPE_DATA);
        }else{
            entry.setType(ENTRY_TYPE_NO_OP);
        }
        if(task.getMainRegion() >= 0){
            entry.setMainRegion(task.getMainRegion());
        }
        entry.setStatus(task.getTxStatus());

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

    public void applyCommit(final ByteTask task){
        if (this.shutdownLatch != null) {
            Utils.runClosureInThread(task.getDone(), new Status( TxMQError.ENODESHUTDOWN.getNumber(), "MQ is shutting down."));
            throw new IllegalStateException("Node is shutting down");
        }
        if(this.logStorage.removeEntry(task.getVersion()) && this.logStorage.removeStatusEntry(task.getVersion())){
            task.getDone().run(Status.OK());
        }
    }

    public byte getStatus(long version){
        TransactionLogEntry entry = this.logStorage.getStatusEntry(version);
        return entry.getStatus();
    }

//    private void publishCommit(InternalRunClosure closure){
//        int retryTimes = 0;
//        final EventTranslator<RunClosure> translator = (event, sequence) -> {
//            event.reset();
//            event.setDone(closure);
//        };
//        while (true) {
//            if (this.commitQueue.tryPublishEvent(translator)) {
//                break;
//            } else {
//                retryTimes++;
//                if (retryTimes > MAX_APPLY_RETRY_TIMES) {
//                    Utils.runClosureInThread(closure,
//                            new Status(TxMQError.COMMITERROR.getNumber(), "MQ commit failed!."));
//                    return;
//                }
//                ThreadHelper.onSpinWait();
//            }
//        }
//    }
//
//    public void addToWaitCommit(long commitIndex){
//        long saveCommitIndex = this.commitIndex;
//        int i = 0;
//        for(long index : waitCommitList){
//            if(index == saveCommitIndex + 1){
//                saveCommitIndex = index;
//            }
//            if(index > commitIndex){
//                break;
//            }
//            i++;
//        }
//        waitCommitList.add(i, commitIndex);
//        for(; i < waitCommitList.size(); i++){
//            long index = waitCommitList.get(i);
//            if(index == saveCommitIndex + 1){
//                saveCommitIndex = index;
//            }else {
//                break;
//            }
//        }
//        if(saveCommitIndex != this.commitIndex){//System.out.println("commit : " + saveCommitIndex);
//            this.logStorage.setCommitLogIndex(saveCommitIndex);
//        }
//    }

//    private long getTxLogId(){
//        return this.TX_LOG_INDEX.incrementAndGet();
//    }

//    private void InitUncommitLog(){
//        long commitedIndex = this.logStorage.getCommitLogIndex(true);
//        long endIndex = this.logStorage.getLastLogIndex();
//        if(commitedIndex == endIndex){
//            return;
//        }
//        List<TransactionLogEntry> list = this.logStorage.getEntries(commitedIndex, endIndex);
//        for(TransactionLogEntry entry : list){
//            InternalRunClosure internalRunClosure = new InternalRunClosure();
//            internalRunClosure.setLog(entry);
//            publishCommit(internalRunClosure);
//        }
//    }

//    public int getMaxVersion(){
//        return this.logStorage.getMaxVersion();
//    }

//    public void setMaxVersion(int maxVersion){
//        this.logStorage.setMaxVersion(maxVersion);
//    }


//    private void reRunUncommitLog(DTGSaveStore store, List<Long> regionIds){
//        List<TransactionLogEntry> unCommitLog = this.logStorage.getUnCommitLog();
//        System.out.println("uncommit log size = " + unCommitLog.size());
//        for(TransactionLogEntry log : unCommitLog){
//            if(log.getStatus() == DTGConstants.TXSUCCESS){System.out.println("tx success = " + log.getVersion());
//                continue;
//            }else{
//                long version = log.getVersion();
//                TransactionLogEntry fullLog = this.logStorage.getEntry(version);
//                String txId = "reRun_" + version;
//                List<EntityEntry> entries = ((TransactionLog) ObjectAndByte.toObject(fullLog.getByteData())).getOps();
//                processRedoLog(store, fullLog, regionIds, txId, log.getStatus());
//            }
//
//            //store.applyRequest(entries, txId, false, false, DTGConstants.FAILOVERRETRIES, true,  version, null );
//        }
//    }

    public void reRunUncommitLog(DTGSaveStore store){
        if(true)return;
        List<TransactionLogEntry> unCommitLog = this.logStorage.getUnCommitLog();
        for(TransactionLogEntry log : unCommitLog){
            List<EntityEntry> entries = ((TransactionLog) ObjectAndByte.toObject(log.getByteData())).getOps();
            long version = log.getVersion();
            String txId = "reRun_" + version;
            store.applyRequest(entries, txId, false, false, DTGConstants.FAILOVERRETRIES, true, version, null);
        }
    }

    private void processRedoLog(DTGSaveStore store, TransactionLogEntry log, List<Long> regionIds, String txId, byte status){
        List<EntityEntry> entries = ((TransactionLog) ObjectAndByte.toObject(log.getByteData())).getOps();
        System.out.println("log " + txId + " status :" + status);
        switch (status){
            case DTGConstants.TXFAILEDFIRST:
                if(regionIds.contains(log.getMainRegion())){
                    rollbackTxInSecond(store, entries, txId, log.getVersion(), log.getMainRegion());
                }
                break;
            case DTGConstants.TXRECEIVED:{
                if(regionIds.contains(log.getMainRegion())){
                    rollbackTxInSecond(store, entries, txId, log.getVersion(), log.getMainRegion());
                }else{
                    TransactionLogEntry entry = new TransactionLogEntry(ENTRY_TYPE_NO_OP, log.getVersion());
                    entry.setStatus(DTGConstants.TXFAILEDFIRST);
                    this.logStorage.appendEntry(entry);
                }
                break;
            }
            case DTGConstants.TXDONEFIRST:{
                if(regionIds.contains(log.getMainRegion())){
                    reAskFirstResult(store, entries, txId, log.getVersion());
                }
                break;
            }
            case DTGConstants.TXSECONDSTART:{
                System.out.println(regionIds + "---------" + log.getMainRegion());
                if(regionIds.contains(log.getMainRegion())) {
                    reCommitAll(store, entries, txId, log.getVersion(), log.getMainRegion());
                }else {
                    commitSelf();
                }
                break;
            }
            case DTGConstants.TXSUCCESS:{
                break;
            }

        }
    }

    private void rollbackTxInSecond(DTGSaveStore store, List<EntityEntry> entries, String txId, long startVersion, long mainRegionId){
        store.applySecondPhase(entries, txId, startVersion, false, mainRegionId, 3, true);
    }

    private void reCommitAll(DTGSaveStore store, List<EntityEntry> entries, String txId, long startVersion, long mainRegionId){
        store.applySecondPhase(entries, txId, startVersion, true, mainRegionId, 3, true);
    }

    private void commitSelf(){

    }

    private void reAskFirstResult(DTGSaveStore store, List<EntityEntry> entries, String txId, long startVersion){System.out.println("reAskFirstResult");
        store.applyRequest(entries, txId, false, false, 3, true, startVersion, null);
    }


}

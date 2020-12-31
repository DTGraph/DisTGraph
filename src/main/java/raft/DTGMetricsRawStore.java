package raft;

import DBExceptions.DTGLockError;
import DBExceptions.TransactionException;
import DBExceptions.TxMQException;
import Element.DTGOperation;
import Element.OperationName;
import LocalDBMachine.LocalDB;
import LocalDBMachine.LocalTransaction;
import LocalDBMachine.LocalTx.TransactionThreadLock;
import LocalDBMachine.LockStatus;
import MQ.ByteTask;
import MQ.DTGMQ;

import Region.DTGLockClosure;
import Region.DTGRegion;
import Region.FirstPhaseClosure;
import UserClient.DTGSaveStore;
import UserClient.Transaction.TransactionLog;
import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.client.FutureHelper;
import com.alipay.sofa.jraft.rhea.client.failover.RetryRunner;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import config.DTGConstants;
import options.DTGMetricsRawStoreOptions;
import options.MQOptions;

import com.codahale.metrics.Timer;
import tool.ObjectAndByte;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.alipay.sofa.jraft.rhea.metrics.KVMetricNames.RPC_REQUEST_HANDLE_TIMER;
import static config.DefaultOptions.MVCC;

/**
 * @author :jinkai
 * @date :Created in 2019/10/22 13:24
 * @description:
 * @modified By:
 * @version:
 */

public class DTGMetricsRawStore implements DTGRawStore, Lifecycle<DTGMetricsRawStoreOptions> {

    private final String regionId;
    private final DTGRawStore rawStore;
    private final Timer timer;
    private final DTGMQ mq;
    final LocalDB localDB;
    final Map<String, Long> TxVersionMap;

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock readLock = this.readWriteLock.readLock();
    private final Lock writeLock = this.readWriteLock.writeLock();

    public DTGMetricsRawStore(long regionId, DTGRawStore rawStore){
        this.regionId = String.valueOf(regionId);
        this.rawStore = rawStore;
        this.timer = KVMetrics.timer(RPC_REQUEST_HANDLE_TIMER, this.regionId);
        this.mq = new DTGMQ();
        this.localDB = (LocalDB) ((DTGRaftRawStore)this.rawStore).getDtgRawStore();
        this.TxVersionMap = new HashMap<>();
    }

    @Override
    public boolean init(DTGMetricsRawStoreOptions opts) {
        File file =new File(opts.getUrl());
        if  (!file.exists()  && !file .isDirectory())
        {
            file .mkdir();
        }
        MQOptions mqopts = new MQOptions();
        mqopts.setLogUri(opts.getUrl() + "\\Log");
        mqopts.setRockDBPath(opts.getUrl() + "\\RockDB");
        mqopts.setSaveStore(opts.getSaveStore());
        if(!mq.init(mqopts)){
            return false;
        }
        return true;
    }

    public void startOther(DTGSaveStore store){
        this.mq.reRunUncommitLog(store);
    }

    public void saveLog(LogStoreClosure closure, long mainRegion) {
        Object data = closure.getLog();
        ByteTask task = new ByteTask();
        task.setMainRegion(mainRegion);
        task.setDone(closure);
        task.setData(ObjectAndByte.toByteArray(data));
        task.setVersion(closure.getVersion());
        task.setTxStatus(DTGConstants.TXRECEIVED);//System.out.println("save log");
        this.mq.apply(task);
    }

    @Override
    public void setLock(final DTGOperation op, final DTGLockClosure closure, DTGRegion region) {
        long version = op.getVersion();
        final LockStatus lock = new LockStatus();
        DTGLockClosure closure1 = new DTGLockClosure() {
            @Override
            public void run(Status status) {
                if(getData() != null){
                    BaseStoreClosure closure2 = new BaseStoreClosure() {
                        @Override
                        public void run(Status status) {
                            if(!status.isOk()){
                                System.out.println("can not add lock!");
                                closure.run(new Status(DTGLockError.FAILED.getNumber(), "can not add lock!"));
                            }else {
                                synchronized (lock){
                                    if(lock.getStatus() == LockStatus.STARTLOCK){
                                        lock.setStatus(LockStatus.ENDLOCK);
                                        lock.notify();
                                    }else {
                                        lock.setStatus(LockStatus.ENDLOCK);
                                    }
                                }
                            }
                        }
                    };
                    DTGOperation operation = (DTGOperation)getData();
                    final EntityStoreClosure c = metricsAdapter(closure2, OperationName.ADDREMOVELIST, operation.getSize());
                    rawStore.ApplyEntityEntries(operation, c);
                }else{
                    synchronized (lock){
                        if(lock.getStatus() == LockStatus.STARTLOCK){
                            lock.setStatus(LockStatus.ENDLOCK);
                            lock.notify();
                        }else {
                            lock.setStatus(LockStatus.ENDLOCK);
                        }
                    }
                }
                if(!status.isOk()){
                    System.out.println("can not add lock!");
                    closure.run(new Status(DTGLockError.FAILED.getNumber(), "can not add lock!"));
                }
            }
        };
        this.localDB.setLock(op, closure1, region);//Todo lock every request
        try {
            synchronized (lock){
                if(lock.getStatus() == LockStatus.INIT){
                    lock.setStatus(LockStatus.STARTLOCK);
                    lock.wait();
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Map<Integer, Object> resultMap = new HashMap<>();
        resultMap.put(-1, true);

        try {
            TransactionThreadLock txLock = new TransactionThreadLock(op.getTxId());
            LocalTransaction tx = new LocalTransaction(localDB.getMemMVCC(), localDB.getDb(), op, resultMap, txLock, region);//System.out.println("run op... ：3  " + region.getId());
            tx.start();
            localDB.addToCommitMap(tx, op.getTxId(), region.getId());
            synchronized (resultMap){
                resultMap.wait(FutureHelper.DEFAULT_TIMEOUT_MILLIS);
            }
            if((boolean)resultMap.get(-1)) {
                closure.setData(resultMap);
                closure.run(Status.OK());
            }else throw new TransactionException();
//            closure.setData(resultMap);
//            closure.run(Status.OK());
        } catch (Throwable throwable) {
            System.out.println(op.getTxId() + "  error!： " + throwable);
            closure.run(new Status(DTGLockError.FAILED.getNumber(), "transaction excute failed!"));
        }

        this.TxVersionMap.put(op.getTxId(), version);
    }

    @Override
    public void clean(long version) {
        internalClean(version, new CompletableFuture<>(), DTGConstants.FAILOVERRETRIES, null);
        this.TxVersionMap.remove(version);
    }

    @Override
    public void commitSuccess(final DTGOperation op, final EntityStoreClosure closure, final DTGRegion region) {
        this.localDB.runOp(op, closure, true, region);
    }

    public void changeStatus(LogStoreClosure closure, byte txStatus, long mainRegion){
        ByteTask task = new ByteTask();
        task.setDone(closure);
        task.setMainRegion(mainRegion);
        task.setVersion(closure.getVersion());
        task.setTxStatus(txStatus);
        this.mq.apply(task);
    }

    private void internalClean( long version, final CompletableFuture<Boolean> future,
                                        int retriesLeft, final Errors lastCause){
        final RetryRunner retryRunner = retryCause -> internalClean(version, future, retriesLeft - 1, retryCause);
        final FailoverClosureImpl<Boolean> closure = new FailoverClosureImpl<>(future, false, retriesLeft, retryRunner, DTGConstants.RETRIYRUNNERWAIT);
        LogStoreClosure logClosure = new LogStoreClosure() {
            @Override
            public void run(Status status) {
                if(status.isOk()){
                    closure.run(Status.OK());
                }
                else{
                    System.out.println("commit log error");
                    closure.setError(getError());
                    closure.run(status);
                }
            }
        };
        logClosure.setVersion(version);
        ByteTask task = new ByteTask();
        task.setDone(logClosure);
        task.setVersion(version);
        this.mq.applyCommit(task);
    }

    @Override
    public void firstPhaseProcessor(DTGOperation op, final FirstPhaseClosure closure, DTGRegion region) {
        //System.out.println(System.currentTimeMillis() + "  firstPhaseProcessor 1: " + op.getTxId() + ",   " + region.getId());
        if(op.getAllEntityEntries() != null){
            LogStoreClosure logClosure1 = new LogStoreClosure() {
                @Override
                public void run(Status status) {
                    if(!status.isOk()){
                        System.out.println("save log error");
                         closure.setError(Errors.TRANSACTION_FIRSTPHASE_ERROR);
                        closure.run(status);
                    }
                }
            };
            logClosure1.setLog(new TransactionLog(op.getTxId(), op.getAllEntityEntries()));
            logClosure1.setVersion(op.getVersion());
            saveLog(logClosure1, op.getMainRegionId());
        }
        //System.out.println(System.currentTimeMillis() + "  firstPhaseProcessor 2: " + op.getTxId() + ",   " + region.getId());
//        DTGLockClosure lockClosure = new DTGLockClosure() {
//            @Override
//            public void run(Status status) {
//                if(hasSendRes()){
//                    return;
//                }
//                sendResult();
//                if(status.isOk()){
//                    closure.setData(getData());
//                    closure.run(Status.OK());
//                }else {
//                    clean(op.getVersion());//just remove log
//                    System.out.println("failed request lock!" + op.getTxId());
//                    closure.setError(Errors.TRANSACTION_LOCK_ERROR);
//                    closure.run(new Status(DTGLockError.FAILED.getNumber(), "request lock failed!"));
//                }
//            }
//        };
//        if(MVCC){
//            this.setLock(op, lockClosure, region);
//        }else{
//            lockClosure.run(Status.OK());
//        }、


        Map<Integer, Object> resultMap = new HashMap<>();
        resultMap.put(-1, true);

        try {
            TransactionThreadLock txLock = new TransactionThreadLock(op.getTxId());
            LocalTransaction tx = new LocalTransaction(localDB.getMemMVCC(), localDB.getDb(), op, resultMap, txLock, region);//System.out.println("run op... ： " + region.getId());

            tx.start();
            synchronized (txLock){
                if(!tx.getCouldCommit()){
                    try {
                        txLock.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            //System.out.println(System.currentTimeMillis() + "  firstPhaseProcessor 3: " + op.getTxId() + ",   " + region.getId());

            if((boolean)resultMap.get(-1)) {
                localDB.addToCommitMap(tx, op.getTxId(), region.getId());
                closure.setData(resultMap);
                closure.run(Status.OK());
            }else throw new TransactionException();

           // System.out.println(System.currentTimeMillis() + "  firstPhaseProcessor 4: " + op.getTxId() + ",   " + region.getId());

//            if(op.getAllEntityEntries() != null){
//                LogStoreClosure logClosure = new LogStoreClosure() {
//                    @Override
//                    public void run(Status status) {
//                        if(status.isOk()){
//                            localDB.addToCommitMap(tx, op.getTxId(), region.getId());
//                            closure.setData(resultMap);
//                            closure.run(Status.OK());
//                            System.out.println(System.currentTimeMillis() + "  firstPhaseProcessor 4: " + op.getTxId() + ",   " + region.getId());
//                        }else{
//                            closure.run(new Status(DTGLockError.FAILED.getNumber(), "transaction excute failed!"));
//                        }
//                    }
//                };
//                logClosure.setLog(new TransactionLog(op.getTxId(), false));
//                logClosure.setVersion(op.getVersion());
//                changeStatus(logClosure, DTGConstants.TXDONEFIRST, op.getMainRegionId());
//            }else{
//                localDB.addToCommitMap(tx, op.getTxId(), region.getId());
//                closure.setData(resultMap);
//                closure.run(Status.OK());
//            }
        } catch (Exception e) {
            System.out.println(op.getTxId() + "  error!： " + e);
            closure.run(new Status(DTGLockError.FAILED.getNumber(), "transaction excute failed!"));
            LogStoreClosure logClosure = new LogStoreClosure() {
                @Override
                public void run(Status status) {

                }
            };
            logClosure.setLog(new TransactionLog(op.getTxId(), false));
            logClosure.setVersion(op.getVersion());
            changeStatus(logClosure, DTGConstants.TXFAILEDFIRST, op.getMainRegionId());
        }

        this.TxVersionMap.put(op.getTxId(), op.getVersion());
    }

    public byte getTxStatus(long version){
        return this.mq.getStatus(version);
    }

    @Override
    public void ApplyEntityEntries(DTGOperation op, EntityStoreClosure closure) {//System.out.println("ApplyEntityEntries : " + op.getTxId());
        final EntityStoreClosure c = metricsAdapter(closure, OperationName.TRANSACTIONOP, op.getSize());
        this.rawStore.ApplyEntityEntries(op, c);
    }

    @Override
    public void readOnlyEntityEntries(DTGOperation op, EntityStoreClosure closure) {
        final EntityStoreClosure c = metricsAdapter(closure, OperationName.READONLY, 0);
        this.rawStore.readOnlyEntityEntries(op, c);
    }

    @Override
    public void secondRead(DTGOperation op, EntityStoreClosure closure, DTGRegion region) {
        this.localDB.runOp(op, closure, true, region);
    }

    @Override
    public void merge() {
    }

    @Override
    public void split() {

    }

    private MetricsClosureAdapter metricsAdapter(final EntityStoreClosure closure, final byte op, int entrySize) {
        return new MetricsClosureAdapter(closure, this.regionId, op,entrySize, timeCtx());
    }

    private Timer.Context timeCtx() {
        return this.timer.time();
    }

    @Override
    public void shutdown() {

    }

}

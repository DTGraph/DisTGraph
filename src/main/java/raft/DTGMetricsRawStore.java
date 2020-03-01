package raft;

import DBExceptions.DTGLockError;
import Element.DTGOperation;
import Element.EntityEntry;
import Element.OperationName;
import LocalDBMachine.LocalDB;
import LocalDBMachine.LocalTransaction;
import LocalDBMachine.LocalTx.TransactionThreadLock;
import LocalDBMachine.LockStatus;
import LocalDBMachine.MVCC.VersionControl;
import MQ.ByteTask;
import MQ.DTGMQ;

import Region.DTGLockClosure;
import Region.DTGRegion;
import Region.FirstPhaseClosure;
import Region.LockProcess;
import UserClient.Transaction.TransactionLog;
import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.client.FutureHelper;
import com.alipay.sofa.jraft.rhea.client.failover.RetryRunner;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import com.alipay.sofa.jraft.util.Bits;
import com.alipay.sofa.jraft.util.Requires;
import config.DTGConstants;
import options.DTGMetricsRawStoreOptions;
import options.MQOptions;
import scala.collection.Iterator;

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
//    final List<String> waitRemoveList;
//    final LockProcess lockProcess;
    final LocalDB localDB;
    //final VersionControl versionControl;
    final Map<String, Long> TxVersionMap;

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock readLock = this.readWriteLock.readLock();
    private final Lock writeLock = this.readWriteLock.writeLock();

    public DTGMetricsRawStore(long regionId, DTGRawStore rawStore){
        this.regionId = String.valueOf(regionId);
        this.rawStore = rawStore;
        this.timer = KVMetrics.timer(RPC_REQUEST_HANDLE_TIMER, this.regionId);
        this.mq = new DTGMQ();
//        this.waitRemoveList = new ArrayList<>();
//        this.lockProcess = new LockProcess();
        this.localDB = (LocalDB) ((DTGRaftRawStore)this.rawStore).getDtgRawStore();
        //this.versionControl = new VersionControl(this.mq.getMaxVersion());
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

    public DTGRawStore getRawStore() {
        return rawStore;
    }

    @Override
    public Iterator localIterator() {
        return null;
    }

    @Override
    public void saveLog(LogStoreClosure closure) {
        Object data = closure.getLog();
        //MQClosure mqClosure = new MQClosure(closure, data);
        ByteTask task = new ByteTask();
        task.setDone(closure);
        task.setData(ObjectAndByte.toByteArray(data));
        task.setVersion(closure.getVersion());
        this.mq.apply(task);
    }

    @Override
    public void setLock(final DTGOperation op, final DTGLockClosure closure, DTGRegion region) {
        //int version = this.versionControl.getTemporalVersion();
        //System.out.println("setLock");
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
                                    //System.out.println("lock notify........");
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
                            //System.out.println("lock notify........");
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
        this.localDB.setLock(op, closure1, region);
        try {
            synchronized (lock){
                if(lock.getStatus() == LockStatus.INIT){
                    lock.setStatus(LockStatus.STARTLOCK);
                    //System.out.println("lock wait........");
                    lock.wait();
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //System.out.println("lock over : " + System.currentTimeMillis());
        Map<Integer, Object> resultMap = new HashMap<>();
        resultMap.put(-1, true);
//
//        final CompletableFuture future = CompletableFuture.runAsync(() -> {
            try {
                TransactionThreadLock txLock = new TransactionThreadLock(op.getTxId());
                LocalTransaction tx = new LocalTransaction(localDB.getDb(), op, resultMap, txLock, region);//System.out.println("run op... ：3  " + region.getId());
                tx.start();
                localDB.addToCommitMap(tx, op.getTxId());
                synchronized (resultMap){
                    resultMap.wait(FutureHelper.DEFAULT_TIMEOUT_MILLIS);//System.out.println("run op... ：5  " + region.getId());
                }
                //tx.commit();
                //System.out.println("tx over : " + System.currentTimeMillis());
                closure.setData(resultMap);
                closure.run(Status.OK());
            } catch (Throwable throwable) {
                System.out.println("error!");//System.out.println("tx error response request" + regionId);
                closure.run(new Status(DTGLockError.FAILED.getNumber(), "transaction excute failed!"));
            }
//        });

        this.TxVersionMap.put(op.getTxId(), version);
    }

    @Override
    public void sendLock(DTGOperation op, EntityStoreClosure closure) {

    }

    @Override
    public void commitSuccess(long version) {
        internalCommitSuccess(version, new CompletableFuture<>(), DTGConstants.FAILOVERRETRIES, null);
        this.TxVersionMap.remove(version);
    }

    private void internalCommitSuccess( long version, final CompletableFuture<Boolean> future,
                                        int retriesLeft, final Errors lastCause){
        final RetryRunner retryRunner = retryCause -> internalCommitSuccess(version, future, retriesLeft - 1, retryCause);
        final FailoverClosureImpl<Boolean> closure = new FailoverClosureImpl<>(future, false, retriesLeft, retryRunner);
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
        saveLog(logClosure);
        ByteTask task = new ByteTask();
        task.setDone(logClosure);
        task.setVersion(version);
        this.mq.applyCommit(task);
    }

    @Override
    public void firstPhaseProcessor(DTGOperation op, final FirstPhaseClosure closure, DTGRegion region) {
        //System.out.println("save log : " + System.currentTimeMillis());
        if(op.getAllEntityEntries() != null){
            LogStoreClosure logClosure = new LogStoreClosure() {
                @Override
                public void run(Status status) {
                    if(!status.isOk()){
                        System.out.println("save log error");
                        closure.setError(getError());
                        closure.run(status);
                    }
                }
            };
            logClosure.setLog(new TransactionLog(op.getTxId(), op.getAllEntityEntries()));
            logClosure.setVersion(op.getVersion());
            saveLog(logClosure);
        }

        //System.out.println("get lock : " + System.currentTimeMillis());
        DTGLockClosure lockClosure = new DTGLockClosure() {
            @Override
            public void run(Status status) {
                if(status.isOk()){
                    closure.setData(getData());
                    closure.run(Status.OK());
                }else {
                    commitSuccess(op.getVersion());//just remove log
                    System.out.println("failed request lock!");
                    closure.setError(getError());
                    closure.run(new Status(DTGLockError.FAILED.getNumber(), "request lock failed!"));
                }
            }
        };
        this.setLock(op, lockClosure, region);
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

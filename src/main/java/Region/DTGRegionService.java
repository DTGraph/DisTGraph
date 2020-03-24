package Region;

import Communication.DTGRpcService;
import Communication.RequestAndResponse.*;
import DBExceptions.TxError;
import DBExceptions.TypeDoesnotExistException;
import Element.DTGOperation;
import Element.EntityEntry;
import Element.OperationName;
import LocalDBMachine.LocalDB;
import PlacementDriver.DTGPlacementDriverClient;
import UserClient.DTGSaveStore;
import UserClient.Transaction.TransactionLog;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rhea.RequestProcessClosure;
import com.alipay.sofa.jraft.rhea.client.FutureGroup;
import com.alipay.sofa.jraft.rhea.client.FutureHelper;
import com.alipay.sofa.jraft.rhea.client.failover.RetryRunner;
import com.alipay.sofa.jraft.rhea.cmd.store.*;
import com.alipay.sofa.jraft.rhea.errors.ApiExceptionHelper;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;
import com.alipay.sofa.jraft.rhea.util.KVParameterRequires;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.util.Requires;
import config.DTGConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static config.MainType.NODETYPE;
import static config.MainType.RELATIONTYPE;
import static config.MainType.TEMPORALPROPERTYTYPE;
import static tool.ObjectAndByte.toByteArray;

/**
 * @author :jinkai
 * @date :Created in 2019/10/17 20:20
 * @description:
 * @modified By:
 * @version:
 */

public class DTGRegionService implements RegionService {

    private static final Logger LOG = LoggerFactory.getLogger(DTGRegionService.class);

    private final DTGRegionEngine regionEngine;
    private final DTGRawStore rawStore;
    private LocalDB localdb;
    private DTGRegion region;
    private DTGRpcService dtgRpcService;
    //lockResultMap <String, List<Long>>,the key represent TxId, the List represent region related to transaction.
    //thie first number of list represent the number of region haven't send success request.
    private Map<TxRepeateVersion, List<Long>> lockResultMap;
    private Map<String, Long> versionMap;
    private Map<String, DTGOperation> transactionOperationMap;
    private Map<String, CompletableFuture> successMap;
    private final Map<String, Byte> txStatus;
    private final Map<String, List<TxRepeateVersion>> txRepeate;
    //private final List<String> failedCommitList;

    public DTGRegionService(DTGRegionEngine regionEngine){
        this.regionEngine = regionEngine;
        this.rawStore = regionEngine.getMetricsRawStore();
        this.localdb = this.regionEngine.getStoreEngine().getlocalDB();
        this.region = regionEngine.getRegion();
        lockResultMap = new ConcurrentHashMap<>();
        this.versionMap = new ConcurrentHashMap<>();
        this.transactionOperationMap = new ConcurrentHashMap<>();
        this.successMap = new ConcurrentHashMap<>();
        this.txStatus = new ConcurrentHashMap<>();
        this.txRepeate = new ConcurrentHashMap<>();
        //this.failedCommitList = new LinkedList<>();
    }


    @Override
    public long getRegionId() {
        return this.region.getId();
    }

    @Override
    public RegionEpoch getRegionEpoch() {
        return this.regionEngine.getRegion().getRegionEpoch();
    }

    @Override
    public void handleTransactionRequest(final TransactionRequest request, final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        System.out.println(" start :" + System.currentTimeMillis());
        System.out.println("get transaction request  " + region.getId());
        final TransactionResponse response = new TransactionResponse();
        try {
            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
            List<EntityEntry> entityEntries = request.getEntries();
            String txId = request.getTxId();
            LogStoreClosure logClosure = new LogStoreClosure() {
                @Override
                public void run(Status status) {
                    if (status.isOk()) {
                        System.out.println("success transaction..:" + txId);
                        response.setValue(true);
                    } else {
                        //System.out.println("failed commit..:" + op.getTxId());
                        response.setValue(false);
                        setFailure(request, response, status, getError());
                    }
                    System.out.println("return aaa.." + txId);
                    System.out.println(" end :" + System.currentTimeMillis());
                    closure.sendResponse(response);
                }
            };
            logClosure.setLog(new TransactionLog(txId, entityEntries));
            this.rawStore.saveLog(logClosure);
        } catch (final Throwable t) {
            System.out.println("failed transaction..");
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setValue(false);
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

//    @Override
//    public void handleFirstPhase(FirstPhaseRequest request, RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
//        this.region.increaseTransactionCount();
//        final FirstPhaseResponse response = new FirstPhaseResponse();
//        response.setRegionId(getRegionId());
//        response.setRegionEpoch(getRegionEpoch());
//        Map<Integer, Object> resultMap = new HashMap<>();
//        resultMap.put(-1, true);
//        try {//System.out.println("run op... ：1  " +region.getId());
//            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());//System.out.println("run op... ：2  " + region.getId());
//            final DTGOperation op = KVParameterRequires
//                    .requireNonNull(request.getDTGOpreration(), "put.DRGOperation");
//            //System.out.println("run op... ：" + op.getTxId());
////            final CompletableFuture<LocalTransaction> future = CompletableFuture.supplyAsync(() -> {
////                try {
////                    TransactionThreadLock txLock = new TransactionThreadLock(op.getTxId());
////                    LocalTransaction tx = new LocalTransaction(localdb.getDb(), op, resultMap, txLock, region);//System.out.println("run op... ：3  " + region.getId());
////                    tx.start();
////                    synchronized (resultMap){
////                        resultMap.wait(FutureHelper.DEFAULT_TIMEOUT_MILLIS);//System.out.println("run op... ：5  " + region.getId());
////                    }
////                    this.localdb.addToCommitMap(txLock, op.getTxId());//System.out.println("run op... ：6  " + region.getId());
////                    return tx;
////                    //return new ExecuteTransactionOp().getTransaction(this.localdb.getDb(), op, resultMap);
//////                    return localdb.getTransaction(op, resultMap);
////                } catch (Throwable throwable) {
////                    System.out.println("error!");System.out.println("tx error response request" + region.getId());
////                    response.setValue(ObjectAndByte.toByteArray(resultMap));
////                    response.setError(Errors.forException(throwable));
////                    closure.sendResponse(response);
////                    return null;
////                }
////            });
//
////            LocalTransaction tx = FutureHelper.get(future, FutureHelper.DEFAULT_TIMEOUT_MILLIS);
////            Requires.requireNonNull(tx, "remote transaction null");
//            //System.out.println("get tx and wait commit... ：" + op.getTxId());
////            this.localdb.addToCommitMap(tx, op.getTxId());
//
//            CompletableFuture.runAsync(() -> {
//                this.rawStore.ApplyEntityEntries(op, new BaseStoreClosure() {
//                    @Override
//                    public void run(final Status status) {
//                        System.out.println("run closure");
//                        if (status.isOk()) {
//                            response.setValue(toByteArray(resultMap));
//                            System.out.println("finish first phase");
//                            //System.out.println(resultMap.size());
//                        } else {
//                            response.setValue(ObjectAndByte.toByteArray(resultMap));
//                            System.out.println("error!" + region.getId());
//                            setFailure(request, response, status, getError());
//                        }
//                        //System.out.println("response request" + region.getId());
//                        closure.sendResponse(response);
//                    }
//                });
//            });
//
//
//        } catch (final Throwable t) {
//            System.out.println("error!" + region.getId());
//            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
//            response.setValue(ObjectAndByte.toByteArray(resultMap));
//            response.setError(Errors.forException(t));//System.out.println("error response request" + region.getId());
//            closure.sendResponse(response);
//        }
//    }


    //private static AtomicInteger t = new AtomicInteger(0);
    @Override
    public void handleFirstPhase(FirstPhaseRequest request, RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        //System.out.println(request.getDTGOpreration().getTxId() +"  get request : " + t.getAndIncrement());
        //System.out.println("handleFirstPhase : prepare first phase! region id = " + getRegionId());
        final FirstPhaseResponse response = new FirstPhaseResponse();
        response.setSelfRegionId(getRegionId());
        if(!isLeader()){
            //System.out.println("handleFirstPhase error : not leader" + request.getDTGOpreration().getTxId());
            response.setError(Errors.NOT_LEADER);//System.out.println("error response request" + region.getId());
            closure.sendResponse(response);
        }

        this.region.increaseTransactionCount();
        //response.setRegionEpoch(getRegionEpoch());
        Map<Integer, Object> resultMap = new HashMap<>();
        resultMap.put(-1, true);
        KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
        //System.out.println("handleFirstPhase : " + request.getRegionEpoch() + ", now epoch: " + getRegionEpoch());
        final DTGOperation op = KVParameterRequires
                .requireNonNull(request.getDTGOpreration(), "put.DRGOperation");
        //System.out.println("run op... ：" + op.getTxId());
//            final CompletableFuture<LocalTransaction> future = CompletableFuture.supplyAsync(() -> {
//                try {
//                    TransactionThreadLock txLock = new TransactionThreadLock(op.getTxId());
//                    LocalTransaction tx = new LocalTransaction(localdb.getDb(), op, resultMap, txLock, region);//System.out.println("run op... ：3  " + region.getId());
//                    tx.start();
//                    synchronized (resultMap){
//                        resultMap.wait(FutureHelper.DEFAULT_TIMEOUT_MILLIS);//System.out.println("run op... ：5  " + region.getId());
//                    }
//                    this.localdb.addToCommitMap(txLock, op.getTxId());//System.out.println("run op... ：6  " + region.getId());
//                    return tx;
//                    //return new ExecuteTransactionOp().getTransaction(this.localdb.getDb(), op, resultMap);
////                    return localdb.getTransaction(op, resultMap);
//                } catch (Throwable throwable) {
//                    System.out.println("error!");System.out.println("tx error response request" + region.getId());
//                    response.setValue(ObjectAndByte.toByteArray(resultMap));
//                    response.setError(Errors.forException(throwable));
//                    closure.sendResponse(response);
//                    return null;
//                }
//            });

//            LocalTransaction tx = FutureHelper.get(future, FutureHelper.DEFAULT_TIMEOUT_MILLIS);
//            Requires.requireNonNull(tx, "remote transaction null");
        //System.out.println("get tx and wait commit... ：" + op.getTxId());
//            this.localdb.addToCommitMap(tx, op.getTxId());

//            CompletableFuture.runAsync(() -> {
//                this.rawStore.ApplyEntityEntries(op, new BaseStoreClosure() {
//                    @Override
//                    public void run(final Status status) {
//                        System.out.println("run closure");
//                        if (status.isOk()) {
//                            response.setValue(toByteArray(resultMap));
//                            System.out.println("finish first phase");
//                            //System.out.println(resultMap.size());
//                        } else {
//                            response.setValue(ObjectAndByte.toByteArray(resultMap));
//                            System.out.println("error!" + region.getId());
//                            setFailure(request, response, status, getError());
//                        }
//                        //System.out.println("response request" + region.getId());
//                        closure.sendResponse(response);
//                    }
//                });
//            });
        String txId = op.getTxId();
        if (this.txStatus.get(txId) != null){
            //synchronized (txStatus){
            byte status = this.txStatus.get(txId);
            if(status == DTGConstants.TXRECEIVED || status == DTGConstants.TXRERUNFIRST){
                response.setValue(toByteArray(resultMap));
                closure.sendResponse(response);
                //System.out.println("handleFirstPhase tx TXRECEIVED : " + op.getTxId());
                return;
            }
            else if(status == DTGConstants.TXDONEFIRST){
                if(request.isFromClient()){
                    response.setValue(toByteArray(resultMap));
                    closure.sendResponse(response);
                   // System.out.println("handleFirstPhase tx TXDONEFIRST : " + op.getTxId());
                }else{
                    TxRepeateVersion txRepeateVersion = new TxRepeateVersion(op.getTxId(), request.getRepeate());
                    callMainRegion(txRepeateVersion, op.getMainRegionId(), true, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
                }
                return;
            }
            else if(status == DTGConstants.TXFAILEDFIRST){
                if(request.isFromClient()){
                    response.setError(Errors.TRANSACTION_FIRSTPHASE_ERROR);//System.out.println("error response request" + region.getId());
                    closure.sendResponse(response);
                    //System.out.println("handleFirstPhase tx TXFAILEDFIRST : " + op.getTxId());
                }
                else {
                    TxRepeateVersion txRepeateVersion = new TxRepeateVersion(op.getTxId(), request.getRepeate());
                    callMainRegion(txRepeateVersion, op.getMainRegionId(), false, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
                }
                return;
            }
            else if(status == DTGConstants.SYNOP || status == DTGConstants.SYNOPFAILED){
                //cleanMainRegionProcess(op.getTxId(), op.getVersion());
                txStatus.put(op.getTxId(), DTGConstants.TXRERUNFIRST);
            }
            //}
            //System.out.println("handleFirstPhase tx exist : " + op.getTxId());
            //return;
        }
        else{
            this.txStatus.put(op.getTxId(), DTGConstants.TXRECEIVED);
        }
        int repeate = 1;
        List<TxRepeateVersion> list;
        if(this.txRepeate.containsKey(txId)){
            list = txRepeate.get(txId);
            repeate = list.get(0).getRepeate() + 1;
        }
        else {
            list = new ArrayList<>();
            txRepeate.put(txId, list);
        }
        TxRepeateVersion thisRepeate = new TxRepeateVersion(txId, repeate);
        list.add(thisRepeate);
        //System.out.println("handleFirstPhase : " + op.getEntityEntries().size());
        try {//System.out.println("run op... ：1  " +op.getTxId());
            FirstPhaseClosure firstClosure = new FirstPhaseClosure() {
                @Override
                public void run(Status status) {
                    if(hasSendRes()){
                        return;
                    }
                    sendResult();
                    if (status.isOk()) {
                        //System.out.println("handleFirstPhase ok :" + txId + ", count = " + t.getAndIncrement());
                        response.setValue(toByteArray(getData()));
                        closure.sendResponse(response);
                        if(!op.isReadOnly()){
                            if(!synOp(op, thisRepeate, new CompletableFuture(), DTGConstants.FAILOVERRETRIES, null)){
                                return;
                            }
                        }
                        txStatus.put(txId, DTGConstants.TXDONEFIRST);
                        callMainRegion(thisRepeate, op.getMainRegionId(), true, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
                    } else {
                        System.out.println("first phase failed commit..:" + txId);
                        response.setValue(toByteArray(resultMap));
                        setFailure(request, response, status, getError());
                        closure.sendResponse(response);
                        txStatus.put(txId, DTGConstants.TXFAILEDFIRST);
                        //failedCommitList.add(txId);
                        callMainRegion(thisRepeate, op.getMainRegionId(), false, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
                    }
                }
            };
            if(op.getMainRegionId() == this.region.getId()){
                mainRegionProcess(op, thisRepeate);
            }
            //System.out.println("run op... ：2  " +op.getTxId());
            this.transactionOperationMap.put(op.getTxId(), op);
            //System.out.println("run op... ：3  " +op.getTxId());
            this.rawStore.firstPhaseProcessor(op, firstClosure, this.region);
            //System.out.println("run op... ：4  " +op.getTxId());
        } catch (final Throwable t) {
            txStatus.put(txId, DTGConstants.TXFAILEDFIRST);
            callMainRegion(thisRepeate, op.getMainRegionId(), false, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
            System.out.println("error!" + region.getId());
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            //response.setValue(ObjectAndByte.toByteArray(resultMap));
            response.setError(Errors.forException(t));//System.out.println("error response request" + region.getId());
            closure.sendResponse(response);
        }
    }



    @Override
    public void handleSecondPhase(SecondPhaseRequest request, RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        final SecondPhaseResponse response = new SecondPhaseResponse();
        String txId = request.getTxId();
        //response.setRegionId(getRegionId());
        //response.setRegionEpoch(getRegionEpoch());
//        try {
            //KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
        if(request.isRequireTxNotNull()){
            KVParameterRequires.requireNonNull(transactionOperationMap.get(txId), "put.DTGOperation");
        }
//            if(!request.isSuccess()){
//                rollbackTx(op.getTxId());
//                response.setValue(true);
//                closure.sendResponse(response);
//                return;
//            }
//            this.rawStore.ApplyEntityEntries(op, new BaseStoreClosure() {
//                @Override
//                public void run(final Status status) {
//                    if (status.isOk()) {
//                        //System.out.println("success commit..:" + op.getTxId());
//                        //System.out.println("second phase ok");
//                        response.setValue(true);
//                        txStatus.remove(op.getTxId());
//                    } else {
//                        System.out.println("failed commit..:" + op.getTxId());
//                        response.setValue(false);
//                        setFailure(request, response, status, getError());
//                    }
//                    //System.out.println("return commit..");
//                    closure.sendResponse(response);
//                }
//            });
//            this.txStatus.remove(txId);
//        } catch (final Throwable t) {
//            System.out.println("failed commit..");
//            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
//            response.setValue(false);
//            response.setError(Errors.forException(t));
//            closure.sendResponse(response);
//        }

            final DTGOperation op = KVParameterRequires.requireNonNull(request.getDTGOpreration(), "putop");
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            internalSecondCommit(op, future, request.isSuccess(), 0, null);
            try {
                if(FutureHelper.get(future)){
                    response.setValue(true);
                    //txStatus.remove(op.getTxId());
                }else {
                    System.out.println("second phase failed commit..:" + op.getTxId());
                    response.setValue(false);
                    response.setError(Errors.STORAGE_ERROR);
                    //setFailure(request, response, status, getError());
                }
                closure.sendResponse(response);
            }catch (Exception e){
                System.out.println("handleSecondPhase failed commit..");
                LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(e));
                response.setValue(false);
                response.setError(Errors.forException(e));
                closure.sendResponse(response);
            }
        //System.out.println("finish commit request");
    }

    private void processLocalSecondPhase(final String txId, long version, final FailoverClosureImpl closure, final boolean isSuccess){
        //System.out.println("processLocalSecondPhase");
        if(!checkStatus(txId)){
            closure.setData(true);
            closure.run(Status.OK());
            return;
        }
        final DTGOperation op = new DTGOperation();
        if(isSuccess){
            op.setType(OperationName.COMMITTRANS);
            //op.setType(OperationName.ROLLBACK);
        }
        else {
            op.setType(OperationName.ROLLBACK);
        }
        op.setTxId(txId);
//        this.rawStore.ApplyEntityEntries(op, new BaseStoreClosure() {
//            @Override
//            public void run(final Status status) {
//                if (status.isOk()) {
//                    //System.out.println("processLocalSecondPhase ok : " + region.getId());
//                    closure.setData(true);
//                    closure.run(Status.OK());
//                    txStatus.remove(txId);
//                } else {
//                    System.out.println("failed commit..:" + op.getTxId());
//                    closure.setData(false);
//                    closure.run(new Status(TxError.FAILED.getNumber(), "can not run transaction"));
//                }
//            }
//        });
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        internalSecondCommit(op, future, isSuccess, DTGConstants.FAILOVERRETRIES, null);
        try {
            if(FutureHelper.get(future)){
                closure.setData(true);
                closure.run(Status.OK());
                txStatus.remove(txId);
            }else {
                internalCallRegion(txId, isSuccess, this.getRegionId(), false, version, closure, Errors.NOT_LEADER, true);
                System.out.println("failed commit..:" + op.getTxId());
                closure.setData(false);
                closure.run(new Status(TxError.FAILED.getNumber(), "can not run transaction"));
            }
        }catch (Exception e){
            System.out.println("failed commit..:" + op.getTxId());
            closure.setData(false);
            closure.run(new Status(TxError.FAILED.getNumber(), "can not run transaction"));
        }

    }

    private void internalSecondCommit(final DTGOperation op, final CompletableFuture<Boolean> future, boolean isSuccess,
                                      int retriesLeft, final Errors lastCause){
        final RetryRunner retryRunner = retryCause -> internalSecondCommit(op, future, isSuccess, retriesLeft - 1, retryCause);
        final FailoverClosureImpl<Boolean> closure = new FailoverClosureImpl<Boolean>(future, retriesLeft, retryRunner, DTGConstants.RETRIYRUNNERWAIT);
        //CompletableFuture<Boolean> future1 = new CompletableFuture();
        //System.out.println("internalSecondCommit : " + op.getTxId());
        this.rawStore.ApplyEntityEntries(op, new BaseStoreClosure() {
            @Override
            public void run(final Status status) {
            if (status.isOk()) {
                future.complete(true);
                closure.setData(true);
                closure.run(Status.OK());
                txStatus.remove(op.getTxId());
            } else {
                System.out.println("internalSecondCommit failed commit..:" + op.getTxId());
                //internalCallRegion(op.getTxId(), isSuccess, getRegionId(), false, op.getVersion(), closure, lastCause, true);
                closure.setError(Errors.TRANSACTION_SECOND_ERROR);
                closure.setData(false);
                closure.run(new Status(TxError.FAILED.getNumber(), "can not run transaction"));
            }
            }
        });
    }

    @Override
    public void handleMergeRequest(MergeRequest request, RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {

    }

    @Override
    public void handleRangeSplitRequest(RangeSplitRequest request, RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {

    }

    public void handleCommitRequest(final CommitRequest request,
                                        final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure){
        //System.out.println("get commit request");

    }

    public void handleFirstPhaseSuccessRequest(final FirstPhaseSuccessRequest request,
                                               final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure){
        //System.out.println("handleFirstPhaseSuccessRequest : ");
        final FirstPhaseSuccessResponse response = new FirstPhaseSuccessResponse();
        try {
            String txId = request.getTxId();
            int repeate= request.getRepeate();
            processFirstPhaseSuccess(new TxRepeateVersion(txId, repeate), request.getSelfRegionId(), request.isSuccess());
            closure.sendResponse(response);
            //System.out.println("success handleFirstPhaseSuccessRequest");
        } catch (final Throwable t) {
            System.out.println("failed request lock ..");
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setValue(false);
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

    private boolean processFirstPhaseSuccess(TxRepeateVersion txRepeateVersion, long regionId, boolean isSuccess){
        String txId = txRepeateVersion.getTxId();
        if(!checkStatus(txId))return true;
        if(!isSuccess){
            this.txStatus.put(txRepeateVersion.getTxId(), DTGConstants.TXROLLBACK);
            //return true;
        }
        //System.out.println("processFirstPhaseSuccess : " + txId);
        List<Long> list = this.lockResultMap.get(txRepeateVersion);
        //System.out.println("processFirstPhaseSuccess : " + txId + list.toString() + ", regionid = " + regionId);
        if(list == null){
            list = new ArrayList<>();
            list.add(-1L);
            list.add(regionId);
            this.lockResultMap.put(txRepeateVersion, list);
        }else {
            long size = list.get(0);
            list.remove(0);
            if(!list.contains(-regionId)){
                list.remove(regionId);
                list.add(-regionId);
                size = size - 1;
            }

//            list.remove(regionId);
//            list.add(-regionId);
//            long size = list.get(0) - 1;
            //list.set(0, size);
            //System.out.println(txId + "  processFirstPhaseSuccess regionId = " + regionId);
            list.add(0, size);
            if(size == 0L){
                secondPhase(txRepeateVersion);
            }
        }
        return true;
    }

    public void HandleLockRequest(final LockRequest request,
                                   final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure){
        final CommitResponse response = new CommitResponse();
        try {
            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
            DTGOperation op = Requires.requireNonNull(request.getDTGOpreration(), "dtgOperation is null!");
            DTGLockClosure lockClosure = new DTGLockClosure() {
                @Override
                public void run(Status status) {
                    if (status.isOk()) {
                        //System.out.println("success transaction..:" + op.getTxId());
                        response.setValue(true);
                    } else {
                        System.out.println("failed commit..:" + op.getTxId());
                        response.setValue(false);
                        setFailure(request, response, status, getError());
                    }
                    //System.out.println("return aaa.." + txId);
                    //System.out.println(" end :" + System.currentTimeMillis());

                    closure.sendResponse(response);
                }
            };
            //logClosure.setLog(new TransactionLog(txId, entityEntries));
            this.rawStore.setLock(op, lockClosure, this.region);
        } catch (final Throwable t) {
            System.out.println("failed request lock ..");
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setValue(false);
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

    @Override
    public void internalFirstPhase(final DTGOperation op, final FailoverClosure closure) {
        //System.out.println(op.getTxId() +" internalFirstPhase : " + t.getAndIncrement());
        this.region.increaseTransactionCount();
        Map<Integer, Object> resultMap = new HashMap<>();
        resultMap.put(-1, true);
        String txId = op.getTxId();
        int repeate = 1;
        List<TxRepeateVersion> list;
        if(this.txRepeate.containsKey(txId)){
            list = txRepeate.get(txId);
            repeate = list.get(0).getRepeate() + 1;
        }
        else {
            list = new ArrayList<>();
            txRepeate.put(txId, list);
        }
        if (this.txStatus.get(txId) != null){
            byte status = this.txStatus.get(txId);
            if(status == DTGConstants.TXRECEIVED || status == DTGConstants.TXRERUNFIRST){
                closure.setData(resultMap);
                closure.run(Status.OK());
                return;
            }
            else if(status == DTGConstants.TXDONEFIRST){
                callMainRegion(new TxRepeateVersion(txId, repeate-1), op.getMainRegionId(), true, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
                closure.run(Status.OK());
                return;
            }
            else if(status == DTGConstants.TXFAILEDFIRST){
                callMainRegion(new TxRepeateVersion(txId, repeate-1), op.getMainRegionId(), false, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
                return;
            }
            else if(status == DTGConstants.SYNOP || status == DTGConstants.SYNOPFAILED){
                //cleanMainRegionProcess(op.getTxId(), op.getVersion());
                txStatus.put(op.getTxId(), DTGConstants.TXRERUNFIRST);
            }
            System.out.println("handleFirstPhase tx exist : " + op.getTxId());
            //return;
        }
        else{
            this.txStatus.put(op.getTxId(), DTGConstants.TXRECEIVED);
        }
        TxRepeateVersion thisRepeate = new TxRepeateVersion(txId, repeate);
        list.add(thisRepeate);
        try {
            FirstPhaseClosure firstClosure = new FirstPhaseClosure() {
                @Override
                public void run(Status status) {
                    if(hasSendRes()){
                        return;
                    }
                    sendResult();
                    if (status.isOk()) {
                        closure.setData(getData());
                        closure.run(Status.OK());
                        if(!op.isReadOnly()){
                            if(!synOp(op, thisRepeate, new CompletableFuture(), DTGConstants.FAILOVERRETRIES, null)){
                                return;
                            }
                        }
                        txStatus.put(txId, DTGConstants.TXDONEFIRST);
                        callMainRegion(thisRepeate, op.getMainRegionId(), true, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
                    } else {
                        closure.run(new Status(TxError.FAILED.getNumber(), "transaction failed"));
                        txStatus.put(txId, DTGConstants.TXFAILEDFIRST);
                        callMainRegion(thisRepeate, op.getMainRegionId(), false, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
                    }
                }
            };
            this.txStatus.put(op.getTxId(), DTGConstants.TXRECEIVED);
            if(op.getMainRegionId() == this.region.getId()){
                mainRegionProcess(op, thisRepeate);
            }
            //System.out.println("run op... ：2  " +op.getTxId());
            this.transactionOperationMap.put(op.getTxId(), op);
            //System.out.println("run op... ：3  " +op.getTxId());
            this.rawStore.firstPhaseProcessor(op, firstClosure, this.region);
        } catch (final Throwable t) {
            txStatus.put(op.getTxId(), DTGConstants.TXFAILEDFIRST);
            callMainRegion(thisRepeate, op.getMainRegionId(), false, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
            System.out.println("error!" + region.getId());
            closure.run(new Status(TxError.FAILED.getNumber(), "transaction failed"));
        }
    }

    private boolean checkStatus(String txId){
        return this.txStatus.containsKey(txId);
    }

    private void mainRegionProcess(DTGOperation op, TxRepeateVersion txRepeateVersion){
        //System.out.println("mainRegionProcess... : " +txId);
        if(!checkStatus(op.getTxId()))return;
        List<Long> list = this.lockResultMap.get(txRepeateVersion);
        List<Long> regionIds = op.getRegionIds();
        //System.out.println(txRepeateVersion.toString() + "mainRegionProcess 1... : " +list + "  " + regionIds);
//        for(int i = 0; i < regionIds.size(); i++){
//            long regionId = regionIds.get(i);
//            if(regionId <  0){
//                regionIds.set(i, -regionId);
//            }
//        }
        long size = regionIds.size();
        if(list != null){
            size = size - list.size();
            for(long regionId : list){
                regionIds.remove(regionId);
                regionIds.add(-regionId);
            }
        }
        list = op.getRegionIds();
        list.add(0, size);
        //System.out.println(txRepeateVersion.toString() + "mainRegionProcess 2... : " +list);
        this.lockResultMap.put(txRepeateVersion, list);//regionIds -> list
        String txId = txRepeateVersion.getTxId();
        this.versionMap.put(txId, op.getVersion());
        //System.out.println("mainRegionProcess... ：4  " +txId);
        CompletableFuture.runAsync(() -> {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            this.successMap.put(txId, future);
            if(!FutureHelper.get(future)){
                reAsk(op, false, txRepeateVersion);
            }
        });
    }

    private void cleanMainRegionProcess(String txId, long version){
        List<TxRepeateVersion> repeateVersions = txRepeate.get(txId);
        //System.out.println("clean tx : " + txId);
        this.rawStore.commitSuccess(version);
        for(TxRepeateVersion txRepeateVersion : repeateVersions){
            this.lockResultMap.remove(txRepeateVersion);
        }
        this.versionMap.remove(txId);
        this.successMap.get(txId).complete(true);
        this.successMap.remove(txId);
        this.transactionOperationMap.remove(txId);
        this.txRepeate.remove(txId);
    }

    public Object getObject(Map resultMap, EntityEntry entry){
        return resultMap.get(entry.getParaId());
    }

    private static void setFailure(final BaseRequest request, final BaseResponse<?> response, final Status status,
                                   final Errors error) {
        response.setError(error == null ? Errors.STORAGE_ERROR : error);
        LOG.error("Failed to handle: {}, status: {}, error: {}.", request, status, error);
    }

    private void callMainRegion(TxRepeateVersion txRepeateVersion, long regionId, boolean isSuccess,final CompletableFuture<Boolean> future,
                                int retriesLeft, final Errors lastCause, boolean forceRfresh){
        //System.out.println("callMainRegion : " + txRepeateVersion.getTxId());
        final RetryRunner retryRunner = retryCause -> callMainRegion(txRepeateVersion, regionId, isSuccess, future, retriesLeft - 1,
                retryCause, true);
        final FailoverClosureImpl<Boolean> closure = new FailoverClosureImpl<>(future, false, retriesLeft, retryRunner, DTGConstants.RETRIYRUNNERWAIT);
        if(regionId == this.getRegionId()){
            if(processFirstPhaseSuccess(txRepeateVersion, regionId, isSuccess)){
                closure.run(Status.OK());
            }else {
                closure.run(new Status(TxError.FAILED.getNumber(), "transaction error in first phase"));
            }
            return;
        }
        internalCallMainRegion(txRepeateVersion, regionId, isSuccess,closure, lastCause, forceRfresh);
    }

    private void internalCallMainRegion(TxRepeateVersion txRepeateVersion, long regionId, boolean isSuccess,FailoverClosureImpl closure,final Errors lastCause, boolean forceRfresh){
        System.out.println("internalCallMainRegion + " + txRepeateVersion.toString());
        if(!checkStatus(txRepeateVersion.getTxId()))return;
        final FirstPhaseSuccessRequest request = new FirstPhaseSuccessRequest();
        request.setSuccess(isSuccess);
        request.setTxId(txRepeateVersion.getTxId());
        request.setRepeate(txRepeateVersion.getRepeate());
        request.setRegionId(regionId);
        //request.setRegionEpoch(region.getRegionEpoch());
        request.setSelfRegionId(this.getRegionId());//System.out.println("internalCallMainRegion send rpc :");
        dtgRpcService.callAsyncWithRpc(request, closure, lastCause, true);
    }

    private void secondPhase(TxRepeateVersion txRepeateVersion){
        String txId = txRepeateVersion.getTxId();
        if(!checkStatus(txId))return;
        //System.out.println("secondPhase" + txId);
        List<Long> list = this.lockResultMap.get(txRepeateVersion);
        Requires.requireTrue(list.remove(0) == 0, "the transaction "+ txId +"have not success");
        //System.out.println(list.toString());
        long version = this.versionMap.get(txId);
        Requires.requireNonNull(list, "the region list of transaction is null");
        final List<CompletableFuture<Boolean>> futures = Lists.newArrayListWithCapacity(list.size());
        boolean isSuccess = true;
        if(txStatus.get(txId) == DTGConstants.TXROLLBACK){
            isSuccess = false;
        }
        for(long regionId : list){
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            Requires.requireTrue(regionId < 0, "the region " + regionId + "haven't success!");
            callRegion(txId, isSuccess, -regionId, version, future, DTGConstants.FAILOVERRETRIES, null, false);
            futures.add(future);
        }
        FutureGroup<Boolean> group = new FutureGroup<Boolean>(futures);
        boolean res = FutureHelper.get(FutureHelper.joinBooleans(group), Long.MAX_VALUE);
        if(res){
            //this.lockResultMap.remove(txId);
            //this.versionMap.remove(txId);
            //this.successMap.get(txId).complete(true);
            //this.successMap.remove(txId);
            //this.rawStore.commitSuccess(version);
            //this.transactionOperationMap.remove(txId);
            cleanMainRegionProcess(txId, version);
            //System.out.println("second phase success : " + txId + ",  " + System.currentTimeMillis());
        }
    }

    private void callRegion(String txId, boolean isSuccess, long regionId, long version, final CompletableFuture<Boolean> future,
                            int retriesLeft, final Errors lastCause, boolean forceRfresh){
        //System.out.println("callRegion : " + txId);
        //if(!checkStatus(txId))return;
        final RetryRunner retryRunner = retryCause -> callRegion(txId, isSuccess, regionId, version, future, retriesLeft - 1,
                retryCause, true);
        final FailoverClosureImpl<Boolean> closure = new FailoverClosureImpl<>(future, false, retriesLeft, retryRunner, DTGConstants.RETRIYRUNNERWAIT);
        CompletableFuture<Boolean> future1 = new CompletableFuture();
        runCallRegion(txId, isSuccess, regionId, version, future1, retriesLeft, null, forceRfresh);
        if(FutureHelper.get(future1)){
            //System.out.println("callRegion success : " + txId);
            closure.setData(true);
            closure.run(Status.OK());
        }else{
            System.out.println("callRegion failed : " + txId);
            closure.setData(false);
            closure.setError(Errors.TRANSACTION_SECOND_ERROR);
            closure.run(new Status(RaftError.ENEWLEADER.getNumber(), "get version failed"));
        }
    }

    private void runCallRegion(String txId, boolean isSuccess, long regionId, long version, final CompletableFuture<Boolean> future,
                            int retriesLeft, final Errors lastCause, boolean forceRfresh){
        final RetryRunner retryRunner = retryCause -> runCallRegion(txId, isSuccess, regionId, version, future, retriesLeft - 1,
                retryCause, true);
        final FailoverClosureImpl<Boolean> closure = new FailoverClosureImpl<>(future, false, retriesLeft, retryRunner, DTGConstants.RETRIYRUNNERWAIT);
        if(regionId == this.getRegionId()){
            processLocalSecondPhase(txId, version, closure, isSuccess);
            return;
        }
        internalCallRegion(txId, isSuccess, regionId,true, version,closure, lastCause, forceRfresh);
    }

    private void internalCallRegion(String txId, boolean isSuccess, long regionId, boolean requireTxNotNull,
                                    long version, FailoverClosureImpl closure, final Errors lastCause, boolean forceRfresh){
        //System.out.println("internalCallRegion" + txId);
        if(!checkStatus(txId)){
            closure.setData(true);
            closure.run(Status.OK());
            return;
        }
        final SecondPhaseRequest request = new SecondPhaseRequest();
        final DTGOperation op = new DTGOperation();
        if(isSuccess){
            op.setType(OperationName.COMMITTRANS);
            //op.setType(OperationName.ROLLBACK);
        }
        else {
            op.setType(OperationName.ROLLBACK);
        }
        op.setTxId(txId);
        request.setTxId(txId);
        request.setRegionId(regionId);
        request.setVersion(version);
        request.setSuccess(isSuccess);
        request.setDTGOpreration(op);
        request.setRequireTxNotNull(requireTxNotNull);
        //request.setRegionEpoch(region.getRegionEpoch());
        dtgRpcService.callAsyncWithRpc(request, closure, lastCause, true);
    }

    private void reAsk(DTGOperation op, boolean fromClient, TxRepeateVersion txRepeateVersion){
        if(!checkStatus(op.getTxId()))return;
        List<Long> regionIds = this.lockResultMap.get(txRepeateVersion);
        List<Long> newRegionIds = new LinkedList<>();
        Map<DTGRegion, List<EntityEntry>> map = dirtributeEntity(op.getAllEntityEntries(), null);
        for(DTGRegion re : map.keySet()){
            newRegionIds.add(re.getId());
        }
        //System.out.println("reAsk " + op.getTxId() + " list : " + newRegionIds);
        for(DTGRegion re : map.keySet()){
            if(regionIds.contains(re.getId())){
                //t.getAndDecrement();
                reCallRegion(map.get(re), re, op.getMainRegionId(), txRepeateVersion, fromClient,
                        op.getVersion(), newRegionIds, op.isReadOnly(), op.getAllEntityEntries());
            }
        }
    }

    private Map<DTGRegion, List<EntityEntry>> dirtributeEntity(final List<EntityEntry> entityEntryList, final Throwable lastCause){
        DTGPlacementDriverClient pdClient = this.regionEngine.getStoreEngine().getPdClient();
        if(lastCause != null){
            pdClient.refreshRouteTable(true);
        }
        LinkedList<EntityEntry> NodeEntityEntryList = new LinkedList<>();
        LinkedList<EntityEntry> RelaEntityEntryList = new LinkedList<>();
        LinkedList<EntityEntry> TempProEntityEntryList = new LinkedList<>();
        for(EntityEntry entityEntry : entityEntryList){
            switch (entityEntry.getType()){
                case NODETYPE:{
                    NodeEntityEntryList.add(entityEntry);
                    break;
                }
                case RELATIONTYPE:{
                    RelaEntityEntryList.add(entityEntry);
                    break;
                }
                case TEMPORALPROPERTYTYPE:{
                    TempProEntityEntryList.add(entityEntry);
                    break;
                }
                default:{
                    try {
                        throw new TypeDoesnotExistException(entityEntry.getType(), "entity type");
                    } catch (TypeDoesnotExistException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        Map[] distributeMap =
                pdClient.findRegionsByEntityEntries(NodeEntityEntryList, ApiExceptionHelper.isInvalidEpoch(lastCause), NODETYPE);
        pdClient.findRegionsByEntityEntries(RelaEntityEntryList, ApiExceptionHelper.isInvalidEpoch(lastCause), RELATIONTYPE, distributeMap);
        pdClient.findRegionsByEntityEntries(TempProEntityEntryList, ApiExceptionHelper.isInvalidEpoch(lastCause), TEMPORALPROPERTYTYPE, distributeMap);
        HashMap<DTGRegion, List<EntityEntry>> regionMap = (HashMap<DTGRegion, List<EntityEntry>>)distributeMap[0];
        for(DTGRegion region : regionMap.keySet()){
            Collections.sort(regionMap.get(region));
        }
        return regionMap;
    }

    public void setDtgRpcService(DTGRpcService dtgRpcService) {
        this.dtgRpcService = dtgRpcService;
    }

    private void reCallRegion(List<EntityEntry> entries, DTGRegion target, long mainRegionId, TxRepeateVersion txRepeateVersion, boolean fromClient,
                              long version, List<Long> regionIds, boolean isReadOnly, List<EntityEntry> allEntries){
        //System.out.println("reCallRegion :" + txRepeateVersion.getTxId());
        if(!checkStatus(txRepeateVersion.getTxId()))return;
        DTGSaveStore saveStore = this.regionEngine.getStoreEngine().getSaveStore();
        DTGOperation op = new DTGOperation(entries, OperationName.TRANSACTIONOP);
        if(region.getId() == mainRegionId){
            op.setAllEntityEntries(allEntries);
            op.setRegionIds(regionIds);
        }
        op.setTxId(txRepeateVersion.getTxId());
        op.setVersion(version);
        op.setMainRegionId(mainRegionId);
        op.setReadOnly(isReadOnly);


        CompletableFuture<Map<Integer, Object>> future = new CompletableFuture();
        saveStore.applyOperation(op, target, future,fromClient,txRepeateVersion.getRepeate(), 0, Errors.NOT_LEADER);
        CompletableFuture.runAsync(() -> {
            CompletableFuture<Boolean> future2 = new CompletableFuture<>();
            this.successMap.put(txRepeateVersion.getTxId(), future2);
            if(!FutureHelper.get(future2)){
                System.out.println("reCallRegion :" + txRepeateVersion);
                reAsk(op, fromClient, txRepeateVersion);
            }
        });
    }

//    private void rollbackTx(String txId){
//        this.rawStore.ApplyEntityEntries(op, new BaseStoreClosure() {
//            @Override
//            public void run(final Status status) {
//                if (status.isOk()) {
//                    //System.out.println("processLocalSecondPhase ok : " + region.getId());
//                    closure.setData(true);
//                    closure.run(Status.OK());
//                    txStatus.remove(op.getTxId());
//                } else {
//                    System.out.println("failed commit..:" + op.getTxId());
//                    closure.setData(false);
//                    closure.run(new Status(TxError.FAILED.getNumber(), "can not run transaction"));
//                }
//            }
//        });
//        txStatus.remove(txId);
//    }
    private boolean synOp(DTGOperation op, TxRepeateVersion txRepeateVersion, CompletableFuture future, int retriesLeft, final Errors lastCause){
        //System.out.println("run synOp : " + op.getTxId());
        if(!checkStatus(op.getTxId()))return false;
//        if(!isLeader()){
//            reAsk(op);
//            cleanMainRegionProcess(op.getTxId(), op.getVersion());
//            return;
//        }
        //final RetryRunner retryRunner = retryCause -> synOp(op, future, retriesLeft - 1, retryCause);
        //final FailoverClosureImpl<Boolean> closure = new FailoverClosureImpl<>(future, false, retriesLeft, retryRunner);
        final CompletableFuture<Boolean> future1 = new CompletableFuture<>();
        this.rawStore.ApplyEntityEntries(op, new BaseStoreClosure() {
            @Override
            public void run(final Status status) {
                //System.out.println("synOp run closure: " + op.getTxId());
                if (status.isOk()) {
                    //System.out.println("success synOp" + op.getTxId());
                    future1.complete(true);
                    //closure.run(Status.OK());
                } else {
                    System.out.println("synOp error : " + status.getErrorMsg() + ", tx status = " + txStatus.get(op.getTxId()));
                    //if(txStatus.get(op.getTxId()) != DTGConstants.TXRERUNFIRST){
                        retryWait();
                        txStatus.put(op.getTxId(), DTGConstants.SYNOP);
                        reAsk(op, true, txRepeateVersion);
                        //future1.complete(false);
                        //return;
                    //}
                    txStatus.put(op.getTxId(), DTGConstants.SYNOPFAILED);
                    future1.complete(false);
                    //cleanMainRegionProcess(op.getTxId(), op.getVersion());
                    //closure.run(new Status(TxError.FAILED.getNumber(), "can not send op to peers"));
                }
            }
        });
        return FutureHelper.get(future1, Long.MAX_VALUE);
    }

    private boolean isLeader(){
        return this.regionEngine.getFsm().isLeader();
        //return this.regionEngine.isLeader();
    }

    private void retryWait(){
        try {
            Thread.sleep(DTGConstants.RETRIYRUNNERWAIT);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    class TxRepeateVersion{
        private int repeate = 0;
        private String txId;

        public TxRepeateVersion(String txId, int repeate){
            this.txId = txId;
            this.repeate = repeate;
        }

        public int getRepeate() {
            return repeate;
        }

        public String getTxId() {
            return txId;
        }

        @Override
        public boolean equals(Object obj) {
            if(this == obj){
                return true;
            }

            if(obj == null){
                return false;
            }

            if(obj instanceof TxRepeateVersion){
                TxRepeateVersion other = (TxRepeateVersion) obj;
                if(this.txId.equals(other.getTxId())
                        && this.repeate == other.getRepeate()){
                    return true;
                }
            }
            return false;
        }

        @Override
        public String toString() {
            return this.txId + ", repeate = " + repeate;
        }
    }
}

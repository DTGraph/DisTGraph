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
    private Map<String, List<Long>> lockResultMap;
    private Map<String, Long> versionMap;
    private Map<String, DTGOperation> transactionOperationMap;
    private Map<String, CompletableFuture> successMap;
    private final Map<String, Byte> txStatus;

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

    @Override
    public void handleFirstPhase(FirstPhaseRequest request, RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        //System.out.println(request.getDTGOpreration().getTxId() +"  get request : " + System.currentTimeMillis());
        //System.out.println("handleFirstPhase : prepare first phase! region id = " + getRegionId());
        this.region.increaseTransactionCount();
        final FirstPhaseResponse response = new FirstPhaseResponse();
        response.setSelfRegionId(getRegionId());
        //response.setRegionEpoch(getRegionEpoch());
        Map<Integer, Object> resultMap = new HashMap<>();
        resultMap.put(-1, true);
        KVParameterRequires.requireSameEpoch(request, getRegionEpoch());//System.out.println("run op... ：2  " + region.getId());
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
        //System.out.println("handleFirstPhase : " + op.getEntityEntries().size());
        try {//System.out.println("run op... ：1  " +region.getId());
            if (this.txStatus.get(txId) != null){
                byte status = this.txStatus.get(txId);
                if(status == DTGConstants.TXRECEIVED){
                    closure.sendResponse(response);
                }
                else if(status == DTGConstants.TXDONEFIRST){
                    callMainRegion(txId, op.getMainRegionId(), true, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
                }
                else if(status == DTGConstants.TXFAILEDFIRST){
                    callMainRegion(txId, op.getMainRegionId(), false, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
                }
                return;
            }
            FirstPhaseClosure firstClosure = new FirstPhaseClosure() {
                @Override
                public void run(Status status) {
                    if (status.isOk()) {
                        //System.out.println("handleFirstPhase ok :" + getRegionId());
                        response.setValue(toByteArray(getData()));
                        closure.sendResponse(response);
                        synOp(op, new CompletableFuture(), DTGConstants.FAILOVERRETRIES, null);

                        txStatus.put(txId, DTGConstants.TXDONEFIRST);
                        callMainRegion(txId, op.getMainRegionId(), true, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
                    } else {
                        //System.out.println("failed commit..:" + getRegionId());
                        response.setValue(toByteArray(resultMap));
                        setFailure(request, response, status, getError());
                        closure.sendResponse(response);
                        txStatus.put(txId, DTGConstants.TXFAILEDFIRST);
                        callMainRegion(txId, op.getMainRegionId(), false, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
                    }
                }
            };
            if(op.getMainRegionId() == this.region.getId()){
                mainRegionProcess(op, txId);
            }
            this.transactionOperationMap.put(op.getTxId(), op);
            this.txStatus.put(op.getTxId(), DTGConstants.TXRECEIVED);
            this.rawStore.firstPhaseProcessor(op, firstClosure, this.region);
        } catch (final Throwable t) {
            txStatus.put(txId, DTGConstants.TXFAILEDFIRST);
            callMainRegion(txId, op.getMainRegionId(), false, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
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
        try {
            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
            final DTGOperation op = KVParameterRequires
                    .requireNonNull(transactionOperationMap.get(txId), "put.DTGOperation");
//            if(!request.isSuccess()){
//                rollbackTx(op.getTxId());
//                response.setValue(true);
//                closure.sendResponse(response);
//                return;
//            }
            this.rawStore.ApplyEntityEntries(op, new BaseStoreClosure() {
                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        //System.out.println("success commit..:" + op.getTxId());
                        //System.out.println("second phase ok");
                        response.setValue((Boolean) getData());
                        txStatus.remove(op.getTxId());
                    } else {
                        System.out.println("failed commit..:" + op.getTxId());
                        response.setValue(false);
                        setFailure(request, response, status, getError());
                    }
                    //System.out.println("return commit..");
                    closure.sendResponse(response);
                }
            });
            this.txStatus.remove(txId);
        } catch (final Throwable t) {
            System.out.println("failed commit..");
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setValue(false);
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
        //System.out.println("finish commit request");
    }

    private void processLocalSecondPhase(final String txId, final FailoverClosureImpl closure, final boolean isSuccess){
        //System.out.println("processLocalSecondPhase");
        final DTGOperation op = new DTGOperation();
        if(isSuccess){
            op.setType(OperationName.COMMITTRANS);
        }
        else {
            op.setType(OperationName.ROLLBACK);
        }
        op.setTxId(txId);
//        if(this.txStatus.get(op.getTxId()) == DTGConstants.TXROLLBACK){
//            rollbackTx(op.getTxId());
//            return;
//        }
        this.rawStore.ApplyEntityEntries(op, new BaseStoreClosure() {
            @Override
            public void run(final Status status) {
                if (status.isOk()) {
                    //System.out.println("processLocalSecondPhase ok : " + region.getId());
                    closure.setData(true);
                    closure.run(Status.OK());
                    txStatus.remove(txId);
                } else {
                    System.out.println("failed commit..:" + op.getTxId());
                    closure.setData(false);
                    closure.run(new Status(TxError.FAILED.getNumber(), "can not run transaction"));
                }
            }
        });
        this.txStatus.remove(txId);
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
            processFirstPhaseSuccess(request.getTxId(), request.getSelfRegionId(), request.isSuccess());
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

    private boolean processFirstPhaseSuccess(String txId, long regionId, boolean isSuccess){
        if(!isSuccess){
            this.txStatus.put(txId, DTGConstants.TXROLLBACK);
        }
        //System.out.println("processFirstPhaseSuccess : " + txId);
        List<Long> list = this.lockResultMap.get(txId);
//        System.out.println(list.toString());
        if(list == null){
            list = new ArrayList<>();
            list.add(-1L);
            list.add(regionId);
        }else {
            list.remove(regionId);
            list.add(-regionId);
            long size = list.get(0) - 1;
            //System.out.println("processFirstPhaseSuccess size = " + size);
            list.set(0, size);
            if(size == 0L){
                secondPhase(txId);
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
        this.region.increaseTransactionCount();
        Map<Integer, Object> resultMap = new HashMap<>();
        resultMap.put(-1, true);
        try {
            String txId = op.getTxId();
            if (this.txStatus.get(txId) != null){
                byte status = this.txStatus.get(txId);
                if(status == DTGConstants.TXRECEIVED){
                    closure.run(Status.OK());
                }
                else if(status == DTGConstants.TXDONEFIRST){
                    callMainRegion(txId, op.getMainRegionId(), true, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
                    closure.run(Status.OK());
                }
                else if(status == DTGConstants.TXFAILEDFIRST){
                    callMainRegion(txId, op.getMainRegionId(), false, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
                }
                return;
            }
            FirstPhaseClosure firstClosure = new FirstPhaseClosure() {
                @Override
                public void run(Status status) {
                    if (status.isOk()) {
                        closure.setData(getData());
                        closure.run(Status.OK());
                        txStatus.put(txId, DTGConstants.TXDONEFIRST);
                        synOp(op, new CompletableFuture(), DTGConstants.FAILOVERRETRIES, null);
                        callMainRegion(txId, op.getMainRegionId(), true, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
                    } else {
                        closure.run(new Status(TxError.FAILED.getNumber(), "transaction failed"));
                        txStatus.put(txId, DTGConstants.TXFAILEDFIRST);
                        callMainRegion(txId, op.getMainRegionId(), false, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
                    }
                }
            };
            if(op.getMainRegionId() == this.region.getId()){
                mainRegionProcess(op, txId);
            }
            this.transactionOperationMap.put(op.getTxId(), op);
            this.txStatus.put(op.getTxId(), DTGConstants.TXRECEIVED);
            this.rawStore.firstPhaseProcessor(op, firstClosure, this.region);
        } catch (final Throwable t) {
            txStatus.put(op.getTxId(), DTGConstants.TXDONEFIRST);
            callMainRegion(op.getTxId(), op.getMainRegionId(), false, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
            System.out.println("error!" + region.getId());
            closure.run(new Status(TxError.FAILED.getNumber(), "transaction failed"));
        }
    }

    private void mainRegionProcess(DTGOperation op, String txId){
        List<Long> list = this.lockResultMap.get(op.getTxId());
        List<Long> regionIds = op.getRegionIds();
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
        this.lockResultMap.put(txId, regionIds);
        this.versionMap.put(txId, op.getVersion());
        CompletableFuture.runAsync(() -> {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            this.successMap.put(txId, future);
            if(!FutureHelper.get(future)){
                reAsk(op);
            }
        });
    }

    public Object getObject(Map resultMap, EntityEntry entry){
        return resultMap.get(entry.getParaId());
    }

    private static void setFailure(final BaseRequest request, final BaseResponse<?> response, final Status status,
                                   final Errors error) {
        response.setError(error == null ? Errors.STORAGE_ERROR : error);
        LOG.error("Failed to handle: {}, status: {}, error: {}.", request, status, error);
    }

    private void callMainRegion(String txId, long regionId, boolean isSuccess,final CompletableFuture<Boolean> future,
                                int retriesLeft, final Errors lastCause, boolean forceRfresh){
        final RetryRunner retryRunner = retryCause -> callMainRegion(txId, regionId, isSuccess, future, retriesLeft - 1,
                retryCause, true);
        final FailoverClosureImpl<Boolean> closure = new FailoverClosureImpl<>(future, false, retriesLeft, retryRunner);
        if(regionId == this.getRegionId()){
            if(processFirstPhaseSuccess(txId, regionId, isSuccess)){
                closure.run(Status.OK());
            }else {
                closure.run(new Status(TxError.FAILED.getNumber(), "transaction error in first phase"));
            }
            return;
        }
        internalCallMainRegion(txId, regionId, isSuccess,closure, lastCause, forceRfresh);
    }

    private void internalCallMainRegion(String txId, long regionId, boolean isSuccess,FailoverClosureImpl closure,final Errors lastCause, boolean forceRfresh){
        //System.out.println("internalCallMainRegion + " + regionId);
        final FirstPhaseSuccessRequest request = new FirstPhaseSuccessRequest();
        request.setSuccess(isSuccess);
        request.setTxId(txId);
        request.setRegionId(regionId);
        //request.setRegionEpoch(region.getRegionEpoch());
        request.setSelfRegionId(this.getRegionId());//System.out.println("internalCallMainRegion send rpc :");
        dtgRpcService.callAsyncWithRpc(request, closure, lastCause, true);
    }

    private void secondPhase(String txId){
        //System.out.println("secondPhase");
        List<Long> list = this.lockResultMap.get(txId);
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
        boolean res = FutureHelper.get(FutureHelper.joinBooleans(group));
        if(res){
            this.lockResultMap.remove(txId);
            this.versionMap.remove(txId);
            this.successMap.get(txId).complete(true);
            this.successMap.remove(txId);
            this.rawStore.commitSuccess(version);
            this.transactionOperationMap.remove(txId);
            System.out.println("second phase success : " + txId + ",  " + System.currentTimeMillis());
        }
    }

    private void callRegion(String txId, boolean isSuccess, long regionId, long version, final CompletableFuture<Boolean> future,
                            int retriesLeft, final Errors lastCause, boolean forceRfresh){
        final RetryRunner retryRunner = retryCause -> callRegion(txId, isSuccess, regionId, version, future, retriesLeft - 1,
                retryCause, true);
        final FailoverClosureImpl<Boolean> closure = new FailoverClosureImpl<>(future, false, retriesLeft, retryRunner);
        if(regionId == this.getRegionId()){
            processLocalSecondPhase(txId, closure, isSuccess);
            return;
        }
        internalCallRegion(txId, isSuccess, regionId, version,closure, lastCause, forceRfresh);
    }

    private void internalCallRegion(String txId, boolean isSuccess, long regionId, long version, FailoverClosureImpl closure, final Errors lastCause, boolean forceRfresh){
        //System.out.println("internalCallRegion");
        final SecondPhaseRequest request = new SecondPhaseRequest();
        final DTGOperation op = new DTGOperation();
        if(isSuccess){
            op.setType(OperationName.COMMITTRANS);
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
        //request.setRegionEpoch(region.getRegionEpoch());
        dtgRpcService.callAsyncWithRpc(request, closure, lastCause, true);
    }

    private void reAsk(DTGOperation op){
        List<Long> regionIds= this.lockResultMap.get(op.getTxId());
        Map<DTGRegion, List<EntityEntry>> map = dirtributeEntity(op.getAllEntityEntries(), null);
        for(DTGRegion re : map.keySet()){
            if(regionIds.contains(re.getId())){
                reCallRegion(map.get(re), re, op.getMainRegionId(), op.getTxId(), op.getVersion());
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

    private void reCallRegion(List<EntityEntry> entries, DTGRegion target, long mainRegionId, String txId, long version){
        DTGSaveStore saveStore = this.regionEngine.getStoreEngine().getSaveStore();
        DTGOperation op = new DTGOperation(entries, OperationName.TRANSACTIONOP);
        op.setTxId(txId);
        op.setVersion(version);
        op.setMainRegionId(mainRegionId);
        //System.out.println("main region id = " + mainRegionId);
        CompletableFuture<Map<Integer, Object>> future = new CompletableFuture();
        saveStore.applyOperation(op, target, future, DTGConstants.FAILOVERRETRIES, null);
        CompletableFuture.runAsync(() -> {
            CompletableFuture<Boolean> future2 = new CompletableFuture<>();
            this.successMap.put(txId, future2);
            if(!FutureHelper.get(future2)){
                reAsk(op);
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
    private void synOp(DTGOperation op, CompletableFuture future, int retriesLeft, final Errors lastCause){
        //System.out.println("run synOp : " + op.getEntityEntries().size());
        //final RetryRunner retryRunner = retryCause -> synOp(op, future, retriesLeft - 1, retryCause);
        //final FailoverClosureImpl<Boolean> closure = new FailoverClosureImpl<>(future, false, retriesLeft, retryRunner);
        this.rawStore.ApplyEntityEntries(op, new BaseStoreClosure() {
            @Override
            public void run(final Status status) {
                //System.out.println("synOp run closure: " + op.getTxId());
                if (status.isOk()) {
                    //closure.run(Status.OK());
                } else {
                    //closure.run(new Status(TxError.FAILED.getNumber(), "can not send op to peers"));
                }
            }
        });
    }
}

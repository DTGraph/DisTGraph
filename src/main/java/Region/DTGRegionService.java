package Region;

import Communication.DTGRpcService;
import Communication.RequestAndResponse.*;
import DBExceptions.EntityEntryException;
import DBExceptions.TransactionException;
import DBExceptions.TxError;
import DBExceptions.TypeDoesnotExistException;
import Element.DTGOperation;
import Element.EntityEntry;
import Element.OperationName;
import PlacementDriver.DTGPlacementDriverClient;
import UserClient.DTGSaveStore;
import UserClient.Transaction.TransactionLog;
import UserClient.TransactionManager;
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
    private DTGRegion region;
    private DTGRpcService dtgRpcService;
    private Map<TxRepeateVersion, List<Long>> lockResultMap;
    private Map<String, Long> versionMap;
    //private Map<String, DTGOperation> transactionOperationMap;
    private Map<String, CompletableFuture> successMap;
    private final Map<String, Byte> txStatus;
    private final Map<String, Map<Integer, Object>> txResult;
    private final Map<String, List<TxRepeateVersion>> txRepeate;
    private TransactionManager versionManager;


    private RegionSet r;
    public DTGRegionService(DTGRegionEngine regionEngine, TransactionManager versionManager){
        this.regionEngine = regionEngine;
        this.rawStore = regionEngine.getMetricsRawStore();
        this.region = regionEngine.getRegion();
        lockResultMap = new ConcurrentHashMap<>();
        this.versionMap = new ConcurrentHashMap<>();
        //this.transactionOperationMap = new ConcurrentHashMap<>();
        this.successMap = new ConcurrentHashMap<>();
        this.txStatus = new ConcurrentHashMap<>();
        this.txRepeate = new ConcurrentHashMap<>();
        this.txResult = new ConcurrentHashMap<>();
        this.versionManager = versionManager;
        //this.failedCommitList = new LinkedList<>();
        r = new RegionSet(this.regionEngine.getStoreEngine().getStoreOpts().getRaftDataPath()  + "aaaa" + region.getId(), this.regionEngine.getStoreEngine().getStoreId(), versionManager);
    }


    @Override
    public long getRegionId() {
        return this.region.getId();
    }

    @Override
    public RegionEpoch getRegionEpoch() {
        return this.regionEngine.getRegion().getRegionEpoch();
    }



    //单副本
    @Override
    public void handleFirstPhase(FirstPhaseRequest request, RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        //System.out.println(System.currentTimeMillis() + "  handleFirstPhase : " + request.getDTGOpreration().getTxId() + ",   " + region.getId());
        final FirstPhaseResponse response = new FirstPhaseResponse();
        response.setSelfRegionId(getRegionId());

        if(this.region.getTransactionCount() > DTGConstants.MAXRUNNINGTX){
            System.out.println(this.region.getTransactionCount() + "       " + DTGConstants.MAXRUNNINGTX);
            response.setError(Errors.TRANSACTION_FULL);
            closure.sendResponse(response);
            return;
        }
        this.region.increaseTransactionCount();
        Map<Integer, Object> resultMap = new HashMap<>();
        resultMap.put(-1, true);
        KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
        final DTGOperation op = KVParameterRequires
                .requireNonNull(request.getDTGOpreration(), "put.DTGOperation");
        String txId = op.getTxId();
        //System.out.println("time :" + System.currentTimeMillis() + ", handle tx " + txId);
        try {
            Map<Integer, Object> rmap = r.processTx(op);
            if(rmap != null){
                response.setValue(toByteArray(rmap));
                closure.sendResponse(response);
                //System.out.println("Transaction Done " + txId);
                return;
            }
        } catch (TypeDoesnotExistException | EntityEntryException e) {
            response.setError(Errors.TRANSACTION_FIRSTPHASE_ERROR);
            closure.sendResponse(response);
            //System.out.println("time :" + System.currentTimeMillis() + "  transaction error " + txId);
            return;
        }


//        if(txId.substring(0,5).equals("reRun")){
//            byte status = ((DTGMetricsRawStore)this.rawStore).getTxStatus(op.getVersion());
//            if(isExist(status, closure, response, false, op, request)){
//                return;
//            }
//        }

        if (this.txStatus.get(txId) != null){
            byte status = this.txStatus.get(txId);
            if(isExist(status, closure, response, false, op, request)){
                closeTx();
                return;
            }
        }
        else{
            this.txStatus.put(op.getTxId(), DTGConstants.TXRECEIVED);
            changeStatus(op.getMainRegionId(), DTGConstants.TXRECEIVED, op.getVersion(), op.getTxId());
        }
        this.versionMap.put(op.getTxId(), op.getVersion());
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
        try {
            FirstPhaseClosure firstClosure = new FirstPhaseClosure() {
                @Override
                public void run(Status status) {
                    if(hasSendRes()){
                        return;
                    }
                    sendResult();
                    if (status.isOk()) {
                        response.setValue(toByteArray(getData()));
                        closure.sendResponse(response);
                        txResult.put(txId, (Map<Integer, Object>)getData());
                        if(op.isReadOnly()){
                            return;
                        }
                        txStatus.put(txId, DTGConstants.TXDONEFIRST);
                        System.out.println("time :" + System.currentTimeMillis() + ", finish first phase " + txId);
//                        if(txId.equals("E8-6A-64-04-DF-455000")){
//                            try {
//                                Thread.sleep(1000000);
//                            } catch (InterruptedException e) {
//                                e.printStackTrace();
//                            }
//                        }
                        callMainRegion(thisRepeate, op.getMainRegionId(), true, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
                    } else {
                        closeTx();
                        System.out.println("first phase failed commit..:" + txId);
                        response.setValue(toByteArray(resultMap));
                        setFailure(request, response, status, getError());
                        closure.sendResponse(response);
                        txStatus.put(txId, DTGConstants.TXFAILEDFIRST);
                        callMainRegion(thisRepeate, op.getMainRegionId(), false, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
                    }
                }
            };
            if(op.getMainRegionId() == this.region.getId()){
                mainRegionProcess(op, thisRepeate);
            }
            //this.transactionOperationMap.put(op.getTxId(), op);
            this.rawStore.firstPhaseProcessor(op, firstClosure, this.region);
            if(op.isReadOnly()){
                txStatus.remove(txId);
                //transactionOperationMap.remove(txId);
                if(txRepeate.containsKey(txId)){
                    txRepeate.remove(txId);
                }
                closeTx();
            }
        } catch (final Throwable t) {
            closeTx();
            txStatus.put(txId, DTGConstants.TXFAILEDFIRST);
            callMainRegion(thisRepeate, op.getMainRegionId(), false, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
            System.out.println("error!" + region.getId());
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.TRANSACTION_FIRSTPHASE_ERROR);
            closure.sendResponse(response);
        }
    }

    private boolean isExist(byte txStatus, RequestProcessClosure<BaseRequest, BaseResponse<?>> closure, FirstPhaseResponse response, boolean isReRun, DTGOperation op, FirstPhaseRequest request){
        switch (txStatus){
            case DTGConstants.TXRECEIVED:{
                if(isReRun){
                    return false;
                }
            }
            case DTGConstants.TXRERUNFIRST:{
                response.setError(Errors.REQUEST_REPEATE);
                closure.sendResponse(response);
                return true;
            }
            case DTGConstants.TXDONEFIRST:{
                if(isReRun){
                    return false;
                }else{
                    response.setValue(toByteArray(txResult.get(op.getTxId())));
                    closure.sendResponse(response);
                }
                return true;
            }
            case DTGConstants.TXFAILEDFIRST:{
                if(isReRun){
                    TxRepeateVersion txRepeateVersion = new TxRepeateVersion(op.getTxId(), request.getRepeate());
                    callMainRegion(txRepeateVersion, op.getMainRegionId(), false, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
                }
                else {
                    response.setError(Errors.REQUEST_REPEATE);
                    closure.sendResponse(response);
                }
                return true;
            }
        }
        return false;
    }

    private boolean isSecondExist(byte txStatus, RequestProcessClosure<BaseRequest, BaseResponse<?>> closure, SecondPhaseResponse response, boolean isReRun){
        switch (txStatus){
            case DTGConstants.TXSECONDSTART:{
                if(isReRun){
                    return false;
                }
            }
            case DTGConstants.TXSUCCESS:{
                response.setError(Errors.REQUEST_REPEATE);
                closure.sendResponse(response);
                return true;
            }
            case DTGConstants.TXDONEFIRST:{
                return false;
            }

        }
        return false;
    }

    private void closeTx(){
        this.region.decreaseTransactionCount();
    }

    //多副本代码
//    @Override
//    public void handleFirstPhase(FirstPhaseRequest request, RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
//        //System.out.println(System.currentTimeMillis() + "  handleFirstPhase : " + request.getDTGOpreration().getTxId());
//        final FirstPhaseResponse response = new FirstPhaseResponse();
//        response.setSelfRegionId(getRegionId());
//        if(!isLeader()){
//            response.setError(Errors.NOT_LEADER);
//            closure.sendResponse(response);
//            return;
//        }
//        if(transactionOperationMap.size() > DTGConstants.MAXRUNNINGTX){
//            response.setError(Errors.TRANSACTION_FULL);
//            closure.sendResponse(response);
//            return;
//        }
//        this.region.increaseTransactionCount();
//        Map<Integer, Object> resultMap = new HashMap<>();
//        resultMap.put(-1, true);
//        KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
//        final DTGOperation op = KVParameterRequires
//                .requireNonNull(request.getDTGOpreration(), "put.DRGOperation");
//        String txId = op.getTxId();
//        if (this.txStatus.get(txId) != null){
//            byte status = this.txStatus.get(txId);
//            if(status == DTGConstants.TXRECEIVED || status == DTGConstants.TXRERUNFIRST){
//                response.setError(Errors.REQUEST_REPEATE);
//                closure.sendResponse(response);
//                return;
//            }
//            else if(status == DTGConstants.TXDONEFIRST){
//                if(request.isFromClient()){
//                    response.setValue(toByteArray(txResult.get(txId)));
//                    closure.sendResponse(response);
//                }else{
//                    TxRepeateVersion txRepeateVersion = new TxRepeateVersion(op.getTxId(), request.getRepeate());
//                    callMainRegion(txRepeateVersion, op.getMainRegionId(), true, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
//                }
//                return;
//            }
//            else if(status == DTGConstants.TXFAILEDFIRST){
//                if(request.isFromClient()){
//                    response.setError(Errors.REQUEST_REPEATE);
//                    closure.sendResponse(response);
//                }
//                else {
//                    TxRepeateVersion txRepeateVersion = new TxRepeateVersion(op.getTxId(), request.getRepeate());
//                    callMainRegion(txRepeateVersion, op.getMainRegionId(), false, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
//                }
//                return;
//            }
//            else if(status == DTGConstants.SYNOP || status == DTGConstants.SYNOPFAILED){
//                txStatus.put(op.getTxId(), DTGConstants.TXRERUNFIRST);
//            }
//        }
//        else{
//            this.txStatus.put(op.getTxId(), DTGConstants.TXRECEIVED);
//        }
//        int repeate = 1;
//        List<TxRepeateVersion> list;
//        if(this.txRepeate.containsKey(txId)){
//            list = txRepeate.get(txId);
//            repeate = list.get(0).getRepeate() + 1;
//        }
//        else {
//            list = new ArrayList<>();
//            txRepeate.put(txId, list);
//        }
//        TxRepeateVersion thisRepeate = new TxRepeateVersion(txId, repeate);
//        list.add(thisRepeate);
//        try {
//            FirstPhaseClosure firstClosure = new FirstPhaseClosure() {
//                @Override
//                public void run(Status status) {
//                if(hasSendRes()){
//                    return;
//                }
//                sendResult();
//                if (status.isOk()) {
//                    if(MVCC){
//                        response.setValue(toByteArray(getData()));
//                        closure.sendResponse(response);
//                        txResult.put(txId, resultMap);
//                    }
//                    if(op.isReadOnly() && MVCC){
//                        return;
//                    } else {
//                        if(!synOp(op, thisRepeate, this)){
//                            return;
//                        }
//                    }
//                    if(!MVCC){
//                        response.setValue(toByteArray(getData()));
//                        closure.sendResponse(response);
//                        txResult.put(txId, resultMap);
//                    }
//                    txStatus.put(txId, DTGConstants.TXDONEFIRST);
//                    callMainRegion(thisRepeate, op.getMainRegionId(), true, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
//                } else {
//                    System.out.println("first phase failed commit..:" + txId);
//                    response.setValue(toByteArray(resultMap));
//                    setFailure(request, response, status, getError());
//                    closure.sendResponse(response);
//                    txStatus.put(txId, DTGConstants.TXFAILEDFIRST);
//                    callMainRegion(thisRepeate, op.getMainRegionId(), false, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
//                }
//                }
//            };
//            if(op.getMainRegionId() == this.region.getId()){
//                mainRegionProcess(op, thisRepeate);
//            }
//            this.transactionOperationMap.put(op.getTxId(), op);
//            this.rawStore.firstPhaseProcessor(op, firstClosure, this.region);
//            if(op.isReadOnly()){
//                txStatus.remove(txId);
//                transactionOperationMap.remove(txId);
//                if(txRepeate.containsKey(txId)){
//                    txRepeate.remove(txId);
//                }
//            }
//        } catch (final Throwable t) {
//            txStatus.put(txId, DTGConstants.TXFAILEDFIRST);
//            callMainRegion(thisRepeate, op.getMainRegionId(), false, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
//            System.out.println("error!" + region.getId());
//            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
//            response.setError(Errors.TRANSACTION_FIRSTPHASE_ERROR);
//            closure.sendResponse(response);
//        }
//    }

    @Override
    public void handleSecondPhase(SecondPhaseRequest request, RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        //System.out.println(System.currentTimeMillis()+ "  handleSecondPhase " + request.getTxId() + ",   " + region.getId());
        final SecondPhaseResponse response = new SecondPhaseResponse();
        String txId = request.getTxId();
        if(request.isRequireTxNotNull()){
            KVParameterRequires.requireTrue(this.txStatus.get(request.getTxId()) == DTGConstants.TXDONEFIRST, "first phase has not done!");
        }
        final DTGOperation op = KVParameterRequires.requireNonNull(request.getDTGOpreration(), "put op");

        if(txId.substring(0,5).equals("reRun")){
            byte status = ((DTGMetricsRawStore)this.rawStore).getTxStatus(op.getVersion());
            if(isSecondExist(status, closure, response, true)){
                return;
            }
        }

        if (this.txStatus.get(txId) != null){
            byte status = this.txStatus.get(txId);
            if(isSecondExist(status, closure, response, false)){
                return;
            }
        }

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
    }

    @Override
    public void handleSecondRead(final SecondReadRequest request,
                                 final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        final SecondReadResponse response = new SecondReadResponse();
        response.setSelfRegionId(getRegionId());
        if(!isLeader()){
            response.setError(Errors.NOT_LEADER);
            closure.sendResponse(response);
            return;
        }
        Map<Integer, Object> resultMap = new HashMap<>();
        resultMap.put(-1, true);
        KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
        final DTGOperation op = KVParameterRequires
                .requireNonNull(request.getDTGOpreration(), "put.DRGOperation");
        String txId = op.getTxId();
        try {
            FirstPhaseClosure firstClosure = new FirstPhaseClosure() {
                @Override
                public void run(Status status) {
                    if(hasSendRes()){
                        return;
                    }
                    sendResult();
                    if (status.isOk()) {
                        response.setValue(toByteArray(getData()));
                        closure.sendResponse(response);
                    } else {
                        System.out.println("first phase failed commit..:" + txId);
                        response.setValue(toByteArray(resultMap));
                        setFailure(request, response, status, getError());
                        closure.sendResponse(response);
                    }
                }
            };
            this.rawStore.firstPhaseProcessor(op, firstClosure, this.region);
        } catch (final Throwable t) {
            System.out.println("error!" + region.getId());
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.TRANSACTION_FIRSTPHASE_ERROR);
            closure.sendResponse(response);
        }
    }

    private void processLocalSecondPhase(final String txId, long version, final FailoverClosureImpl closure, final boolean isSuccess){
        //System.out.println("processLocalSecondPhase " + txId + ",   " + region.getId());
        if(!checkStatus(txId)){
            closure.setData(true);
            closure.run(Status.OK());
            return;
        }
        final DTGOperation op = new DTGOperation();
        if(isSuccess){
            op.setType(OperationName.COMMITTRANS);
        }
        else {
            op.setType(OperationName.ROLLBACK);
        }
        op.setTxId(txId);
        op.setVersion(version);
        op.setMainRegionId(this.region.getId());
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

    private void internalSecondCommit(final DTGOperation op, final CompletableFuture<Boolean> future, final boolean isSuccess,
                                      int retriesLeft, final Errors lastCause){
        //System.out.println(System.currentTimeMillis() + "  internalSecondCommit : " + op.getTxId() + ",    " + region.getId());
        final RetryRunner retryRunner = retryCause -> internalSecondCommit(op, future, isSuccess, retriesLeft - 1, retryCause);
        final FailoverClosureImpl<Boolean> closure = new FailoverClosureImpl<Boolean>(future, retriesLeft, retryRunner, DTGConstants.RETRIYRUNNERWAIT);
        long startVersion = this.versionMap.get(op.getTxId());
        if(!isSuccess){
            changeStatus(op.getMainRegionId(), DTGConstants.TXROLLBACK, startVersion, op.getTxId());
        }else{
            changeStatus(op.getMainRegionId(), DTGConstants.TXSECONDSTART, startVersion, op.getTxId());
        }
        this.rawStore.commitSuccess(op, new BaseStoreClosure() {
            @Override
            public void run(final Status status) {
            if (status.isOk()) {
                changeStatus(op.getMainRegionId(), DTGConstants.TXSUCCESS, startVersion, op.getTxId());
                future.complete(true);
                closure.setData(true);
                closure.run(Status.OK());
                txStatus.remove(op.getTxId());//System.out.println("all success :" + op.getTxId());
            } else {
                System.out.println("internalSecondCommit failed commit..:" + op.getTxId());
                closure.setError(Errors.TRANSACTION_SECOND_ERROR);
                closure.setData(false);
                closure.run(new Status(TxError.FAILED.getNumber(), "can not run transaction"));
            }
            }
        }, region);
        cleanRegionProcess(op.getTxId(), startVersion);
    }

    @Override
    public void handleMergeRequest(MergeRequest request, RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {

    }

    @Override
    public void handleRangeSplitRequest(RangeSplitRequest request, RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {

    }

    public void handleFirstPhaseSuccessRequest(final FirstPhaseSuccessRequest request,
                                               final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure){
        //System.out.println("handleFirstPhaseSuccessRequest "  + request.getTxId() + ",   "+ region.getId());
        final FirstPhaseSuccessResponse response = new FirstPhaseSuccessResponse();
        try {
            String txId = request.getTxId();
            int repeate= request.getRepeate();
            processFirstPhaseSuccess(new TxRepeateVersion(txId, repeate), request.getSelfRegionId(), request.isSuccess());
            closure.sendResponse(response);
        } catch (final Throwable t) {
            System.out.println("failed request lock ..");
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setValue(false);
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

    private boolean processFirstPhaseSuccess(TxRepeateVersion txRepeateVersion, long regionId, boolean isSuccess){
        //System.out.println(System.currentTimeMillis() + "  processFirstPhaseSuccess : " + txRepeateVersion.toString() + ",    " + region.getId());
        String txId = txRepeateVersion.getTxId();
        if(!checkStatus(txId))return true;
        if(!isSuccess){
            this.txStatus.put(txRepeateVersion.getTxId(), DTGConstants.TXROLLBACK);
        }
        List<Long> list = this.lockResultMap.get(txRepeateVersion);
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
            list.add(0, size);
            if(size == 0L){
                secondPhase(txRepeateVersion);
            }
        }
        return true;
    }

//    @Override
//    public void internalFirstPhase(final DTGOperation op, final FailoverClosure closure) {
//        this.region.increaseTransactionCount();
//        Map<Integer, Object> resultMap = new HashMap<>();
//        resultMap.put(-1, true);
//        String txId = op.getTxId();
//        int repeate = 1;
//        List<TxRepeateVersion> list;
//        if(this.txRepeate.containsKey(txId)){
//            list = txRepeate.get(txId);
//            repeate = list.get(0).getRepeate() + 1;
//        }
//        else {
//            list = new ArrayList<>();
//            txRepeate.put(txId, list);
//        }
//        if(txId.substring(0,5).equals("reRun")){
//            byte status = ((DTGMetricsRawStore)this.rawStore).getTxStatus(op.getVersion());
//            if(isExist(status, closure, response, true, op, request)){
//                return;
//            }
//        }
//
//        if (this.txStatus.get(txId) != null){
//            byte status = this.txStatus.get(txId);
//            if(isExist(status, closure, response, false, op, request)){
//                return;
//            }
//        }
//        else{
//            this.txStatus.put(op.getTxId(), DTGConstants.TXRECEIVED);
//            changeStatus(op.getMainRegionId(), DTGConstants.TXRECEIVED, op.getVersion(), op.getTxId());
//        }
//
//        TxRepeateVersion thisRepeate = new TxRepeateVersion(txId, repeate);
//        list.add(thisRepeate);
//        try {
//            FirstPhaseClosure firstClosure = new FirstPhaseClosure() {
//                @Override
//                public void run(Status status) {
//                    if(hasSendRes()){
//                        return;
//                    }
//                    sendResult();
//                    if (status.isOk()) {
//                        if(MVCC){
//                            closure.setData(getData());
//                            closure.run(Status.OK());
//                        }
//                        if(!op.isReadOnly()){
//                            if(!synOp(op, thisRepeate, this)){
//                                cleanMainRegionProcess(txId, op.getVersion());
//                                return;
//                            }
//                        }
//                        txStatus.put(txId, DTGConstants.TXDONEFIRST);
//                        //changeStatus(op.getMainRegionId(), DTGConstants.TXDONEFIRST, op.getVersion(), op.getTxId());
//                        callMainRegion(thisRepeate, op.getMainRegionId(), true, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
//                    } else {
//                        closure.run(new Status(TxError.FAILED.getNumber(), "transaction failed"));
//                        txStatus.put(txId, DTGConstants.TXFAILEDFIRST);
//                        callMainRegion(thisRepeate, op.getMainRegionId(), false, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
//                    }
//                }
//            };
//            this.txStatus.put(op.getTxId(), DTGConstants.TXRECEIVED);
//            if(op.getMainRegionId() == this.region.getId()){
//                mainRegionProcess(op, thisRepeate);
//            }
//            //this.transactionOperationMap.put(op.getTxId(), op);
//            this.rawStore.firstPhaseProcessor(op, firstClosure, this.region);
//        } catch (final Throwable t) {
//            txStatus.put(op.getTxId(), DTGConstants.TXFAILEDFIRST);
//            callMainRegion(thisRepeate, op.getMainRegionId(), false, new CompletableFuture<Boolean>(), DTGConstants.FAILOVERRETRIES, null, false);
//            System.out.println("error!" + region.getId());
//            closure.run(new Status(TxError.FAILED.getNumber(), "transaction failed"));
//        }
//    }

    private boolean checkStatus(String txId){
        return this.txStatus.containsKey(txId);
    }

    private void mainRegionProcess(DTGOperation op, TxRepeateVersion txRepeateVersion){
        if(op.isReadOnly())return;
        if(!checkStatus(op.getTxId()))return;
        List<Long> list = this.lockResultMap.get(txRepeateVersion);
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
        this.lockResultMap.put(txRepeateVersion, list);
        String txId = txRepeateVersion.getTxId();
        //this.versionMap.put(txId, op.getVersion());
        CompletableFuture.runAsync(() -> {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            this.successMap.put(txId, future);
            boolean result = false;
            while(true){
                try{
                    result = FutureHelper.get(future);
                    if(result) {
                        break;
                    }else{
                        reAsk(op, false, txRepeateVersion);
                        future = new CompletableFuture<>();
                    }
                }catch (Exception e){
                    reAsk(op, false, txRepeateVersion);
                    future = new CompletableFuture<>();
                }
            }
        });
    }

    private void cleanRegionProcess(String txId, long version){
        //System.out.println("cleanMainRegionProcess");
        List<TxRepeateVersion> repeateVersions = txRepeate.get(txId);
        this.rawStore.clean(version);
        for(TxRepeateVersion txRepeateVersion : repeateVersions){
            this.lockResultMap.remove(txRepeateVersion);
        }
        if(txResult.containsKey(txId)){
            this.txResult.remove(txId);
        }
        if(versionMap.containsKey(txId)){
            this.versionMap.remove(txId);
        }
        if(successMap.containsKey(txId)){
            this.successMap.get(txId).complete(true);
            this.successMap.remove(txId);
        }
        if(txRepeate.containsKey(txId)){
            this.txRepeate.remove(txId);
        }
        if(versionMap.containsKey(txId)){
            this.versionMap.remove(txId);
        }
        closeTx();
    }

    private static void setFailure(final BaseRequest request, final BaseResponse<?> response, final Status status,
                                   final Errors error) {
        response.setError(error == null ? Errors.STORAGE_ERROR : error);
        LOG.error("Failed to handle: {}, status: {}, error: {}.", request, status, error);
    }

    private void callMainRegion(TxRepeateVersion txRepeateVersion, long regionId, boolean isSuccess,final CompletableFuture<Boolean> future,
                                int retriesLeft, final Errors lastCause, boolean forceRfresh){
        //System.out.println(System.currentTimeMillis() + "  callMainRegion : " + txRepeateVersion.getTxId() + ",   " + region.getId());
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
        //System.out.println(System.currentTimeMillis() + "  internalCallMainRegion : " + txRepeateVersion.getTxId() + ",   " + region.getId());
        if(!checkStatus(txRepeateVersion.getTxId()))return;
        final FirstPhaseSuccessRequest request = new FirstPhaseSuccessRequest();
        request.setSuccess(isSuccess);
        request.setTxId(txRepeateVersion.getTxId());
        request.setRepeate(txRepeateVersion.getRepeate());
        request.setRegionId(regionId);
        request.setSelfRegionId(this.getRegionId());
        dtgRpcService.callAsyncWithRpc(request, closure, lastCause, true);
    }

    private boolean secondPhase(TxRepeateVersion txRepeateVersion) {
//
//        System.out.println("CLOSE NOW!!!!!");
//        try {
//            Thread.sleep(100000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        //System.out.println(System.currentTimeMillis() + "  secondPhase : " + txRepeateVersion.getTxId() + ",   " + region.getId());

        String txId = txRepeateVersion.getTxId();
        if(!checkStatus(txId))return true;
        long startVersion = this.versionMap.get(txId);
        List<Long> list = this.lockResultMap.get(txRepeateVersion);
        Requires.requireTrue(list.remove(0) == 0, "the transaction "+ txId +"have not success");
        //long startVersion = this.versionMap.get(txId);
        Requires.requireNonNull(list, "the region list of transaction is null");
        final List<CompletableFuture<Boolean>> futures = Lists.newArrayListWithCapacity(list.size());
        boolean isSuccess;

        final CompletableFuture<Boolean> logFuture = new CompletableFuture<>();
        if(txStatus.get(txId) == DTGConstants.TXROLLBACK){
            isSuccess = false;
            changeStatus(this.region.getId(), DTGConstants.TXROLLBACK, startVersion, txId);
            //**************************
            return true;
            //delete this to rollback
        }else{
            isSuccess = true;
            changeStatus(this.region.getId(), DTGConstants.TXSECONDSTART, startVersion, txId);
        }
        long endVersion = 0;

        try {
            endVersion = getEndVersion();
        } catch (TransactionException e) {
            return false;
        }

        for(long regionId : list){
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            Requires.requireTrue(regionId < 0, "the region " + regionId + "haven't success!");
            callRegion(txId, isSuccess, -regionId, endVersion, future, DTGConstants.FAILOVERRETRIES, null, false);
            futures.add(future);
        }
        FutureGroup<Boolean> group = new FutureGroup<Boolean>(futures);
        boolean res = FutureHelper.get(FutureHelper.joinBooleans(group), Long.MAX_VALUE);
        if(res){
            final CompletableFuture<Boolean> logFuture2 = new CompletableFuture<>();
            changeStatus(this.region.getId(), DTGConstants.TXSUCCESS, startVersion, txId);
        }
        return true;
    }

    private void  changeStatus(long mainRegion, byte status, long version, String txId){
        final StatusLock lock = new StatusLock();
        LogStoreClosure logClosure = new LogStoreClosure() {
            @Override
            public void run(Status status) {
                if(status.isOk()){
                    //future.complete(true);
                }else{
                    //future.complete(false);
                }
            }
        };
        logClosure.setLog(new TransactionLog(txId, false));
        logClosure.setVersion(version);
        ((DTGMetricsRawStore)rawStore).changeStatus(logClosure, status, mainRegion);
    }

    private long getEndVersion() throws TransactionException {
        long version;
        CompletableFuture<Long> future = new CompletableFuture();
        versionManager.applyRequestVersion(future);
        version = FutureHelper.get(future);
        if(version == -1){
            throw new TransactionException("get an error version!");
        }
        return version;
    }

    private void callRegion(String txId, boolean isSuccess, long regionId, long version, final CompletableFuture<Boolean> future,
                            int retriesLeft, final Errors lastCause, boolean forceRfresh){
        //System.out.println("callRegion : " + regionId);
        final RetryRunner retryRunner = retryCause -> callRegion(txId, isSuccess, regionId, version, future, retriesLeft - 1,
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
        //System.out.println("internalCallRegion 1---- " + txId + ",  " + regionId);
//        if(!checkStatus(txId)){
//            closure.setData(true);
//            closure.run(Status.OK());
//            return;
//        }
        final SecondPhaseRequest request = new SecondPhaseRequest();
        final DTGOperation op = new DTGOperation();
        if(isSuccess){
            op.setType(OperationName.COMMITTRANS);
        }
        else {
            op.setType(OperationName.ROLLBACK);
        }
        op.setVersion(version);
        op.setTxId(txId);
        request.setTxId(txId);
        request.setRegionId(regionId);
        op.setMainRegionId(this.region.getId());
        request.setVersion(version);
        request.setSuccess(isSuccess);
        request.setDTGOpreration(op);
        request.setRequireTxNotNull(requireTxNotNull);
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
        for(DTGRegion re : map.keySet()){
            if(regionIds.contains(re.getId())){
                reCallRegion(map.get(re), re, op.getMainRegionId(), txRepeateVersion, fromClient,
                        op.getVersion(), newRegionIds, op.isReadOnly(), op.getAllEntityEntries(), DTGConstants.FAILOVERRETRIES);
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
                              long version, List<Long> regionIds, boolean isReadOnly, List<EntityEntry> allEntries, int retriesLeft){
        if(!checkStatus(txRepeateVersion.getTxId()))return;
        DTGSaveStore saveStore = this.regionEngine.getStoreEngine().getSaveStore();

        CompletableFuture<Boolean> future1 = new CompletableFuture();
        final RetryRunner retryRunner = retryCause -> reCallRegion(entries, target, mainRegionId, txRepeateVersion, fromClient,
                version, regionIds, isReadOnly, allEntries, retriesLeft-1);
        final FailoverClosure<Boolean> closure = new FailoverClosureImpl<Boolean>(future1, retriesLeft, retryRunner, DTGConstants.RETRIYRUNNERWAIT);

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
        saveStore.internalRegionPut(region, op, fromClient,txRepeateVersion.getRepeate(), future, 0, Errors.NOT_LEADER);
        try {
            closure.setData(true);
            closure.run(Status.OK());
            FutureHelper.get(future);
        }catch (Exception e){
            closure.setData(false);
            closure.run(new Status(-1, "reCall transaction failed, transaction op id: %s", op.getTxId()));
        }
    }

    private boolean synOp(DTGOperation op, TxRepeateVersion txRepeateVersion, final FirstPhaseClosure firstClosure){

        //暂时取消副本
        if(true){
            return true;
        }

        if(!checkStatus(op.getTxId()))return false;
        final CompletableFuture<Boolean> future1 = new CompletableFuture<>();
        this.rawStore.ApplyEntityEntries(op, new BaseStoreClosure() {
            @Override
            public void run(final Status status) {
                if (status.isOk()) {
                    firstClosure.setData(getData());
                    future1.complete(true);
                } else {
                    System.out.println("synOp error : " + status.getErrorMsg() + ", tx status = " + txStatus.get(op.getTxId()));
                    retryWait();
                    txStatus.put(op.getTxId(), DTGConstants.SYNOP);
                    reAsk(op, true, txRepeateVersion);
                    txStatus.put(op.getTxId(), DTGConstants.SYNOPFAILED);
                    future1.complete(false);
                }
            }
        });
        return FutureHelper.get(future1, Long.MAX_VALUE);
    }

    private boolean isLeader(){
        return this.regionEngine.getFsm().isLeader();
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
        public int hashCode(){
            int result = txId != null ? txId.hashCode() : 0;
            result = 31 * result + repeate;
            return result;
        }

        @Override
        public String toString() {
            return this.txId + ", repeate = " + repeate;
        }
    }

    class StatusLock{
        private boolean hasSave = false;

        public boolean isHasSave() {
            return hasSave;
        }

        public void setHasSave() {
            this.hasSave = true;
        }
    }
}

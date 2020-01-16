package Region;

import Communication.RequestAndResponse.*;
import Element.DTGOperation;
import Element.EntityEntry;
import LocalDBMachine.LocalDB;
import LocalDBMachine.LocalTransaction;
import LocalDBMachine.LocalTx.TransactionThreadLock;
import UserClient.Transaction.TransactionLog;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.RequestProcessClosure;
import com.alipay.sofa.jraft.rhea.client.FutureHelper;
import com.alipay.sofa.jraft.rhea.cmd.store.*;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;
import com.alipay.sofa.jraft.rhea.util.KVParameterRequires;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.util.Requires;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.BaseStoreClosure;
import raft.DTGRawStore;
import raft.LogStoreClosure;
import tool.ObjectAndByte;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

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

    public DTGRegionService(DTGRegionEngine regionEngine){
        this.regionEngine = regionEngine;
        this.rawStore = regionEngine.getMetricsRawStore();
        this.localdb = this.regionEngine.getStoreEngine().getlocalDB();
        this.region = regionEngine.getRegion();
    }


    @Override
    public long getRegionId() {
        return this.regionEngine.getRegion().getId();
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

    @Override
    public void handleFirstPhase(FirstPhaseRequest request, RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        this.region.increaseTransactionCount();
        final FirstPhaseResponse response = new FirstPhaseResponse();
        response.setRegionId(getRegionId());
        response.setRegionEpoch(getRegionEpoch());
        Map<Integer, Object> resultMap = new HashMap<>();
        resultMap.put(-1, true);
        try {//System.out.println("run op... ：1  " +region.getId());
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

            CompletableFuture.runAsync(() -> {
                this.rawStore.ApplyEntityEntries(op, new BaseStoreClosure() {
                    @Override
                    public void run(final Status status) {
                        System.out.println("run closure");
                        if (status.isOk()) {
                            response.setValue(toByteArray(resultMap));
                            System.out.println("finish first phase");
                            //System.out.println(resultMap.size());
                        } else {
                            response.setValue(ObjectAndByte.toByteArray(resultMap));
                            System.out.println("error!" + region.getId());
                            setFailure(request, response, status, getError());
                        }
                        //System.out.println("response request" + region.getId());
                        closure.sendResponse(response);
                    }
                });
            });


        } catch (final Throwable t) {
            System.out.println("error!" + region.getId());
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setValue(ObjectAndByte.toByteArray(resultMap));
            response.setError(Errors.forException(t));//System.out.println("error response request" + region.getId());
            closure.sendResponse(response);
        }
    }

    @Override
    public void handleSecondPhase(SecondPhaseRequest request, RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
        final SecondPhaseResponse response = new SecondPhaseResponse();
        response.setRegionId(getRegionId());
        response.setRegionEpoch(getRegionEpoch());
        try {
            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
            final DTGOperation op = KVParameterRequires
                    .requireNonNull(request.getDTGOpreration(), "put.DTGOperation");
            this.rawStore.ApplyEntityEntries(op, new BaseStoreClosure() {
                @Override
                public void run(final Status status) {
                    if (status.isOk()) {
                        //System.out.println("success commit..:" + op.getTxId());
                        System.out.println("second first phase");
                        response.setValue((Boolean) getData());
                    } else {
                        System.out.println("failed commit..:" + op.getTxId());
                        response.setValue(false);
                        setFailure(request, response, status, getError());
                    }
                    System.out.println("return commit..");
                    closure.sendResponse(response);
                }
            });
        } catch (final Throwable t) {
            System.out.println("failed commit..");
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setValue(false);
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
        //System.out.println("finish commit request");
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

//    public void handlePutRequest(final PutRequest request,
//                                 final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
//        final PutResponse response = new PutResponse();
//        response.setRegionId(getRegionId());
//        response.setRegionEpoch(getRegionEpoch());
//        try {
//            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
//            final byte[] key = KVParameterRequires.requireNonNull(request.getKey(), "put.key");
//            final byte[] value = KVParameterRequires.requireNonNull(request.getValue(), "put.value");
//            this.rawKVStore.put(key, value, new BaseKVStoreClosure() {
//
//                @Override
//                public void run(final Status status) {
//                    if (status.isOk()) {
//                        response.setValue((Boolean) getData());
//                    } else {
//                        setFailure(request, response, status, getError());
//                    }
//                    closure.sendResponse(response);
//                }
//            });
//        } catch (final Throwable t) {
//            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
//            response.setError(Errors.forException(t));
//            closure.sendResponse(response);
//        }
//    }

//    public void handleBatchPutRequest(final BatchPutRequest request,
//                                      final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
//        final BatchPutResponse response = new BatchPutResponse();
//        response.setRegionId(getRegionId());
//        response.setRegionEpoch(getRegionEpoch());
//        try {
//            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
//            final List<KVEntry> kvEntries = KVParameterRequires
//                    .requireNonEmpty(request.getKvEntries(), "put.kvEntries");
//            this.rawKVStore.put(kvEntries, new BaseKVStoreClosure() {
//
//                @Override
//                public void run(final Status status) {
//                    if (status.isOk()) {
//                        response.setValue((Boolean) getData());
//                    } else {
//                        setFailure(request, response, status, getError());
//                    }
//                    closure.sendResponse(response);
//                }
//            });
//        } catch (final Throwable t) {
//            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
//            response.setError(Errors.forException(t));
//            closure.sendResponse(response);
//        }
//    }
//
//    public void handlePutIfAbsentRequest(final PutIfAbsentRequest request,
//                                         final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
//        final PutIfAbsentResponse response = new PutIfAbsentResponse();
//        response.setRegionId(getRegionId());
//        response.setRegionEpoch(getRegionEpoch());
//        try {
//            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
//            final byte[] key = KVParameterRequires.requireNonNull(request.getKey(), "putIfAbsent.key");
//            final byte[] value = KVParameterRequires.requireNonNull(request.getValue(), "putIfAbsent.value");
//            this.rawKVStore.putIfAbsent(key, value, new BaseKVStoreClosure() {
//
//                @Override
//                public void run(final Status status) {
//                    if (status.isOk()) {
//                        response.setValue((byte[]) getData());
//                    } else {
//                        setFailure(request, response, status, getError());
//                    }
//                    closure.sendResponse(response);
//                }
//            });
//        } catch (final Throwable t) {
//            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
//            response.setError(Errors.forException(t));
//            closure.sendResponse(response);
//        }
//    }
//
//    public void handleGetAndPutRequest(final GetAndPutRequest request,
//                                       final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
//        final GetAndPutResponse response = new GetAndPutResponse();
//        response.setRegionId(getRegionId());
//        response.setRegionEpoch(getRegionEpoch());
//        try {
//            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
//            final byte[] key = KVParameterRequires.requireNonNull(request.getKey(), "getAndPut.key");
//            final byte[] value = KVParameterRequires.requireNonNull(request.getValue(), "getAndPut.value");
//            this.rawKVStore.getAndPut(key, value, new BaseKVStoreClosure() {
//
//                @Override
//                public void run(final Status status) {
//                    if (status.isOk()) {
//                        response.setValue((byte[]) getData());
//                    } else {
//                        setFailure(request, response, status, getError());
//                    }
//                    closure.sendResponse(response);
//                }
//            });
//        } catch (final Throwable t) {
//            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
//            response.setError(Errors.forException(t));
//            closure.sendResponse(response);
//        }
//    }

//    public void handleCompareAndPutRequest(final CompareAndPutRequest request,
//                                           final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
//        final CompareAndPutResponse response = new CompareAndPutResponse();
//        response.setRegionId(getRegionId());
//        response.setRegionEpoch(getRegionEpoch());
//        try {
//            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
//            final byte[] key = KVParameterRequires.requireNonNull(request.getKey(), "compareAndPut.key");
//            final byte[] expect = KVParameterRequires.requireNonNull(request.getExpect(), "compareAndPut.expect");
//            final byte[] update = KVParameterRequires.requireNonNull(request.getUpdate(), "compareAndPut.update");
//            this.rawKVStore.compareAndPut(key, expect, update, new BaseKVStoreClosure() {
//
//                @Override
//                public void run(final Status status) {
//                    if (status.isOk()) {
//                        response.setValue((Boolean) getData());
//                    } else {
//                        setFailure(request, response, status, getError());
//                    }
//                    closure.sendResponse(response);
//                }
//            });
//        } catch (final Throwable t) {
//            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
//            response.setError(Errors.forException(t));
//            closure.sendResponse(response);
//        }
//    }
//
//    public void handleDeleteRequest(final DeleteRequest request,
//                                    final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
//        final DeleteResponse response = new DeleteResponse();
//        response.setRegionId(getRegionId());
//        response.setRegionEpoch(getRegionEpoch());
//        try {
//            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
//            final byte[] key = KVParameterRequires.requireNonNull(request.getKey(), "delete.key");
//            this.rawKVStore.delete(key, new BaseKVStoreClosure() {
//
//                @Override
//                public void run(final Status status) {
//                    if (status.isOk()) {
//                        response.setValue((Boolean) getData());
//                    } else {
//                        setFailure(request, response, status, getError());
//                    }
//                    closure.sendResponse(response);
//                }
//            });
//        } catch (final Throwable t) {
//            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
//            response.setError(Errors.forException(t));
//            closure.sendResponse(response);
//        }
//    }
//
//    public void handleDeleteRangeRequest(final DeleteRangeRequest request,
//                                         final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
//        final DeleteRangeResponse response = new DeleteRangeResponse();
//        response.setRegionId(getRegionId());
//        response.setRegionEpoch(getRegionEpoch());
//        try {
//            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
//            final byte[] startKey = KVParameterRequires.requireNonNull(request.getStartKey(), "deleteRange.startKey");
//            final byte[] endKey = KVParameterRequires.requireNonNull(request.getEndKey(), "deleteRange.endKey");
//            this.rawKVStore.deleteRange(startKey, endKey, new BaseKVStoreClosure() {
//
//                @Override
//                public void run(final Status status) {
//                    if (status.isOk()) {
//                        response.setValue((Boolean) getData());
//                    } else {
//                        setFailure(request, response, status, getError());
//                    }
//                    closure.sendResponse(response);
//                }
//            });
//        } catch (final Throwable t) {
//            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
//            response.setError(Errors.forException(t));
//            closure.sendResponse(response);
//        }
//    }
//
//    public void handleBatchDeleteRequest(final BatchDeleteRequest request,
//                                         final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
//        final BatchDeleteResponse response = new BatchDeleteResponse();
//        response.setRegionId(getRegionId());
//        response.setRegionEpoch(getRegionEpoch());
//        try {
//            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
//            final List<byte[]> keys = KVParameterRequires.requireNonEmpty(request.getKeys(), "delete.keys");
//            this.rawKVStore.delete(keys, new BaseKVStoreClosure() {
//
//                @Override
//                public void run(final Status status) {
//                    if (status.isOk()) {
//                        response.setValue((Boolean) getData());
//                    } else {
//                        setFailure(request, response, status, getError());
//                    }
//                    closure.sendResponse(response);
//                }
//            });
//        } catch (final Throwable t) {
//            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
//            response.setError(Errors.forException(t));
//            closure.sendResponse(response);
//        }
//    }

//    public void handleMergeRequest(final MergeRequest request,
//                                   final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
//        final MergeResponse response = new MergeResponse();
//        response.setRegionId(getRegionId());
//        response.setRegionEpoch(getRegionEpoch());
//        try {
//            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
//            final byte[] key = KVParameterRequires.requireNonNull(request.getKey(), "merge.key");
//            final byte[] value = KVParameterRequires.requireNonNull(request.getValue(), "merge.value");
//            this.rawKVStore.merge(key, value, new BaseKVStoreClosure() {
//
//                @Override
//                public void run(final Status status) {
//                    if (status.isOk()) {
//                        response.setValue((Boolean) getData());
//                    } else {
//                        setFailure(request, response, status, getError());
//                    }
//                    closure.sendResponse(response);
//                }
//            });
//        } catch (final Throwable t) {
//            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
//            response.setError(Errors.forException(t));
//            closure.sendResponse(response);
//        }
//    }
//
//    public void handleGetRequest(final GetRequest request,
//                                 final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
//        final GetResponse response = new GetResponse();
//        response.setRegionId(getRegionId());
//        response.setRegionEpoch(getRegionEpoch());
//        try {
//            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
//            final byte[] key = KVParameterRequires.requireNonNull(request.getKey(), "get.key");
//            this.rawKVStore.get(key, request.isReadOnlySafe(), new BaseKVStoreClosure() {
//
//                @Override
//                public void run(final Status status) {
//                    if (status.isOk()) {
//                        response.setValue((byte[]) getData());
//                    } else {
//                        setFailure(request, response, status, getError());
//                    }
//                    closure.sendResponse(response);
//                }
//            });
//        } catch (final Throwable t) {
//            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
//            response.setError(Errors.forException(t));
//            closure.sendResponse(response);
//        }
//    }
//
//    public void handleMultiGetRequest(final MultiGetRequest request,
//                                      final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
//        final MultiGetResponse response = new MultiGetResponse();
//        response.setRegionId(getRegionId());
//        response.setRegionEpoch(getRegionEpoch());
//        try {
//            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
//            final List<byte[]> keys = KVParameterRequires.requireNonEmpty(request.getKeys(), "multiGet.keys");
//            this.rawKVStore.multiGet(keys, request.isReadOnlySafe(), new BaseKVStoreClosure() {
//
//                @SuppressWarnings("unchecked")
//                @Override
//                public void run(final Status status) {
//                    if (status.isOk()) {
//                        response.setValue((Map<ByteArray, byte[]>) getData());
//                    } else {
//                        setFailure(request, response, status, getError());
//                    }
//                    closure.sendResponse(response);
//                }
//            });
//        } catch (final Throwable t) {
//            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
//            response.setError(Errors.forException(t));
//            closure.sendResponse(response);
//        }
//    }
//
//    public void handleScanRequest(final ScanRequest request,
//                                  final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
//        final ScanResponse response = new ScanResponse();
//        response.setRegionId(getRegionId());
//        response.setRegionEpoch(getRegionEpoch());
//        try {
//            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
//            this.rawKVStore.scan(request.getStartKey(), request.getEndKey(), request.getLimit(),
//                    request.isReadOnlySafe(), request.isReturnValue(), new BaseKVStoreClosure() {
//
//                        @SuppressWarnings("unchecked")
//                        @Override
//                        public void run(final Status status) {
//                            if (status.isOk()) {
//                                response.setValue((List<KVEntry>) getData());
//                            } else {
//                                setFailure(request, response, status, getError());
//                            }
//                            closure.sendResponse(response);
//                        }
//                    });
//        } catch (final Throwable t) {
//            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
//            response.setError(Errors.forException(t));
//            closure.sendResponse(response);
//        }
//    }
//
//    public void handleGetSequence(final GetSequenceRequest request,
//                                  final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
//        final GetSequenceResponse response = new GetSequenceResponse();
//        response.setRegionId(getRegionId());
//        response.setRegionEpoch(getRegionEpoch());
//        try {
//            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
//            final byte[] seqKey = KVParameterRequires.requireNonNull(request.getSeqKey(), "sequence.seqKey");
//            final int step = KVParameterRequires.requireNonNegative(request.getStep(), "sequence.step");
//            this.rawKVStore.getSequence(seqKey, step, new BaseKVStoreClosure() {
//
//                @Override
//                public void run(final Status status) {
//                    if (status.isOk()) {
//                        response.setValue((Sequence) getData());
//                    } else {
//                        setFailure(request, response, status, getError());
//                    }
//                    closure.sendResponse(response);
//                }
//            });
//        } catch (final Throwable t) {
//            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
//            response.setError(Errors.forException(t));
//            closure.sendResponse(response);
//        }
//    }
//
//    public void handleResetSequence(final ResetSequenceRequest request,
//                                    final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
//        final ResetSequenceResponse response = new ResetSequenceResponse();
//        response.setRegionId(getRegionId());
//        response.setRegionEpoch(getRegionEpoch());
//        try {
//            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
//            final byte[] seqKey = KVParameterRequires.requireNonNull(request.getSeqKey(), "sequence.seqKey");
//            this.rawKVStore.resetSequence(seqKey, new BaseKVStoreClosure() {
//
//                @Override
//                public void run(final Status status) {
//                    if (status.isOk()) {
//                        response.setValue((Boolean) getData());
//                    } else {
//                        setFailure(request, response, status, getError());
//                    }
//                    closure.sendResponse(response);
//                }
//            });
//        } catch (final Throwable t) {
//            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
//            response.setError(Errors.forException(t));
//            closure.sendResponse(response);
//        }
//    }
//
//    public void handleKeyLockRequest(final KeyLockRequest request,
//                                     final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
//        final KeyLockResponse response = new KeyLockResponse();
//        response.setRegionId(getRegionId());
//        response.setRegionEpoch(getRegionEpoch());
//        try {
//            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
//            final byte[] key = KVParameterRequires.requireNonNull(request.getKey(), "lock.key");
//            final byte[] fencingKey = this.regionEngine.getRegion().getStartKey();
//            final DistributedLock.Acquirer acquirer = KVParameterRequires.requireNonNull(request.getAcquirer(),
//                    "lock.acquirer");
//            KVParameterRequires.requireNonNull(acquirer.getId(), "lock.id");
//            KVParameterRequires.requirePositive(acquirer.getLeaseMillis(), "lock.leaseMillis");
//            this.rawKVStore.tryLockWith(key, fencingKey, request.isKeepLease(), acquirer, new BaseKVStoreClosure() {
//
//                @Override
//                public void run(final Status status) {
//                    if (status.isOk()) {
//                        response.setValue((DistributedLock.Owner) getData());
//                    } else {
//                        setFailure(request, response, status, getError());
//                    }
//                    closure.sendResponse(response);
//                }
//            });
//        } catch (final Throwable t) {
//            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
//            response.setError(Errors.forException(t));
//            closure.sendResponse(response);
//        }
//    }
//
//    public void handleKeyUnlockRequest(final KeyUnlockRequest request,
//                                       final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
//        final KeyUnlockResponse response = new KeyUnlockResponse();
//        response.setRegionId(getRegionId());
//        response.setRegionEpoch(getRegionEpoch());
//        try {
//            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
//            final byte[] key = KVParameterRequires.requireNonNull(request.getKey(), "unlock.key");
//            final DistributedLock.Acquirer acquirer = KVParameterRequires.requireNonNull(request.getAcquirer(),
//                    "lock.acquirer");
//            KVParameterRequires.requireNonNull(acquirer.getId(), "lock.id");
//            this.rawKVStore.releaseLockWith(key, acquirer, new BaseKVStoreClosure() {
//
//                @Override
//                public void run(final Status status) {
//                    if (status.isOk()) {
//                        response.setValue((DistributedLock.Owner) getData());
//                    } else {
//                        setFailure(request, response, status, getError());
//                    }
//                    closure.sendResponse(response);
//                }
//            });
//        } catch (final Throwable t) {
//            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
//            response.setError(Errors.forException(t));
//            closure.sendResponse(response);
//        }
//    }
//
//    public void handleNodeExecuteRequest(final NodeExecuteRequest request,
//                                         final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
//        final NodeExecuteResponse response = new NodeExecuteResponse();
//        response.setRegionId(getRegionId());
//        response.setRegionEpoch(getRegionEpoch());
//        try {
//            KVParameterRequires.requireSameEpoch(request, getRegionEpoch());
//            final NodeExecutor executor = KVParameterRequires
//                    .requireNonNull(request.getNodeExecutor(), "node.executor");
//            this.rawKVStore.execute(executor, true, new BaseKVStoreClosure() {
//
//                @Override
//                public void run(final Status status) {
//                    if (status.isOk()) {
//                        response.setValue((Boolean) getData());
//                    } else {
//                        setFailure(request, response, status, getError());
//                    }
//                    closure.sendResponse(response);
//                }
//            });
//        } catch (final Throwable t) {
//            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
//            response.setError(Errors.forException(t));
//            closure.sendResponse(response);
//        }
//    }
//
//    public void handleRangeSplitRequest(final RangeSplitRequest request,
//                                        final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure) {
//        final RangeSplitResponse response = new RangeSplitResponse();
//        response.setRegionId(getRegionId());
//        response.setRegionEpoch(getRegionEpoch());
//        try {
//            // do not need to check the region epoch
//            final Long newRegionId = KVParameterRequires.requireNonNull(request.getNewRegionId(),
//                    "rangeSplit.newRegionId");
//            this.regionEngine.getStoreEngine().applySplit(request.getRegionId(), newRegionId, new BaseKVStoreClosure() {
//
//                @Override
//                public void run(final Status status) {
//                    if (status.isOk()) {
//                        response.setValue((Boolean) getData());
//                    } else {
//                        setFailure(request, response, status, getError());
//                    }
//                    closure.sendResponse(response);
//                }
//            });
//        } catch (final Throwable t) {
//            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
//            response.setError(Errors.forException(t));
//            closure.sendResponse(response);
//        }
//    }

    public Object getObject(Map resultMap, EntityEntry entry){
        return resultMap.get(entry.getParaId());
    }

    private static void setFailure(final BaseRequest request, final BaseResponse<?> response, final Status status,
                                   final Errors error) {
        response.setError(error == null ? Errors.STORAGE_ERROR : error);
        LOG.error("Failed to handle: {}, status: {}, error: {}.", request, status, error);
    }
}

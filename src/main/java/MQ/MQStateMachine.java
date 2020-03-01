//package MQ;
//import DBExceptions.TxMQError;
//import DBExceptions.TypeDoesnotExistException;
//import Element.DTGOperation;
//import Element.EntityEntry;
//import Element.OperationName;
//import PlacementDriver.DTGPlacementDriverClient;
//import PlacementDriver.DefaultPlacementDriverClient;
//import Region.DTGRegion;
//import Region.DTGRegionEngine;
//import UserClient.DTGSaveStore;
//import UserClient.Transaction.TransactionLog;
//import com.alipay.sofa.jraft.Iterator;
//import com.alipay.sofa.jraft.Lifecycle;
//import com.alipay.sofa.jraft.Status;
//import com.alipay.sofa.jraft.rhea.client.FutureGroup;
//import com.alipay.sofa.jraft.rhea.client.FutureHelper;
//import com.alipay.sofa.jraft.rhea.client.failover.RetryRunner;
//import com.alipay.sofa.jraft.rhea.errors.ApiExceptionHelper;
//import com.alipay.sofa.jraft.rhea.errors.Errors;
//import com.alipay.sofa.jraft.rhea.util.Lists;
//import com.alipay.sofa.jraft.util.Requires;
//import config.DTGConstants;
//import options.MQStateMachineOptions;
//import raft.FailoverClosure;
//import raft.FailoverClosureImpl;
//import tool.ObjectAndByte;
//
//import java.util.*;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.TimeoutException;
//
//import static config.MainType.*;
//import static config.MainType.TEMPORALPROPERTYTYPE;
//import static tool.ObjectAndByte.toObject;
//
//
///**
// * @author :jinkai
// * @date :Created in 2020/1/4 16:40
// * @description：
// * @modified By:
// * @version:
// */
//
//public class MQStateMachine implements Lifecycle<DTGSaveStore> {
//
//    private DTGSaveStore saveStore;
//
//    @Override
//    public boolean init(DTGSaveStore saveStore) {
//        this.saveStore = saveStore;
//        Requires.requireNonNull(saveStore, "DTGSaveStore is null");
//        return true;
//    }
//
//    public void onApply(List<DTGMQ.RunClosure> tasks) {
//        System.out.println("on apply");
//        for(DTGMQ.RunClosure task : tasks){
//            System.out.println("task size:" + tasks.size());
//            DTGMQ.InternalRunClosure closure = (DTGMQ.InternalRunClosure)task.done;
//            closure.run(Status.OK());
////            CompletableFuture.runAsync(() -> {
////                System.out.println("run task:");
////                TransactionLog log = (TransactionLog)ObjectAndByte.toObject(closure.getLog().getData().array());
////                if(firstPhaseCommit(log) != null){
////                    if(secondPhase(log.getTxId())){
////                        closure.run(Status.OK());
////                    }else {
////                        Status status = new Status(TxMQError.SCEONDPHASEERROR.getNumber(), "second phase error");
////                        closure.run(status);
////                    }
////                }
////            });
//        }
//    }
//
//    private Map<Integer, Object> firstPhaseCommit(TransactionLog log){
//        System.out.println("first phase");
//
//        Map<Integer, Object> result =  saveStore.applyRequest(log.getOps(), log.getTxId(), DTGConstants.FAILOVERRETRIES, null, true);
//        return result;
//    }
//
//    private boolean secondPhase(String txId){
//        System.out.println("second phase");
//        return saveStore.applyCommitRequest(txId, true, DTGConstants.FAILOVERRETRIES);
//    }
//
//    @Override
//    public void shutdown() {
//
//    }
//}

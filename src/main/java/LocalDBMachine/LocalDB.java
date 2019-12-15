package LocalDBMachine;

import Element.DTGOperation;
import Element.OperationName;
import LocalDBMachine.LocalTx.TransactionThreadLock;
import Region.DTGRegion;
import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.client.FutureHelper;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import raft.EntityStoreClosure;
import raft.DTGRawStore;
import scala.collection.Iterator;
import options.LocalDBOption;

import java.util.HashMap;
import java.util.Map;

/**
 * @author :jinkai
 * @date :Created in 2019/10/22 9:37
 * @description:
 * @modified By:
 * @version:
 */

public class LocalDB implements DTGRawStore, Lifecycle<LocalDBOption> {

//    private Map<String, LocalTransaction> waitCommitMap;
    private Map<String, TransactionThreadLock> waitCommitMap;
    private GraphDatabaseService db;


    @Override
    public boolean init(LocalDBOption opts) {
        this.waitCommitMap = new HashMap<>();
        db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(opts.getDbPath() )
                .loadPropertiesFromFile("")
                .newGraphDatabase();
        return true;
    }

    public void shutdown(){
        db.shutdown();
    }

    public GraphDatabaseService getDb() {
        return db;
    }

    @Override
    public Iterator localIterator() {
        return null;
    }

    @Override
    public void ApplyEntityEntries(DTGOperation op, EntityStoreClosure closure) {
        //runOp(op, closure);
    }

    @Override
    public void readOnlyEntityEntries(DTGOperation op, EntityStoreClosure closure) {

    }

    @Override
    public void merge() {

    }

    @Override
    public void split() {

    }

    public void runOp(DTGOperation op, EntityStoreClosure closure, boolean isLeader, DTGRegion region){
        switch (op.getType()){
            case OperationName.TRANSACTIONOP:{
                Map<Integer, Object> resultMap = new HashMap<>();
                try {
                    if(!isLeader){
                        //System.out.println("start get transaction");
                        TransactionThreadLock txLock = new TransactionThreadLock(op.getTxId());
                        LocalTransaction tx = new LocalTransaction(this.db, op, resultMap, txLock, region);
                        tx.start();
                        synchronized (resultMap){
                            resultMap.wait(FutureHelper.DEFAULT_TIMEOUT_MILLIS);
                        }
                        this.addToCommitMap(txLock, op.getTxId());
                        //LocalTransaction tx = getTransaction(op, resultMap);
//                        LocalTransaction tx = new ExecuteTransactionOp().getTransaction(this.db, op, resultMap);
//                        System.out.println("finish get transaction");
//                        addToCommitMap(tx, op.getTxId());
                    }
                    if(closure != null){
                        closure.setData(resultMap);
                        closure.run(Status.OK());
                    }
                } catch (Throwable throwable) {
                    if(closure != null){
                        closure.setError(Errors.forException(throwable));
                        closure.run(new Status(-1, "request lock failed, transaction op id: %s", op.getTxId()));
                    }
                }
                break;
            }
            case OperationName.COMMITTRANS:{
                try {
                    //System.out.println("start commit");
                    commitTx(op.getTxId());
                    //System.out.println("finish commit");
                    if(closure != null){
                        closure.setData(true);
                        closure.run(Status.OK());
                    }
                }catch (Throwable e){
                    if(closure != null){
                        System.out.println("commit error");
                        closure.setData(false);
                        closure.setError(Errors.forException(e));
                        closure.run(new Status(-1, "request lock failed, transaction op id: %s", op.getTxId()));
                    }
                }
                break;
            }
            case OperationName.ROLLBACK:{
                try {
                    removeTx(op.getTxId());
                    if(closure != null){
                        closure.setData(true);
                        closure.run(Status.OK());
                    }
                }catch (Throwable e){
                    if(closure != null){
                        closure.setData(false);
                        closure.setError(Errors.forException(e));
                        closure.run(new Status(-1, "request lock failed, transaction op id: %s", op.getTxId()));
                    }
                }
                break;
            }
        }
    }

//    public void addToCommitMap(LocalTransaction tx, String id){
//        System.out.println("wait commit, id : " + id);
//        if(waitCommitMap.containsKey(id)){
//            removeTx(id);
//        }
//        waitCommitMap.put(id, tx);
//    }

    public void addToCommitMap(TransactionThreadLock lock, String id) throws InterruptedException {
        //System.out.println("wait commit, id : " + id);
        if(waitCommitMap.containsKey(id)){
            removeTx(id);//System.out.println("remove commit, id : " + id);
        }
        waitCommitMap.put(id, lock);//System.out.println("put commit, id : " + id);
    }

    public void commitTx(String id) throws InterruptedException {
        TransactionThreadLock lock = waitCommitMap.get(id);
        //System.out.println(id);
        synchronized (lock){
            lock.commit();
            lock.notify();
        }
        synchronized (lock.getCommitLock()){
            lock.getCommitLock().wait();
        }
        waitCommitMap.remove(id);//System.out.println("remove commit, id : " + id);
    }

    public void removeTx(String id) throws InterruptedException {
        TransactionThreadLock lock = waitCommitMap.get(id);
        synchronized (lock){
            lock.rollback();
            lock.notify();
        }
        waitCommitMap.remove(id);//System.out.println("remove commit, id : " + id);
    }

//    public void commitTx(String id){
//        waitCommitMap.get(id).commit();
//        waitCommitMap.remove(id);
//    }
//
//    public void removeTx(String id){
//        waitCommitMap.get(id).rollback();
//        waitCommitMap.remove(id);
//    }

//    public LocalTransaction getTransaction(DTGOpreration op, Map<Integer, Object> resultMap) throws Throwable{
//        if(op.getTxId().equals("E8-6A-64-04-DF-451")){
//            int a = 0;
//        }
//        Map<Integer, Object> tempMap = new HashMap<>();
//        LocalTransaction transaction = new LocalTransaction(db);
//        List<EntityEntry> Entries = op.getEntityEntries();
//        for(EntityEntry entityEntry : Entries){
//            switch (entityEntry.getType()){
//                case EntityEntry.NODETYPE:{
//                    switch (entityEntry.getOperationType()){
//                        case EntityEntry.ADD:{
//                            if(entityEntry.getKey() == null){
//                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
//                                Node node = transaction.addNode(entityEntry.getId());
//                                tempMap.put(entityEntry.getTransactionNum(), node);
//                                //resultMap.put(entityEntry.getTransactionNum(), node);
//                                break;
//                            }
//
//                            Node node = null;
//                            if(entityEntry.getId() >= 0){ node = transaction.getNodeById(entityEntry.getId()); }
//                            else if( entityEntry.getId() == -2){ node = (Node) tempMap.get(entityEntry.getParaId());}
//                            else throw new EntityEntryException(entityEntry);
//
//                            if(entityEntry.isTemporalProperty()){
//                                if(entityEntry.getOther() != -1){
//                                    transaction.setNodeTemporalProperty(node, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getOther(), entityEntry.getValue());
//                                }
//                                else transaction.setNodeTemporalProperty(node, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getValue());
//                                break;
//                            }
//                            else transaction.setNodeProperty(node, entityEntry.getKey(), entityEntry.getValue());
//                            break;
//                        }
//                        case EntityEntry.GET:{
//                            if(entityEntry.getKey() == null){
//                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
//                                Node node = transaction.getNodeById(entityEntry.getId());
//                                tempMap.put(entityEntry.getTransactionNum(), node);
////                                resultMap.put(entityEntry.getTransactionNum(), node);
//                            }
//
//                            Node node = null;
//                            if(entityEntry.getId() >= 0){ node = transaction.getNodeById(entityEntry.getId()); }
//                            else if( entityEntry.getId() == -2){ node = (Node) tempMap.get(entityEntry.getParaId());}
//                            else throw new EntityEntryException(entityEntry);
//
//                            if(entityEntry.isTemporalProperty()){
//                                Object res = transaction.getNodeTemporalProperty(node, entityEntry.getKey(), entityEntry.getStart());
//                                tempMap.put(entityEntry.getTransactionNum(), res);
//                                resultMap.put(entityEntry.getTransactionNum(), res);
//                            }
//                            else {
//                                Object res = transaction.getNodeProperty(node, entityEntry.getKey());
//                                tempMap.put(entityEntry.getTransactionNum(), res);
//                                resultMap.put(entityEntry.getTransactionNum(), res);
//                            }
//                            break;
//                        }
//                        case EntityEntry.REMOVE:{
//                            Node node = null;
//                            if(entityEntry.getId() >= 0){ node = transaction.getNodeById(entityEntry.getId()); }
//                            else if( entityEntry.getId() == -2){ node = (Node) tempMap.get(entityEntry.getParaId());}
//                            else throw new EntityEntryException(entityEntry);
//
//                            if(entityEntry.getKey() == null){
//                                transaction.deleteNode(node);
//                            }
//                            else if(entityEntry.isTemporalProperty()){
//                                transaction.deleteNodeTemporalProperty(node, entityEntry.getKey());
//                            }
//                            else transaction.deleteNodeProperty(node, entityEntry.getKey());
//                            break;
//                        }
//                        case EntityEntry.SET:{
//                            Node node = null;
//                            if(entityEntry.getId() >= 0){ node = transaction.getNodeById(entityEntry.getId()); }
//                            else if( entityEntry.getId() == -2){ node = (Node) tempMap.get(entityEntry.getParaId());}
//                            else throw new EntityEntryException(entityEntry);
//
//                            if(entityEntry.getKey() == null)throw new EntityEntryException(entityEntry);
//                            if(entityEntry.isTemporalProperty()){
//                                if(entityEntry.getOther() != -1){
//                                    transaction.setNodeTemporalProperty(node, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getOther(), entityEntry.getValue());
//                                }
//                                else transaction.setNodeTemporalProperty(node, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getValue());
//                                break;
//                            }
//                            else transaction.setNodeProperty(node, entityEntry.getKey(), entityEntry.getValue());
//                            break;
//                        }
//                        default:{
//                            throw new TypeDoesnotExistException(entityEntry.getType(), "node operation type");
//                        }
//                    }
//                    break;
//                }
//                case EntityEntry.RELATIONTYPE:{
//                    switch (entityEntry.getOperationType()){
//                        case EntityEntry.ADD:{
//                            if(entityEntry.getKey() == null){
//                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
//                                Relationship relationship = transaction.addRelationship(entityEntry.getId(), entityEntry.getStart(), entityEntry.getOther());
////                                resultMap.put(entityEntry.getTransactionNum(), relationship);
//                                tempMap.put(entityEntry.getTransactionNum(), relationship);
//                                break;
//                            }
//
//                            Relationship relationship = null;
//                            if(entityEntry.getId() >= 0){ relationship = transaction.getRelationshipById(entityEntry.getId()); }
//                            else if( entityEntry.getId() == -2){ relationship = (Relationship) tempMap.get(entityEntry.getParaId());}
//                            else throw new EntityEntryException(entityEntry);
//
//                            if(entityEntry.isTemporalProperty()){
//                                if(entityEntry.getOther() != -1){
//                                    transaction.setRelationTemporalProperty(relationship, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getOther(), entityEntry.getValue());
//                                }
//                                else transaction.setRelationTemporalProperty(relationship, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getValue());
//                                break;
//                            }
//                            else transaction.setRelationProperty(relationship, entityEntry.getKey(), entityEntry.getValue());
//                            break;
//                        }
//                        case EntityEntry.GET:{
//                            if(entityEntry.getKey() == null){
//                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
//                                Relationship relationship = transaction.getRelationshipById(entityEntry.getId());
////                                resultMap.put(entityEntry.getTransactionNum(), relationship);
//                                tempMap.put(entityEntry.getTransactionNum(), relationship);
//                            }
//
//                            Relationship relationship = null;
//                            if(entityEntry.getId() >= 0){ relationship = transaction.getRelationshipById(entityEntry.getId()); }
//                            else if( entityEntry.getId() == -2){ relationship = (Relationship) tempMap.get(entityEntry.getParaId());}
//                            else throw new EntityEntryException(entityEntry);
//
//                            if(entityEntry.isTemporalProperty()){
//                                Object res = transaction.getRelationTemporalProperty(relationship, entityEntry.getKey(), entityEntry.getStart());
//                                resultMap.put(entityEntry.getTransactionNum(), res);
//                                tempMap.put(entityEntry.getTransactionNum(), res);
//                            }
//                            else {
//                                Object res = transaction.getRelationProperty(relationship, entityEntry.getKey());
//                                resultMap.put(entityEntry.getTransactionNum(), res);
//                                tempMap.put(entityEntry.getTransactionNum(), res);
//                            }
//                            break;
//                        }
//                        case EntityEntry.REMOVE:{
//                            Relationship relationship = null;
//                            if(entityEntry.getId() >= 0){ relationship = transaction.getRelationshipById(entityEntry.getId()); }
//                            else if( entityEntry.getId() == -2){ relationship = (Relationship) tempMap.get(entityEntry.getParaId());}
//                            else throw new EntityEntryException(entityEntry);
//
//                            if(entityEntry.getKey() == null){
//                                transaction.deleteRelation(relationship);
//                            }
//                            else if(entityEntry.isTemporalProperty()){
//                                transaction.deleteRelationTemporalProperty(relationship, entityEntry.getKey());
//                            }
//                            else transaction.deleteRelationProperty(relationship, entityEntry.getKey());
//                            break;
//                        }
//                        case EntityEntry.SET:{
//                            Node node = null;
//                            if(entityEntry.getId() >= 0){ node = transaction.getNodeById(entityEntry.getId()); }
//                            else if( entityEntry.getId() == -2){ node = (Node) tempMap.get(entityEntry.getParaId());}
//                            else throw new EntityEntryException(entityEntry);
//
//                            if(entityEntry.getKey() == null)throw new EntityEntryException(entityEntry);
//                            if(entityEntry.isTemporalProperty()){
//                                if(entityEntry.getOther() != -1){
//                                    transaction.setNodeTemporalProperty(node, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getOther(), entityEntry.getValue());
//                                }
//                                else transaction.setNodeTemporalProperty(node, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getValue());
//                                break;
//                            }
//                            else transaction.setNodeProperty(node, entityEntry.getKey(), entityEntry.getValue());
//                            break;
//                        }
//                        default:{
//                            throw new TypeDoesnotExistException(entityEntry.getType(), "node operation type");
//                        }
//                    }
//                    break;
//                }
//                default:{
//                    throw new TypeDoesnotExistException(entityEntry.getType(), "entity type");
//                }
//            }
//        }
//        System.out.println("success transaction operation");
//        return transaction;
//    }
}

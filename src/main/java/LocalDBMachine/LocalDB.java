package LocalDBMachine;

import DBExceptions.DTGLockError;
import DBExceptions.EntityEntryException;
import DBExceptions.RegionStoreException;
import DBExceptions.TypeDoesnotExistException;
import Element.DTGOperation;
import Element.EntityEntry;
import Element.OperationName;
import LocalDBMachine.LocalTx.TransactionThreadLock;
import Region.DTGLockClosure;
import Region.DTGRegion;
import Region.FirstPhaseClosure;
import Region.LockProcess;
import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.client.FutureHelper;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import raft.EntityStoreClosure;
import raft.DTGRawStore;
import raft.LogStoreClosure;
import scala.collection.Iterator;
import options.LocalDBOption;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static config.MainType.NODETYPE;
import static config.MainType.RELATIONTYPE;

/**
 * @author :jinkai
 * @date :Created in 2019/10/22 9:37
 * @description:
 * @modified By:
 * @version:
 */

public class LocalDB implements DTGRawStore, Lifecycle<LocalDBOption> {

    private Map<String, LocalTransaction> waitCommitMap;
    private GraphDatabaseService db;

    //private List<String> waitRemoveList;
    private LockProcess lockProcess;


    @Override
    public boolean init(LocalDBOption opts) {
        this.waitCommitMap = new HashMap<>();
        //this.waitRemoveList = new ArrayList<>();
        this.lockProcess = new LockProcess();
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
    public void saveLog(LogStoreClosure closure) {

    }

    @Override
    public void setLock(final DTGOperation op, final DTGLockClosure closure, DTGRegion region) {
        List<EntityEntry> entityEntries= op.getEntityEntries();
        List<EntityEntry> removeList = new ArrayList<>();
        for(EntityEntry entry : entityEntries){
            if(entry.getId() == -2){
                int paraId = entry.getParaId();
                entry.setId(entityEntries.get(paraId).getId());
                //System.out.println("paraId = " + paraId + " id = " + entityEntries.get(paraId).getId());
            }
            boolean isWaitRemove = this.lockProcess.isWaitRemove(entry.getId(), entry.getType(), op.getTxId());
            if(isWaitRemove){
                closure.run(new Status(DTGLockError.HASREMOVED.getNumber(), "the object has removed!"));
            }
            if(entry.getOperationType() == EntityEntry.REMOVE){
                removeList.add(entry);
            }
        }
        if(removeList.size() > 0){
            DTGOperation operation = new DTGOperation(removeList, OperationName.ADDREMOVELIST);
            closure.setData(operation);
        }
        closure.run(Status.OK());
    }

    @Override
    public void sendLock(DTGOperation op, EntityStoreClosure closure) {

    }

    @Override
    public void commitSuccess(long version) {

    }

    @Override
    public void firstPhaseProcessor(DTGOperation op, final FirstPhaseClosure closure, DTGRegion region) {

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
                        resultMap.put(-1, true);
                         //System.out.println("runOp : start get transaction");
                        TransactionThreadLock txLock = new TransactionThreadLock(op.getTxId());
                        LocalTransaction tx = new LocalTransaction(this.db, op, resultMap, txLock, region);
                        tx.start();
                        synchronized (resultMap){
                            resultMap.wait(FutureHelper.DEFAULT_TIMEOUT_MILLIS);
                        }
                        //tx.commit();
                        //this.addToCommitMap(txLock, op.getTxId());
                        //LocalTransaction tx = getTransaction(op, resultMap);
//                        LocalTransaction tx = new ExecuteTransactionOp().getTransaction(this.db, op, resultMap);
                        //System.out.println("finish get transaction");
                        addToCommitMap(tx, op.getTxId());
                    }
                    if(closure != null){
                       // System.out.println("localDB runOp ok");
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
            case OperationName.COMMITTRANS:{//System.out.println("start commit");
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
            case OperationName.ROLLBACK:{//System.out.println("start rollback");
                try {
                    //removeTx(op.getTxId());
                    rollbackTx(op.getTxId());
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

    public void addToCommitMap(LocalTransaction tx, String id){
        //System.out.println("wait commit, id : " + id);
//        if(waitCommitMap.containsKey(id)){
//            removeTx(id);
//        }
        waitCommitMap.put(id, tx);
    }

//    public void addToCommitMap(TransactionThreadLock lock, String id) throws InterruptedException {
//        //System.out.println("wait commit, id : " + id);
//        if(waitCommitMap.containsKey(id)){
//            removeTx(id);//System.out.println("remove commit, id : " + id);
//        }
//        waitCommitMap.put(id, lock);//System.out.println("put commit, id : " + id);
//    }

    public void commitTx(String id) throws InterruptedException, TypeDoesnotExistException, EntityEntryException, RegionStoreException {

//        TransactionThreadLock lock = waitCommitMap.get(id);
//        System.out.println(id);
//        synchronized (lock){
//            lock.commit();
//            lock.notify();
//        }
//        synchronized (lock.getCommitLock()){
//            lock.getCommitLock().wait();
//        }

        LocalTransaction tx = waitCommitMap.get(id);
        while(tx == null){
            Thread.sleep(10);
            tx = waitCommitMap.get(id);
        }
        //System.out.println("start commitTx");
        if(tx.isLaterCommit()){
            TransactionThreadLock lock = tx.getLock();
            TransactionThreadLock newlock = new TransactionThreadLock(id);
            tx.setLock(newlock);
            synchronized (lock){
                newlock.commit();
                lock.notify();
            }

            synchronized (newlock){
                newlock.wait();
            }
        }
        //System.out.println("start commitTxaaaaaa");
        if(tx.isHighA()){
            tx.updateVersion();
        }
        //System.out.println("start commitTxbbbbbbbbbbbb");
        waitCommitMap.remove(id);//System.out.println("remove commit, id : " + id);
    }

    public void rollbackTx(String id) throws InterruptedException, EntityEntryException {
        LocalTransaction tx = waitCommitMap.get(id);
        if(tx.isLaterCommit()){
            TransactionThreadLock lock = tx.getLock();
            synchronized (lock){
                lock.rollback();
                lock.notify();
            }
        }
        tx.rollbackAdd();
        waitCommitMap.remove(id);
    }

//    public void removeTx(String id) throws InterruptedException {
//        TransactionThreadLock lock = waitCommitMap.get(id);
//        synchronized (lock){
//            lock.rollback();
//            lock.notify();
//        }
//        waitCommitMap.remove(id);//System.out.println("remove commit, id : " + id);
//    }




    public boolean addRemoveLock(DTGOperation op, EntityStoreClosure closure){
        List<EntityEntry> entityEntries= op.getEntityEntries();
        try(Transaction tx = db.beginTx()){
            for(EntityEntry entry : entityEntries){

                //long entryRealId = -2;
//                if(entry.getId() == -2){
//                    int paraId = entry.getParaId();
//                    entry.setId(entityEntries.get(paraId).getId());
//                    System.out.println("paraId = " + paraId + " id = " + entityEntries.get(paraId).getId());
////                    entryRealId = entityEntries.get(paraId).getId();
//                }
//                else {
//                    entryRealId = entry.getId();
//                }
                long maxVersion;
                if(entry.getKey() == null){
                    maxVersion = op.getVersion();
                }
                else{
                    maxVersion = getMaxVersion(entry);
                }
                if(!this.lockProcess.addRemove(entry.getId(), entry.getType(), op.getTxId(), maxVersion)){
                    //System.out.println("lockProcess can not add lock");
                    closure.run(new Status(DTGLockError.FAILED.getNumber(), "can't add lock to object!"));
                    return false;
                }
            }
            tx.success();
        }
        if(closure != null){
            closure.setData(true);
            closure.run(Status.OK());
        }
        return true;
    }

    private long getMaxVersion(EntityEntry entry){
        switch (entry.getType()){
            case NODETYPE:{
                long version = -1;
                Node node = this.db.getNodeById(entry.getId());
                if(entry.isTemporalProperty()){
                    version = (long)node.getTemporalProperty(entry.getKey(), entry.getStart());
                }else{
                    version = (long)node.getProperty(entry.getKey());
                }
                return version;
            }
            case RELATIONTYPE:{
                long version = -1;
                Relationship relation = this.db.getRelationshipById(entry.getId());
                if(entry.isTemporalProperty()){
                    version = (long)relation.getTemporalProperty(entry.getKey(), entry.getStart());
                }else{
                    version = (long)relation.getProperty(entry.getKey());
                }
                return version;
            }
        }
        return -1;
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

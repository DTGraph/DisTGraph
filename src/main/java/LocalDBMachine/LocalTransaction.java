package LocalDBMachine;

import DBExceptions.EntityEntryException;
import DBExceptions.RegionStoreException;
import DBExceptions.TransactionException;
import DBExceptions.TypeDoesnotExistException;
import Element.DTGOperation;
import Element.EntityEntry;
import LocalDBMachine.LocalTx.TransactionThreadLock;
import Region.DTGRegion;
import com.alipay.sofa.jraft.util.Bits;
import config.DTGConstants;
import config.RelType;
import org.neo4j.graphdb.*;
import scala.Array;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static config.MainType.NODETYPE;
import static config.MainType.RELATIONTYPE;

/**
 * @author :jinkai
 * @date :Created in 2019/10/17 19:15
 * @description:
 * @modified By:
 * @version:
 */

public class LocalTransaction extends Thread {

    private GraphDatabaseService db;
    private Transaction transaction;
    private String txId;
    private Boolean couldCommit;
    private DTGOperation op;
    private Map<Integer, Object> resultMap;
    private TransactionThreadLock lock;
    private DTGRegion region;
    private int nodeTransactionAdd;
    private int relationTransactionAdd;
    private int[] actionType;// 0:skip, 1:add node, 2:remove node, 3:add relation, 4:remove relation
    private long[] actionId;
    private long version = 0;//if version = 0, it means no version;
    List<EntityEntry> newEntries;
    private String errorMessage;
    private boolean laterCommit = false;

//    public LocalTransaction(GraphDatabaseService db, DTGOperation op, Map<Integer, Object> res, TransactionThreadLock lock, DTGRegion region, int version){
//        this.db = db;
//        this.txId = op.getTxId();
//        couldCommit = false;
//        this.resultMap = res;
//        this.lock = lock;
//        this.op = op;
//        this.region = region;
//        nodeTransactionAdd = 0;
//        relationTransactionAdd = 0;
//        actionType = new int[op.getEntityEntries().size()];
//        actionId = new long[op.getEntityEntries().size()];
//        this.version = version;
//    }

    public LocalTransaction(GraphDatabaseService db, DTGOperation op, Map<Integer, Object> res, TransactionThreadLock lock, DTGRegion region){
        this.db = db;
        this.txId = op.getTxId();
        couldCommit = false;
        this.resultMap = res;
        this.lock = lock;
        this.op = op;
        this.region = region;
        nodeTransactionAdd = 0;
        relationTransactionAdd = 0;
        actionType = new int[op.getEntityEntries().size()];
        actionId = new long[op.getEntityEntries().size()];
        newEntries = new ArrayList<>();
        if(op.getVersion() >= 0){
            this.version = op.getVersion();
        }else {
            this.version = -1;
        }

    }

    @Override
    public void run() {
        try {
            //System.out.println("start run : " + System.currentTimeMillis());
            transaction = db.beginTx();
            synchronized (resultMap){
                if(this.version < 0){
                    getTransaction();
                }
                else {
                    //System.out.println("start mvcc : " + System.currentTimeMillis());
                    getMVCCTransaction(version);
                    //System.out.println("end mvcc : " + System.currentTimeMillis());
                    //System.out.println("getMVCCTransaction done!");
                }
                resultMap.notify();
            }
            if(laterCommit){
                if(couldCommit == false){
                    synchronized (lock){
                        lock.wait();
                    }
                    couldCommit = true;
                    System.out.println("Start commit in thread");
                    if(lock.isShouldCommit()){
                        commit();
                    }
                    else {
                        rollback();
                    }
                    synchronized (lock){
                        lock.notify();
                    }
                }
//                synchronized (lock){
//                    if(couldCommit == false){
//                        //System.out.println("aaaaaaaa" + lock.getTxId());
//                        lock.wait();
//                        couldCommit = true;
//                        //System.out.println("Start commit in thread");
//                        if(lock.isShouldCommit()){
//                            commit();
//                        }
//                        else {
//                            rollback();
//                        }
//                        //System.out.println("end commit in thread");
//                    }
//                }
            }
            else{
                commit();
            }
            //System.out.println("end run : " + System.currentTimeMillis());
//            synchronized (lock){
//                if(couldCommit == false){
//                    //System.out.println("aaaaaaaa" + lock.getTxId());
//                    lock.wait();
//                    couldCommit = true;
//                    //System.out.println("Start commit in thread");
//                    if(lock.isShouldCommit()){
//                        commit();
//                    }
//                    else {
//                        rollback();
//                    }
//                    //System.out.println("end commit in thread");
//                }
//            }
//            synchronized (lock.getCommitLock()){
//                lock.getCommitLock().notify();
//            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (EntityEntryException | RegionStoreException | TypeDoesnotExistException e) {
            errorMessage = "unknow error";
            transaction.failure();
            transaction.close();
        } catch (TransactionException e) {
            e.printStackTrace();
        }
    }

    public TransactionThreadLock getLock(){
        return this.lock;
    }

    public boolean isLaterCommit(){
        return this.laterCommit;
    }

    public boolean isHighA(){
        return this.op.isHighA();
    }

    public Node addNode(long id, int i) throws RegionStoreException {
        region.addNode();
        nodeTransactionAdd++;
        actionId[i] = id;
        actionType[i] = 1;
        EntityEntry entry = new EntityEntry();
        entry.setId(id);
        entry.setType(NODETYPE);
        entry.setOperationType(EntityEntry.ADD);
        entry.setIsTemporalProperty(false);
        newEntries.add(entry);
        return db.createNode(id);
    }

    public Node getNodeById(long id){
        return db.getNodeById(id);
    }

    public Relationship getRelationshipById(long id){
        return db.getRelationshipById(id);
    }

    public Relationship addRelationship(long startNode, long endNode, long id, int i) throws RegionStoreException {
        Node start = getNodeById(startNode);
        region.addRelation();
        relationTransactionAdd++;
        actionId[i] = id;
        actionType[i] = 3;
        EntityEntry entry = new EntityEntry();
        entry.setId(id);
        entry.setType(RELATIONTYPE);
        entry.setOperationType(EntityEntry.ADD);
        entry.setIsTemporalProperty(false);
        newEntries.add(entry);
        return start.createRelationshipTo(id, endNode, RelType.ROAD_TO);
    }

    public void setNodeProperty(Node node, String key, Object value){
        node.setProperty(key, value);
    }

    public void setNodeProperty(Node node, String key, Object value, long version, boolean highA, List<EntityEntry> newlist){
        if(!highA){
            setNodePropertyVersion(node, key, version);
        }
        else {
            EntityEntry entry = new EntityEntry();
            entry.setType(NODETYPE);
            entry.setOperationType(EntityEntry.SET);
            entry.setIsTemporalProperty(false);
            entry.setId(node.getId());
            entry.setKey(key);
            entry.setValue(value);
            newlist.add(entry);
        }
        node.setProperty(getVersionKey(version, key), value);
    }

    public void setNodePropertyVersion(Node node, String key, long version){
        long maxVersion;
        if(!node.hasProperty(key)){
            maxVersion = DTGConstants.DEFAULT_MAX_VERSION;
        }else {
            Object res = node.getProperty(key);
            maxVersion = (long)res;
        }
        if(maxVersion < version){
            node.setProperty(key, version);
        }
    }

    public void setNodeTemporalProperty(Node node, String key, int start, int end,  Object value){
        node.setTemporalProperty(key, start, end, value);
    }

    public void setNodeTemporalProperty(Node node, String key, int start, int end,  Object value,
                                        long version, boolean highA, List<EntityEntry> newlist){
        if(!highA){
            setNodeTemporalPropertyVersion(node, key, start, end, version);
        }
        else{
            EntityEntry entry = new EntityEntry();
            entry.setType(NODETYPE);
            entry.setOperationType(EntityEntry.SET);
            entry.setIsTemporalProperty(true);
            entry.setId(node.getId());
            entry.setKey(key);
            entry.setValue(value);
            entry.setStart(start);
            entry.setOther(end);
            newlist.add(entry);
        }
        node.setTemporalProperty(getVersionKey(version, key), start, end, value);
    }

    public void setNodeTemporalPropertyVersion(Node node, String key, int start, int end, long version){
        long maxVersion;
        try {
            Object res = node.getTemporalProperty(key, start);
            maxVersion = (long)res;
        }catch (Exception e){
            maxVersion = DTGConstants.DEFAULT_MAX_VERSION;
        }
        //long maxVersion = (long)node.getTemporalProperty(key, start);
        if(maxVersion < version){
            node.setTemporalProperty(key,start, end, version);
        }
    }

    public void setNodeTemporalProperty(Node node, String key, int time,  Object value){
        node.setTemporalProperty(key, time, value);
    }

    public void setNodeTemporalProperty(Node node, String key, int time,  Object value,
                                        long version, boolean highA, List<EntityEntry> newlist){
        if(!highA){
            setNodeTemporalPropertyVersion(node, key, time, version);
        }
        else {
            EntityEntry entry = new EntityEntry();
            entry.setType(NODETYPE);
            entry.setOperationType(EntityEntry.SET);
            entry.setIsTemporalProperty(true);
            entry.setId(node.getId());
            entry.setKey(key);
            entry.setValue(value);
            entry.setStart(time);
            entry.setOther(-1);
            newlist.add(entry);
        }
        node.setTemporalProperty(getVersionKey(version, key), time, value);
    }

    public void setNodeTemporalPropertyVersion(Node node, String key, int time, long version){
        long maxVersion;
        try {
            Object res = node.getTemporalProperty(key, time);
            maxVersion = (long)res;
        }catch (Exception e){
            maxVersion = DTGConstants.DEFAULT_MAX_VERSION;
        }
        if(maxVersion < version){
            node.setTemporalProperty(key,time, version);
        }
    }

    public void deleteNode(Node node, int i) throws RegionStoreException {
        region.removeNode();
        nodeTransactionAdd--;
        actionId[i] = node.getId();
        actionType[i] = 2;
        node.delete();
    }

    public void deleteNodeProperty(Node node, String key){

        node.removeProperty(key);
    }

    public void deleteNodeTemporalProperty(Node node, String key){
        node.removeTemporalProperty(key);
    }

    public Object getNodeProperty(Node node, String key){
        long maxVersion;
        if(!node.hasProperty(key)){
            maxVersion = DTGConstants.DEFAULT_MAX_VERSION;
        }else {
            Object res = node.getProperty(key);
            maxVersion = (long)res;
        }
        //long maxVersion = (long)node.getProperty(key);
        return getNodeProperty(node, key, maxVersion);
    }

    public Object getNodeProperty(Node node, String key, long version){
        //System.out.println("get property id = " + node.getId() + ", key = " + key);
        return node.getProperty(getVersionKey(version, key));
    }

    public Object getNodeTemporalProperty(Node node, String key, int time){
        long maxVersion;
        try {
            Object res = node.getTemporalProperty(key, time);
            maxVersion = (long)res;
            //System.out.println("res maxVersion = " + maxVersion);
        }catch (Exception e){
            maxVersion = DTGConstants.DEFAULT_MAX_VERSION;
            //System.out.println("maxVersion = " + maxVersion);
        }
        //long maxVersion = (long)node.getTemporalProperty(key, time);
        //System.out.println("maxVersion =  aaa");
        return getNodeTemporalProperty(node, key, time, maxVersion);
    }

    public Object getNodeTemporalProperty(Node node, String key, int time, long version){
        //System.out.println("get NodeTemporalProperty id = " + node.getId() + ", key = " + key);
        return node.getTemporalProperty(getVersionKey(version, key), time);
    }

    public void setRelationProperty(Relationship relationship, String key, Object value){
        relationship.setProperty(key, value);
    }

    public void setRelationProperty(Relationship relationship, String key, Object value,
                                    long version, boolean highA, List<EntityEntry> newlist){
        if(!highA){
            setRelationPropertyVersion(relationship, key, version);
        }
        else {
            EntityEntry entry = new EntityEntry();
            entry.setType(RELATIONTYPE);
            entry.setOperationType(EntityEntry.SET);
            entry.setIsTemporalProperty(false);
            entry.setId(relationship.getId());
            entry.setKey(key);
            entry.setValue(value);
            newlist.add(entry);
        }
        relationship.setProperty(getVersionKey(version, key), value);
    }

    public void setRelationPropertyVersion(Relationship relationship, String key, long version){
        long maxVersion;
        if(!relationship.hasProperty(key)){
            maxVersion = DTGConstants.DEFAULT_MAX_VERSION;
        }else {
            Object res = relationship.getProperty(key);
            maxVersion = (long)res;
        }
        if(maxVersion < version){
            relationship.setProperty(key, version);
        }
    }

    public void setRelationTemporalProperty(Relationship relationship, String key, int start, int end,  Object value){
        relationship.setTemporalProperty(key, start, end, value);
    }

    public void setRelationTemporalProperty(Relationship relationship, String key, int start, int end,
                                            Object value, long version, boolean highA, List<EntityEntry> newlist){
        if(!highA){
            setRelationTemporalPropertyVersion(relationship, key, start, end, version);
        }
        else{
            EntityEntry entry = new EntityEntry();
            entry.setType(RELATIONTYPE);
            entry.setOperationType(EntityEntry.SET);
            entry.setIsTemporalProperty(true);
            entry.setId(relationship.getId());
            entry.setKey(key);
            entry.setValue(value);
            entry.setStart(start);
            entry.setOther(end);
            newlist.add(entry);
        }
        relationship.setTemporalProperty(getVersionKey(version, key), start, end, value);
    }

    public void setRelationTemporalPropertyVersion(Relationship relationship, String key, int start, int end, long version){
        long maxVersion;
        try {
            Object res = relationship.getTemporalProperty(key, start);
            maxVersion = (long)res;
        }catch (Exception e){
            maxVersion = DTGConstants.DEFAULT_MAX_VERSION;
        }
        if(maxVersion < version){
            relationship.setTemporalProperty(key, start, end, version);
        }
    }

    public void setRelationTemporalProperty(Relationship relationship, String key, int time,  Object value){
        relationship.setTemporalProperty(key, time, value);
    }

    public void setRelationTemporalProperty(Relationship relationship, String key, int time,
                                            Object value, long version, boolean highA, List<EntityEntry> newlist){
        if(!highA){
            setRelationTemporalPropertyVersion(relationship, key, time, version);
        }
        else{
            EntityEntry entry = new EntityEntry();
            entry.setType(RELATIONTYPE);
            entry.setOperationType(EntityEntry.SET);
            entry.setIsTemporalProperty(true);
            entry.setId(relationship.getId());
            entry.setKey(key);
            entry.setValue(value);
            entry.setStart(time);
            entry.setOther(-1);
            newlist.add(entry);
        }
        relationship.setTemporalProperty(getVersionKey(version, key), time, value);
    }

    public void setRelationTemporalPropertyVersion(Relationship relationship, String key, int time, long version){
        long maxVersion;
        try {
            Object res = relationship.getTemporalProperty(key, time);
            maxVersion = (long)res;
        }catch (Exception e){
            maxVersion = DTGConstants.DEFAULT_MAX_VERSION;
        }
        //long maxVersion = (long)relationship.getTemporalProperty(key, time);
        if(maxVersion < version){
            relationship.setTemporalProperty(key, time, version);
        }
    }

    public void deleteRelation(Relationship relationship, int i) throws RegionStoreException {
        region.removeRelation();
        relationTransactionAdd--;
        actionId[i] = relationship.getId();
        actionType[i] = 4;
        relationship.delete();
    }

    public void deleteRelationProperty(Relationship relationship, String key){
        relationship.removeProperty(key);
    }

    public void deleteRelationTemporalProperty(Relationship relationship, String key){
        relationship.removeTemporalProperty(key);
    }

    public Object getRelationProperty(Relationship relationship, String key){
//        Object res = relationship.getProperty(key);
//        long maxVersion;
//        if(res == null){
//            maxVersion = 0;
//        }else {
//            maxVersion = (long)res;
//        }

        long maxVersion;
        if(!relationship.hasProperty(key)){
            maxVersion = DTGConstants.DEFAULT_MAX_VERSION;
        }else {
            Object res = relationship.getProperty(key);
            maxVersion = (long)res;
        }

        //long maxVersion = (long)relationship.getProperty(key);
        return getRelationProperty(relationship, key, maxVersion);
    }

    public Object getRelationProperty(Relationship relationship, String key, long version){
        return relationship.getProperty(getVersionKey(version, key));
    }

    public Object getRelationTemporalProperty(Relationship relationship, String key, int time){
        long maxVersion;
        try {
            Object res = relationship.getTemporalProperty(key, time);
            maxVersion = (long)res;
        }catch (Exception e){
            maxVersion = DTGConstants.DEFAULT_MAX_VERSION;
        }
        //long maxVersion = (long)relationship.getTemporalProperty(key, time);
        return getRelationTemporalProperty(relationship, key, time, maxVersion);
    }

    public Object getRelationTemporalProperty(Relationship relationship, String key, int time, long version){
        return relationship.getTemporalProperty(getVersionKey(version, key), time);
    }

    public boolean checkStatus(PropertyContainer object) throws TransactionException {
        boolean res = (boolean)object.getProperty("hasPrepare");
        if(!res){
            errorMessage = "relation or node does not exist";
            throw new TransactionException("the object not exist");
        }
        return true;
    }

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public void commit() throws TypeDoesnotExistException {
        //System.out.println("commit" + op.getTxId());
        //long t1 = System.currentTimeMillis();
        transaction.success();
        //long t2 = System.currentTimeMillis();
        transaction.close();
        couldCommit = true;

        //System.out.println("commit" + op.getTxId());
        //long t3 = System.currentTimeMillis();
        //System.out.println("t1 = " + (t1 -t2) + ", t2 = " + (t3 - t2));
    }

    public void rollback(){
        System.out.println("rollback");
        this.region.removeNode(nodeTransactionAdd);
        this.region.removeRelation(relationTransactionAdd);
        transaction.failure();
        transaction.close();
    }

    public void getTransaction() throws EntityEntryException, RegionStoreException, TypeDoesnotExistException{
        Map<Integer, Object> tempMap = new HashMap<>();
        List<EntityEntry> Entries = op.getEntityEntries();//System.out.println("Entries size = " + Entries.size());
        int i = 0;
        for(EntityEntry entityEntry : Entries){
            switch (entityEntry.getType()){
                case NODETYPE:{
                    switch (entityEntry.getOperationType()){
                        case EntityEntry.ADD:{
                            if(entityEntry.getKey() == null){
                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
                                Node node = addNode(entityEntry.getId(), i);
                                tempMap.put(entityEntry.getTransactionNum(), node);System.out.println("new node id = " + entityEntry.getId());
                                //resultMap.put(entityEntry.getTransactionNum(), node);
                                break;
                            }

                            Node node = null;
                            if(entityEntry.getId() >= 0){ node = getNodeById(entityEntry.getId()); }
                            else if( entityEntry.getId() == -2){ node = (Node) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);

                            if(entityEntry.isTemporalProperty()){
                                if(entityEntry.getOther() != -1){
                                    setNodeTemporalProperty(node, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getOther(), entityEntry.getValue());
                                }
                                else setNodeTemporalProperty(node, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getValue());
                                break;
                            }
                            else setNodeProperty(node, entityEntry.getKey(), entityEntry.getValue());
                            break;
                        }
                        case EntityEntry.GET:{
                            if(entityEntry.getKey() == null){
                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
                                Node node = getNodeById(entityEntry.getId());
                                tempMap.put(entityEntry.getTransactionNum(), node);
//                                resultMap.put(entityEntry.getTransactionNum(), node);
                                break;
                            }

                            Node node = null;
                            if(entityEntry.getId() >= 0){ node = getNodeById(entityEntry.getId()); }
                            else if( entityEntry.getId() == -2){ node = (Node) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);

                            if(entityEntry.isTemporalProperty()){
                                Object res = getNodeTemporalProperty(node, entityEntry.getKey(), entityEntry.getStart());
                                tempMap.put(entityEntry.getTransactionNum(), res);
                                resultMap.put(entityEntry.getTransactionNum(), res);
                            }
                            else {
                                Object res = getNodeProperty(node, entityEntry.getKey());
                                tempMap.put(entityEntry.getTransactionNum(), res);
                                resultMap.put(entityEntry.getTransactionNum(), res);
                            }
                            break;
                        }
                        case EntityEntry.REMOVE:{
                            Node node = null;
                            if(entityEntry.getId() >= 0){ node = getNodeById(entityEntry.getId()); }
                            else if( entityEntry.getId() == -2){ node = (Node) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);

                            if(entityEntry.getKey() == null){
                                deleteNode(node, i);
                            }
                            else if(entityEntry.isTemporalProperty()){
                                deleteNodeTemporalProperty(node, entityEntry.getKey());
                            }
                            else deleteNodeProperty(node, entityEntry.getKey());
                            break;
                        }
                        case EntityEntry.SET:{
                            Node node = null;
                            if(entityEntry.getId() >= 0){ node = getNodeById(entityEntry.getId()); }
                            else if( entityEntry.getId() == -2){ node = (Node) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);

                            if(entityEntry.getKey() == null)throw new EntityEntryException(entityEntry);
                            if(entityEntry.isTemporalProperty()){
                                if(entityEntry.getOther() != -1){
                                    setNodeTemporalProperty(node, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getOther(), entityEntry.getValue());
                                }
                                else setNodeTemporalProperty(node, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getValue());
                                break;
                            }
                            else setNodeProperty(node, entityEntry.getKey(), entityEntry.getValue());
                            break;
                        }
                        default:{
                            throw new TypeDoesnotExistException(entityEntry.getType(), "node operation type");
                        }
                    }
                    break;
                }
                case RELATIONTYPE:{
                    switch (entityEntry.getOperationType()){
                        case EntityEntry.ADD:{
                            if(entityEntry.getKey() == null){
                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
                                Relationship relationship = addRelationship(entityEntry.getId(), entityEntry.getStart(), entityEntry.getOther(), i);
//                                resultMap.put(entityEntry.getTransactionNum(), relationship);
                                tempMap.put(entityEntry.getTransactionNum(), relationship);
                                break;
                            }

                            Relationship relationship = null;
                            if(entityEntry.getId() >= 0){ relationship = getRelationshipById(entityEntry.getId()); }
                            else if( entityEntry.getId() == -2){ relationship = (Relationship) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);

                            if(entityEntry.isTemporalProperty()){
                                if(entityEntry.getOther() != -1){
                                    setRelationTemporalProperty(relationship, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getOther(), entityEntry.getValue());
                                }
                                else setRelationTemporalProperty(relationship, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getValue());
                                break;
                            }
                            else setRelationProperty(relationship, entityEntry.getKey(), entityEntry.getValue());
                            break;
                        }
                        case EntityEntry.GET:{
                            if(entityEntry.getKey() == null){
                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
                                Relationship relationship = getRelationshipById(entityEntry.getId());
//                                resultMap.put(entityEntry.getTransactionNum(), relationship);
                                tempMap.put(entityEntry.getTransactionNum(), relationship);
                                break;
                            }

                            Relationship relationship = null;
                            if(entityEntry.getId() >= 0){ relationship = getRelationshipById(entityEntry.getId()); }
                            else if( entityEntry.getId() == -2){ relationship = (Relationship) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);

                            if(entityEntry.isTemporalProperty()){
                                Object res = getRelationTemporalProperty(relationship, entityEntry.getKey(), entityEntry.getStart());
                                resultMap.put(entityEntry.getTransactionNum(), res);
                                tempMap.put(entityEntry.getTransactionNum(), res);
                            }
                            else {
                                Object res = getRelationProperty(relationship, entityEntry.getKey());
                                resultMap.put(entityEntry.getTransactionNum(), res);
                                tempMap.put(entityEntry.getTransactionNum(), res);
                            }
                            break;
                        }
                        case EntityEntry.REMOVE:{
                            Relationship relationship = null;
                            if(entityEntry.getId() >= 0){ relationship = getRelationshipById(entityEntry.getId()); }
                            else if( entityEntry.getId() == -2){ relationship = (Relationship) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);

                            if(entityEntry.getKey() == null){
                                deleteRelation(relationship, i);
                            }
                            else if(entityEntry.isTemporalProperty()){
                                deleteRelationTemporalProperty(relationship, entityEntry.getKey());
                            }
                            else deleteRelationProperty(relationship, entityEntry.getKey());
                            break;
                        }
                        case EntityEntry.SET:{
                            Relationship relationship = null;
                            if(entityEntry.getId() >= 0){ relationship = getRelationshipById(entityEntry.getId()); }
                            else if( entityEntry.getId() == -2){ relationship = (Relationship) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);

                            if(entityEntry.getKey() == null)throw new EntityEntryException(entityEntry);
                            if(entityEntry.isTemporalProperty()){
                                if(entityEntry.getOther() != -1){
                                    setRelationTemporalProperty(relationship, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getOther(), entityEntry.getValue());
                                }
                                else setRelationTemporalProperty(relationship, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getValue());
                                break;
                            }
                            else setRelationProperty(relationship, entityEntry.getKey(), entityEntry.getValue());
                            break;
                        }
                        default:{
                            throw new TypeDoesnotExistException(entityEntry.getType(), "node operation type");
                        }
                    }
                    break;
                }
                default:{
                    throw new TypeDoesnotExistException(entityEntry.getType(), "entity type");
                }
            }
            i++;
        }
        //System.out.println("success transaction operation");
    }

    public void getMVCCTransaction(long version) throws EntityEntryException, RegionStoreException, TypeDoesnotExistException, TransactionException {
        List<EntityEntry> newEntries = new ArrayList<>();
        Map<Integer, Object> tempMap = new HashMap<>();
        List<EntityEntry> Entries = op.getEntityEntries();
        boolean isHighA = op.isHighA();
        int i = 0;
        for(EntityEntry entityEntry : Entries){
            //System.out.println("getMVCCTransaction " + i);
            switch (entityEntry.getType()){
                case NODETYPE:{
                    switch (entityEntry.getOperationType()){
                        case EntityEntry.ADD:{
                            if(entityEntry.getKey() == null){
                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
                                Node node = addNode(entityEntry.getId(), i);
                                node.setProperty("hasPrepare", false);
                                tempMap.put(entityEntry.getTransactionNum(), node);System.out.println("new node id = " + entityEntry.getId());
                                //resultMap.put(entityEntry.getTransactionNum(), node);
                                break;
                            }

                            Node node = null;
                            if(entityEntry.getId() >= 0){
                                node = getNodeById(entityEntry.getId());
                                checkStatus(node);
                            }
                            else if( entityEntry.getId() == -2){ node = (Node) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);
                            if(entityEntry.isTemporalProperty()){
                                if(entityEntry.getOther() != -1){
                                    setNodeTemporalProperty(node, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getOther(),
                                            entityEntry.getValue(), version, isHighA, newEntries);
                                }
                                else setNodeTemporalProperty(node, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getValue(),
                                        version, isHighA, newEntries);
                                break;
                            }
                            else setNodeProperty(node, entityEntry.getKey(), entityEntry.getValue(), version, isHighA, newEntries);
                            break;
                        }
                        case EntityEntry.GET:{
                            if(entityEntry.getKey() == null){
                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
                                Node node = getNodeById(entityEntry.getId());
                                tempMap.put(entityEntry.getTransactionNum(), node);
//                                resultMap.put(entityEntry.getTransactionNum(), node);
                                break;
                            }

                            Node node = null;
                            if(entityEntry.getId() >= 0){ node = getNodeById(entityEntry.getId()); }
                            else if( entityEntry.getId() == -2){ node = (Node) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);

                            if(entityEntry.isTemporalProperty()){
                                Object res = getNodeTemporalProperty(node, entityEntry.getKey(), entityEntry.getStart());
                                tempMap.put(entityEntry.getTransactionNum(), res);
                                resultMap.put(entityEntry.getTransactionNum(), res);
                            }
                            else {
                                Object res = getNodeProperty(node, entityEntry.getKey());
                                tempMap.put(entityEntry.getTransactionNum(), res);
                                resultMap.put(entityEntry.getTransactionNum(), res);
                            }
                            break;
                        }
                        case EntityEntry.REMOVE:{
                            Node node = null;
                            if(entityEntry.getId() >= 0){ node = getNodeById(entityEntry.getId()); }
                            else if( entityEntry.getId() == -2){ node = (Node) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);

                            if(entityEntry.getKey() == null){
                                deleteNode(node, i);
                            }
                            else if(entityEntry.isTemporalProperty()){
                                deleteNodeTemporalProperty(node, entityEntry.getKey());
                            }
                            else deleteNodeProperty(node, entityEntry.getKey());
                            this.laterCommit = true;
                            break;
                        }
                        case EntityEntry.SET:{
                            Node node = null;
                            if(entityEntry.getId() >= 0){ node = getNodeById(entityEntry.getId()); }
                            else if( entityEntry.getId() == -2){ node = (Node) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);

                            if(entityEntry.getKey() == null)throw new EntityEntryException(entityEntry);
                            if(entityEntry.isTemporalProperty()){
                                if(entityEntry.getOther() != -1){
                                    setNodeTemporalProperty(node, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getOther(),
                                            entityEntry.getValue(), version, isHighA, newEntries);
                                }
                                else setNodeTemporalProperty(node, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getValue(),
                                        version, isHighA, newEntries);
                                break;
                            }
                            else setNodeProperty(node, entityEntry.getKey(), entityEntry.getValue(), version, isHighA, newEntries);
                            break;
                        }
                        default:{
                            throw new TypeDoesnotExistException(entityEntry.getType(), "node operation type");
                        }
                    }
                    break;
                }
                case RELATIONTYPE:{
                    switch (entityEntry.getOperationType()){
                        case EntityEntry.ADD:{
                            if(entityEntry.getKey() == null){
                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
                                Relationship relationship = addRelationship(entityEntry.getId(), entityEntry.getStart(), entityEntry.getOther(), i);
//                                resultMap.put(entityEntry.getTransactionNum(), relationship);
                                relationship.setProperty("hasPrepare", false);
                                tempMap.put(entityEntry.getTransactionNum(), relationship);
                                break;
                            }

                            Relationship relationship = null;
                            if(entityEntry.getId() >= 0){
                                relationship = getRelationshipById(entityEntry.getId());
                                checkStatus(relationship);
                            }
                            else if( entityEntry.getId() == -2){ relationship = (Relationship) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);

                            if(entityEntry.isTemporalProperty()){
                                if(entityEntry.getOther() != -1){
                                    setRelationTemporalProperty(relationship, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getOther(),
                                            entityEntry.getValue(), version, isHighA, newEntries);
                                }
                                else setRelationTemporalProperty(relationship, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getValue(),
                                        version, isHighA, newEntries);
                                break;
                            }
                            else setRelationProperty(relationship, entityEntry.getKey(), entityEntry.getValue(), version, isHighA, newEntries);
                            break;
                        }
                        case EntityEntry.GET:{
                            if(entityEntry.getKey() == null){
                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
                                Relationship relationship = getRelationshipById(entityEntry.getId());
//                                resultMap.put(entityEntry.getTransactionNum(), relationship);
                                tempMap.put(entityEntry.getTransactionNum(), relationship);
                                break;
                            }

                            Relationship relationship = null;
                            if(entityEntry.getId() >= 0){ relationship = getRelationshipById(entityEntry.getId()); }
                            else if( entityEntry.getId() == -2){ relationship = (Relationship) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);

                            if(entityEntry.isTemporalProperty()){
                                Object res = getRelationTemporalProperty(relationship, entityEntry.getKey(), entityEntry.getStart());
                                resultMap.put(entityEntry.getTransactionNum(), res);
                                tempMap.put(entityEntry.getTransactionNum(), res);
                            }
                            else {
                                Object res = getRelationProperty(relationship, entityEntry.getKey());
                                resultMap.put(entityEntry.getTransactionNum(), res);
                                tempMap.put(entityEntry.getTransactionNum(), res);
                            }
                            break;
                        }
                        case EntityEntry.REMOVE:{
                            Relationship relationship = null;
                            if(entityEntry.getId() >= 0){ relationship = getRelationshipById(entityEntry.getId()); }
                            else if( entityEntry.getId() == -2){ relationship = (Relationship) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);

                            if(entityEntry.getKey() == null){
                                deleteRelation(relationship, i);
                            }
                            else if(entityEntry.isTemporalProperty()){
                                deleteRelationTemporalProperty(relationship, entityEntry.getKey());
                            }
                            else deleteRelationProperty(relationship, entityEntry.getKey());
                            this.laterCommit = true;
                            break;
                        }
                        case EntityEntry.SET:{
                            Relationship relationship = null;
                            if(entityEntry.getId() >= 0){ relationship = getRelationshipById(entityEntry.getId()); }
                            else if( entityEntry.getId() == -2){ relationship = (Relationship) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);

                            if(entityEntry.getKey() == null)throw new EntityEntryException(entityEntry);
                            if(entityEntry.isTemporalProperty()){
                                if(entityEntry.getOther() != -1){
                                    setRelationTemporalProperty(relationship, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getOther(),
                                            entityEntry.getValue(), version, isHighA, newEntries);
                                }
                                else setRelationTemporalProperty(relationship, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getValue(),
                                        version, isHighA, newEntries);
                                break;
                            }
                            else setRelationProperty(relationship, entityEntry.getKey(), entityEntry.getValue(), version, isHighA, newEntries);
                            break;
                        }
                        default:{
                            throw new TypeDoesnotExistException(entityEntry.getType(), "node operation type");
                        }
                    }
                    break;
                }
                default:{
                    throw new TypeDoesnotExistException(entityEntry.getType(), "entity type");
                }
            }
            i++;
        }
        //op.setEntityEntries(newEntries);
    }

    public void rollbackAdd() throws EntityEntryException {
        while(!couldCommit){}
        Transaction rollbacktransaction = db.beginTx();
        for(EntityEntry entityEntry : this.newEntries){
            if(entityEntry.getType() == NODETYPE){
                if(entityEntry.getOperationType() == EntityEntry.ADD){
                    Node node = null;
                    if(entityEntry.getId() >= 0){ node = getNodeById(entityEntry.getId()); }
                    else throw new EntityEntryException(entityEntry);
                    if(entityEntry.getKey() == null){
                        node.delete();
                        break;
                    }
                }
            }
            else if(entityEntry.getType() == RELATIONTYPE){
                if(entityEntry.getOperationType() == EntityEntry.ADD){
                    Relationship relationship = null;
                    if(entityEntry.getId() >= 0){ relationship = getRelationshipById(entityEntry.getId()); }
                    else throw new EntityEntryException(entityEntry);
                    if(entityEntry.getKey() == null){
                        relationship.delete();
                        break;
                    }
                }
            }
        }
        rollbacktransaction.success();
        rollbacktransaction.close();
    }

    public void updateVersion() throws TypeDoesnotExistException, EntityEntryException, RegionStoreException {
        //System.out.println("updateVersion");
        while(!couldCommit){
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        for(int i = 0; i < op.getEntityEntries().size(); i++){
            switch (actionType[i]){
                case 0:break;
                case 1:{
                    this.region.addToNodeRange(actionId[i]);
                    break;
                }
                case 2:{
                    this.region.removeFromNodeRange(actionId[i]);
                    break;
                }
                case 3:{
                    this.region.addToRelationRange(actionId[i]);
                    break;
                }
                case 4:{
                    this.region.removeFromRelationRange(actionId[i]);
                    break;
                }
                default:{
                    throw new TypeDoesnotExistException(actionType[i]);
                }
            }
        }
        if(!op.isHighA()){
            return;
        }
        Transaction updatetransaction = db.beginTx();
        for(EntityEntry entityEntry : this.newEntries){
            switch (entityEntry.getType()){
                case NODETYPE:{
                    switch (entityEntry.getOperationType()){
                        case EntityEntry.ADD:{
                            Node node = null;
                            if(entityEntry.getId() >= 0){ node = getNodeById(entityEntry.getId()); }
                            else throw new EntityEntryException(entityEntry);
                            if(entityEntry.getKey() == null){
                                node.setProperty("hasPrepare", true);
                                break;
                            }
                            if(entityEntry.isTemporalProperty()){
                                if(entityEntry.getOther() != -1){
                                    setNodeTemporalPropertyVersion(node, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getOther(), version);
                                }
                                else setNodeTemporalPropertyVersion(node, entityEntry.getKey(), entityEntry.getStart(), version);
                                break;
                            }
                            else setNodePropertyVersion(node, entityEntry.getKey(), version);
                            break;
                        }
                        case EntityEntry.SET:{
                            Node node = null;
                            if(entityEntry.getId() >= 0){ node = getNodeById(entityEntry.getId()); }
                            else throw new EntityEntryException(entityEntry);
                            if(entityEntry.getKey() == null)throw new EntityEntryException(entityEntry);
                            if(entityEntry.isTemporalProperty()){
                                if(entityEntry.getOther() != -1){
                                    setNodeTemporalPropertyVersion(node, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getOther(), version);
                                }
                                else setNodeTemporalPropertyVersion(node, entityEntry.getKey(), entityEntry.getStart(), version);
                                break;
                            }
                            else setNodePropertyVersion(node, entityEntry.getKey(), version);
                            break;
                        }
                        default:{
                            throw new TypeDoesnotExistException(entityEntry.getType(), "node operation type");
                        }
                    }
                    break;
                }
                case RELATIONTYPE:{
                    switch (entityEntry.getOperationType()){
                        case EntityEntry.ADD:{
                            Relationship relationship = null;
                            if(entityEntry.getId() >= 0){ relationship = getRelationshipById(entityEntry.getId()); }
                            else throw new EntityEntryException(entityEntry);
                            if(entityEntry.getKey() == null){
                                relationship.setProperty("hasPrepare", true);
                                break;
                            }
                            if(entityEntry.isTemporalProperty()){
                                if(entityEntry.getOther() != -1){
                                    setRelationTemporalPropertyVersion(relationship, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getOther(), version);
                                }
                                else setRelationTemporalPropertyVersion(relationship, entityEntry.getKey(), entityEntry.getStart(), version);
                                break;
                            }
                            else setRelationPropertyVersion(relationship, entityEntry.getKey(), version);
                            break;
                        }
                        case EntityEntry.SET:{
                            Relationship relationship = null;
                            if(entityEntry.getId() >= 0){ relationship = getRelationshipById(entityEntry.getId()); }
                            else throw new EntityEntryException(entityEntry);
                            if(entityEntry.getKey() == null)throw new EntityEntryException(entityEntry);
                            if(entityEntry.isTemporalProperty()){
                                if(entityEntry.getOther() != -1){
                                    setRelationTemporalPropertyVersion(relationship, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getOther(), version);
                                }
                                else setRelationTemporalPropertyVersion(relationship, entityEntry.getKey(), entityEntry.getStart(), version);
                                break;
                            }
                            else setRelationPropertyVersion(relationship, entityEntry.getKey(), version);
                            break;
                        }
                        default:{
                            throw new TypeDoesnotExistException(entityEntry.getType(), "node operation type");
                        }
                    }
                    break;
                }
                default:{
                    throw new TypeDoesnotExistException(entityEntry.getType(), "entity type");
                }
            }
        }
        updatetransaction.success();
        updatetransaction.close();
    }

    public void setLock(TransactionThreadLock lock) {
        this.lock = lock;
    }

    private String getVersionKey(long version, String key){
        return key + "/" + version;
    }
}

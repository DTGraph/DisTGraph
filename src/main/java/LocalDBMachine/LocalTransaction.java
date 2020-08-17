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
import tool.OutPutCsv;

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
    private boolean isolateRead = false;
    public int hasDone = -1;
    private Map<Integer, Long> requireVersion;
    private Map<Integer, Long> returnVersion;

    public LocalTransaction(GraphDatabaseService db, DTGOperation op, Map<Integer, Object> res, TransactionThreadLock lock, DTGRegion region){
        this.db = db;
        this.txId = op.getTxId();
        couldCommit = false;
        this.resultMap = res;
        this.lock = lock;
        this.op = op;
        this.region = region;
        this.isolateRead = op.isIsolateRead();
        nodeTransactionAdd = 0;
        relationTransactionAdd = 0;
        actionType = new int[op.getEntityEntries().size()];
        actionId = new long[op.getEntityEntries().size()];
        newEntries = new ArrayList<>();
        this.returnVersion = new HashMap<>();
        if(op.getVersion() >= 0){
            this.version = op.getVersion();
        }else {
            this.version = -1;
        }

    }

    public LocalTransaction(GraphDatabaseService db, DTGOperation op, Map<Integer, Object> res, TransactionThreadLock lock,
                            DTGRegion region, Map<Integer, Long> requireVersion){
        this(db, op, res, lock, region);
        this.requireVersion = requireVersion;
    }

    @Override
    public void run() {
        try {
            transaction = db.beginTx();
            synchronized (resultMap){
                if(this.requireVersion != null){
                    getMVCCReadTransaction();
                }
                else if(this.version < 0){
                    getTransaction();
                }
                else {
                    getMVCCTransaction(version);
                }
                resultMap.notify();
            }
            if(laterCommit){
                if(couldCommit == false){
                    synchronized (lock){
                        lock.wait();
                    }
                    couldCommit = true;
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
            }
            else{
                commit();
            }
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

    private Node addNode(long id, int i) throws RegionStoreException {
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

    private Node getNodeById(int txNum, long id){
        this.returnVersion.put(txNum, -1l);
        return db.getNodeById(id);
    }

    private Relationship getRelationshipById(int txNum, long id){
        this.returnVersion.put(txNum, -1l);
        return db.getRelationshipById(id);
    }

    private Relationship addRelationship(long startNode, long endNode, long id, int i) throws RegionStoreException {
        Node start = getNodeById(-1, startNode);
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

    private void setNodeProperty(Node node, String key, Object value){
        node.setProperty(key, value);
    }

    private void setNodeProperty(Node node, String key, Object value, long version, boolean highA, List<EntityEntry> newlist){
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

    private void setNodePropertyVersion(Node node, String key, long version){
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

    private void setNodeTemporalProperty(Node node, String key, int start, int end,  Object value){
        node.setTemporalProperty(key, start, end, value);
    }

    private void setNodeTemporalProperty(Node node, String key, int start, int end,  Object value,
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

    private void setNodeTemporalPropertyVersion(Node node, String key, int start, int end, long version){
        long maxVersion;
        try {
            Object res = node.getTemporalProperty(key, start);
            maxVersion = (long)res;
        }catch (Exception e){
            maxVersion = DTGConstants.DEFAULT_MAX_VERSION;
        }
        if(maxVersion < version){
            node.setTemporalProperty(key,start, end, version);
        }
    }

    private void setNodeTemporalProperty(Node node, String key, int time,  Object value){
        node.setTemporalProperty(key, time, value);
    }

    private void setNodeTemporalProperty(Node node, String key, int time,  Object value,
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

    private void setNodeTemporalPropertyVersion(Node node, String key, int time, long version){
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

    private void deleteNode(Node node, int i) throws RegionStoreException {
        region.removeNode();
        nodeTransactionAdd--;
        actionId[i] = node.getId();
        actionType[i] = 2;
        node.delete();
    }

    private void deleteNodeProperty(Node node, String key){
        node.removeProperty(key);
    }

    private void deleteNodeTemporalProperty(Node node, String key){
        node.removeTemporalProperty(key);
    }

    private Object getNodeProperty(int txNum, Node node, String key){
        long maxVersion;
        if(!node.hasProperty(key)){
            maxVersion = DTGConstants.DEFAULT_MAX_VERSION;
        }else {
            Object res = node.getProperty(key);
            maxVersion = (long)res;
        }
        return getNodeProperty(txNum, node, key, maxVersion);
    }

    private Object getNodeProperty(int txNum, Node node, String key, long version){
        this.returnVersion.put(txNum, version);
        return node.getProperty(getVersionKey(version, key));
    }

    private Object getNodeTemporalProperty(int txNum, Node node, String key, int time){
        long maxVersion;
        try {
            Object res = node.getTemporalProperty(key, time);
            maxVersion = (long)res;
        }catch (Exception e){
            maxVersion = DTGConstants.DEFAULT_MAX_VERSION;
        }
        return getNodeTemporalProperty(txNum, node, key, time, maxVersion);
    }

    private Object getNodeTemporalProperty(int txNum, Node node, String key, int time, long version){
        this.returnVersion.put(txNum, version);
        return node.getTemporalProperty(getVersionKey(version, key), time);
    }

    private void setRelationProperty(Relationship relationship, String key, Object value){
        relationship.setProperty(key, value);
    }

    private void setRelationProperty(Relationship relationship, String key, Object value,
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

    private void setRelationPropertyVersion(Relationship relationship, String key, long version){
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

    private void setRelationTemporalProperty(Relationship relationship, String key, int start, int end,  Object value){
        relationship.setTemporalProperty(key, start, end, value);
    }

    private void setRelationTemporalProperty(Relationship relationship, String key, int start, int end,
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

    private void setRelationTemporalPropertyVersion(Relationship relationship, String key, int start, int end, long version){
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

    private void setRelationTemporalProperty(Relationship relationship, String key, int time,  Object value){
        relationship.setTemporalProperty(key, time, value);
    }

    private void setRelationTemporalProperty(Relationship relationship, String key, int time,
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

    private void setRelationTemporalPropertyVersion(Relationship relationship, String key, int time, long version){
        long maxVersion;
        try {
            Object res = relationship.getTemporalProperty(key, time);
            maxVersion = (long)res;
        }catch (Exception e){
            maxVersion = DTGConstants.DEFAULT_MAX_VERSION;
        }
        if(maxVersion < version){
            relationship.setTemporalProperty(key, time, version);
        }
    }

    private void deleteRelation(Relationship relationship, int i) throws RegionStoreException {
        region.removeRelation();
        relationTransactionAdd--;
        actionId[i] = relationship.getId();
        actionType[i] = 4;
        relationship.delete();
    }

    private void deleteRelationProperty(Relationship relationship, String key){
        relationship.removeProperty(key);
    }

    private void deleteRelationTemporalProperty(Relationship relationship, String key){
        relationship.removeTemporalProperty(key);
    }

    private Object getRelationProperty(int txNum, Relationship relationship, String key){
        long maxVersion;
        if(!relationship.hasProperty(key)){
            maxVersion = DTGConstants.DEFAULT_MAX_VERSION;
        }else {
            Object res = relationship.getProperty(key);
            maxVersion = (long)res;
        }
        return getRelationProperty(txNum, relationship, key, maxVersion);
    }

    private Object getRelationProperty(int txNum, Relationship relationship, String key, long version){
        this.returnVersion.put(txNum, version);
        return relationship.getProperty(getVersionKey(version, key));
    }

    private Object getRelationTemporalProperty(int txNum, Relationship relationship, String key, int time){
        long maxVersion;
        try {
            Object res = relationship.getTemporalProperty(key, time);
            maxVersion = (long)res;
        }catch (Exception e){
            maxVersion = DTGConstants.DEFAULT_MAX_VERSION;
        }
        return getRelationTemporalProperty(txNum, relationship, key, time, maxVersion);
    }

    private Object getRelationTemporalProperty(int txNum, Relationship relationship, String key, int time, long version){
        this.returnVersion.put(txNum, version);
        return relationship.getTemporalProperty(getVersionKey(version, key), time);
    }

    private boolean checkStatus(PropertyContainer object) throws TransactionException {
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
        transaction.success();
        transaction.close();
        couldCommit = true;
        //System.out.println("commit" + op.getTxId());
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
        List<EntityEntry> Entries = op.getEntityEntries();
        int i = 0;
        for(EntityEntry entityEntry : Entries){
            switch (entityEntry.getType()){
                case NODETYPE:{
                    switch (entityEntry.getOperationType()){
                        case EntityEntry.ADD:{
                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
                                Node node = addNode(entityEntry.getId(), i);
                                tempMap.put(entityEntry.getTransactionNum(), node);System.out.println("new node id = " + entityEntry.getId());
                                break;
                            }

                            Node node = null;
                            if(entityEntry.getId() >= 0){ node = getNodeById(entityEntry.getTransactionNum(), entityEntry.getId()); }
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
                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
                                Node node = getNodeById(entityEntry.getTransactionNum(), entityEntry.getId());
                                tempMap.put(entityEntry.getTransactionNum(), node);
                                break;
                            }

                            Node node = null;
                            if(entityEntry.getId() >= 0){ node = getNodeById(entityEntry.getTransactionNum(), entityEntry.getId()); }
                            else if( entityEntry.getId() == -2){ node = (Node) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);

                            if(entityEntry.isTemporalProperty()){
                                Object res = getNodeTemporalProperty(entityEntry.getTransactionNum(), node, entityEntry.getKey(), entityEntry.getStart());
                                tempMap.put(entityEntry.getTransactionNum(), res);
                                resultMap.put(entityEntry.getTransactionNum(), res);
                            }
                            else {
                                Object res = getNodeProperty(entityEntry.getTransactionNum(), node, entityEntry.getKey());
                                tempMap.put(entityEntry.getTransactionNum(), res);
                                resultMap.put(entityEntry.getTransactionNum(), res);
                            }
                            break;
                        }
                        case EntityEntry.REMOVE:{
                            Node node = null;
                            if(entityEntry.getId() >= 0){ node = getNodeById(-1, entityEntry.getId()); }
                            else if( entityEntry.getId() == -2){ node = (Node) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);

                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
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
                            if(entityEntry.getId() >= 0){ node = getNodeById(-1, entityEntry.getId()); }
                            else if( entityEntry.getId() == -2){ node = (Node) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);

                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING))throw new EntityEntryException(entityEntry);
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
                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
                                Relationship relationship = addRelationship(entityEntry.getId(), entityEntry.getStart(), entityEntry.getOther(), i);
                                tempMap.put(entityEntry.getTransactionNum(), relationship);
                                break;
                            }

                            Relationship relationship = null;
                            if(entityEntry.getId() >= 0){ relationship = getRelationshipById(-1, entityEntry.getId()); }
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
                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
                                Relationship relationship = getRelationshipById(entityEntry.getTransactionNum(), entityEntry.getId());
                                tempMap.put(entityEntry.getTransactionNum(), relationship);
                                break;
                            }

                            Relationship relationship = null;
                            if(entityEntry.getId() >= 0){ relationship = getRelationshipById(entityEntry.getTransactionNum(), entityEntry.getId()); }
                            else if( entityEntry.getId() == -2){ relationship = (Relationship) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);

                            if(entityEntry.isTemporalProperty()){
                                Object res = getRelationTemporalProperty(entityEntry.getTransactionNum(), relationship, entityEntry.getKey(), entityEntry.getStart());
                                resultMap.put(entityEntry.getTransactionNum(), res);
                                tempMap.put(entityEntry.getTransactionNum(), res);
                            }
                            else {
                                Object res = getRelationProperty(entityEntry.getTransactionNum(), relationship, entityEntry.getKey());
                                resultMap.put(entityEntry.getTransactionNum(), res);
                                tempMap.put(entityEntry.getTransactionNum(), res);
                            }
                            break;
                        }
                        case EntityEntry.REMOVE:{
                            Relationship relationship = null;
                            if(entityEntry.getId() >= 0){ relationship = getRelationshipById(entityEntry.getTransactionNum(), entityEntry.getId()); }
                            else if( entityEntry.getId() == -2){ relationship = (Relationship) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);

                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
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
                            if(entityEntry.getId() >= 0){ relationship = getRelationshipById(-1, entityEntry.getId()); }
                            else if( entityEntry.getId() == -2){ relationship = (Relationship) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);

                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING))throw new EntityEntryException(entityEntry);
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
        this.resultMap.put(-1, this.returnVersion);
    }

    public void getMVCCTransaction(long version) throws EntityEntryException, RegionStoreException, TypeDoesnotExistException, TransactionException {
        //List<EntityEntry> newEntries = new ArrayList<>();
        Map<Integer, Object> tempMap = new HashMap<>();
        List<EntityEntry> Entries = op.getEntityEntries();
        boolean isHighA = op.isHighA();
        int i = 0;
        for(EntityEntry entityEntry : Entries){
            switch (entityEntry.getType()){
                case NODETYPE:{
                    switch (entityEntry.getOperationType()){
                        case EntityEntry.ADD:{
                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
                                Node node = addNode(entityEntry.getId(), i);
                                node.setProperty("hasPrepare", false);
                                tempMap.put(entityEntry.getTransactionNum(), node);
                                break;
                            }

                            Node node = null;
                            if(entityEntry.getId() >= 0){
                                node = getNodeById(-1, entityEntry.getId());
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
                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
                                Node node = getNodeById(entityEntry.getTransactionNum(), entityEntry.getId());
                                tempMap.put(entityEntry.getTransactionNum(), node);
                                break;
                            }

                            Node node = null;
                            if(entityEntry.getId() >= 0){ node = getNodeById(entityEntry.getTransactionNum(), entityEntry.getId()); }
                            else if( entityEntry.getId() == -2){ node = (Node) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);

                            if(entityEntry.isTemporalProperty()){
                                Object res = getNodeTemporalProperty(entityEntry.getTransactionNum(), node, entityEntry.getKey(), entityEntry.getStart());
                                tempMap.put(entityEntry.getTransactionNum(), res);
                                resultMap.put(entityEntry.getTransactionNum(), res);
                            }
                            else {
                                Object res = getNodeProperty(entityEntry.getTransactionNum(), node, entityEntry.getKey());
                                tempMap.put(entityEntry.getTransactionNum(), res);
                                resultMap.put(entityEntry.getTransactionNum(), res);
                            }
                            break;
                        }
                        case EntityEntry.REMOVE:{
                            Node node = null;
                            if(entityEntry.getId() >= 0){ node = getNodeById(entityEntry.getTransactionNum(), entityEntry.getId()); }
                            else if( entityEntry.getId() == -2){ node = (Node) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);

                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
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
                            if(entityEntry.getId() >= 0){ node = getNodeById(entityEntry.getTransactionNum(), entityEntry.getId()); }
                            else if( entityEntry.getId() == -2){ node = (Node) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);

                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING))throw new EntityEntryException(entityEntry);
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
                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
                                Relationship relationship = addRelationship(entityEntry.getId(), entityEntry.getStart(), entityEntry.getOther(), i);
                                relationship.setProperty("hasPrepare", false);
                                tempMap.put(entityEntry.getTransactionNum(), relationship);
                                break;
                            }

                            Relationship relationship = null;
                            if(entityEntry.getId() >= 0){
                                relationship = getRelationshipById(-1, entityEntry.getId());
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
                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
                                Relationship relationship = getRelationshipById(entityEntry.getTransactionNum(), entityEntry.getId());
                                tempMap.put(entityEntry.getTransactionNum(), relationship);
                                break;
                            }

                            Relationship relationship = null;
                            if(entityEntry.getId() >= 0){ relationship = getRelationshipById(entityEntry.getTransactionNum(), entityEntry.getId()); }
                            else if( entityEntry.getId() == -2){ relationship = (Relationship) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);

                            if(entityEntry.isTemporalProperty()){
                                Object res = getRelationTemporalProperty(entityEntry.getTransactionNum(), relationship, entityEntry.getKey(), entityEntry.getStart());
                                resultMap.put(entityEntry.getTransactionNum(), res);
                                tempMap.put(entityEntry.getTransactionNum(), res);
                            }
                            else {
                                Object res = getRelationProperty(entityEntry.getTransactionNum(), relationship, entityEntry.getKey());
                                resultMap.put(entityEntry.getTransactionNum(), res);
                                tempMap.put(entityEntry.getTransactionNum(), res);
                            }
                            break;
                        }
                        case EntityEntry.REMOVE:{
                            Relationship relationship = null;
                            if(entityEntry.getId() >= 0){ relationship = getRelationshipById(entityEntry.getTransactionNum(), entityEntry.getId()); }
                            else if( entityEntry.getId() == -2){ relationship = (Relationship) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);

                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
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
                            if(entityEntry.getId() >= 0){ relationship = getRelationshipById(-1, entityEntry.getId()); }
                            else if( entityEntry.getId() == -2){ relationship = (Relationship) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);

                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING))throw new EntityEntryException(entityEntry);
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
        this.resultMap.put(-1, this.returnVersion);
    }

    public void getMVCCReadTransaction() throws EntityEntryException, RegionStoreException, TypeDoesnotExistException, TransactionException {
        //List<EntityEntry> newEntries = new ArrayList<>();
        Map<Integer, Object> tempMap = new HashMap<>();
        List<EntityEntry> Entries = op.getEntityEntries();
        for(EntityEntry entityEntry : Entries){
            long newversion = this.requireVersion.get(entityEntry.getTransactionNum());
            switch (entityEntry.getType()){
                case NODETYPE:{
                    switch (entityEntry.getOperationType()){
                        case EntityEntry.GET:{
                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
                                Node node = getNodeById(entityEntry.getTransactionNum(), entityEntry.getId());
                                tempMap.put(entityEntry.getTransactionNum(), node);
                                break;
                            }

                            Node node = null;
                            if(entityEntry.getId() >= 0){ node = getNodeById(entityEntry.getTransactionNum(), entityEntry.getId()); }
                            else if( entityEntry.getId() == -2){ node = (Node) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);

                            if(entityEntry.isTemporalProperty()){
                                Object res = getNodeTemporalProperty(entityEntry.getTransactionNum(), node, entityEntry.getKey(), entityEntry.getStart(), newversion);
                                tempMap.put(entityEntry.getTransactionNum(), res);
                                resultMap.put(entityEntry.getTransactionNum(), res);
                            }
                            else {
                                Object res = getNodeProperty(entityEntry.getTransactionNum(), node, entityEntry.getKey(), newversion);
                                tempMap.put(entityEntry.getTransactionNum(), res);
                                resultMap.put(entityEntry.getTransactionNum(), res);
                            }
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
                        case EntityEntry.GET:{
                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
                                Relationship relationship = getRelationshipById(entityEntry.getTransactionNum(), entityEntry.getId());
                                tempMap.put(entityEntry.getTransactionNum(), relationship);
                                break;
                            }

                            Relationship relationship = null;
                            if(entityEntry.getId() >= 0){ relationship = getRelationshipById(entityEntry.getTransactionNum(), entityEntry.getId()); }
                            else if( entityEntry.getId() == -2){ relationship = (Relationship) tempMap.get(entityEntry.getParaId());}
                            else throw new EntityEntryException(entityEntry);

                            if(entityEntry.isTemporalProperty()){
                                Object res = getRelationTemporalProperty(entityEntry.getTransactionNum(), relationship, entityEntry.getKey(), entityEntry.getStart(), newversion);
                                resultMap.put(entityEntry.getTransactionNum(), res);
                                tempMap.put(entityEntry.getTransactionNum(), res);
                            }
                            else {
                                Object res = getRelationProperty(entityEntry.getTransactionNum(), relationship, entityEntry.getKey(), newversion);
                                resultMap.put(entityEntry.getTransactionNum(), res);
                                tempMap.put(entityEntry.getTransactionNum(), res);
                            }
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
        this.resultMap.put(-1, this.returnVersion);
    }

    public void rollbackAdd() throws EntityEntryException, InterruptedException {
        while(!couldCommit){
            Thread.sleep(10);
        }
        System.out.println("rollbackAdd");
        Transaction rollbacktransaction = db.beginTx();
        for(EntityEntry entityEntry : this.newEntries){
            if(entityEntry.getType() == NODETYPE){
                if(entityEntry.getOperationType() == EntityEntry.ADD){
                    Node node = null;
                    if(entityEntry.getId() >= 0){ node = getNodeById(-1, entityEntry.getId()); }
                    else throw new EntityEntryException(entityEntry);
                    if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
                        node.delete();
                        break;
                    }
                }
            }
            else if(entityEntry.getType() == RELATIONTYPE){
                if(entityEntry.getOperationType() == EntityEntry.ADD){
                    Relationship relationship = null;
                    if(entityEntry.getId() >= 0){ relationship = getRelationshipById(-1, entityEntry.getId()); }
                    else throw new EntityEntryException(entityEntry);
                    if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
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
                            if(entityEntry.getId() >= 0){ node = getNodeById(entityEntry.getTransactionNum(), entityEntry.getId()); }
                            else throw new EntityEntryException(entityEntry);
                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
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
                            if(entityEntry.getId() >= 0){ node = getNodeById(-1, entityEntry.getId()); }
                            else throw new EntityEntryException(entityEntry);
                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING))throw new EntityEntryException(entityEntry);
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
                            if(entityEntry.getId() >= 0){ relationship = getRelationshipById(-1, entityEntry.getId()); }
                            else throw new EntityEntryException(entityEntry);
                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
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
                            if(entityEntry.getId() >= 0){ relationship = getRelationshipById(-1, entityEntry.getId()); }
                            else throw new EntityEntryException(entityEntry);
                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING))throw new EntityEntryException(entityEntry);
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

    public DTGOperation getOp(){
        return this.op;
    }
}

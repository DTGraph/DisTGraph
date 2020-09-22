package LocalDBMachine;

import DBExceptions.*;
import Element.DTGOperation;
import Element.EntityEntry;
import LocalDBMachine.LocalTx.TransactionThreadLock;
import Region.DTGRegion;
import config.DTGConstants;
import config.RelType;
import org.neo4j.graphdb.*;
import tool.MemMvcc.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static config.DTGConstants.DELETESELF;
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
    private String txId;
    private Boolean couldCommit = false;
    private DTGOperation op;
    private Map<Integer, Object> resultMap;
    private TransactionThreadLock lock;
    private long startVersion = 0;//if version = 0, it means no version;
    private long endVersion = 0;
    List<EntityEntry> newEntries;
    private Map<Integer, Long> returnVersion;
    private MemMVCC memMVCC;

    public LocalTransaction(MemMVCC memMVCC, GraphDatabaseService db, DTGOperation op, Map<Integer, Object> res, TransactionThreadLock lock, DTGRegion region){
        this.db = db;
        this.txId = op.getTxId();
        couldCommit = false;
        this.resultMap = res;
        this.lock = lock;
        this.op = op;
        newEntries = new ArrayList<>();
        this.returnVersion = new HashMap<>();
        this.memMVCC = memMVCC;
        if(op.getVersion() >= 0){
            this.startVersion = op.getVersion();
        }else {
            this.startVersion = -1;
        }

    }

    public LocalTransaction(MemMVCC memMVCC, GraphDatabaseService db, DTGOperation op, Map<Integer, Object> res, TransactionThreadLock lock,
                             DTGRegion region, Map<Integer, Long> requireVersion){
        this(memMVCC, db, op, res, lock, region);
    }

    public void setEndVersion(long endVersion) {
        this.endVersion = endVersion;
    }

    @Override
    public void run() {
        try {
            synchronized (resultMap){//System.out.println("11111111");
                saveToMvcc(this.startVersion);//System.out.println("222222222");
            }
            if(couldCommit == false){//System.out.println("33333333");
                synchronized (lock){
                    lock.notify();
                    couldCommit = true;//System.out.println("4444444444");
                    lock.wait();
                }//System.out.println("55555555");
                if(lock.isShouldCommit()){//System.out.println("6666666     " + this.endVersion);
                    commitMVCC(this.startVersion, this.endVersion);//System.out.println("7777777");
                }
                else {
                    //rollback();
                }
                synchronized (lock){
                    lock.notify();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public TransactionThreadLock getLock(){
        return this.lock;
    }

    private NodeObject commitNode(long id, long startVersion, long endVersion){
        MVCCObject resO = this.memMVCC.commitNode(id, startVersion, endVersion);
        if(resO.isMaxVerion()){
            Node node;
            try{
                node = db.getNodeById(id);
            }catch (Exception e){
                node = db.createNode(id);//System.out.println("ADD NODE : " + id);
            }
            NodeObject memNode = (NodeObject) resO.getValue();
            if(memNode == null){
                node.delete();
                return null;
            }
            Map<String, Object> updateList = memNode.getNeedUpdate();
            for(Map.Entry<String, Object> entry : updateList.entrySet()){
                try{
                    if((byte)entry.getValue() == DELETESELF){
                        node.removeProperty(entry.getKey());
                    }
                }catch (Exception e){
                }
                node.setProperty(entry.getKey(), entry.getValue());
            }
            memNode.setRealObjectInDB(node);
            memNode.cleanUpdateMap();
            return memNode;
        }
        NodeObject n = (NodeObject)resO.getValue();
        n.setRealObjectInDB(null);
        return n;
    }

    private RelationObject commitRelation(long id, long startVersion, long endVersion, long start, long end){
        MVCCObject resO = this.memMVCC.commitRelation(id, startVersion, endVersion);
        if(resO.isMaxVerion()){
            Relationship r;
            try{
                r = db.getRelationshipById(id);
            }catch (Exception e){
                Node startNode = db.getNodeById(start);
                r = startNode.createRelationshipTo(id, end, RelType.ROAD_TO);
            }

            RelationObject memR = (RelationObject) resO.getValue();
            if(memR == null){
                r.delete();
                return null;
            }
            Map<String, Object> updateList = memR.getNeedUpdate();
            for(Map.Entry<String, Object> entry : updateList.entrySet()){
                try{
                    if((byte)entry.getValue() == DELETESELF){
                        r.removeProperty(entry.getKey());
                    }
                }catch (Exception e){
                }
                r.setProperty(entry.getKey(), entry.getValue());
            }
            memR.cleanUpdateMap();
            memR.setRealObjectInDB(r);
            return memR;
        }
        RelationObject n = (RelationObject)resO.getValue();
        n.setRealObjectInDB(null);
        return n;
    }

    private void commitRelTemp(RelationObject rel, String key, long startVersion, long endVersion, int startTime, int endTime) throws IdNotExistException {
        List<TimeMVCCObject> listO = this.memMVCC.commitAddRelTemp(rel, key, startTime, endTime, startVersion, endVersion);
        Relationship r = rel.getRealObjectInDB();
        if(r == null){
            r = db.getRelationshipById(rel.getId());
        }
        for(TimeMVCCObject o : listO){
            if(o.isMaxVerion()){
                r.setTemporalProperty(key, o.getStartTime(), o.getEndTime(), o.getValue());
            }
        }
    }

    private void commitNodeTemp(NodeObject node, String key, long startVersion, long endVersion, int startTime, int endTime) throws IdNotExistException {
        List<TimeMVCCObject> listO = this.memMVCC.commitAddNodeTemp(node, key, startTime, endTime, startVersion, endVersion);
        Node n = node.getRealObjectInDB();
        if(n == null){
            n = db.getNodeById(node.getId());
        }
        for(TimeMVCCObject o : listO){
            if(o.isMaxVerion()){
                System.out.println("update db time :" + o.getStartTime() + " - " + o.getEndTime());
                n.setTemporalProperty(key, o.getStartTime(), o.getEndTime(), o.getValue());
            }
        }
    }

    private void commitDeleteRelTemp(RelationObject rel, String key, long startVersion, long endVersion) throws IdNotExistException {
        this.memMVCC.commitDeleteRelTemporalProperty(startVersion, endVersion, rel, key);
        Relationship r = rel.getRealObjectInDB();
        if(r == null){
            r = db.getRelationshipById(rel.getId());
        }
        r.removeTemporalProperty(key);
    }

    private void commitDeleteNodeTemp(NodeObject node, String key, long startVersion, long endVersion) throws IdNotExistException {
        this.memMVCC.commitDeleteNodeTemporalProperty(startVersion, endVersion, node, key);
        Node n = node.getRealObjectInDB();
        if(n == null){
            n = db.getNodeById(node.getId());
        }
        n.removeTemporalProperty(key);
    }

    public NodeObject getNode(long version, EntityEntry entityEntry, Map tempMap, boolean isCommit) throws EntityEntryException {
        NodeObject node;
        if(entityEntry.getId() >= 0){
            node =  this.memMVCC.getNodeById(entityEntry.getId(), version, isCommit);
            if(node != null){
                node.setRealObjectInDB(null);
            }
        }
        else if( entityEntry.getId() == -2){ node = (NodeObject) tempMap.get(entityEntry.getParaId());}
        else throw new EntityEntryException(entityEntry);
        if(node == null){
            Node dbNode = db.getNodeById(entityEntry.getId());
            node = new NodeObject(entityEntry.getId(), version);
            node.setRealObjectInDB(dbNode);
            node.initAllProperty(dbNode.getAllProperties());
        }
        return node;
    }

    public RelationObject getRelation(long version, EntityEntry entityEntry, Map tempMap, boolean isCommit) throws EntityEntryException {
        RelationObject relation;
        if(entityEntry.getId() >= 0){
            relation = this.memMVCC.getRelById(entityEntry.getId(), version, isCommit);
            if(relation != null){
                relation.setRealObjectInDB(null);
            }
        }
        else if( entityEntry.getId() == -2){ relation = (RelationObject) tempMap.get(entityEntry.getParaId());}
        else throw new EntityEntryException(entityEntry);
        if(relation == null){
            Relationship rel = db.getRelationshipById(entityEntry.getId());
            relation = new RelationObject(entityEntry.getId(), version, rel.getStartNode().getId(), rel.getEndNode().getId());
            relation.initAllProperty(rel.getAllProperties());
            relation.setRealObjectInDB(rel);
        }
        return relation;
    }

    public void saveToMvcc(long version) throws EntityEntryException, TypeDoesnotExistException, TransactionException, ObjectExistException, IdNotExistException, RegionStoreException {
        //List<EntityEntry> newEntries = new ArrayList<>();
        Transaction transaction = db.beginTx();
        Map<Integer, Object> tempMap = new HashMap<>();
        List<EntityEntry> Entries = op.getEntityEntries();
        for(EntityEntry entityEntry : Entries){
            switch (entityEntry.getType()){
                case NODETYPE:{
                    switch (entityEntry.getOperationType()){
                        case EntityEntry.ADD:{
                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
                                NodeObject node = this.memMVCC.addNode(entityEntry.getId(), version);
                                tempMap.put(entityEntry.getTransactionNum(), node);
                                break;
                            }

                            NodeObject node = getNode(version, entityEntry, tempMap, false);
                            if(entityEntry.isTemporalProperty()){
                                if(entityEntry.getOther() != -1){
                                    this.memMVCC.setNodeTemporalProperty(version, node, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getOther(), entityEntry.getValue());
                                }
                                else
                                    this.memMVCC.setNodeTemporalProperty(version, node, entityEntry.getKey(), entityEntry.getStart(), Long.MAX_VALUE, entityEntry.getValue());
                                break;
                            }
                            else
                                this.memMVCC.setNodeProperty(version, node, entityEntry.getKey(), entityEntry.getValue());
                            break;
                        }
                        case EntityEntry.GET:{
                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
                                NodeObject node = this.memMVCC.getNodeById(entityEntry.getId(), version, false);
                                if(node == null){
                                    Node dbNode = db.getNodeById(entityEntry.getId());
                                    node = new NodeObject(entityEntry.getId(), version);
                                    node.setRealObjectInDB(dbNode);
                                    node.initAllProperty(dbNode.getAllProperties());
                                }
                                tempMap.put(entityEntry.getTransactionNum(), node);
                                break;
                            }

                            NodeObject node = getNode(version, entityEntry, tempMap, false);

                            if(entityEntry.isTemporalProperty()){
                                Object res = this.memMVCC.getNodeTemporalProperty(version, node, entityEntry.getKey(), entityEntry.getStart());
                                if(res == null){
                                    res = node.getRealObjectInDB().getTemporalProperty(entityEntry.getKey(), entityEntry.getStart());
                                }
                                tempMap.put(entityEntry.getTransactionNum(), res);
                                resultMap.put(entityEntry.getTransactionNum(), res);
                            }
                            else {
                                Object res = node.getProperty(entityEntry.getKey());
                                if(res == null){
                                    res = node.getRealObjectInDB().getProperty(entityEntry.getKey());
                                }
                                tempMap.put(entityEntry.getTransactionNum(), res);
                                resultMap.put(entityEntry.getTransactionNum(), res);
                            }
                            break;
                        }
                        case EntityEntry.REMOVE:{
                            NodeObject node = getNode(version, entityEntry, tempMap, false);

                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
                                this.memMVCC.deleteNode(version, node.getId());
                            }
                            else if(entityEntry.isTemporalProperty()){
                                this.memMVCC.deleteNodeTemporalProperty(version, node, entityEntry.getKey());
                            }
                            else this.memMVCC.deleteNodeProperty(version, node, entityEntry.getKey());
                            break;
                        }
                        case EntityEntry.SET:{
                            NodeObject node = getNode(version, entityEntry, tempMap, false);

                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING))throw new EntityEntryException(entityEntry);
                            if(entityEntry.isTemporalProperty()){
                                if(entityEntry.getOther() != -1){
                                    this.memMVCC.setNodeTemporalProperty(version, node, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getOther(), entityEntry.getValue());
                                }
                                else
                                    this.memMVCC.setNodeTemporalProperty(version, node, entityEntry.getKey(), entityEntry.getStart(), Long.MAX_VALUE, entityEntry.getValue());
                                break;
                            }
                            else
                                this.memMVCC.setNodeProperty(version, node, entityEntry.getKey(), entityEntry.getValue());
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
                                RelationObject relation = this.memMVCC.addRelationship(version, entityEntry.getStart(), entityEntry.getOther(), entityEntry.getId());
                                if(relation == null){
                                    Relationship rel = db.getRelationshipById(entityEntry.getId());
                                    relation = new RelationObject(entityEntry.getId(), version, rel.getStartNode().getId(), rel.getEndNode().getId());
                                    relation.initAllProperty(rel.getAllProperties());
                                    relation.setRealObjectInDB(rel);
                                }
                                tempMap.put(entityEntry.getTransactionNum(), relation);
                                break;
                            }

                            RelationObject relationship = getRelation(version, entityEntry, tempMap, false);

                            if(entityEntry.isTemporalProperty()){
                                if(entityEntry.getOther() != -1){
                                    this.memMVCC.setRelationTemporalProperty(version, relationship, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getOther(), entityEntry.getValue());
                                }
                                else
                                    this.memMVCC.setRelationTemporalProperty(version, relationship, entityEntry.getKey(), entityEntry.getStart(), Long.MAX_VALUE, entityEntry.getValue());
                                break;
                            }
                            else
                                this.memMVCC.setRelationProperty(version, relationship, entityEntry.getKey(), entityEntry.getValue());
                            break;
                        }
                        case EntityEntry.GET:{
                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
                                RelationObject relationship = this.memMVCC.getRelById(entityEntry.getId(), version, false);
                                if(relationship == null){
                                    Relationship rel = db.getRelationshipById(entityEntry.getId());
                                    relationship = new RelationObject(entityEntry.getId(), version, rel.getStartNode().getId(), rel.getEndNode().getId());
                                    relationship.initAllProperty(rel.getAllProperties());
                                    relationship.setRealObjectInDB(rel);
                                }
                                tempMap.put(entityEntry.getTransactionNum(), relationship);
                                break;
                            }

                            RelationObject relationship = getRelation(version, entityEntry, tempMap, false);

                            if(entityEntry.isTemporalProperty()){
                                Object res = this.memMVCC.getRelationTemporalProperty(version, relationship, entityEntry.getKey(), entityEntry.getStart());
                                if(res == null){
                                    res = relationship.getRealObjectInDB().getTemporalProperty(entityEntry.getKey(), entityEntry.getStart());
                                }
                                resultMap.put(entityEntry.getTransactionNum(), res);
                                tempMap.put(entityEntry.getTransactionNum(), res);
                            }
                            else {
                                Object res = this.memMVCC.getRelationProperty(version, relationship, entityEntry.getKey());
                                if(res == null){
                                    res = relationship.getRealObjectInDB().getTemporalProperty(entityEntry.getKey(), entityEntry.getStart());
                                }
                                resultMap.put(entityEntry.getTransactionNum(), res);
                                tempMap.put(entityEntry.getTransactionNum(), res);
                            }
                            break;
                        }
                        case EntityEntry.REMOVE:{
                            RelationObject relationship = getRelation(version, entityEntry, tempMap, false);

                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
                                this.memMVCC.deleteRelation(version, entityEntry.getId());
                            }
                            else if(entityEntry.isTemporalProperty()){
                                this.memMVCC.deleteRelTemporalProperty(version, relationship, entityEntry.getKey());
                            }
                            else
                                this.memMVCC.deleteRelProperty(version, relationship, entityEntry.getKey());
                            break;
                        }
                        case EntityEntry.SET:{
                            RelationObject relationship = getRelation(version, entityEntry, tempMap, false);

                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING))throw new EntityEntryException(entityEntry);
                            if(entityEntry.isTemporalProperty()){
                                if(entityEntry.getOther() != -1){
                                    this.memMVCC.setRelationTemporalProperty(version, relationship, entityEntry.getKey(), entityEntry.getStart(), entityEntry.getOther(), entityEntry.getValue());
                                }
                                else
                                    this.memMVCC.setRelationTemporalProperty(version, relationship, entityEntry.getKey(), entityEntry.getStart(), Long.MAX_VALUE, entityEntry.getValue());
                                break;
                            }
                            else
                                this.memMVCC.setRelationProperty(version, relationship, entityEntry.getKey(), entityEntry.getValue());
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
        transaction.success();
        transaction.close();
    }

    public void commitMVCC(long startVersion, long endVersion) throws EntityEntryException, IdNotExistException, TypeDoesnotExistException {
        Transaction transaction = db.beginTx();
        Map<Integer, Object> tempMap = new HashMap<>();
        List<EntityEntry> Entries = op.getEntityEntries();
        for(EntityEntry entityEntry : Entries){
            switch (entityEntry.getType()){
                case NODETYPE:{
                    switch (entityEntry.getOperationType()){
                        case EntityEntry.ADD:{
                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
                                if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
                                NodeObject node = commitNode(entityEntry.getId(), startVersion, endVersion);
                                tempMap.put(entityEntry.getTransactionNum(), node);
                                break;
                            }

                            NodeObject node = getNode(startVersion, entityEntry, tempMap, true);
                            if(entityEntry.isTemporalProperty()){
                                if(entityEntry.getOther() != -1){
                                    commitNodeTemp(node, entityEntry.getKey(), startVersion, endVersion, entityEntry.getStart(), entityEntry.getOther());
                                }
                                else
                                    commitNodeTemp(node, entityEntry.getKey(), startVersion, endVersion, entityEntry.getStart(), Integer.MAX_VALUE);
                                break;
                            }
                            else
                                commitNode(node.getId(), startVersion, endVersion);
                            break;
                        }
                        case EntityEntry.GET:{
                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
                                NodeObject node = getNode(startVersion, entityEntry, tempMap, true);
                                tempMap.put(entityEntry.getTransactionNum(), node);
                                break;
                            }

                            NodeObject node = getNode(startVersion, entityEntry, tempMap, true);

                            if(entityEntry.isTemporalProperty()){
                                Object res = this.memMVCC.getNodeTemporalProperty(startVersion, node, entityEntry.getKey(), entityEntry.getStart());
                                if(res == null){
                                    //if(node == null)System.out.println("node is null");
                                    System.out.println("get temp pro:" + entityEntry.getKey() + ", time :" + entityEntry.getStart());
                                    res = node.getRealObjectInDB().getTemporalProperty(entityEntry.getKey(), entityEntry.getStart());
                                }
                                tempMap.put(entityEntry.getTransactionNum(), res);
                            }
                            else {
                                Object res = this.memMVCC.getNodeProperty(node, entityEntry.getKey());
                                if(res == null){
                                    res = node.getRealObjectInDB().getProperty(entityEntry.getKey());
                                }
                                tempMap.put(entityEntry.getTransactionNum(), res);
                            }
                            break;
                        }
                        case EntityEntry.REMOVE:{
                            //NodeObject node = getNode(startVersion, entityEntry, tempMap);

                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
                                commitNode(entityEntry.getId(), startVersion, endVersion);
                            }
                            else if(entityEntry.isTemporalProperty()){
                                commitDeleteNodeTemp(null, entityEntry.getKey(), startVersion, endVersion);
                            }
                            else commitNode(entityEntry.getId(), startVersion, endVersion);
                            break;
                        }
                        case EntityEntry.SET:{
                            NodeObject node = getNode(startVersion, entityEntry, tempMap, true);

                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING))throw new EntityEntryException(entityEntry);
                            if(entityEntry.isTemporalProperty()){
                                if(entityEntry.getOther() != -1){
                                    commitNodeTemp(node, entityEntry.getKey(), startVersion, endVersion, entityEntry.getStart(), entityEntry.getOther());
                                }
                                else
                                    commitNodeTemp(node, entityEntry.getKey(), startVersion, endVersion, entityEntry.getStart(), Integer.MAX_VALUE);
                                break;
                            }
                            else
                                commitNode(node.getId(), startVersion, endVersion);
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
                                RelationObject rel = commitRelation(entityEntry.getId(), startVersion, endVersion, entityEntry.getStart(), entityEntry.getOther());
                                tempMap.put(entityEntry.getTransactionNum(), rel);
                                break;
                            }

                            RelationObject relationship = getRelation(startVersion, entityEntry, tempMap, true);

                            if(entityEntry.isTemporalProperty()){
                                if(entityEntry.getOther() != -1){
                                    commitRelTemp(relationship, entityEntry.getKey(), startVersion, endVersion, entityEntry.getStart(), entityEntry.getOther());
                                }
                                else
                                    commitRelTemp(relationship, entityEntry.getKey(), startVersion, endVersion, entityEntry.getStart(), Integer.MAX_VALUE);
                                break;
                            }
                            else{
                                commitRelation(relationship.getId(), startVersion, endVersion, relationship.getStartNode(), relationship.getEndNode());
                            }
                            break;
                        }
                        case EntityEntry.GET:{
                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
                                RelationObject relationship = getRelation(startVersion, entityEntry, tempMap, true);
                                tempMap.put(entityEntry.getTransactionNum(), relationship);
                                break;
                            }

                            RelationObject relationship = getRelation(startVersion, entityEntry, tempMap, true);

                            if(entityEntry.isTemporalProperty()){
                                Object res = this.memMVCC.getRelationTemporalProperty(startVersion, relationship, entityEntry.getKey(), entityEntry.getStart());
                                if(res == null){
                                    res = relationship.getRealObjectInDB().getTemporalProperty(entityEntry.getKey(), entityEntry.getStart());
                                }
                                tempMap.put(entityEntry.getTransactionNum(), res);
                            }
                            else {
                                Object res = this.memMVCC.getRelationProperty(startVersion, relationship, entityEntry.getKey());
                                if(res == null){
                                    res = relationship.getRealObjectInDB().getTemporalProperty(entityEntry.getKey(), entityEntry.getStart());
                                }
                                tempMap.put(entityEntry.getTransactionNum(), res);
                            }
                            break;
                        }
                        case EntityEntry.REMOVE:{
                            //RelationObject relationship = getRelation(startVersion, entityEntry, tempMap);

                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
                                commitRelation(entityEntry.getId(), startVersion, endVersion, entityEntry.getStart(), entityEntry.getOther());
                            }
                            else if(entityEntry.isTemporalProperty()){
                                commitDeleteRelTemp(null, entityEntry.getKey(), startVersion, endVersion);
                            }
                            else {
                                commitRelation(entityEntry.getId(), startVersion, endVersion, entityEntry.getStart(), entityEntry.getOther());
                            }
                            break;
                        }
                        case EntityEntry.SET:{
                            RelationObject relationship = getRelation(startVersion, entityEntry, tempMap, true);

                            if(entityEntry.getKey().equals(DTGConstants.NULLSTRING))throw new EntityEntryException(entityEntry);
                            if(entityEntry.isTemporalProperty()){
                                if(entityEntry.getOther() != -1){
                                    commitRelTemp(relationship, entityEntry.getKey(), startVersion, endVersion, entityEntry.getStart(), entityEntry.getOther());
                                }
                                else
                                    commitRelTemp(relationship, entityEntry.getKey(), startVersion, endVersion, entityEntry.getStart(), Integer.MAX_VALUE);
                                break;
                            }
                            else{
                                commitRelation(relationship.getId(), startVersion, endVersion, relationship.getStartNode(), relationship.getEndNode());
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
        transaction.success();
        transaction.close();
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

    public Boolean getCouldCommit() {
        return couldCommit;
    }
}

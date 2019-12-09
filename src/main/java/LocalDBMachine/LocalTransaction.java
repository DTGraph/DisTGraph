package LocalDBMachine;

import DBExceptions.EntityEntryException;
import DBExceptions.RegionStoreException;
import DBExceptions.TypeDoesnotExistException;
import Element.DTGOperation;
import Element.EntityEntry;
import LocalDBMachine.LocalTx.TransactionThreadLock;
import Region.DTGRegion;
import config.RelType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

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
    }

    @Override
    public void run() {
        try {
            transaction = db.beginTx();
            synchronized (resultMap){
                getTransaction();
                resultMap.notify();
            }
            synchronized (lock){
                if(couldCommit == false){
                    //System.out.println("aaaaaaaa" + lock.getTxId());
                    lock.wait();
                    couldCommit = true;
                    //System.out.println("Start commit in thread");
                    if(lock.isShouldCommit()){
                        commit();
                    }
                    else {
                        rollback();
                    }
                    //System.out.println("end commit in thread");
                }
            }
            synchronized (lock.getCommitLock()){
                lock.getCommitLock().notify();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (EntityEntryException e) {
            transaction.failure();
        } catch (RegionStoreException e) {
            transaction.failure();
        } catch (TypeDoesnotExistException e) {
            transaction.failure();
        }
    }

    public Node addNode(long id, int i) throws RegionStoreException {
        region.addNode();
        nodeTransactionAdd++;
        actionId[i] = id;
        actionType[i] = 1;
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
        return start.createRelationshipTo(id, endNode, RelType.ROAD_TO);
    }

    public void setNodeProperty(Node node, String key, Object value){
        node.setProperty(key, value);
    }

    public void setNodeTemporalProperty(Node node, String key, int start, int end,  Object value){
        node.setTemporalProperty(key, start, end, value);
    }

    public void setNodeTemporalProperty(Node node, String key, int time,  Object value){
        node.setTemporalProperty(key, time, value);
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

    public void deleteNodeTemporalProperty(Node node, String key ){
        node.removeTemporalProperty(key);
    }

    public Object getNodeProperty(Node node, String key){
        return node.getProperty(key);
    }

    public Object getNodeTemporalProperty(Node node, String key, int time){
        return node.getTemporalProperty(key, time);
    }

    public void setRelationProperty(Relationship relationship, String key, Object value){
        relationship.setProperty(key, value);
    }

    public void setRelationTemporalProperty(Relationship relationship, String key, int start, int end,  Object value){
        relationship.setTemporalProperty(key, start, end, value);
    }

    public void setRelationTemporalProperty(Relationship relationship, String key, int time,  Object value){
        relationship.setTemporalProperty(key, time, value);
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
        return relationship.getProperty(key);
    }

    public Object getRelationTemporalProperty(Relationship relationship, String key, int time){
        return relationship.getTemporalProperty(key, time);
    }

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public void commit() throws TypeDoesnotExistException {
        //System.out.println("commit");
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
        transaction.success();
        transaction.close();
    }

    public void rollback(){
        //System.out.println("rollback");
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
                                tempMap.put(entityEntry.getTransactionNum(), node);//System.out.println("new node id = " + entityEntry.getId());
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
}

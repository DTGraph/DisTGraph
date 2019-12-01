package LocalDBMachine;

import config.RelType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

/**
 * @author :jinkai
 * @date :Created in 2019/10/17 19:15
 * @description:
 * @modified By:
 * @version:
 */

public class LocalTransactiona {

    private GraphDatabaseService db;
    private Transaction tx;
    private long txId;

    public LocalTransactiona(GraphDatabaseService db){
        this.db = db;
        tx = db.beginTx();
    }

    public Node addNode(long id){
        return db.createNode(id);
    }

    public Node getNodeById(long id){
        return db.getNodeById(id);
    }

    public Relationship getRelationshipById(long id){
        return db.getRelationshipById(id);
    }

    public Relationship addRelationship(long startNode, long endNode, long id){
        Node start = getNodeById(startNode);
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

    public void deleteNode(Node node){
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

    public void deleteRelation(Relationship relationship){
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

    public long getTxId() {
        return txId;
    }

    public void setTxId(long txId) {
        this.txId = txId;
    }

    public void commit(){
        tx.success();
        tx.close();
    }

    public void rollback(){
        tx.failure();
        tx.close();
    }
}

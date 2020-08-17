package LocalDBMachine;

import DBExceptions.RegionStoreException;
import Element.EntityEntry;
import config.DTGConstants;
import config.RelType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static config.MainType.NODETYPE;
import static config.MainType.RELATIONTYPE;

public class MemMVCC implements AutoCloseable {

    private final ConcurrentHashMap<String, Object> MVCCMap;

    public MemMVCC(){
        this.MVCCMap = new ConcurrentHashMap<>();
    }

    @Override
    public void close() throws Exception {

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


}

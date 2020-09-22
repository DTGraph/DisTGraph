package LocalDBMachine;

import DBExceptions.IdNotExistException;
import DBExceptions.ObjectExistException;
import DBExceptions.RegionStoreException;
import Element.EntityEntry;
import config.DTGConstants;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import tool.MemMvcc.*;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static config.DTGConstants.DELETESELF;
import static config.MainType.NODETYPE;
import static config.MainType.RELATIONTYPE;

public class MemMVCC implements AutoCloseable {

    private final ConcurrentHashMap<String, DTGSortedList> MVCCStaticMap;
    private final ConcurrentHashMap<String, Object> MVCCNodeTemPropertyMap;
    private final ConcurrentHashMap<String, Object> MVCCRelTemPropertyMap;

    public MemMVCC(){
        this.MVCCStaticMap = new ConcurrentHashMap<>();
        this.MVCCNodeTemPropertyMap = new ConcurrentHashMap<>();
        this.MVCCRelTemPropertyMap = new ConcurrentHashMap<>();
    }

    @Override
    public void close() throws Exception {

    }

    private String getStaticKey(byte type, long objectId, String key){
        return type + "/" + objectId + "/" + key;
    }

    private String getTempKey(long objectId, String key){
        return objectId + "/" + key;
    }

    private String getTempTimeKey(long startTime, long endTime){
        return startTime + "/" + endTime;
    }

    private TempSortedList getMap(byte type, String key, long version) throws IdNotExistException {
        Object map;
        if(type == NODETYPE){
            map = MVCCNodeTemPropertyMap.get(key);
        }else if(type == RELATIONTYPE){
            map = MVCCNodeTemPropertyMap.get(key);
        }else{
            return null;
        }
        TempSortedList returnMap;
        if(map instanceof DeleteInfo){
            if(((DeleteInfo) map).isVisible(version)){
                returnMap = (TempSortedList)((DeleteInfo) map).getOldValue();
            }else {
                throw new IdNotExistException();
            }
        }else{
            returnMap = (TempSortedList)map;
        }
        if(map == null){
            returnMap = new TempSortedList();
            if(type == NODETYPE){
                MVCCNodeTemPropertyMap.put(key, returnMap);
            }else if(type == RELATIONTYPE){
                MVCCNodeTemPropertyMap.put(key, returnMap);
            }
        }
        return returnMap;
    }

    public NodeObject addNode(long id, long version) throws ObjectExistException {
        String key = getStaticKey(NODETYPE, id, "");
        DTGSortedList list = MVCCStaticMap.get(key);
        NodeObject node = new NodeObject(id, version);
        MVCCObject o;
        if(list == null){
            list = new DTGSortedList();
            o = new MVCCObject(version, node);
            list.insert(o);
            this.MVCCStaticMap.put(key, list);
        }else {
            throw new ObjectExistException();
        }
        return (NodeObject) o.getValue();
    }

    public MVCCObject commitNode(long id, long startVersion, long endVersion) {
        String key = getStaticKey(NODETYPE, id, "");
        DTGSortedList list = MVCCStaticMap.get(key);
        return list.commitObject(startVersion, endVersion);
    }

    public NodeObject getNodeById(long id, long version, boolean isCommit) {
        String key = getStaticKey(NODETYPE, id, "");
        DTGSortedList list = MVCCStaticMap.get(key);
        if(list == null){
            return null;
        }else {
            Object res;//list.printList();list.printCommitList();//System.out.println(version);System.out.println(id);
            if(isCommit){
                MVCCObject o = list.find(version);
                if(o == null){
                    res = list.findData(version).getValue();
                }else{
                    res = o.getValue();
                }
            }else{
                res = list.findData(version).getValue();
            }

            if(res == null){
                return null;
            }
            return (NodeObject)res;
        }
    }

    public RelationObject getRelById(long id, long version, boolean isCommit) {
        String key = getStaticKey(RELATIONTYPE, id, "");
        DTGSortedList list = MVCCStaticMap.get(key);
        if(list == null){
            return null;
        }else {
            Object res;
            if(isCommit){
                res = list.find(version).getValue();
            }else{
                res = list.findData(version).getValue();
            }
            if(res == null){
                return null;
            }
            return (RelationObject) res;
        }
    }

    public RelationObject addRelationship(long version, long startNode, long endNode, long id) throws ObjectExistException {
        String key = getStaticKey(RELATIONTYPE, id, "");
        DTGSortedList list = MVCCStaticMap.get(key);
        RelationObject r = new RelationObject(id, version, startNode,endNode);
        MVCCObject o;
        if(list == null){
            list = new DTGSortedList();
            o = new MVCCObject(version, r);
            list.insert(o);
            this.MVCCStaticMap.put(key, list);
        }else {
            throw new ObjectExistException();
        }
        return r;
    }

    public MVCCObject commitRelation(long id, long startVersion, long endVersion) {
        String key = getStaticKey(RELATIONTYPE, id, "");
        DTGSortedList list = MVCCStaticMap.get(key);
        return list.commitObject(startVersion, endVersion);
    }

    public void setNodeProperty(long version, NodeObject node, String key, Object value){
        String mapkey = getStaticKey(NODETYPE, node.getId(), "");
        DTGSortedList list = MVCCStaticMap.get(mapkey);
        GraphObject newNode;
        if(node.getVersion() != version){
            newNode = node.copy();
        }else{
            newNode = node;
        }
        newNode.setProperty(key, value);
        if(list == null) {
            list = new DTGSortedList();
            this.MVCCStaticMap.put(mapkey, list);
        }
        if(newNode != node){
            MVCCObject o = new MVCCObject(version, newNode);
            list.insert(o);
        }
    }

    public void setNodeTemporalProperty(long version, NodeObject node, String key, long start, long end,  Object value) throws IdNotExistException {
        String listKey = getTempKey(node.getId(), key);
        TempSortedList map = getMap(NODETYPE, listKey, version);
        if(map == null)return;
        map.insert(version, value, start, end);
    }

    public List<TimeMVCCObject> commitAddNodeTemp(NodeObject node, String key, long startTime, long endTime, long startVersion, long endVersion) throws IdNotExistException {
        String listKey = getTempKey(node.getId(), key);
        TempSortedList map = getMap(NODETYPE, listKey, startVersion);
        if(map == null)return null;
        return map.commitObject(startVersion, endVersion, startTime, endTime);
    }

    public void deleteNode(long version, long id) throws IdNotExistException {
        String key = getStaticKey(NODETYPE, id, "");
        DTGSortedList list = MVCCStaticMap.get(key);
        MVCCObject o;
        if(list == null){
            list = new DTGSortedList();
            o = new MVCCObject(version, null);
            list.insert(o);
            this.MVCCStaticMap.put(key, list);
        }else {
            o = new MVCCObject(version, null);
            list.insert(o);
        }
    }

    public void deleteNodeProperty(long version, NodeObject node, String key){
        String mapkey = getStaticKey(NODETYPE, node.getId(), "");
        DTGSortedList list = MVCCStaticMap.get(mapkey);
        MVCCObject o;
        GraphObject newNode;
        if(node.getVersion() != version){
            newNode = node.copy();
        }else{
            newNode = node;
        }
        newNode.removeProperty(key);
        if(list == null) {
            list = new DTGSortedList();
            this.MVCCStaticMap.put(mapkey, list);
        }
        if(newNode != node){
            o = new MVCCObject(version, newNode);
            list.insert(o);
        }
    }

    public void deleteNodeTemporalProperty(long version, NodeObject node, String key) throws IdNotExistException {
        String listKey = getTempKey(node.getId(), key);
        TempSortedList map = getMap(NODETYPE, listKey, version);
        if(map == null)throw new IdNotExistException();
        DeleteInfo deleteInfo = new DeleteInfo(map, version);
        this.MVCCNodeTemPropertyMap.put(listKey, deleteInfo);
    }

    public void commitDeleteNodeTemporalProperty(long startVersion, long endVersion, NodeObject node, String key){
        //need or not ?
    }

    public Object getNodeProperty(NodeObject node, String key){
        return node.getProperty(key);
    }

    public Object getNodeTemporalProperty(long version, NodeObject node, String key, long time) throws IdNotExistException {
        String listKey = getTempKey(node.getId(), key);
        TempSortedList map = getMap(NODETYPE, listKey, version);
        return map.get(version, time);
    }

    public void setRelationProperty(long version, RelationObject relationship, String key, Object value){
        String mapkey = getStaticKey(RELATIONTYPE, relationship.getId(), "");
        DTGSortedList list = MVCCStaticMap.get(mapkey);
        GraphObject newRel;
        if(relationship.getVersion() != version){
            newRel = relationship.copy();
        }else{
            newRel = relationship;
        }
        newRel.setProperty(key, value);
        if(list == null) {
            list = new DTGSortedList();
            this.MVCCStaticMap.put(mapkey, list);
        }
        if(newRel != relationship){
            MVCCObject o = new MVCCObject(version, newRel);
            list.insert(o);
        }
    }

    public void setRelationTemporalProperty(long version, RelationObject relationship, String key, long start, long end,  Object value) throws IdNotExistException {
        String listKey = getTempKey(relationship.getId(), key);
        TempSortedList map = getMap(RELATIONTYPE, listKey, version);
        if(map == null)return;
        map.insert(version, value, start, end);
    }

    public List<TimeMVCCObject> commitAddRelTemp(RelationObject relationship, String key, long startTime, long endTime, long startVersion, long endVersion) throws IdNotExistException {
        String listKey = getTempKey(relationship.getId(), key);
        TempSortedList map = getMap(RELATIONTYPE, listKey, startVersion);
        if(map == null)return null;
        return map.commitObject(startVersion, endVersion, startTime, endTime);
    }

    public void deleteRelation(long version, long id){
        String key = getStaticKey(RELATIONTYPE, id, "");
        DTGSortedList list = MVCCStaticMap.get(key);
        MVCCObject o;
        if(list == null){
            list = new DTGSortedList();
            o = new MVCCObject(version, null);
            list.insert(o);
            this.MVCCStaticMap.put(key, list);
        }else {
            o = new MVCCObject(version, null);
            list.insert(o);
        }
    }

    public Object getRelationProperty(long version, RelationObject relationship, String key) throws IdNotExistException {
        return relationship.getProperty(key);
    }

    public Object getRelationTemporalProperty(long version, RelationObject relationship, String key, long time) throws IdNotExistException {
        String listKey = getTempKey(relationship.getId(), key);
        TempSortedList map = getMap(RELATIONTYPE, listKey, version);
        return map.get(version, time);
    }

    public void deleteRelTemporalProperty(long version, RelationObject relationship, String key) throws IdNotExistException {
        String listKey = getTempKey(relationship.getId(), key);
        TempSortedList map = getMap(RELATIONTYPE, listKey, version);
        if(map == null)throw new IdNotExistException();
        DeleteInfo deleteInfo = new DeleteInfo(map, version);
        this.MVCCRelTemPropertyMap.put(listKey, deleteInfo);
    }

    public void commitDeleteRelTemporalProperty(long startVersion, long endVersion, RelationObject r, String key){
        //need or not ?
    }

    public void deleteRelProperty(long version, RelationObject relation, String key){
        String mapkey = getStaticKey(RELATIONTYPE, relation.getId(), "");
        DTGSortedList list = MVCCStaticMap.get(mapkey);
        GraphObject newRel;
        if(relation.getVersion() != version){
            newRel = relation.copy();
        }else{
            newRel = relation;
        }
        newRel.removeProperty(key);
        if(list == null) {
            list = new DTGSortedList();
            this.MVCCStaticMap.put(mapkey, list);
        }
        if(newRel != relation){
            MVCCObject o = new MVCCObject(version, newRel);
            list.insert(o);
        }
    }

    public void clean(long version){
        //this.MVCCStaticMap.c
    }

    private class DeleteInfo{
        private Object oldValue;
        private long startVersion;
        private long endVersionversion;

        public DeleteInfo(Object oldValue, long version){
            this.startVersion = version;
            this.oldValue = oldValue;
        }

        public boolean isVisible(long requestVersion){
            return this.startVersion > requestVersion;
        }

        public Object getOldValue(){
            return this.oldValue;
        }
    }


}


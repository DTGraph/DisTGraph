package Region;

import DBExceptions.EntityEntryException;
import DBExceptions.TransactionException;
import DBExceptions.TypeDoesnotExistException;
import Element.DTGOperation;
import Element.EntityEntry;
import MQ.codec.v2.MQV2LogEntryCodecFactory;
import UserClient.TransactionManager;
import com.alipay.sofa.jraft.rhea.client.FutureHelper;
import config.DTGConstants;
import options.MQLogStorageOptions;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static config.MainType.NODETYPE;
import static config.MainType.RELATIONTYPE;


public class RegionSet {

    //private static HashMap save = new HashMap<String, Object>();
    private String path;
    private RegionSet2 rsave;
    ExecutorService pool;
    private long storeid;
    private TransactionManager versionManager;

    public RegionSet(String path, long storeid, TransactionManager versionManager){
        this.path = path;
        rsave = new RegionSet2(path);
        MQLogStorageOptions lsOpts = new MQLogStorageOptions();
        lsOpts.setLogEntryCodecFactory(MQV2LogEntryCodecFactory.getInstance());
        rsave.init(lsOpts);
        pool = Executors.newFixedThreadPool(3);
        this.storeid = storeid;
        this.versionManager = versionManager;
        System.out.println("store id is : " + storeid);
    }



    public Map<Integer, Object> processTx(final DTGOperation op) throws EntityEntryException, TypeDoesnotExistException {
        if(!op.isIsolateRead()){
            return null;
        }
        List<EntityEntry> entityEntries = op.getEntityEntries();//System.out.println("checking");
        Map<Integer, Object> res = check(entityEntries, op.getTxId(), op.getMainRegionId());
//        try {
//            System.out.println("start sleep....");
//            Thread.sleep(1000000);
//            System.out.println("end sleep....");
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        if(res == null)throw new EntityEntryException();
        else if((boolean)res.get(-1) == true){
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    getTransaction(entityEntries, op.getTxId());
//
//                    try {
//                        System.out.println("start sleep....");
//                        Thread.sleep(1000000);
//                        System.out.println("end sleep....");
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
                    //System.out.println("tx commit:" + op.getTxId());
                }
            });
        }
        return res;
    }

    private long getEndVersion() throws TransactionException {
        long version;
        CompletableFuture<Long> future = new CompletableFuture();
        versionManager.applyRequestVersion(future);
        version = FutureHelper.get(future);
        if(version == -1){
            throw new TransactionException("get an error version!");
        }
        return version;
    }

    private String getKey(byte type, boolean isT, long id, String key){
        String res = type+ "-" + isT + "-" + id + "-" + key;
        //System.out.println(res);
        return res;
    }

//    private void setTemporalProperty(String key, Object value, long start, long end){
//        List<TimeObject> list = (List)save.get(key);
//        if(list == null){
//            list = new LinkedList<>();
//            save.put(key, list);
//        }
//        int n;
//        for(n = 0; n < list.size(); n++){
//            TimeObject object = list.get(n);
//            if(object.time >= start){
//                break;
//            }
//        }
//        if(n >= list.size()){
//            TimeObject object = new TimeObject();
//            object.o = value;
//            object.time = start;
//            list.add(object);
//        }else{
//            TimeObject object = list.get(n);
//            if(object.time == start)object.o = value;
//            else{
//                object = new TimeObject();
//                object.o = value;
//                object.time = start;
//                list.add(n, object);
//            }
//        }
//        while(n < list.size()){
//            TimeObject object = list.get(n);
//            if(object.time >= end)break;
//            object.o = value;
//            n++;
//        }
//        if(n < list.size()){
//            TimeObject object = list.get(n);
//            if(object.time == end)object.o = value;
//            else{
//                object = new TimeObject();
//                object.o = value;
//                object.time = end;
//                list.add(n, object);
//            }
//        }else{
//            TimeObject object = list.get(n-1);
//            if(object.time < end){
//                object = new TimeObject();
//                object.o = value;
//                object.time = end;
//                list.add(n, object);
//            }
//        }
//        rsave.appendEntry(key, list);
//    }

    private void setTemporalProperty(String key, Object value, long start, long end){
        List<TimeObject> list = (List)rsave.getEntry(key);
        if(list == null){
            list = new LinkedList<>();
        }
        int n;
        for(n = 0; n < list.size(); n++){
            TimeObject object = list.get(n);
            if(object.time >= start){
                break;
            }
        }
        if(n >= list.size()){
            TimeObject object = new TimeObject();
            object.o = value;
            object.time = start;
            list.add(object);
        }else{
            TimeObject object = list.get(n);
            if(object.time == start)object.o = value;
            else{
                object = new TimeObject();
                object.o = value;
                object.time = start;
                list.add(n, object);
            }
        }
        while(n < list.size()){
            TimeObject object = list.get(n);
            if(object.time >= end)break;
            object.o = value;
            n++;
        }
        if(n < list.size()){
            TimeObject object = list.get(n);
            if(object.time == end)object.o = value;
            else{
                object = new TimeObject();
                object.o = value;
                object.time = end;
                list.add(n, object);
            }
        }else{
            TimeObject object = list.get(n-1);
            if(object.time < end){
                object = new TimeObject();
                object.o = value;
                object.time = end;
                list.add(n, object);
            }
        }
        rsave.appendEntry(key, list);
    }


//    private Object getTemporalProperty(String key, long time, EntityEntry entry) throws EntityEntryException {
//        List<TimeObject> list = (List)save.get(key);
//        if(list == null){
//            throw new EntityEntryException(entry);
//        }
//        for(int i = 0; i < list.size(); i++){
//            TimeObject object = list.get(i);
//            if(object.time == time){
//                return object.o;
//            }else if(object.time > time){
//                if(i > 0){
//                    return list.get(i-1).o;
//                }else {
//                    return null;
//                }
//            }
//        }
//        return null;
//    }

    private Object getTemporalProperty(String key, long time, EntityEntry entry) throws EntityEntryException {
        List<TimeObject> list = (List)rsave.getEntry(key);
        if(list == null){
            throw new EntityEntryException(entry);
        }
        for(int i = 0; i < list.size(); i++){
            TimeObject object = list.get(i);
            if(object.time == time){
                return object.o;
            }else if(object.time > time){
                if(i > 0){
                    return list.get(i-1).o;
                }else {
                    return null;
                }
            }
        }
        return null;
    }

//    private void getNodeRel(byte type, long id, EntityEntry entry) throws EntityEntryException {
//        String key = getKey(type, false, id, DTGConstants.NULLSTRING);
//        if(!save.containsKey(key)){
//            throw new EntityEntryException(entry);
//        }else{
//            save.put(key, "node");
//        }
//    }

    private void getNodeRel(byte type, long id, EntityEntry entry,  Map<String, Object> tempProMap) throws EntityEntryException {

        String key = getKey(type, false, id, DTGConstants.NULLSTRING);
        //System.out.println("get node : " + key);
        if(tempProMap.containsKey(key))return;
        else if(rsave.getEntry(key) == null){
            throw new EntityEntryException(entry);
        }
//        else{
//            rsave.appendEntry(key, "node");
//        }
    }

    private List<Long> getNodeRelation(long id, boolean isStart){
        if(isStart){
            return (List)rsave.getEntry("Node-" + id + "-start");
        }
        return (List)rsave.getEntry("Node-" + id + "-end");
    }

    private long getRelationNode(long id, boolean isStart){
        if(isStart){
            //System.out.println("Relation-" + id + "-start");
            return (long)rsave.getEntry("Relation-" + id + "-start");
        }//System.out.println("Relation-" + id + "-end");
        return (long)rsave.getEntry("Relation-" + id + "-end");
    }

    private void setNodeRelation(long id, boolean isStart, long relationId){
        List<Long> list;
        String key = "";
        if(isStart){
            key = "Node-" + id + "-start";
        }else{
            key = "Node-" + id + "-end";
        }
        list = (List)rsave.getEntry(key);
        if(list == null){
            list = new ArrayList<>();
        }
        list.add(relationId);
        rsave.appendEntry(key, list);
    }

    private void setRelationNode(long id, boolean isStart, long nodeId){
        String key = "";
        if(isStart){
            key = "Relation-" + id + "-start";
        }else{
            key = "Relation-" + id + "-end";
        }//System.out.println(key + " : " + nodeId);
        rsave.appendEntry(key, nodeId);
    }

//    public Map<Integer, Object> getTransaction(List<EntityEntry> Entries) throws EntityEntryException, TypeDoesnotExistException {
//        Map<Integer, Object> resultMap = new HashMap<>();
//        for(EntityEntry entityEntry : Entries){
//            switch (entityEntry.getOperationType()){
//                case EntityEntry.ADD:{
//                    if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
//                        if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
//                        String key = getKey(entityEntry.getType(), entityEntry.isTemporalProperty(), entityEntry.getId(), entityEntry.getKey());
//                        if(save.containsKey(key)){
//                            throw new EntityEntryException(entityEntry);
//                        }else{
//                            save.put(key, "node");
//                        }
//                        break;
//                    }
//
//                    String key;
//                    if(entityEntry.getId() >= 0){
//                        getNodeRel(entityEntry.getType(), entityEntry.getId(), entityEntry);
//                        key = getKey(entityEntry.getType(), entityEntry.isTemporalProperty(), entityEntry.getId(), entityEntry.getKey());
//                    }
//                    else if( entityEntry.getId() == -2){
//                        getNodeRel(entityEntry.getType(), entityEntry.getParaId(), entityEntry);
//                        key = getKey(entityEntry.getType(), entityEntry.isTemporalProperty(), entityEntry.getParaId(), entityEntry.getKey());
//                    }
//                    else throw new EntityEntryException(entityEntry);
//
//                    if(entityEntry.isTemporalProperty()){
//                        if(entityEntry.getOther() != -1){
//                            setTemporalProperty(key, entityEntry.getValue(), entityEntry.getStart(), entityEntry.getOther());
//                        }
//                        else setTemporalProperty(key, entityEntry.getValue(), entityEntry.getStart(), 999999999);
//                        break;
//                    }
//                    else save.put(key, entityEntry.getValue());
//                    break;
//                }
//                case EntityEntry.GET:{
//                    if(entityEntry.getKey().equals(DTGConstants.NULLSTRING)){
//                        if(entityEntry.getId() < 0)throw new EntityEntryException(entityEntry);
//                        getNodeRel(entityEntry.getType(), entityEntry.getId(), entityEntry);
//                        break;
//                    }
//
//                    String key;
//                    if(entityEntry.getId() >= 0){
//                        key = getKey(entityEntry.getType(), entityEntry.isTemporalProperty(), entityEntry.getId(), entityEntry.getKey());
//                        getNodeRel(entityEntry.getType(), entityEntry.getId(), entityEntry);
//                    }
//                    else if( entityEntry.getId() == -2){
//                        key = getKey(entityEntry.getType(), entityEntry.isTemporalProperty(), entityEntry.getParaId(), entityEntry.getKey());
//                        getNodeRel(entityEntry.getType(), entityEntry.getParaId(), entityEntry);
//                    }
//                    else throw new EntityEntryException(entityEntry);
//
//                    if(entityEntry.isTemporalProperty()){
//                        Object res = getTemporalProperty(key, entityEntry.getStart(), entityEntry);
//                        resultMap.put(entityEntry.getTransactionNum(), res);
//                    }
//                    else {
//                        Object res = save.get(key);
//                        resultMap.put(entityEntry.getTransactionNum(), res);
//                    }
//                    break;
//                }
//                case EntityEntry.REMOVE:{
//                    String key;
//                    if(entityEntry.getId() >= 0){
//                        getNodeRel(entityEntry.getType(), entityEntry.getId(), entityEntry);
//                        key = getKey(entityEntry.getType(), entityEntry.isTemporalProperty(), entityEntry.getId(), entityEntry.getKey());
//                    }
//                    else if( entityEntry.getId() == -2){
//                        getNodeRel(entityEntry.getType(), entityEntry.getParaId(), entityEntry);
//                        key = getKey(entityEntry.getType(), entityEntry.isTemporalProperty(), entityEntry.getParaId(), entityEntry.getKey());
//                    }
//                    else throw new EntityEntryException(entityEntry);
//
//                    save.remove(key);
//                    break;
//                }
//                case EntityEntry.SET:{
//                    String key;
//                    if(entityEntry.getId() >= 0){
//                        getNodeRel(entityEntry.getType(), entityEntry.getId(), entityEntry);
//                        key = getKey(entityEntry.getType(), entityEntry.isTemporalProperty(), entityEntry.getId(), entityEntry.getKey());
//                    }
//                    else if( entityEntry.getId() == -2){
//                        getNodeRel(entityEntry.getType(), entityEntry.getParaId(), entityEntry);
//                        key = getKey(entityEntry.getType(), entityEntry.isTemporalProperty(), entityEntry.getParaId(), entityEntry.getKey());
//                    }
//                    else throw new EntityEntryException(entityEntry);
//
//                    if(entityEntry.isTemporalProperty()){
//                        if(entityEntry.getOther() != -1){
//                            setTemporalProperty(key, entityEntry.getValue(), entityEntry.getStart(), entityEntry.getOther());
//                        }
//                        else setTemporalProperty(key, entityEntry.getValue(), entityEntry.getStart(), 999999999);
//                        break;
//                    }
//                    else save.put(key, entityEntry.getValue());
//                    break;
//                }
//                default:{
//                    throw new TypeDoesnotExistException(entityEntry.getType(), "node operation type");
//                }
//            }
//        }

    public void getTransaction(List<EntityEntry> Entries, String txId) {
        boolean output = false;
        Map<Integer, Long> tempMap = new HashMap<>();

        if(Entries.size() > 100){
            this.rsave.setBatch();
        }
        for (EntityEntry entityEntry : Entries) {
            long realId = entityEntry.getId();
            //System.out.println(entityEntry.toString());
            switch (entityEntry.getOperationType()) {
                case EntityEntry.ADD: {
                    if (entityEntry.getKey().equals(DTGConstants.NULLSTRING)) {
                        String key = getKey(entityEntry.getType(), entityEntry.isTemporalProperty(), entityEntry.getId(), entityEntry.getKey());
                        rsave.appendEntry(key, "node");
                        if(entityEntry.getType() == RELATIONTYPE){
                            long startId = tempMap.get(entityEntry.getStart());
                            long endId = tempMap.get(entityEntry.getOther());
                            if(startId == -2){
                                startId = tempMap.get(startId);
                            }
                            if(endId == -2){
                                endId = tempMap.get(endId);
                            }
                            setRelationNode(realId, true, startId);
                            setRelationNode(realId, false, endId);//System.out.println("start id = " + startId + ", end id = " + endId);
                        }
                        tempMap.put(entityEntry.getTransactionNum(), entityEntry.getId());
                        break;
                    }

                    String key;
                    if (entityEntry.getId() >= 0) {
                        key = getKey(entityEntry.getType(), entityEntry.isTemporalProperty(), entityEntry.getId(), entityEntry.getKey());
                    } else {
                        realId = tempMap.get(entityEntry.getParaId());
                        key = getKey(entityEntry.getType(), entityEntry.isTemporalProperty(), realId, entityEntry.getKey());
                    }
                    if (entityEntry.isTemporalProperty()) {
                        if (entityEntry.getOther() != -1) {
                            setTemporalProperty(key, entityEntry.getValue(), entityEntry.getStart(), entityEntry.getOther());
                        } else setTemporalProperty(key, entityEntry.getValue(), entityEntry.getStart(), 999999999);
                        break;
                    } else rsave.appendEntry(key, entityEntry.getValue());
                    break;
                }
                case EntityEntry.GET: {
                    if (entityEntry.getKey().equals(DTGConstants.NULLSTRING)) {
                        tempMap.put(entityEntry.getTransactionNum(), entityEntry.getId());
                        break;
                    }
                    if (entityEntry.getId() == -2) {
                        realId = tempMap.get(entityEntry.getParaId());
                    }
                    if(entityEntry.getStart() == 1){
                        if(entityEntry.getType() == NODETYPE){
//                                    List<Long> list = getNodeRelation(realId, true);
//                                    tempMap.put(entityEntry.getTransactionNum(), list);
                        }else {
                            long startId = getRelationNode(realId, true);
                            tempMap.put(entityEntry.getTransactionNum(), startId);
                        }
                    }else if(entityEntry.getOther() == 1){
                        if(entityEntry.getType() == NODETYPE){
//                                    List<Long> list = getNodeRelation(realId, false);
//                                    tempMap.put(entityEntry.getTransactionNum(), list);
                        }else {
                            long endId = getRelationNode(realId, false);
                            tempMap.put(entityEntry.getTransactionNum(), endId);
                        }
                    }
                    break;
                }
                case EntityEntry.REMOVE: {
                    String key;
                    if (entityEntry.getId() >= 0) {
                        key = getKey(entityEntry.getType(), entityEntry.isTemporalProperty(), entityEntry.getId(), entityEntry.getKey());
                    } else {
                        realId = tempMap.get(entityEntry.getParaId());
                        key = getKey(entityEntry.getType(), entityEntry.isTemporalProperty(), realId, entityEntry.getKey());
                    }

                    rsave.removeEntry(key);
                    break;
                }
                case EntityEntry.SET: {
                    String key;
                    if (entityEntry.getId() >= 0) {
                        key = getKey(entityEntry.getType(), entityEntry.isTemporalProperty(), entityEntry.getId(), entityEntry.getKey());
                    } else {
                        realId = tempMap.get(entityEntry.getParaId());
                        key = getKey(entityEntry.getType(), entityEntry.isTemporalProperty(), realId, entityEntry.getKey());
                    }
                    //System.out.println(key);
                    if (entityEntry.isTemporalProperty()) {
                        if (entityEntry.getOther() != -1) {
                            setTemporalProperty(key, entityEntry.getValue(), entityEntry.getStart(), entityEntry.getOther());
                        } else setTemporalProperty(key, entityEntry.getValue(), entityEntry.getStart(), 999999999);
                        break;
                    } else {
                        rsave.appendEntry(key, entityEntry.getValue());
                    }
                    break;
                }
                case EntityEntry.GETLIMIT:break;
            }
            if(realId % 3 == storeid){
                //System.out.println(entityEntry.toString());
                output = true;
            }
        }
        if(Entries.size() > 100){
            this.rsave.closeBatch();
        }
        if(output){
            System.out.println(System.currentTimeMillis() + "  " + txId + " : second done !");
        }
    }

    private List<TimeObject> addTemporalProperty(String key, Object value, long start, long end){
        List<TimeObject> list = (List)rsave.getEntry(key);
        if(list == null){
            list = new LinkedList<>();
        }
        int n;
        for(n = 0; n < list.size(); n++){
            TimeObject object = list.get(n);
            if(object.time >= start){
                break;
            }
        }
        if(n >= list.size()){
            TimeObject object = new TimeObject();
            object.o = value;
            object.time = start;
            list.add(object);
        }else{
            TimeObject object = list.get(n);
            if(object.time == start)object.o = value;
            else{
                object = new TimeObject();
                object.o = value;
                object.time = start;
                list.add(n, object);
            }
        }
        while(n < list.size()){
            TimeObject object = list.get(n);
            if(object.time >= end)break;
            object.o = value;
            n++;
        }
        if(n < list.size()){
            TimeObject object = list.get(n);
            if(object.time == end)object.o = value;
            else{
                object = new TimeObject();
                object.o = value;
                object.time = end;
                list.add(n, object);
            }
        }else{
            TimeObject object = list.get(n-1);
            if(object.time < end){
                object = new TimeObject();
                object.o = value;
                object.time = end;
                list.add(n, object);
            }
        }
        return list;
    }

    private Object getTemporalPropertylocal(String key, long time, EntityEntry entry, Map<String, Object> tempProMap) throws EntityEntryException {
        List<TimeObject> list;
        if(tempProMap.containsKey(key)){
            list = (List<TimeObject>) tempProMap.get(key);
        }else{
            list = (List)rsave.getEntry(key);
        }
        if(list == null){
            throw new EntityEntryException(entry);
        }
        for(int i = 0; i < list.size(); i++){
            TimeObject object = list.get(i);
            if(object.time == time){
                return object.o;
            }else if(object.time > time){
                if(i > 0){
                    return list.get(i-1).o;
                }else {
                    return null;
                }
            }
        }
        return null;
    }

    private Map<Integer, Object> check(List<EntityEntry> Entries, String txId, long mainRegionId){
        boolean output = false;
        int count = 0;
        List<String> removeKey = new ArrayList<>();
        Map<Integer, Long> tempMap = new HashMap<>();
        Map<String, Object> tempProMap = new HashMap<>();
        Map<Integer, Object> resultMap = new HashMap<>();
        long realId = 0;
        try {
            for (EntityEntry entityEntry : Entries) {
                realId = entityEntry.getId();
                //System.out.println(entityEntry.toString());
                switch (entityEntry.getOperationType()) {
                    case EntityEntry.ADD: {
                        if (entityEntry.getKey().equals(DTGConstants.NULLSTRING)) {
                            if (entityEntry.getId() < 0) {
                                if(realId % 3 == storeid){
                                    System.out.println("error id : " + entityEntry.getId());
                                    return null;
                                }
                                resultMap.put(-1, false);
                                return resultMap;
                            }
                            String key = getKey(entityEntry.getType(), entityEntry.isTemporalProperty(), entityEntry.getId(), entityEntry.getKey());
                            if (rsave.getEntry(key) != null) {
                                if(realId % 3 == storeid){
                                    System.out.println("error object : " + key);
                                    return null;
                                }
                                resultMap.put(-1, false);
                                return resultMap;
                            } else {
                                if(entityEntry.getType() == RELATIONTYPE){
                                    long startId = tempMap.get(entityEntry.getStart());
                                    long endId = tempMap.get(entityEntry.getOther());
                                    if(startId == -2){
                                        startId = tempMap.get(startId);
                                    }
                                    if(endId == -2){
                                        endId = tempMap.get(endId);
                                    }
                                    tempProMap.put("Relation-" + realId + "-start", startId);
                                    tempProMap.put("Relation-" + realId + "-end", endId);
                                }
                                tempProMap.put(key, entityEntry.getType());
                                tempMap.put(entityEntry.getTransactionNum(), entityEntry.getId());
                            }
                            break;
                        }

                        if (entityEntry.getId() == -2) {
                            realId = tempMap.get(entityEntry.getParaId());
                        } else if(entityEntry.getId() < 0) throw new EntityEntryException(entityEntry);
                        if(removeKey.contains(getKey(entityEntry.getType(), false, realId, DTGConstants.NULLSTRING))){
                            if(realId % 3 == storeid){
                                System.out.println("error object : " + getKey(entityEntry.getType(), false, realId, DTGConstants.NULLSTRING));
                                return null;
                            }
                            resultMap.put(-1, false);
                            return resultMap;
                        }
                        getNodeRel(entityEntry.getType(), realId, entityEntry, tempProMap);
                        String key = getKey(entityEntry.getType(), entityEntry.isTemporalProperty(), realId, entityEntry.getKey());
                        if (entityEntry.isTemporalProperty()) {
                            if (entityEntry.getOther() != -1) {
                                List<TimeObject> lsit = addTemporalProperty(key, entityEntry.getValue(), entityEntry.getStart(), entityEntry.getOther());
                                tempProMap.put(key, lsit);
                            } else {
                                List<TimeObject> lsit = addTemporalProperty(key, entityEntry.getValue(), entityEntry.getStart(), 999999999);
                                tempProMap.put(key, lsit);
                            }
                            break;
                        } else tempProMap.put(key, entityEntry.getValue());
                        break;
                    }
                    case EntityEntry.GET: {
                        if (entityEntry.getKey().equals(DTGConstants.NULLSTRING)) {
                            if (entityEntry.getId() < 0) throw new EntityEntryException(entityEntry);
                            if(removeKey.contains(getKey(entityEntry.getType(), false, entityEntry.getId(), DTGConstants.NULLSTRING))){
                                if(realId % 3 == storeid){
                                    System.out.println("error object : " + getKey(entityEntry.getType(), false, entityEntry.getId(), DTGConstants.NULLSTRING));
                                    return null;
                                }
                                resultMap.put(-1, false);
                                return resultMap;
                            }
                            getNodeRel(entityEntry.getType(), entityEntry.getId(), entityEntry, tempProMap);
                            tempMap.put(entityEntry.getTransactionNum(), entityEntry.getId());
                            break;
                        }

                        if (entityEntry.getId() == -2) {
                            realId = tempMap.get(entityEntry.getParaId());
                        } else if(entityEntry.getId() < 0) throw new EntityEntryException(entityEntry);
                        if(removeKey.contains(getKey(entityEntry.getType(), false, realId, DTGConstants.NULLSTRING))){
                            if(realId % 3 == storeid){
                                System.out.println("error object : " + getKey(entityEntry.getType(), false, realId, DTGConstants.NULLSTRING));
                                return null;
                            }
                            resultMap.put(-1, false);
                            return resultMap;
                        }
                        getNodeRel(entityEntry.getType(), realId, entityEntry, tempProMap);
                        String key = getKey(entityEntry.getType(), entityEntry.isTemporalProperty(), realId, entityEntry.getKey());

                        if(removeKey.contains(key)){
                            if(realId % 3 == storeid){
                                System.out.println("error object : " + getKey(entityEntry.getType(), false, entityEntry.getId(), DTGConstants.NULLSTRING));
                                return null;
                            }
                            resultMap.put(-1, false);
                            return resultMap;
                        }
                        if (entityEntry.isTemporalProperty()) {
                            Object res = getTemporalPropertylocal(key, entityEntry.getStart(), entityEntry, tempProMap);
                            if(res == null){
                                if(realId % 3 == storeid){
                                    System.out.println("error object : " + key);
                                    return null;
                                }
                                resultMap.put(-1, false);
                                return resultMap;
                            }
                            resultMap.put(entityEntry.getTransactionNum(), res);
                        } else {
                            if(entityEntry.getStart() == 1){
                                if(entityEntry.getType() == NODETYPE){
//                                    List<Long> list = getNodeRelation(realId, true);
//                                    tempMap.put(entityEntry.getTransactionNum(), list);
                                }else {
                                    long startId;
                                    if(tempProMap.containsKey("Relation-" + realId + "-start")){
                                        startId = (long)tempProMap.get("Relation-" + realId + "-start");
                                    }else{
                                        startId = getRelationNode(realId, true);
                                    }
                                    resultMap.put(entityEntry.getTransactionNum(), startId);
                                    tempMap.put(entityEntry.getTransactionNum(), startId);
                                }
                            }else if(entityEntry.getOther() == 1){
                                if(entityEntry.getType() == NODETYPE){
//                                    List<Long> list = getNodeRelation(realId, false);
//                                    tempMap.put(entityEntry.getTransactionNum(), list);
                                }else {
                                    long endId;
                                    if(tempProMap.containsKey("Relation-" + realId + "-end")){
                                        endId = (long)tempProMap.get("Relation-" + realId + "-end");
                                    }else{
                                         endId = getRelationNode(realId, false);
                                    }
                                    resultMap.put(entityEntry.getTransactionNum(), endId);
                                    tempMap.put(entityEntry.getTransactionNum(), endId);
                                }
                            }else{
                                Object res;
                                if(tempProMap.containsKey(key)){
                                    res = tempProMap.get(key);
                                }else{
                                    res = rsave.getEntry(key);
                                }
                                if(res == null){
                                    if(realId % 3 == storeid){
                                        System.out.println("error object : " + key);
                                        return null;
                                    }
                                    resultMap.put(-1, false);
                                    return resultMap;
                                }
                                resultMap.put(entityEntry.getTransactionNum(), res);
                            }
                        }
                        break;
                    }
                    case EntityEntry.REMOVE: {
                        if (entityEntry.getId() == -2) {
                            realId = tempMap.get(entityEntry.getParaId());
                        } else if(entityEntry.getId() < 0) throw new EntityEntryException(entityEntry);
                        if(removeKey.contains(getKey(entityEntry.getType(), false, realId, DTGConstants.NULLSTRING))){
                            if(realId % 3 == storeid){
                                System.out.println("error object : " + getKey(entityEntry.getType(), false, realId, DTGConstants.NULLSTRING));
                                return null;
                            }
                            resultMap.put(-1, false);
                            return resultMap;
                        }
                        getNodeRel(entityEntry.getType(), realId, entityEntry, tempProMap);
                        String key = getKey(entityEntry.getType(), entityEntry.isTemporalProperty(), realId, entityEntry.getKey());

                        if(removeKey.contains(key)){
                            if(realId % 3 == storeid){
                                System.out.println("error object : " + key);
                                return null;
                            }
                            resultMap.put(-1, false);
                            return resultMap;
                        }
                        removeKey.add(key);//System.out.println(key);
                        break;
                    }
                    case EntityEntry.SET: {
                        if (entityEntry.getId() == -2) {
                            realId = tempMap.get(entityEntry.getParaId());
                        } else if(entityEntry.getId() < 0) throw new EntityEntryException(entityEntry);
                        if(removeKey.contains(getKey(entityEntry.getType(), false, realId, DTGConstants.NULLSTRING))){
                            if(realId % 3 == storeid){
                                System.out.println("error object : " + getKey(entityEntry.getType(), false, realId, DTGConstants.NULLSTRING));
                                return null;
                            }
                            resultMap.put(-1, false);
                            return resultMap;
                        }
                        getNodeRel(entityEntry.getType(), realId, entityEntry, tempProMap);
                        String key = getKey(entityEntry.getType(), entityEntry.isTemporalProperty(), realId, entityEntry.getKey());

                        if (entityEntry.isTemporalProperty()) {
                            if (entityEntry.getOther() != -1) {
                                List<TimeObject> lsit = addTemporalProperty(key, entityEntry.getValue(), entityEntry.getStart(), entityEntry.getOther());
                                tempProMap.put(key, lsit);
                            } else {
                                List<TimeObject> lsit = addTemporalProperty(key, entityEntry.getValue(), entityEntry.getStart(), 999999999);
                                tempProMap.put(key, lsit);
                            }
                            break;
                        } else tempProMap.put(key, entityEntry.getValue());
                        break;
                    }
                    case EntityEntry.GETLIMIT:{
                        if (entityEntry.getId() == -2) {
                            realId = tempMap.get(entityEntry.getParaId());
                        } else if(entityEntry.getId() < 0) throw new EntityEntryException(entityEntry);
                        if(removeKey.contains(getKey(entityEntry.getType(), false, realId, DTGConstants.NULLSTRING))){
                            if(realId % 3 == storeid){
                                System.out.println("error object : " + getKey(entityEntry.getType(), false, realId, DTGConstants.NULLSTRING));
                                return null;
                            }
                            resultMap.put(-1, false);
                            return resultMap;
                        }
                        getNodeRel(entityEntry.getType(), realId, entityEntry, tempProMap);
                        String key = getKey(entityEntry.getType(), entityEntry.isTemporalProperty(), realId, entityEntry.getKey());

                        if(removeKey.contains(key)){
                            if(realId % 3 == storeid){
                                System.out.println("error object : " + getKey(entityEntry.getType(), false, entityEntry.getId(), DTGConstants.NULLSTRING));
                                return null;
                            }
                            resultMap.put(-1, false);
                            return resultMap;
                        }
                        if (entityEntry.isTemporalProperty()) {
                            Object res = getTemporalPropertylocal(key, entityEntry.getStart(), entityEntry, tempProMap);
                            if(res == null){
                                if(realId % 3 == storeid){
                                    System.out.println("error object : " + key);
                                    return null;
                                }
                                resultMap.put(-1, false);
                                return resultMap;
                            }
                            if((int)res >= (int)entityEntry.getValue() && (int)res < entityEntry.getOther()){
                                resultMap.put(entityEntry.getTransactionNum(), res);
                            }else{
                                resultMap.put(entityEntry.getTransactionNum(), DTGConstants.NULLSTRING);
                            }
                        } else {
                            Object res;
                            if(tempProMap.containsKey(key)){
                                res = tempProMap.get(key);
                            }else{
                                res = rsave.getEntry(key);
                            }
                            if(res == null){
                                if(realId % 3 == storeid){
                                    System.out.println("error object : " + key);
                                    return null;
                                }
                                resultMap.put(-1, false);
                                return resultMap;
                            }
                            if((int)res >= (int)entityEntry.getValue() && (int)res < entityEntry.getOther()){
                                resultMap.put(entityEntry.getTransactionNum(), res);
                            }else{
                                resultMap.put(entityEntry.getTransactionNum(), DTGConstants.NULLSTRING);
                            }
                        }
                        break;
                    }
                    default: {
                        throw new TypeDoesnotExistException(entityEntry.getType(), "node operation type");
                    }
                }
                if(realId % 3 == storeid){
                    count++;
                    output = true;
                }
            }
        }catch (Exception e){
            if(realId % 3 == storeid){
                System.out.println(e);
                return null;
            }
            resultMap.put(-1, false);

            return resultMap;
        }

        if(output){
            System.out.println(System.currentTimeMillis() + "  " + txId + " : finish " + count + " operation");
            System.out.println(System.currentTimeMillis() + "  " + txId + " : first done !");
            System.out.println(System.currentTimeMillis() + "  " + txId + " : get end version = " + mainRegionId);
        }
        resultMap.put(-((int)storeid + 2) , count);
        resultMap.put(-1, true);
        return resultMap;
    }

}

class TimeObject implements Serializable {
    long time;
    Object o;

    @Override
    public String toString(){
        return "time : " + time + " value : " + o.toString();
    }
}

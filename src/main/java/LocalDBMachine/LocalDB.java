package LocalDBMachine;

import DBExceptions.*;
import Element.DTGOperation;
import Element.EntityEntry;
import Element.OperationName;
import LocalDBMachine.LocalTx.TransactionThreadLock;
import LocalDBMachine.LocalTx.TxLockManager;
import MQ.DTGMQ;
import Region.DTGLockClosure;
import Region.DTGRegion;
import Region.FirstPhaseClosure;
import Region.LockProcess;
import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.client.FutureHelper;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.util.Utils;
import options.MQOptions;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import raft.EntityStoreClosure;
import raft.DTGRawStore;
import raft.LogStoreClosure;
import options.LocalDBOption;
import tool.ObjectAndByte;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static config.DefaultOptions.MVCC;
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
    private MemMVCC memMVCC;

    private LockProcess lockProcess;
    private final DTGMQ logMQ;
    private ExecutorService fixedThreadPool;
    private final HashMap<String, List<Long>> objectOpMap;
    private TxLockManager lockManager;

    public LocalDB(){
        this.logMQ = new DTGMQ();
        fixedThreadPool = Executors.newFixedThreadPool(Utils.cpus());
        this.objectOpMap = new HashMap<>();
        this.lockManager = new TxLockManager();
    }

    @Override
    public boolean init(LocalDBOption opts) {
        this.waitCommitMap = new HashMap<>();
        this.lockProcess = new LockProcess();
        db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(opts.getDbPath() )
                .loadPropertiesFromFile("")
                .newGraphDatabase();
        this.memMVCC = new MemMVCC();
        MQOptions mqopts = new MQOptions();
        mqopts.setLogUri(opts.getDbPath() + "\\RegionTxLog");
        mqopts.setRockDBPath(opts.getDbPath() + "\\RegionTxRockDB");
        mqopts.setLocalDB(this);
        if(!logMQ.init(mqopts)){
            return false;
        }
        return true;
    }

    public void shutdown(){
        db.shutdown();
    }

    public GraphDatabaseService getDb() {
        return db;
    }

    @Override
    public void setLock(final DTGOperation op, final DTGLockClosure closure, DTGRegion region) {
        List<EntityEntry> entityEntries= op.getEntityEntries();
        List<EntityEntry> removeList = new ArrayList<>();
        for(EntityEntry entry : entityEntries){
            if(entry.getId() == -2){
                int paraId = entry.getParaId();
                entry.setId(entityEntries.get(paraId).getId());
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
    public void clean(long version) {

    }

    @Override
    public void commitSuccess(DTGOperation op, final EntityStoreClosure closure, final DTGRegion region) {

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
    public void secondRead(DTGOperation op, EntityStoreClosure closure, DTGRegion region) {

    }

    @Override
    public void merge() {

    }

    @Override
    public void split() {

    }

    public void runOp(DTGOperation op, EntityStoreClosure closure, boolean isLeader, DTGRegion region){
        if(closure != null){
            localRun(op, closure, isLeader, region);
        }
        else{
            fixedThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    localRun(op, closure, isLeader, region);
                }
            });
        }
    }

    private void localRun(DTGOperation op, EntityStoreClosure closure, boolean isLeader, DTGRegion region){//System.out.println("run tx :" + op.getTxId() + ", " + region.getId());
        String key = getkey(op.getTxId(), region.getId());
        switch (op.getType()){
            case OperationName.TRANSACTIONOP:{
                if(!MVCC){
                    this.lockManager.lock(op);
                }
                Map<Integer, Object> resultMap = new HashMap<>();
                try {
                    if((!isLeader && !this.waitCommitMap.containsKey(key)) || !MVCC){
                        resultMap.put(-1, true);
                        TransactionThreadLock txLock = new TransactionThreadLock(op.getTxId());
                        LocalTransaction tx = new LocalTransaction(this.memMVCC, this.db, op, resultMap, txLock, region);

                        tx.start();//System.out.println("aaaaaaaaaaaaa");
                        synchronized (txLock){
                            if(!tx.getCouldCommit()){
                                try {
                                    txLock.wait();
                                } catch (InterruptedException e) {
                                    System.out.println(e);
                                    throw new TransactionException();
                                }
                            }
                        }
                        //System.out.println("bbbbbbbbbbbbbb");
                        addToCommitMap(tx, key);
                    }
                    if(closure != null){
                        if((boolean)resultMap.get(-1)) {
                            closure.setData(resultMap);//System.out.println("ddddddddddddd");
                            closure.run(Status.OK());
                        }else throw new TransactionException();
                    }//System.out.println("cccccccccc");
                } catch (Throwable e) {
                    if(closure != null){//System.out.println("eeeeeee");
                        System.out.println("local transaction error" + op.getTxId());
                        closure.setError(Errors.forException(e));
                        closure.run(new Status(-1, "local transaction failed, transaction op id: %s", op.getTxId()));
                    }
                }
                break;
            }
            case OperationName.COMMITTRANS:{
                try {
                    if(commitTx(key, op.getVersion()) && closure != null){
                        closure.setData(true);
                        closure.run(Status.OK());
                    }else {
                        throw new Exception("commit error");
                    }
                }catch (Throwable e){
                    if(closure != null){
                        System.out.println("commit error" + op.getTxId());
                        closure.setData(false);
                        closure.setError(Errors.forException(e));
                        closure.run(new Status(-1, "local transaction commit failed, transaction op id: %s", op.getTxId()));
                    }
                }
                break;
            }
            case OperationName.ROLLBACK:{System.out.println("start rollback" + op.getTxId());
                try {
                    rollbackTx(key);
                    if(rollbackTx(key) && closure != null){
                        closure.setData(true);
                        closure.run(Status.OK());
                    }else {
                        throw new Exception("rollback error");
                    }
                }catch (Throwable e){
                    if(closure != null){
                        closure.setData(false);
                        closure.setError(Errors.forException(e));
                        closure.run(new Status(-1, "local transaction rollback failed, transaction op id: %s", op.getTxId()));
                    }
                }
                break;
            }
            case OperationName.SCEONDREAD:{
                Map<Integer, Object> resultMap = new HashMap<>();
                try {
                    resultMap.put(-1, true);
                    Map<Integer, Long> firstGetList = (Map<Integer, Long>)ObjectAndByte.toObject(op.getOpData());
                    checkVersion(op, firstGetList, resultMap, region);
                    synchronized (resultMap){
                        resultMap.wait(FutureHelper.DEFAULT_TIMEOUT_MILLIS);
                    }
                    if(closure != null){
                        closure.setData(resultMap);
                        closure.run(Status.OK());
                    }
                } catch (Throwable e) {
                    if(closure != null){
                        System.out.println("local transaction error" + op.getTxId());
                        closure.setError(Errors.forException(e));
                        closure.run(new Status(-1, "local transaction failed, transaction op id: %s", op.getTxId()));
                    }
                }
                break;
            }
        }
    }

    public void addToCommitMap(LocalTransaction tx, String key){
        setTxFirstDone(tx.getOp());
        waitCommitMap.put(key, tx);
    }

    public void addToCommitMap(LocalTransaction tx, String id, long regionId){
        addToCommitMap(tx, getkey(id, regionId));
    }

    private void setTxFirstDone(DTGOperation op){
        List<EntityEntry> entityEntries = op.getEntityEntries();
        for(EntityEntry entry : entityEntries){
            addObjectOp(entry.getType(), entry.getId(), entry.getKey(), entry.getStart(), op.getVersion());
        }
    }

    private void setTxDone(DTGOperation op){
        List<EntityEntry> entityEntries = op.getEntityEntries();
        for(EntityEntry entry : entityEntries){
            removeObjectOp(entry.getType(), entry.getId(), entry.getKey(), entry.getStart(), op.getVersion());
        }
    }

    //AtomicInteger count = new AtomicInteger(0);
    public boolean commitTx(String id, long endVersion) throws InterruptedException {
//        System.out.println(System.currentTimeMillis() + "  start commitTx1 : " + id);
        LocalTransaction tx = waitCommitMap.get(id);
        int i = 0;
        while(tx == null){
//            if(i > DTGConstants.MAXWAITTIME){
//                return false;
//            }
            i++;
            //System.out.println("tx = null ：" + id);
            Thread.sleep(10);
            tx = waitCommitMap.get(id);
        }
        TransactionThreadLock lock = tx.getLock();
        synchronized (lock){
            tx.setEndVersion(endVersion);
            lock.commit();
            lock.notify();
        }
        setTxDone(tx.getOp());
        waitCommitMap.remove(id);
        System.out.println("time :" + System.currentTimeMillis() + ", end commitTx : " + id);// + "   " + count.getAndIncrement());
        return true;
    }

    public boolean rollbackTx(String id) throws InterruptedException, EntityEntryException {
        LocalTransaction tx = waitCommitMap.get(id);
        int i = 0;
        while(tx == null){
//            if(i > DTGConstants.MAXWAITTIME){
//                return false;
//            }
            i++;
            Thread.sleep(10);
            tx = waitCommitMap.get(id);
        }
        TransactionThreadLock lock = tx.getLock();
        synchronized (lock){
            lock.rollback();
            lock.notify();
        }
        waitCommitMap.remove(id);
        System.out.println("rollback tx : " + id);
        return true;
    }

    public boolean addRemoveLock(DTGOperation op, EntityStoreClosure closure){
        List<EntityEntry> entityEntries= op.getEntityEntries();
        try(Transaction tx = db.beginTx()){
            for(EntityEntry entry : entityEntries){
                long maxVersion;
                if(entry.getKey() == null){
                    maxVersion = op.getVersion();
                }
                else{
                    maxVersion = getMaxVersion(entry);
                }
                if(!this.lockProcess.addRemove(entry.getId(), entry.getType(), op.getTxId(), maxVersion)){
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

    public Map<Integer, Object> checkVersion(DTGOperation op , Map<Integer, Long> firstGetList, Map<Integer, Object> updateRes, DTGRegion region){
        List<EntityEntry> entityEntries = op.getEntityEntries();
        List<EntityEntry> newEntries = new ArrayList<>();
        Map<Integer, Long> newVersionMap = new HashMap<>();
        for(EntityEntry entry : entityEntries){
            List<Long> versions = getObjectRunningOp(entry.getType(), entry.getId(), entry.getKey(), entry.getStart());
            for(long version : versions){
                if(version < op.getVersion() && version > firstGetList.get(entry.getTransactionNum())){
                    if(firstGetList.containsKey(version)){
                        entityEntries.add(entry);
                        newVersionMap.put(entry.getTransactionNum(), version);
                    }
                }
            }
        }
        DTGOperation newOp = new DTGOperation(newEntries, OperationName.TRANSACTIONOP);
        TransactionThreadLock txLock = new TransactionThreadLock("secR/" + op.getTxId());
        LocalTransaction tx = new LocalTransaction(this.memMVCC, this.db, newOp, updateRes, txLock, region);

        tx.start();
        return updateRes;
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

    private String getkey(String txId, long regionId){
        return txId + "" + regionId;
    }

    private void addObjectOp(byte type, long id, String prokey, long time, long version){
        String key = getObjectOpKey(type, id, prokey, time);
        if(this.objectOpMap.containsKey(key)){
            this.objectOpMap.get(key).add(version);
        }else{
            List<Long> list = new LinkedList<>();
            list.add(version);
            this.objectOpMap.put(key, list);
        }
    }

    private void removeObjectOp(byte type, long id, String prokey, long time, long version){
        String key = getObjectOpKey(type, id, prokey, time);
        if(!this.objectOpMap.containsKey(key)){
            return;
        }
        List<Long> list = this.objectOpMap.get(key);
        list.remove(version);
        if(list.isEmpty()){
            this.objectOpMap.remove(key);
        }
    }

    private List<Long> getObjectRunningOp(byte type, long id, String prokey, long time){
        String key = getObjectOpKey(type, id, prokey, time);
        return this.objectOpMap.get(key);
    }

    private String getObjectOpKey(byte type, long id, String prokey, long time){
        String key = "";
        switch (type){
            case NODETYPE:{
                key = key + "node/";
                break;
            }
            case RELATIONTYPE:{
                key = key + "relation/";
                break;
            }
            default:{
                key = key + "default/";
            }
        }
        key = key + id + "/" + prokey + "/" + time;
        return key;
    }

    public MemMVCC getMemMVCC() {
        return memMVCC;
    }
}

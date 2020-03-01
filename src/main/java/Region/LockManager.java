package Region;

import DBExceptions.DTGLockError;
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.client.FutureHelper;
import com.alipay.sofa.jraft.util.Bits;
import config.MainType;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author :jinkai
 * @date :Created in 2020/1/14 18:41
 * @description：
 * @modified By:
 * @version:
 */

public class LockManager implements Lifecycle {

    //object id save as byte, the form is byte[](type+id);
    //type 0x01 represent node; 0x02 represent relation;
    final Map<String, ArrayList<DTGLock>>             lockMap;
    final Map<String, List<RemoveClosure>>            waitRemoveList;

    private final ReadWriteLock                       readWriteLock = new ReentrantReadWriteLock();
    private final Lock                                readLock      = this.readWriteLock.readLock();
    private final Lock                                writeLock     = this.readWriteLock.writeLock();

    public LockManager(){
        this.lockMap = new ConcurrentHashMap<>();
        this.waitRemoveList = new ConcurrentHashMap<>();
    }

    public class RemoveClosure implements Closure{
        private CompletableFuture<Boolean> done;
        private String                     txId;
        private String                     index;

        public RemoveClosure(CompletableFuture<Boolean> done, String txId, String index){
            this.done = done;
            this.txId = txId;
            this.index = index;
        }

        public CompletableFuture<Boolean> getDone() {
            return done;
        }

        public String getTxId() {
            return txId;
        }

        @Override
        public void run(Status status) {
            if(status.isOk()){
                List<RemoveClosure> list = waitRemoveList.get(index);
                if(list != null){
                    for(int i = list.size() - 1; i >= 0; i--){
                        list.get(i).getDone().complete(true);
                    }
                    waitRemoveList.remove(index);
                }
                done.complete(true);
            }
            else {
                done.complete(false);
            }
        }

    }

    private class DTGLock{

        private byte type;
        private String txId;
        private long maxVersion;
        //private byte state;

        public DTGLock(byte type, String txId, long maxVersion){
            this.txId = txId;
            this.type = type;
            this.maxVersion = maxVersion;
        }

        public String getTxId() {
            return txId;
        }

        public byte getType() {
            return type;
        }

        public long getMaxVersion() {
            return maxVersion;
        }

        //        public byte getState() {
//            return state;
//        }
//
//        public void setState(byte state) {
//            this.state = state;
//        }
    }

    @Override
    public boolean init(Object opts) {
        return true;
    }

    public boolean addLock(long id, byte type, String txId, byte lockType, long maxVersion){
        if(lockType == MainType.REMOVE){
            return addRemoveLock(id, type, txId, maxVersion);
        }
        this.readLock.lock();
        boolean couldunlock = true;
        try {
            String index = getIndex(id, type);
            putIntoLockMap(index, txId, MainType.REMOVE, maxVersion);
            if(isInRemoveList(index)){
                List<RemoveClosure> list = waitRemoveList.get(index);
                couldunlock = false;
                this.readLock.unlock();
                if(waitRemoveOpreation(list.get(list.size() - 1))){
                    removeInLockMap(index, txId);
                    return false;
                }
            }
            return true;
        }finally {
            if(couldunlock){
                this.readLock.unlock();
            }
        }
    }

    public boolean addRemoveLock(long id, byte type, String txId, long maxVersion){
        this.readLock.lock();
        boolean couldunlock = true;
        try {
            String index = getIndex(id, type);
            putIntoLockMap(index, txId, MainType.REMOVE, maxVersion);
            List<RemoveClosure> list = waitRemoveList.get(index);
            if(list == null){
                list = new ArrayList<>();
                waitRemoveList.put(index, list);
                RemoveClosure closure = new RemoveClosure(new CompletableFuture<Boolean>(), txId, index);
                list.add(closure);
                //this.TxVersionMap.put(txId, maxVersion);
                return true;
            }else {
                RemoveClosure closure = new RemoveClosure(new CompletableFuture<Boolean>(), txId, index);
                list.add(closure);
                couldunlock = false;
                this.readLock.unlock();
                if(waitRemoveOpreation(list.get(list.size() - 2))){
                    removeLock(id, type, txId, MainType.REMOVE);
                    return false;
                }else {
                    //this.TxVersionMap.put(txId, maxVersion);
                    return true;
                }
            }
        }finally {
            if(couldunlock){
                this.readLock.unlock();
            }
        }

    }

    private boolean waitRemoveOpreation(RemoveClosure closure){
        CompletableFuture<Boolean> future = closure.getDone();
        return FutureHelper.get(future);
    }

    private void putIntoLockMap(String index, String txId,  byte lockType, long maxVersion){
        DTGLock lock = new DTGLock(lockType, txId, maxVersion);
        ArrayList<DTGLock> waitList = this.lockMap.get(index);
        if(waitList != null){
            waitList.add(lock);
        }else {
            waitList = new ArrayList<DTGLock>();
            waitList.add(lock);
            this.lockMap.put(index, waitList);
        }
    }

    public boolean removeLock(long id, byte type, String txId, byte lockType){
        this.writeLock.lock();
        try {
            String index = getIndex(id, type);
            removeInLockMap(index, txId);
            if(lockType == MainType.REMOVE){
                List<RemoveClosure> list = waitRemoveList.get(index);
                if(list != null){
                    for(int i = 0; i < list.size(); i++){
                        RemoveClosure closure = list.get(i);
                        if(closure.index.equals(index)){System.out.println("remove lock");
                            Status status = new Status(DTGLockError.HASREMOVED.getNumber(), "the data has been removed");
                            closure.run(status);
                            list.remove(i);
                            break;
                        }
                    }
                }
                //this.TxVersionMap.remove(txId);
            }
            return true;
        }finally {
            this.writeLock.unlock();
        }

    }

    public boolean finishLock(long id, byte type, String txId, byte lockType){
        this.writeLock.lock();
        try {
            String index = getIndex(id, type);
            removeInLockMap(index, txId);
            if(lockType == MainType.REMOVE){
                List<RemoveClosure> list = waitRemoveList.get(index);
                if(list != null){
                    for(int i = 0; i < list.size(); i++){
                        RemoveClosure closure = list.get(i);
                        if(closure.index.equals(index)){
                            list.remove(i);
                            closure.run(Status.OK());
                            break;
                        }
                    }
                }
                //this.TxVersionMap.remove(txId);
            }
            return true;
        }finally {
            this.writeLock.unlock();
        }

    }

    private boolean removeInLockMap(String index, String txId){
        ArrayList<DTGLock> waitList = lockMap.get(index);

        for(int i = 0; i < waitList.size(); i++){
            DTGLock lock = waitList.get(i);
            if(lock.txId.equals(txId)){
                waitList.remove(i);
                break;
            }
        }
        if(waitList.size() == 0){
            lockMap.remove(index);
        }
        return true;
    }

    public String getIndex(long id, byte type){
        byte[] idIndex = new byte[8 + 1];
        idIndex[0] = type;
        Bits.putLong(idIndex, 1, id);
        try {
            return new String(idIndex ,"utf-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return "";
    }


    public boolean isInRemoveList(long id, byte type, long version){
        if(!isInRemoveList(id, type)){
            return false;
        }
        long maxVersion = this.lockMap.get(getIndex(id, type)).get(0).getMaxVersion();
        if(maxVersion > version){
            return true;
        }else {
            return false;
        }
    }


    public boolean isInRemoveList(long id, byte type){
        String index = getIndex(id, type);
        return isInRemoveList(index);
    }

    public boolean isInRemoveList(String index){
        return waitRemoveList.containsKey(index);
    }

    public List<String> getTxBeforeNow(long id, byte type, String txId){
        this.readLock.lock();
        try{
            List<String> txList = new ArrayList<>();
            String index = getIndex(id, type);
            List waitQueue = lockMap.get(index);
            Iterator<DTGLock> it = waitQueue.iterator();
            while (it.hasNext()){
                DTGLock lock = it.next();
                if(lock.txId.equals(txId)){
                    break;
                }
                txList.add(lock.txId);
            }
            return txList;
        }finally {
            this.readLock.unlock();
        }
    }

    public boolean exchangeLockOrder(long id, byte type, String tx1, String tx2){
        this.readLock.lock();
        try {
            List<String> txList = new ArrayList<>();
            String index = getIndex(id, type);
            List<DTGLock> waitQueue = lockMap.get(index);
            DTGLock exchange = null;
            int exchangeIndex = -1;
            for(int i = 0; i < waitQueue.size(); i++){
                DTGLock lock = waitQueue.get(i);
                if(lock.getTxId().equals(tx1) || lock.getTxId().equals(tx2)){
                    if(exchange != null){
                        //if()
                        waitQueue.set(exchangeIndex, lock);
                        waitQueue.set(i, exchange);
                        break;
                    }
                    else {
                        exchangeIndex = i;
                        exchange = lock;
                    }
                }
            }
        }finally {
            this.readLock.unlock();
        }
        return true;
    }

    @Override
    public void shutdown() {

    }
}

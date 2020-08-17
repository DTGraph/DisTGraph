package LocalDBMachine.LocalTx;

import Element.DTGOperation;
import Element.EntityEntry;
import LocalDBMachine.LocalDB;
import com.alipay.sofa.jraft.rhea.client.FutureHelper;
import config.MainType;
import org.neo4j.kernel.impl.util.register.NeoRegister;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class TxLockManager {

    private Map<ObjectTimeLock, LinkedList<EntityEntry>> entryLockQueue;
    private Map<Long, AtomicInteger> txCountQueue;
    private Map<Long, Integer> txLockQueue;
    private LinkedList<EntityEntry> runningList;

    public TxLockManager(){
        this.entryLockQueue = new HashMap<>();
        this.txCountQueue = new HashMap<>();
        this.txLockQueue = new HashMap<>();
        this.runningList = new LinkedList<>();
    }

    public boolean lock(DTGOperation op){
        int countLock = 0;
        for(EntityEntry entry : op.getEntityEntries()){
            if(lockEntry(entry)){
                countLock++;
            }
        }
        if(countLock == 0){
            System.out.println(op.getVersion() + " has not lock ");
//            activeTx(false, op.getVersion());
            return true;
        }else {
            txCountQueue.put(op.getVersion(), new AtomicInteger(countLock));
            System.out.println(op.getVersion() + " has lock " + countLock);
            Integer lock = new Integer(0);
            synchronized (lock){
                try {
                    this.txLockQueue.put(op.getVersion(), lock);
                    lock.wait(FutureHelper.DEFAULT_TIMEOUT_MILLIS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        return true;
    }

    public boolean unlock(DTGOperation op){
        for(EntityEntry entry : op.getEntityEntries()){
            unlockEntry(entry);
        }
        return true;
    }

    private boolean lockEntry(EntityEntry entry){
        ObjectTimeLock lock = new ObjectTimeLock(entry.getType(), entry.getId(), entry.getKey());
        boolean hasLock = false;
        LinkedList<EntityEntry> queue = null;
        synchronized (entryLockQueue){
            if(!entryLockQueue.containsKey(lock)){
                queue = new LinkedList<>();
                this.entryLockQueue.put(lock, queue);
            }else {
                queue = entryLockQueue.get(lock);
            }
        }
        synchronized (queue){
            int versionIndex = -1;
            for (int t = queue.size() - 1; t >= 0; t--){
                EntityEntry entry1 = queue.get(t);
                if(versionIndex == -1 && entry1.getTxVersion() <= entry.getTxVersion()){
                    versionIndex = t + 1;
                }
                if(versionIndex != -1 && !(entry1.getStart() >= entry.getOther() || entry1.getOther() <= entry.getStart())){
                    if(entry1.getTxVersion() != entry.getTxVersion() && ((entry1.getOperationType() != MainType.LOCKGET &&
                            entry1.getOperationType() != MainType.GET) || entry.getOperationType() != MainType.GET)){
                        if(entry.getOperationType() == MainType.GET){
                            entry.setOperationType(MainType.LOCKGET);
                        }
                        hasLock = true;
                        break;
                    }
                }
            }
            if(versionIndex <= 0){
                queue.add(entry);
            }else {
                queue.add(versionIndex, entry);
            }
            if(!hasLock){
                this.runningList.add(entry);
            }
        }
        return hasLock;
    }

    private boolean unlockEntry(EntityEntry entry){
        ObjectTimeLock lock = new ObjectTimeLock(entry.getType(), entry.getId(), entry.getKey());
        LinkedList<EntityEntry> queue = null;
        synchronized (entryLockQueue){
            if(!entryLockQueue.containsKey(lock)){
                queue = new LinkedList<>();
                this.entryLockQueue.put(lock, queue);
            }else {
                queue = entryLockQueue.get(lock);
            }
        }
        synchronized (queue){
            queue.remove(entry);
            for (int t = 0; t < queue.size(); t++){
                EntityEntry entry1 = queue.get(t);
                if(entry.getTxVersion() == entry1.getTxVersion() || entry.getOperationType() == MainType.GET)continue;
                if(this.runningList.contains(entry1))continue;
                if(couldUnlock(entry1, queue)){
                    entry1.setOperationType(MainType.GET);
                    decreaseCheckAndActive(entry1.getTxVersion());
                    this.runningList.add(entry1);
                }
            }
        }
        return true;
    }

    private boolean couldUnlock(EntityEntry entry, LinkedList<EntityEntry> queue){
        int index = 0;
        EntityEntry entry1 = queue.get(index);
        while (entry != entry1){
            if(entry1.getTxVersion() != entry.getTxVersion() &&
                    !(entry1.getStart() >= entry.getOther() || entry1.getOther() <= entry.getStart())){
                if((entry1.getOperationType() != MainType.LOCKGET &&
                        entry1.getOperationType() != MainType.GET) || entry.getOperationType() != MainType.LOCKGET){
                    return false;
                }
            }
            entry1 = queue.get(++index);
        }
        return true;
    }

    private void decreaseCheckAndActive(long txVersion){
        AtomicInteger t = txCountQueue.get(txVersion);
        int num = t.decrementAndGet();
        if(num == 0){
            activeTx(true, txVersion);
        }
    }

    private void activeTx(boolean needRemoveQueue, long txVersion){
        if (needRemoveQueue){
            txCountQueue.remove(txVersion);
        }
        if(txLockQueue.containsKey(txVersion)){
            Integer lock = txLockQueue.get(txVersion);
            synchronized (lock){
                lock.notify();
            }
        }
        System.out.println("run tx " + txVersion);
    }


}

package Region;

import Element.EntityEntry;
import com.alipay.sofa.jraft.Lifecycle;

import java.util.List;

/**
 * @author :jinkai
 * @date :Created in 2020/1/15 12:17
 * @description：
 * @modified By:
 * @version:
 */

public class LockProcess implements Lifecycle {
    private LockManager lockManager = new LockManager();


    @Override
    public boolean init(Object opts) {
        lockManager.init(null);
        return true;
    }

    public synchronized boolean requestLock(List<EntityEntry> entityEntries, String txId){
        for(EntityEntry entityEntry : entityEntries){
            long id = entityEntry.getId();
            if(id >= 0){
                byte lockType = entityEntry.getOperationType();
                byte type = entityEntry.getType();
                if(!lockManager.addLock(id, type, txId, lockType)){
                    return false;
                }
            }
        }
        return true;
    }

    public synchronized boolean requestUnlock(List<EntityEntry> entityEntries, String txId){
        for(EntityEntry entityEntry : entityEntries){
            long id = entityEntry.getId();
            if(id >= 0){
                byte lockType = entityEntry.getOperationType();
                byte type = entityEntry.getType();
                if(!lockManager.addLock(id, type, txId, lockType)){
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public void shutdown() {

    }
}

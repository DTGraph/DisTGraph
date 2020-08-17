package LockTest;

import Element.DTGOperation;
import Element.EntityEntry;
import LocalDBMachine.LocalTx.TxLockManager;
import org.junit.Test;

import java.util.LinkedList;

import static config.MainType.NODETYPE;

public class TxLockTest {

    @Test
    public void addLockAndTest(){
        TxLockManager lockManager = new TxLockManager();
        LinkedList<DTGOperation> list = new LinkedList<>();
        for(int i = 0; i < 10; i++){
            DTGOperation op = new DTGOperation();
            LinkedList<EntityEntry> entityEntryList = new LinkedList<>();
            for(int j = 0; j < 5; j++){
                EntityEntry entry = new EntityEntry();
                entry.setTransactionNum(j);
                entry.setType(NODETYPE);
                entry.setOperationType(EntityEntry.GET);
                entry.setIsTemporalProperty(true);
                entry.setId(1);
                entry.setStart(j);
                entry.setOther(j+1);
                entry.setKey("aa");
                entry.setValue(1);
                entry.setTxVersion(i);
                entityEntryList.add(entry);
            }
            op.setEntityEntries(entityEntryList);
            op.setVersion(i);
            list.add(op);
            lockManager.lock(op);
            if(i >= 3){
                DTGOperation osp = list.get(i - 3);
                System.out.println("free" + osp.getVersion());
                lockManager.unlock(osp);
            }
        }
    }

    @Test
    public void addLockAndTest2(){
        TxLockManager lockManager = new TxLockManager();
        DTGOperation op1 = new DTGOperation();
        LinkedList<EntityEntry> entityEntryList = new LinkedList<>();
        EntityEntry entry = new EntityEntry();
        entry.setTransactionNum(1);
        entry.setType(NODETYPE);
        entry.setOperationType(EntityEntry.SET);
        entry.setIsTemporalProperty(true);
        entry.setId(1);
        entry.setStart(0);
        entry.setOther(10);
        entry.setKey("aa");
        entry.setValue(1);
        entry.setTxVersion(1);
        entityEntryList.add(entry);
        op1.setEntityEntries(entityEntryList);
        op1.setVersion(1);
        lockManager.lock(op1);

        DTGOperation op2 = new DTGOperation();
        LinkedList<EntityEntry> entityEntryList2 = new LinkedList<>();
        EntityEntry entry2 = new EntityEntry();
        entry2.setTransactionNum(1);
        entry2.setType(NODETYPE);
        entry2.setOperationType(EntityEntry.SET);
        entry2.setIsTemporalProperty(true);
        entry2.setId(1);
        entry2.setStart(5);
        entry2.setOther(15);
        entry2.setKey("aa");
        entry2.setValue(1);
        entry2.setTxVersion(2);
        entityEntryList2.add(entry2);
        op2.setEntityEntries(entityEntryList2);
        op2.setVersion(2);
        lockManager.lock(op2);

        DTGOperation op3 = new DTGOperation();
        LinkedList<EntityEntry> entityEntryList3 = new LinkedList<>();
        EntityEntry entry3 = new EntityEntry();
        entry3.setTransactionNum(1);
        entry3.setType(NODETYPE);
        entry3.setOperationType(EntityEntry.SET);
        entry3.setIsTemporalProperty(true);
        entry3.setId(1);
        entry3.setStart(10);
        entry3.setOther(15);
        entry3.setKey("aa");
        entry3.setValue(1);
        entry3.setTxVersion(3);
        entityEntryList3.add(entry3);
        op3.setEntityEntries(entityEntryList3);
        op3.setVersion(3);
        lockManager.lock(op3);

        DTGOperation op4 = new DTGOperation();
        LinkedList<EntityEntry> entityEntryList4 = new LinkedList<>();
        EntityEntry entry4 = new EntityEntry();
        entry4.setTransactionNum(1);
        entry4.setType(NODETYPE);
        entry4.setOperationType(EntityEntry.SET);
        entry4.setIsTemporalProperty(true);
        entry4.setId(1);
        entry4.setStart(1);
        entry4.setOther(5);
        entry4.setKey("aa");
        entry4.setValue(1);
        entry4.setTxVersion(4);
        entityEntryList4.add(entry4);
        op4.setEntityEntries(entityEntryList4);
        op4.setVersion(4);
        lockManager.lock(op4);

        lockManager.unlock(op1);

    }

    @Test
    public void addLockAndTest3(){
        TxLockManager lockManager = new TxLockManager();
        DTGOperation op1 = new DTGOperation();
        LinkedList<EntityEntry> entityEntryList = new LinkedList<>();
        EntityEntry entry = new EntityEntry();
        entry.setTransactionNum(1);
        entry.setType(NODETYPE);
        entry.setOperationType(EntityEntry.SET);
        entry.setIsTemporalProperty(true);
        entry.setId(1);
        entry.setStart(0);
        entry.setOther(10);
        entry.setKey("aa");
        entry.setValue(1);
        entry.setTxVersion(1);
        entityEntryList.add(entry);
        op1.setEntityEntries(entityEntryList);
        op1.setVersion(1);
        lockManager.lock(op1);

        DTGOperation op2 = new DTGOperation();
        LinkedList<EntityEntry> entityEntryList2 = new LinkedList<>();
        EntityEntry entry2 = new EntityEntry();
        entry2.setTransactionNum(1);
        entry2.setType(NODETYPE);
        entry2.setOperationType(EntityEntry.GET);
        entry2.setIsTemporalProperty(true);
        entry2.setId(1);
        entry2.setStart(0);
        entry2.setOther(10);
        entry2.setKey("aa");
        entry2.setValue(1);
        entry2.setTxVersion(2);
        entityEntryList2.add(entry2);
        op2.setEntityEntries(entityEntryList2);
        op2.setVersion(2);
        lockManager.lock(op2);

        DTGOperation op3 = new DTGOperation();
        LinkedList<EntityEntry> entityEntryList3 = new LinkedList<>();
        EntityEntry entry3 = new EntityEntry();
        entry3.setTransactionNum(1);
        entry3.setType(NODETYPE);
        entry3.setOperationType(EntityEntry.GET);
        entry3.setIsTemporalProperty(true);
        entry3.setId(1);
        entry3.setStart(0);
        entry3.setOther(10);
        entry3.setKey("aa");
        entry3.setValue(1);
        entry3.setTxVersion(3);
        entityEntryList3.add(entry3);
        op3.setEntityEntries(entityEntryList3);
        op3.setVersion(3);
        lockManager.lock(op3);

        DTGOperation op4 = new DTGOperation();
        LinkedList<EntityEntry> entityEntryList4 = new LinkedList<>();
        EntityEntry entry4 = new EntityEntry();
        entry4.setTransactionNum(1);
        entry4.setType(NODETYPE);
        entry4.setOperationType(EntityEntry.SET);
        entry4.setIsTemporalProperty(true);
        entry4.setId(1);
        entry4.setStart(1);
        entry4.setOther(5);
        entry4.setKey("aa");
        entry4.setValue(1);
        entry4.setTxVersion(4);
        entityEntryList4.add(entry4);
        op4.setEntityEntries(entityEntryList4);
        op4.setVersion(4);
        lockManager.lock(op4);

        lockManager.unlock(op1);

    }
}

package chaos;

import Element.DTGOperation;
import Element.EntityEntry;
import Element.OperationName;
import LocalDBMachine.LocalTransaction;
import LocalDBMachine.LocalTx.TransactionThreadLock;
import LocalDBMachine.MemMVCC;
import Region.DTGRegion;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.util.HashMap;
import java.util.LinkedList;

import static config.MainType.NODETYPE;

public class TransactionTest {

    @Test
    public void test() {
        GraphDatabaseService db= new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder("D:\\garbage\\8087\\DTG_DB" )
                .loadPropertiesFromFile("")
                .newGraphDatabase();
        DTGRegion region = new DTGRegion(1000,1000);
        MemMVCC memMVCC = new MemMVCC();
        for(int i = 0; i < 1; i++){
            LinkedList<EntityEntry> entityEntryList = new LinkedList<>();
            for(int j = 0; j < 5; j++){
                EntityEntry entry = new EntityEntry();
                entry.setId(i*10 +j);System.out.println("node id = " + entry.getId());
                entry.setTransactionNum(j);
                entry.setType(NODETYPE);
                entry.setOperationType(EntityEntry.ADD);
                entry.setIsTemporalProperty(false);
                entityEntryList.add(entry);
            }
            DTGOperation op = new DTGOperation(entityEntryList, OperationName.TRANSACTIONOP);

            op.setTxId(i + "");
            op.setVersion(1);
            long start = System.currentTimeMillis();

            TxThread thread = new TxThread(memMVCC, db, op, region, start, i);

            thread.start();
//            LocalTransaction3 tx = new LocalTransaction3(db, op, new HashMap<Integer, Object>(), new TransactionThreadLock(i + ""), region);
//            tx.run();
//            long end = System.currentTimeMillis();
//            System.out.println(end - start);
        }

        try {
            Thread.sleep(30000);
            //db.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

class TxThread extends Thread{

    DTGOperation op;
    DTGRegion region;
    long start;
    int i;
    GraphDatabaseService db;
    MemMVCC memMVCC;

    public TxThread(MemMVCC memMVCC, GraphDatabaseService db, DTGOperation op, DTGRegion region, long start, int i){
        this.op = op;
        this.region = region;
        this.start = start;
        this.i = i;
        this.db = db;
        this.memMVCC = memMVCC;
    }

    @Override
    public void run() {
        TransactionThreadLock lock = new TransactionThreadLock(i + "");
        HashMap map = new HashMap<Integer, Object>();
        LocalTransaction tx = new LocalTransaction(this.memMVCC, db, op, map, lock, region);
        tx.start();
        synchronized (lock){
            if(!tx.getCouldCommit()){
                try {
                    System.out.println("prepeare lock");
                    lock.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("lock done");
        }
        synchronized (lock){
            tx.setEndVersion(op.getVersion() + 1);
            lock.commit();
            lock.notify();
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }
}



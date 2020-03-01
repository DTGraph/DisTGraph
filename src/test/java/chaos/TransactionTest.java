package chaos;

import Element.DTGOperation;
import Element.EntityEntry;
import Element.OperationName;
import LocalDBMachine.LocalTransaction;
import LocalDBMachine.LocalTx.TransactionThreadLock;
import Region.DTGRegion;
import UserClient.DTGDatabase;
import UserClient.Transaction.DTGTransaction;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import static config.MainType.NODETYPE;

public class TransactionTest {

    @Test
    public void test() {
        GraphDatabaseService db= new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder("D:\\garbage\\8087\\DTG_DB" )
                .loadPropertiesFromFile("")
                .newGraphDatabase();
        DTGRegion region = new DTGRegion(1000,1000);
        for(int i = 0; i < 10; i++){
            LinkedList<EntityEntry> entityEntryList = new LinkedList<>();
            for(int j = 0; j < 5; j++){
                EntityEntry entry = new EntityEntry();
                entry.setId(i*10 +j);//System.out.println("node id = " + entry.getId());
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

            TxThread thread = new TxThread(db, op, region, start, i);

            thread.start();
//            LocalTransaction tx = new LocalTransaction(db, op, new HashMap<Integer, Object>(), new TransactionThreadLock(i + ""), region);
//            tx.run();
//            long end = System.currentTimeMillis();
//            System.out.println(end - start);
        }

        try {
            Thread.sleep(3000);
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

    public TxThread(GraphDatabaseService db, DTGOperation op, DTGRegion region, long start, int i){
        this.op = op;
        this.region = region;
        this.start = start;
        this.i = i;
        this.db = db;
    }

    @Override
    public void run() {
        LocalTransaction tx = new LocalTransaction(db, op, new HashMap<Integer, Object>(), new TransactionThreadLock(i + ""), region);
        tx.run();
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }
}



package performance;

import Element.NodeAgent;
import UserClient.DTGDatabase;
import UserClient.Transaction.DTGTransaction;
import org.junit.Test;
import performance.tool.OutPutCsv;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class addTemporalTest {

    @Test
    public void addTests() throws InterruptedException {
        DTGDatabase db = new DTGDatabase();
        db.init("127.0.0.1", 10086, "D:\\garbage");
        OutPutCsv output = new OutPutCsv("D:\\DTG\\test\\addTemporal000-2.csv", "i,start,end,cost");

        for(int i = 0; i < 1; i++){
            long start = System.currentTimeMillis();
            try (DTGTransaction tx = db.CreateTransaction()){
                for(int j = 0; j < 100; j++){
                    db.addNode();
                }
                tx.start();
            }
        }
        Thread.sleep(10000);

        long start = System.currentTimeMillis();
        System.out.println(System.currentTimeMillis());
        for(int i = 0; i < 1000; i++){
            TxThread2 a = new TxThread2(db, i, start, output);
            a.start();

        }

        try {
            Thread.sleep(2000000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        output.close();
    }
}

class TxThread2 extends Thread{

    DTGDatabase db;
    int i;
    long start;
    OutPutCsv output;
    private static AtomicInteger t1 = new AtomicInteger(0);
    private static AtomicInteger t2 = new AtomicInteger(0);

    public TxThread2(DTGDatabase db, int i, long start, OutPutCsv output){
        this.db = db;
        this.i = i;
        this.start = start;
        this.output = output;
    }

    @Override
    public void run() {
        try (DTGTransaction tx = db.CreateTransaction()){
            for(int j = 0; j < 100; j++){
                NodeAgent node = db.getNodeById(j);
                node.setTemporalProperty("a", i, i+1);
            }
            System.out.println("start  : " + tx.getTxId() + ", :" + t1.getAndIncrement());
            tx.start();

            Map<Integer, Object> map = tx.start();
            long end = System.currentTimeMillis();
            System.out.println("end  : " + tx.getTxId() + ", :" + t2.getAndIncrement());
            output.write(tx.getTxId(), Long.toString(start), Long.toString(end), Long.toString(end - start));
        }
    }
}

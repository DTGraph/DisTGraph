package performance;

import Element.NodeAgent;
import UserClient.DTGDatabase;
import UserClient.Transaction.DTGTransaction;
import org.junit.Test;
import tool.OutPutCsv;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class addTemporalTest {

    @Test
    public void addTests() throws InterruptedException {
        DTGDatabase db = new DTGDatabase();
        db.init("127.0.0.1", 10086, "D:\\garbage");
        OutPutCsv output = new OutPutCsv("D:\\garbage\\addTemporal0-lin.csv", "i,start,end,cost");

        for(int i = 0; i < 1; i++){
            long start = System.currentTimeMillis();
            try (DTGTransaction tx = db.CreateTransaction()){
                for(int j = 0; j < 20; j++){
                    db.addNode();
                }
                tx.start();
            }
        }
        //Thread.sleep(200000);

        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(1);
        ExecutorService pool = Executors.newFixedThreadPool(8);
        long start = System.currentTimeMillis();
        System.out.println(System.currentTimeMillis());
        for(int i = 0; i < 1; i++){
            TxThread2 a = new TxThread2(db, fixedThreadPool, i, start, output);
            pool.execute(a);
        }

        Thread.sleep(200000);

        output.close();
    }
}

class TxThread2 extends Thread{

    DTGDatabase db;
    int i;
    long start;
    OutPutCsv output;
    ExecutorService fixedThreadPool;
    private static AtomicInteger t1 = new AtomicInteger(0);
    private static AtomicInteger t2 = new AtomicInteger(0);

    public TxThread2(DTGDatabase db, ExecutorService fixedThreadPool , int i, long start, OutPutCsv output){
        this.db = db;
        this.i = i;
        this.start = start;
        this.output = output;
        this.fixedThreadPool =fixedThreadPool;
    }

    @Override
    public void run() {
        long start2 = System.currentTimeMillis();
        try (DTGTransaction tx = db.CreateTransaction()){
            for(int j = 0; j < 20; j++){
                db.addNode();
//                NodeAgent node = db.getNodeById(j);
//                //node.setProperty("a", i);
//                node.setTemporalProperty("a", i, i+1,i+1);
            }
            //System.out.println("start  : " + tx.getTxId() + ", :" + t1.getAndIncrement());
            tx.start();
            Map<Integer, Object> map = tx.start();
            long end = System.currentTimeMillis();
            //System.out.println("end  : " + tx.getTxId() + ", :" + t2.getAndIncrement());

            fixedThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    output.write(Long.toString(start), Long.toString(start2), Long.toString(end), Long.toString(end - start2));
                }
            });


        }
    }
}

package performance;

import Element.NodeAgent;
import UserClient.DTGDatabase;
import UserClient.Transaction.DTGTransaction;
import com.alipay.sofa.jraft.entity.LocalStorageOutter;
import org.junit.Test;
import tool.OutPutCsv;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class addConCurrency {

    static int allCount = 10;
    static int pool_size = 2;
    static AtomicInteger count = new AtomicInteger(0);
    static long[][] sta = new long[allCount][3];

    @Test
    public void addTests(){
        File file = new File("D:\\garbage\\txId");
        if(file.exists()){
            file.delete();
        }

        DTGDatabase db = new DTGDatabase();
        db.init("192.168.1.178", 10086, "D:\\garbage");

        OutPutCsv output = new OutPutCsv("D:\\distribute\\test\\addInConcurrency5000-10-00000000-" + pool_size + ".csv", "start,start2,end,cost");

        try (DTGTransaction tx = db.CreateTransaction()){
            //NodeAgent node = db.getNodeById(0);
            for(int i = 0; i < 1000; i++){
                db.addNode();
                db.addNode();
                db.addNode();
                db.addNode();
                db.addNode();
            }

//            NodeAgent node = db.addNode();
//            node.setProperty("sss", "111");
//            node.setTemporalProperty("aaa", 1, 6,"222");
////            int s1 = node.getProperty("sss");
//            int s2 = node.getNodeTemporalProperty("aaa", 3);
            Thread.sleep(30);
            Map<Integer, Object> map = tx.start();

//            System.out.println(map.get(s1));
//            System.out.println(map.get(s2));
//            map.get(0);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ExecutorService pool = Executors.newFixedThreadPool(pool_size);

        long start = System.currentTimeMillis();
        System.out.println(System.currentTimeMillis());
        for(int i = 0; i < allCount; i++){
            TxThread a = new TxThread(db, i, start, output);
            pool.execute(a);
        }


        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("all done");
        for(int i = 0; i < allCount; i++){
            output.write(Long.toString(sta[i][0]), Long.toString(sta[i][1]), Long.toString(sta[i][2]), Long.toString(sta[i][2] - sta[i][1]));
        }
        System.out.println("all done");

    }

}

class TxThread extends Thread{

    DTGDatabase db;
    int i;
    long start;
    OutPutCsv output;

    public TxThread(DTGDatabase db, int i, long start, OutPutCsv output){
        this.db = db;
        this.i = i;
        this.start = start;
        this.output = output;
    }

    @Override
    public void run() {
        //System.out.println("start : " + count);
        long start2 = System.currentTimeMillis();
        try (DTGTransaction tx = db.CreateTransaction()){
            for(int j = 0; j < 5000; j++){
                NodeAgent n = db.getNodeById(j);
                n.setTemporalProperty("a", i, i+1, "aaaa");
            }

            Map<Integer, Object> map = tx.start();
            map.get(0);
            long end = System.currentTimeMillis();

            addConCurrency.sta[i][0] = start;
            addConCurrency.sta[i][1] = start2;
            addConCurrency.sta[i][2] = end;
        }finally {
            return;
        }
    }
}





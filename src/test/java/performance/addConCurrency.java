package performance;

import UserClient.DTGDatabase;
import UserClient.Transaction.DTGTransaction;
import com.alipay.sofa.jraft.entity.LocalStorageOutter;
import org.junit.Test;
import tool.OutPutCsv;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class addConCurrency {

    @Test
    public void addTests(){
        DTGDatabase db = new DTGDatabase();
        db.init("127.0.0.1", 10086, "D:\\garbage");
        File file = new File("D:\\garbage\\txId");
        if(file.exists()){
            file.delete();
        }
        file = null;
        OutPutCsv output = new OutPutCsv("D:\\DTG\\test\\addInConcurrency50-80-6.csv", "start,end,cost");

//        try (DTGTransaction tx = db.CreateTransaction()){
//            db.addNode();
//            tx.start();
//        }
        try (DTGTransaction tx = db.CreateTransaction()){
            db.addNode();
            db.addNode();
            db.addNode();
            db.addNode();
            db.addNode();
            Thread.sleep(30);
            Map<Integer, Object> map = tx.start();
            map.get(0);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long start = System.currentTimeMillis();
        System.out.println(System.currentTimeMillis());
        for(int i = 0; i < 1; i++){
            TxThread a = new TxThread(db, i, start, output);
            a.start();
        }

        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        output.close();
    }
}

class TxThread extends Thread{

    DTGDatabase db;
    int i;
    long start;
    private ExecutorService fixedThreadPool;
    OutPutCsv output;

    public TxThread(DTGDatabase db, int i, long start, OutPutCsv output){
        this.db = db;
        this.i = i;
        this.start = start;
        fixedThreadPool = Executors.newFixedThreadPool(1);
        this.output = output;
    }

    @Override
    public void run() {
        int count = 0;
        while(count < 1){
            //System.out.println("start : " + count);
            try (DTGTransaction tx = db.CreateTransaction()){
                db.addNode();
                db.addNode();
                db.addNode();
                db.addNode();
                db.addNode();
                Thread.sleep(30);
                Map<Integer, Object> map = tx.start();
                map.get(0);
                long end = System.currentTimeMillis();
                //System.out.println("end  : " + count);
                fixedThreadPool.execute(new Runnable() {
                    @Override
                    public void run() {
                        output.write(Long.toString(start), Long.toString(end), Long.toString(end - start));
                    }
                });
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            count++;
        }
    }
}

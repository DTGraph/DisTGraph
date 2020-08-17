package performance.DTG;

import Element.NodeAgent;
import UserClient.DTGDatabase;
import UserClient.Transaction.DTGTransaction;
import org.junit.Test;
import tool.OutPutCsv;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DTGAddNodeTest {

    @Test
    public void addTests(){
        DTGDatabase db = new DTGDatabase();
        db.init("127.0.0.1", 10086, "D:\\garbage");
        int threadPoolNum = 1;
        File file = new File("D:\\garbage\\txId");
        if(file.exists()){
            file.delete();
        }
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(1);
        OutPutCsv output = new OutPutCsv("D:\\garbage\\addInConcurrency50-80-6.csv", "start,runtime,end,cost");

        ExecutorService threadPool = Executors.newFixedThreadPool(threadPoolNum);

        try (DTGTransaction tx = db.CreateTransaction()){
            for(int i = 0; i < 5; i++){
                NodeAgent node = db.addNode();
                node.setProperty("name", "1");
            }
            Thread.sleep(30);
            Map<Integer, Object> map = tx.start();
            map.get(0);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long start = System.currentTimeMillis();
        System.out.println(System.currentTimeMillis());
        for(int i = 0; i < 50; i++){
            DTGThread thread = new DTGThread(db, i, start, fixedThreadPool, output);
            threadPool.submit(thread);
        }

        threadPool.shutdown();
        while(true){
            if(threadPool.isTerminated()){
                System.out.println("所有的子线程都结束了！");
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        output.close();
    }
}

class DTGThread extends Thread{

    DTGDatabase db;
    int i;
    long start;
    ExecutorService fixedThreadPool;
    OutPutCsv output;

    public DTGThread(DTGDatabase db, int i, long start, ExecutorService fixedThreadPool, OutPutCsv output){
        this.db = db;
        this.i = i;
        this.start = start;
        this.fixedThreadPool = fixedThreadPool;
        this.output = output;
    }

    @Override
    public void run() {
        long runtime = System.currentTimeMillis();
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
            fixedThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    output.write(Long.toString(start), Long.toString(runtime), Long.toString(end), Long.toString(end - start));
                }
            });
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

package performance.ACID;

import Element.NodeAgent;
import UserClient.DTGDatabase;
import UserClient.Transaction.DTGTransaction;
import org.junit.Test;
import tool.OutPutCsv;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import Element.NodeAgent;
import UserClient.DTGDatabase;
import UserClient.Transaction.DTGTransaction;
import org.junit.Test;
import sun.nio.cs.ext.MacArabic;
import tool.OutPutCsv;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TX1 {


    @Test
    public void initTests() {
        File file = new File("D:\\garbage\\txId");
        if (file.exists()) {
            file.delete();
        }

        DTGDatabase db = new DTGDatabase();
        db.init("127.0.0.1", 10086, "D:\\garbage");
        int threadPoolNum = 3;

        try (DTGTransaction tx = db.CreateTransaction()) {
            NodeAgent node = db.addNode();
            node.setTemporalProperty("status", 1, 5, "jam");
            System.out.println(node.getTransactionObjectId());
            //int t2 = node.getNodeTemporalProperty("status", 3);
            Thread.sleep(30);
            Map<Integer, Object> map = tx.start(3);
            //System.out.println(map.get(t2));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long start = System.currentTimeMillis();
        System.out.println(System.currentTimeMillis());
    }

    @Test
    public void commitReadTests() {
        File file = new File("D:\\garbage\\txId");
//        if (file.exists()) {
//            file.delete();
//        }
        DTGDatabase db = new DTGDatabase();
        db.init("127.0.0.1", 10086, "D:\\garbage");
        int threadPoolNum = 3;

        ExecutorService threadPool = Executors.newFixedThreadPool(threadPoolNum);

        threadPool.execute(new Runnable() {
            @Override
            public void run() {
                try (DTGTransaction tx = db.CreateTransaction()) {
                    NodeAgent node = db.getNodeById(0);
                    node.setTemporalProperty("status", 1, 5, "smooth");
//                    int t2 = node.getTemporalProperty("status", 3);
//                    int t3 = node.getTemporalProperty("status", 3);
                    Thread.sleep(30);
                    Map<Integer, Object> map = tx.start(3);
                    map.get(0);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        threadPool.execute(new Runnable() {
            @Override
            public void run() {
                try (DTGTransaction tx = db.CreateTransaction()) {
                    NodeAgent node = db.getNodeById(0);
                    //node.setTemporalProperty("status", 1, 5, "smooth");
                    int t2 = node.getTemporalProperty("status", 3);
                    Thread.sleep(30);
                    Map<Integer, Object> map = tx.start(3);
                    map.get(0);
                    System.out.println(map.get(t2));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            Thread.sleep(300000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}

package TransactionTest;

import Element.NodeAgent;
import Element.RelationshipAgent;
import LocalDBMachine.LocalTransaction;
import UserClient.DTGDatabase;
import UserClient.Transaction.DTGTransaction;

import java.io.IOException;
import java.util.Map;

/**
 * @author :jinkai
 * @date :Created in 2019/10/25 16:40
 * @description:
 * @modified By:
 * @version:
 */

public class remoteClient {

    public static void main(String[] args) throws InterruptedException {
        DTGDatabase db = new DTGDatabase();
        db.init("127.0.0.1", 10086, "D:\\garbage");

        try (DTGTransaction tx = db.CreateTransaction()){
//            NodeAgent node = db.addNode();
//            node.setProperty("chaos.aa", "tt");
//            node.setTemporalProperty("cd", 1, "ss");
            //int id = node.getTransactionObjectId();
            db.addNode();
            db.addNode();
            db.addNode();
            db.addNode();
            db.addNode();
            db.addNode();
            db.addNode();
            db.addNode();
            db.addNode();
            db.addNode();
            db.addNode();
            db.addNode();
            db.addNode();
            db.addNode();
            db.addNode();

            tx.start();
            //Map<Integer, Object> map = tx.start();
            //System.out.println(map.get(id).toString());
            //tx.commit();
        }

        Thread.sleep(1000);

//        try (DTGTransaction tx = db.CreateTransaction()){
////            NodeAgent node = db.addNode();
////            node.setProperty("chaos.aa", "tt");
////            node.setTemporalProperty("cd", 1, "ss");
//            //int id = node.getTransactionObjectId();
//            NodeAgent node = db.getNodeById(0);
//            node.deleteself();
//
//            tx.start();
//            //Map<Integer, Object> map = tx.start();
//            //System.out.println(map.get(id).toString());
//            //tx.commit();
//        }

        long start = System.currentTimeMillis();
        //System.out.println(start);
        for(int i = 0; i < 250; i++){
            //System.out.println("start : " + System.currentTimeMillis());
            TxThread a = new TxThread(db, i, start);
            a.start();
        }

//        for(int i = 0; i < 10; i++){
//            long start = System.currentTimeMillis();
//            try (DTGTransaction tx = db.CreateTransaction()){
////            NodeAgent node = db.addNode();
////            node.setProperty("chaos.aa", "tt");
////            node.setTemporalProperty("cd", 1, "ss");
//                //int id = node.getTransactionObjectId();
//                db.addNode();
//                db.addNode();
//                db.addNode();
//                db.addNode();
//                db.addNode();
//
//                Map<Integer, Object> map = tx.start();
//                System.out.println(map.get(-1));
//                //Map<Integer, Object> map = tx.start();
//                //System.out.println(map.get(id).toString());
//                //tx.commit();
//            }
//            long end = System.currentTimeMillis();
//            System.out.println("cost : " + (end - start));
//        }

//        try (DTGTransaction tx = db.CreateTransaction()){
////            NodeAgent node = db.addNode();
////            node.setProperty("chaos.aa", "tt");
////            node.setTemporalProperty("cd", 1, "ss");
//            //int id = node.getTransactionObjectId();
//            db.addNode();
//            db.addNode();
//            db.addNode();
//            db.addNode();
//            db.addNode();
//
//            tx.start();
//            //Map<Integer, Object> map = tx.start();
//            //System.out.println(map.get(id).toString());
//            //tx.commit();
//        }

//        try (DTGTransaction tx = db.CreateTransaction()){
////            NodeAgent node = db.getNodeById(0);
////            int id1 = node.getProperty("chaos.aa");
////            int id2 = node.getNodeTemporalProperty("cd", 1);
////            node.setTemporalProperty("cd", 2, "sss");
////            node.setProperty("chaos.aa", "ttt");
//            db.addNode();
//            db.addNode();
//            db.addNode();
//            db.addNode();
//            db.addNode();
//            db.addNode();
//
//            Map<Integer, Object> map = tx.start();
////            System.out.println(map.get(id1));
////            System.out.println(map.get(id2));
//            //tx.commit();
//        }
//
//        try (DTGTransaction tx = db.CreateTransaction()){
////            NodeAgent node = db.getNodeById(0);
////            int id1 = node.getProperty("chaos.aa");
////            int id2 = node.getNodeTemporalProperty("cd", 2);
//            db.addNode();
//            db.addNode();
//            db.addNode();
//            db.addNode();
//            db.addNode();
//            db.addNode();
//
//            //tx.start();
//            Map<Integer, Object> map = tx.start();
////            System.out.println(map.get(id1));
////            System.out.println(map.get(id2));
//            //tx.commit();
//        }
//
//        try (DTGTransaction tx = db.CreateTransaction()){
////            NodeAgent node = db.getNodeById(0);
////            int id1 = node.getProperty("chaos.aa");
////            int id2 = node.getNodeTemporalProperty("cd", 2);
//            db.addNode();
//            db.addNode();
//            db.addNode();
//            db.addNode();
//            db.addNode();
//            db.addNode();
//
//            //tx.start();
//            Map<Integer, Object> map = tx.start();
////            System.out.println(map.get(id1));
////            System.out.println(map.get(id2));
//            //tx.commit();
//        }
//
        try {
            Thread.sleep(1000000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

//        try (DTGTransaction tx = db.CreateTransaction()){
//            db.addNode();
//            db.addNode();
//            db.addNode();
//            db.addNode();
//            tx.start();
//            //tx.commit();
//        }
//
//        try {
//            Thread.sleep(10000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//
//        try (DTGTransaction tx = db.CreateTransaction()){
//            db.addNode();
//            db.addNode();
//            db.addNode();
//            db.addNode();
//            tx.start();
//            //tx.commit();
//        }


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            db.shutdown();
        }));
    }
}

class TxThread extends Thread{

    DTGDatabase db;
    int i;
    long start;

    public TxThread(DTGDatabase db, int i, long start){
        this.db = db;
        this.i = i;
        this.start = start;
    }

    @Override
    public void run() {
        try (DTGTransaction tx = db.CreateTransaction()){
            //System.out.println(" i : " + System.currentTimeMillis());
            //NodeAgent node = db.addNode();
            //node.setProperty("chaos.aa", "tt");
            //node.setTemporalProperty("cd", 1, "ss");
            //int id = node.getTransactionObjectId();
            db.addNode();
            db.addNode();
            db.addNode();
            db.addNode();
            db.addNode();

            Map<Integer, Object> map = tx.start();
            //System.out.println(map.get(-1));
            //Map<Integer, Object> map = tx.start();
            //tx.commit();
            long end = System.currentTimeMillis();
            //System.out.println("end :" + end);
            System.out.println(this.i + " - cost : " + (end - this.start));
            //tx.commit();
        }
    }
}

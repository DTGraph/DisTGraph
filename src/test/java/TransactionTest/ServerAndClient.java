package TransactionTest;

import Element.NodeAgent;
import UserClient.DTGDatabase;
import UserClient.Transaction.DTGTransaction;
import storage.ClusterServer;

import java.util.Map;

/**
 * @author :jinkai
 * @date :Created in 2019/10/24 14:00
 * @description:
 * @modified By:
 * @version:
 */

public class ServerAndClient {

    public static void main(String[] args) {
        DTGDatabase db = new ClusterServer("127.0.0.1", 8184, "D:\\garbage\\8084").getDB();

        try (DTGTransaction tx = db.CreateTransaction()){
            NodeAgent node = db.addNode();
            node.setProperty("chaos.aa", "tt");
            node.setTemporalProperty("cd", 1, "ss");
            //int id = node.getTransactionObjectId();
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

        try (DTGTransaction tx = db.CreateTransaction()){
//            NodeAgent node = db.getNodeById(0);
//            int id1 = node.getProperty("chaos.aa");
//            int id2 = node.getNodeTemporalProperty("cd", 1);
//            node.setTemporalProperty("cd", 2, "sss");
//            node.setProperty("chaos.aa", "ttt");
            db.addNode();
            db.addNode();
            db.addNode();
            db.addNode();
            db.addNode();
            db.addNode();

            Map<Integer, Object> map = tx.start();
//            System.out.println(map.get(id1));
//            System.out.println(map.get(id2));
            //tx.commit();
        }

        try (DTGTransaction tx = db.CreateTransaction()){
//            NodeAgent node = db.getNodeById(0);
//            int id1 = node.getProperty("chaos.aa");
//            int id2 = node.getNodeTemporalProperty("cd", 2);
            db.addNode();
            db.addNode();
            db.addNode();
            db.addNode();
            db.addNode();
            db.addNode();

            //tx.start();
            Map<Integer, Object> map = tx.start();
//            System.out.println(map.get(id1));
//            System.out.println(map.get(id2));
            //tx.commit();
        }

        try {
            Thread.sleep(1000000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {System.out.println("START SHUTDOWN");
            db.shutdown();
        }));

    }
}

package TransactionTest;

import Element.NodeAgent;
import Element.RelationshipAgent;
import LocalDBMachine.LocalTransaction;
import UserClient.DTGDatabase;
import UserClient.Transaction.DTGTransaction;

import java.io.IOException;

/**
 * @author :jinkai
 * @date :Created in 2019/10/25 16:40
 * @description:
 * @modified By:
 * @version:
 */

public class remoteClient {

    public static void main(String[] args){
        DTGDatabase db = new DTGDatabase();
        db.init("127.0.0.1", 10086, "D:\\garbage");
        try (DTGTransaction tx = db.CreateTransaction()){
            System.out.println(tx.getTxId());
            NodeAgent node = db.addNode();
            node.setProperty("a","this is a");
            node.setTemporalProperty("b", 0, 3, "this is b");
            tx.start(null);
            tx.commit();
        }
        System.out.println("END : " + System.currentTimeMillis());
        try (DTGTransaction tx = db.CreateTransaction()){
            System.out.println(tx.getTxId());
            NodeAgent node = db.getNodeById(0);
            NodeAgent node2 = db.addNode();
            db.addNode();
            RelationshipAgent r = db.addRelationship(node, node2);
            r.setProperty("a","this is a");
            r.setTemporalProperty("b", 0, 3, "this is b");
            tx.start(null);
            tx.commit();
        }
        System.out.println("END : " + System.currentTimeMillis());

        try (DTGTransaction tx = db.CreateTransaction()){
            db.addNode();
            db.addNode();
            db.addNode();
            db.addNode();
            tx.start(null);
            tx.commit();
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            db.shutdown();
        }));
    }
}

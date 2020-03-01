import config.RelType;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import sun.management.FileSystem;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;


public class simple {

    @Test
    public void test() {

//        Map<Integer, int[]> ccc = new HashMap<>();
//        int[] b = new int[3];
//        b[0] = 1;
//        b[1] = 2;
//        System.out.println(b[0]);
//        ccc.put(1, b);
//        change(ccc);
//        System.out.println(b[0]);
//        int[] chaos.aa = ccc.get(1);
//        System.out.println(chaos.aa[0]);
        GraphDatabaseService db= new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder("D:\\garbage\\8085\\DTG_DB" )
                .loadPropertiesFromFile("")
                .newGraphDatabase();
        //Node node1, node2;
        Transaction tx = db.beginTx();
          db.getNodeById(0);
//        System.out.println(node1.getProperty("d"));
//        node1 =  db.createNode();
//        System.out.println(node1.getId());
//        node1 =  db.createNode();
//        System.out.println(node1.getId());
//        node2 =  db.createNode();
//        System.out.println(node1.getId());
//        node1.setProperty("d",2);
        tx.success();
        tx.close();
//        for(int i = 0; i < 10; i++){
//            long start = System.currentTimeMillis();
//            //Transaction tx = db.beginTx();
//            try(Transaction tx = db.beginTx()){
//                //System.out.println(db.getNodeById(22222).getTemporalProperty("yys", 2));
//                //System.out.println(db.getNodeById(1).getProperty("d"));
//                //System.out.println(db.getRelationshipById(99).getTemporalProperty("travel_time", 2));
//
//                db.createNode();
//                db.createNode();
//                db.createNode();
//                db.createNode();
//                db.createNode();
//
////            Node node = db.getNodeById(50);
////            System.out.println(node.getProperty("chaos.aa"));
////            System.out.println(node.getProperty(getKey("chaos.aa", 0)));
////            int a =2;
////            long id = 22222;
////            node1 =  db.createNode();
////            System.out.println(node1.getId());
////            node1 =  db.createNode(id);
////            System.out.println(node1.getId());
//                //           node2 =  db.createNode();
////            System.out.println(node1.getId());
////            node1.setProperty("d",2);
////            node1.setTemporalProperty("yys", 0, 3, "asasasa");
////            db.getNodeById(1).setProperty("d",2);
////            node2 =  db.createNode();
////            node1 = db.getNodeById(13);
////            Relationship r = node1.createRelationshipTo(99, node2.getId(), RelType.ROAD_TO);//System.out.println(r.getId());
////            r.setTemporalProperty("travel_time", 0,3, 0);
////            System.out.println(r.getId());
//                tx.success();
//                //tx.close();
//            }
//            long end = System.currentTimeMillis();
//            System.out.println(end - start);
//        }



//        try(Transaction tx = db.beginTx()){
//            db.getNodeById(0).delete();
//            tx.success();
//        }

        try {
            Thread.sleep(3000);
            //db.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public String getKey(String key, long version){
        return key + "/" + version;
    }

//    public void change(Map<Integer, int[]> v){
//        int[] a = v.get(1);
//        a[0] = 2;
//    }

}

class a{
    int t;
    public a(){
        t = 0;
    }


    public int getT() {
        return t;
    }

    public void setT(int t) {
        this.t = t;
    }
}


package performance.DTG;

import Element.NodeAgent;
import Element.RelationshipAgent;
import UserClient.DTGDatabase;
import UserClient.Transaction.DTGTransaction;
import org.junit.Test;
import tool.ObjectAndByte;
import tool.OutPutCsv;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static config.DTGConstants.NULLSTRING;

class Obtest implements Serializable{
    String str = "this is value";

    @Override
    public String toString(){
        return str;
    }
}

public class simpleTest {

    public void outputCost(DTGTransaction tx, long start, long end){
        System.out.println(System.currentTimeMillis() + "  " + tx.getTxId() + " : transaction done! cost : " + (end - start) + "ms");
    }

    @Test
    public void addTests() throws InterruptedException {
        File file = new File("D:\\garbage\\txId");
//        if(file.exists()){
//            file.delete();
//        }
        DTGDatabase db = new DTGDatabase();
        db.init("127.0.0.1", 10086, "D:\\garbage");

        for(int i = 0; i < 1; i++){
            long start = System.currentTimeMillis();
            try (DTGTransaction tx = db.CreateTransaction()){
                int t1 = -1, t2 = -1, t3 = -1;
                for(int j = 0; j < 1; j++){
                    NodeAgent node1 = db.addNode();
                    node1.setProperty("p3", "p4");
                    NodeAgent node2 = db.addNode();
                    node2.setProperty("p3", "p5");
                    NodeAgent node3 = db.addNode();
                    node3.setProperty("p3", "p5");
//
//                    RelationshipAgent r = db.getRelationshipById(0);
//                    NodeAgent node3 = r.getStartNode();
//                    t3 = node3.getProperty("p3");
//                    node3.setProperty("p3", "p15");
//                    t2 = node3.getProperty("p3");
//                    r.deleteTemporalProperty("tr1");
                    //t1 = r.getEndNode();
//                    r.setProperty("r2", "r3");
//                    r.deleteProperty("r1");
//                    t2 = r.getNodeTemporalProperty("tr1", 1);
//                    r.setTemporalProperty("tr1", 1, 10, "tr2");
//                    node1.setTemporalProperty("name", 1,5, "sssss");
//                    NodeAgent node1 = db.getNodeById(1);
//                    NodeAgent node2 = db.getNodeById(1364);
//                    node1.deleteTemporalProperty("name");
//                    t3 = node1.getTemporalProperty("name", 5);
//                    node1.setTemporalProperty("name2", 1,5, "sssss2");
//                    node1.setTemporalProperty("name", 1,5, "sssss1");
//                    t2 = node1.getTemporalProperty("name2", 5);
//                    t2 = node1.getProperty("p2");
//                    node1.setProperty("p2", "vp4");
//                    node1.deleteProperty("p3");
//                    t3 = node1.getProperty("p3");
//                    node.setProperty("p2", "p4");
//                    t2 = node.getProperty("p1");
//                    node1.deleteProperty("p1");
//                    node1.deleteProperty("p3");
//                    node1.deleteProperty("p3");
//                    t3 = r1.getProperty("length");
//                    t2 = r2.getProperty("length");

//                    t2 = node.getProperty("r1");
//                    t2 = r1.getTemporalProperty("carnumber", 3);
//                    t3 = r2.getTemporalProperty("carnumber", 3);
//                    t1 = r.getNodeTemporalProperty("tr1", 1);
//                    t1 = r.getProperty("r1");
//                    t2 = r.getProperty("r2");
                }
                Map map = tx.start(t1);
                long end = System.currentTimeMillis();
                outputCost(tx, start, end);
                System.out.println(map.get(t3));
                System.out.println(map.get(t2));
            }
        }

        Thread.sleep(10000);

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void interfaceTest(){

        DTGDatabase db = new DTGDatabase();
        db.init("127.0.0.1", 10086, "D:\\garbage");

        for(int i = 0; i < 1; i++){
            System.out.println();
            System.out.println();
            long start = System.currentTimeMillis();
            try (DTGTransaction tx = db.CreateTransaction()){
                int t2 = -1, t3 = -1;
                //添加节点
                NodeAgent node1 = db.addNode();
                NodeAgent node2 = db.addNode();
                //添加边
                RelationshipAgent r = db.addRelationship(node1, node2);

//                RelationshipAgent r = db.getRelationshipById(0);
                //添加静态属性
                r.setProperty("name", "Beihang Road");
                //添加时态属性
                r.setTemporalProperty("status", 1,15, "sss");
                //获取静态属性
                t2 = r.getProperty("name");
                //获取时态属性
                t3 = r.getTemporalProperty("status", 3);
//                //删除属性
//                node1.deleteProperty("name");
//                r.getTemporalPropertyBetweenAAndB("status", 3, 15, 10);

                //获取结果集
                Map map = tx.start();
                long end = System.currentTimeMillis();
                //输出结果
                outputCost(tx.getTxId(), start, end);
                System.out.println(map.get(t2));
                System.out.println(map.get(t3));
            }
        }
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void importTopo(){
        DTGDatabase db = new DTGDatabase();
        db.init("127.0.0.1", 10086, "D:\\garbage");

        String line = "";
        BufferedReader in = null, tempIn = null;
        try {
            in = new BufferedReader(new FileReader("D:\\distribute\\topo2.csv"));
            line = in.readLine();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println();
        System.out.println();
        long startT = System.currentTimeMillis();

        try (DTGTransaction tx = db.CreateTransaction()){
            NodeAgent[] nodes = new NodeAgent[5050];
            for(int i = 0; i < 5050; i++){
                nodes[i] = db.addNode();
            }
            for(int i = 0; i < 5028; i++){//System.out.println(line);
                line = in.readLine();
                String[] info = line.split(",");
                NodeAgent start = nodes[Integer.parseInt(info[2])];
                NodeAgent end = nodes[Integer.parseInt(info[3])];
                RelationshipAgent r = db.addRelationship(start, end);
                r.setProperty("length", Integer.parseInt(info[4]));
            }
            tx.start();
            long end = System.currentTimeMillis();
            outputCost(tx.getTxId(), startT, end);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Map<Integer, Object> importTopo(int t){
        DTGDatabase db = new DTGDatabase();
        db.init("127.0.0.1", 10086, "D:\\garbage");

        String line = "";
        BufferedReader in = null, tempIn = null;
        try {
            in = new BufferedReader(new FileReader("D:\\distribute\\topo2.csv"));
            line = in.readLine();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println();
        System.out.println();
        long startT = System.currentTimeMillis();

        try (DTGTransaction tx = db.CreateTransaction()){
            NodeAgent[] nodes = new NodeAgent[5050];
            for(int i = 0; i < 5050; i++){
                nodes[i] = db.addNode();
            }
            for(int i = 0; i < 5028; i++){//System.out.println(line);
                line = in.readLine();
                String[] info = line.split(",");
                NodeAgent start = nodes[Integer.parseInt(info[2])];
                NodeAgent end = nodes[Integer.parseInt(info[3])];
                RelationshipAgent r = db.addRelationship(start, end);
                r.setProperty("length", Integer.parseInt(info[4]));
            }
            Map<Integer, Object> res = tx.start();
            long end = System.currentTimeMillis();
            outputCost(tx.getTxId(), startT, end);
            return res;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Test
    public void importSomeData(){
        //String tempPath = "D:\\distribute\\temp.csv";

        DTGDatabase db = new DTGDatabase();
        db.init("127.0.0.1", 10086, "D:\\garbage");

        String line = "";
        BufferedReader in = null, tempIn = null;
        try {
            tempIn = new BufferedReader(new FileReader("D:\\distribute\\temp2.csv"));
            line = tempIn.readLine();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println();
        System.out.println();
        long startT = System.currentTimeMillis();

        for(int j = 0; j < 20; j++){
            try (DTGTransaction tx = db.CreateTransaction()){
                RelationshipAgent[] relations = new RelationshipAgent[5028];
                for(int i = 0; i < 5028; i++){
                    relations[i] = db.getRelationshipById(i);
                }
                for(int i = 0; i < 5028; i++){//System.out.println(line);
                    line = tempIn.readLine();
                    String[] info = line.split(",");
                    RelationshipAgent r = relations[Integer.parseInt(info[1])];
                    int time = Integer.parseInt(info[0]);
                    //System.out.println(time);
                    r.setTemporalProperty("status", time, time+5 , info[2]);
                    r.setTemporalProperty("carnumber", time, time+5 , Integer.parseInt(info[3]));
                    r.setTemporalProperty("crosstime", time, time+5 , info[4]);
                }
                tx.start(-1);
                long end = System.currentTimeMillis();
                outputCost(tx.getTxId(), startT, end);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public int addTimes = 20;
    public Map<Integer, Object> importSomeData(int t){
        //String tempPath = "D:\\distribute\\temp.csv";

        DTGDatabase db = new DTGDatabase();
        db.init("127.0.0.1", 10086, "D:\\garbage");

        String line = "";
        BufferedReader in = null, tempIn = null;
        try {
            tempIn = new BufferedReader(new FileReader("D:\\distribute\\temp2.csv"));
            line = tempIn.readLine();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println();
        System.out.println();

        Map<Integer, Object> res = null;
        for(int j = 0; j < 2; j++){
            try (DTGTransaction tx = db.CreateTransaction()){
                long startT = System.currentTimeMillis();
                RelationshipAgent[] relations = new RelationshipAgent[5028];
                for(int i = 0; i < 5028; i++){
                    relations[i] = db.getRelationshipById(i);
                }
                for(int i = 0; i < 5028; i++){//System.out.println(line);
                    line = tempIn.readLine();
                    String[] info = line.split(",");
                    RelationshipAgent r = relations[Integer.parseInt(info[1])];
                    int time = Integer.parseInt(info[0]);
                    //System.out.println(time);
                    r.setTemporalProperty("status", time, time+5 , info[2]);
                    r.setTemporalProperty("carnumber", time, time+5 , Integer.parseInt(info[3]));
                    r.setTemporalProperty("crosstime", time, time+5 , info[4]);
                }
                res = tx.start(-1);
                long end = System.currentTimeMillis();
                outputCost(tx.getTxId(), startT, end);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return res;
    }

    //@Test
    public Map<Integer, Object> readSomeData(int time){
        //String tempPath = "D:\\distribute\\temp.csv";

        DTGDatabase db = new DTGDatabase();
        db.init("127.0.0.1", 10086, "D:\\garbage");
        //int time = 4;
        Map map;
        int[] status = new int[5028];
        int[] carnumber = new int[5028];
        int[] crosstime = new int[5028];
        Map<Integer, Object> res = new HashMap<>();
        for(int j = 0; j < 1; j++){

            System.out.println();
            System.out.println();
            long startT = System.currentTimeMillis();

            try (DTGTransaction tx = db.CreateTransaction()){
                for(int i = 0; i < 5028; i++){
                    RelationshipAgent r = db.getRelationshipById(i);
                    status[i] = r.getTemporalProperty("status", time);
                    carnumber[i] = r.getTemporalProperty("carnumber", time);
                    crosstime[i] = r.getTemporalProperty("crosstime", time);
                }
                map = tx.start();
                long end = System.currentTimeMillis();
                res.put(-2, map.get(-2));
                res.put(-3, map.get(-3));
                res.put(-4, map.get(-4));
                for(int i = 0; i < 5028; i++){
                    String st;
                    switch ((String)map.get(status[i])){
                        case "0" :{
                            st = "smooth";
                            break;
                        }
                        case "1" :{
                            st = "jam";
                            break;
                        }
                        case "2" :{
                            st = "slow";
                            break;
                        } default:
                            st = "stop";
                    }
                    int car = (int)map.get(carnumber[i]);
                    String ctime = (String)map.get(crosstime[i]);
                    res.put(i, "road " + i + " info [status:" + st + ", car number:" + car + ", cross time:" + ctime + "]");
                    //System.out.println("road " + i + " info [status:" + st + ", car number:" + car + ", cross time:" + ctime + "]");
                }
                outputCost(tx.getTxId(), startT, end);
            }
        }
        return res;
    }

    @Test
    public void readSomeData(){
        //String tempPath = "D:\\distribute\\temp.csv";

        DTGDatabase db = new DTGDatabase();
        db.init("127.0.0.1", 10086, "D:\\garbage");
        //int time = 4;
        Map map;
        int[] status = new int[5028];
        int[] carnumber = new int[5028];
        int[] crosstime = new int[5028];
        for(int j = 0; j < 1; j++){

            System.out.println();
            System.out.println();
            long startT = System.currentTimeMillis();

            try (DTGTransaction tx = db.CreateTransaction()){
                for(int i = 0; i < 5028; i++){
                    RelationshipAgent r = db.getRelationshipById(i);
                    status[i] = r.getTemporalProperty("status", 4);
                    carnumber[i] = r.getTemporalProperty("carnumber", 4);
                    crosstime[i] = r.getTemporalProperty("crosstime", 4);
                }
                map = tx.start();
                long end = System.currentTimeMillis();
                for(int i = 0; i < 5028; i++){
                    String st;
                    switch ((String)map.get(status[i])){
                        case "0" :{
                            st = "smooth";
                            break;
                        }
                        case "1" :{
                            st = "jam";
                            break;
                        }
                        case "2" :{
                            st = "slow";
                            break;
                        } default:
                            st = "stop";
                    }
                    int car = (int)map.get(carnumber[i]);
                    String ctime = (String)map.get(crosstime[i]);
//                    resList.add("road " + i + " info [status:" + st + ", car number:" + car + ", cross time:" + ctime + "]");
                    System.out.println("road " + i + " info [status:" + st + ", car number:" + car + ", cross time:" + ctime + "]");
                }
                outputCost(tx.getTxId(), startT, end);
            }
        }
    }

    @Test
    public void readSomeLimitData(){
        //String tempPath = "D:\\distribute\\temp.csv";

        DTGDatabase db = new DTGDatabase();
        db.init("127.0.0.1", 10086, "D:\\garbage");
        int time = 4;
        Map map;
        int[] carnumber = new int[5028];
        int[] lengths = new int[5028];
        for(int j = 0; j < 1; j++){

            System.out.println();
            System.out.println();
            long startT = System.currentTimeMillis();
            try (DTGTransaction tx = db.CreateTransaction()){
                for(int i = 0; i < 5028; i++){
                    RelationshipAgent r = db.getRelationshipById(i);
                    carnumber[i] = r.getTemporalPropertyBetweenAAndB("carnumber", time, 15, 10);
//                    lengths[i] = r.getPropertyBetweenAAndB("length", 15, 10);
                }
                map = tx.start();
                long end = System.currentTimeMillis();
                for(int i = 0; i < 5028; i++){
                    String car = (String)(map.get(carnumber[i]) + "");//System.out.println("road " + i + " info [status:" + ", car number:" + car + "]");
                    if(!car.equals(NULLSTRING)){
                        System.out.println("road " + i + " info [ car in the road :" + car + "]");
                    }
                }
                outputCost(tx.getTxId(), startT, end);
            }
        }
    }

    public Map<Integer, Object> readSomeLimitData(int max, int min, int time){
        //String tempPath = "D:\\distribute\\temp.csv";

        DTGDatabase db = new DTGDatabase();
        db.init("127.0.0.1", 10086, "D:\\garbage");
        Map map;
        int[] carnumber = new int[5028];
        int[] lengths = new int[5028];
        Map<Integer, Object> res = new HashMap<>();
        for(int j = 0; j < 1; j++){

            System.out.println();
            System.out.println();
            long startT = System.currentTimeMillis();
            try (DTGTransaction tx = db.CreateTransaction()){
                for(int i = 0; i < 5028; i++){
                    RelationshipAgent r = db.getRelationshipById(i);
                    carnumber[i] = r.getTemporalPropertyBetweenAAndB("carnumber", time, max, min);
                }
                map = tx.start();
                long end = System.currentTimeMillis();
                res.put(-2, map.get(-2));
                res.put(-3, map.get(-3));
                res.put(-4, map.get(-4));
                int mm = 0;
                for(int i = 0; i < 5028; i++){
                    String car = (String)(map.get(carnumber[i]) + "");//System.out.println("road " + i + " info [status:" + ", car number:" + car + "]");
                    if(!car.equals(NULLSTRING)){
                        res.put(mm, "road " + i + " info [ car in the road :" + car + "]");
                        mm++;
                        //System.out.println("road " + i + " info [ car in the road :" + car + "]");
                    }
                }
                outputCost(tx.getTxId(), startT, end);
            }
        }
        return res;
    }

    public Map<String, List<Integer>> getStoreInfo(){
        DTGDatabase db = new DTGDatabase();
        db.init("127.0.0.1", 10086, "D:\\garbage");
        return db.getStoreInfo();
    }

    public void outputCost(String txId, long start, long end){
        System.out.println(System.currentTimeMillis() + "  " + txId + " : transaction done! cost : " + ((end - start)/4) + "ms");
    }


}

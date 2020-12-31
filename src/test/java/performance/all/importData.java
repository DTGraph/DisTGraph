package performance.all;

import Element.NodeAgent;
import UserClient.DTGDatabase;
import UserClient.Transaction.DTGTransaction;
import org.junit.Test;
import performance.addConCurrency;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class importData {

    ExecutorService pool = Executors.newFixedThreadPool(2);

    @Test
    public void importData(){

        File file = new File("D:\\garbage\\txId");
        if(file.exists()){
            file.delete();
        }

        DTGDatabase db = new DTGDatabase();
        db.init("192.168.1.178", 10086, "D:\\garbage");

        try {
            BufferedReader in = new BufferedReader(new FileReader("D:\\学习\\TGraph\\TGraph\\temporal.data1456550604218.data"));
            String line = in.readLine();
            int count = 0;
            int threshold = 10;
            int txSize = 10;
            final List<Temp> temps = new ArrayList<>();
            int t = 0;
            while(count < threshold){
                //System.out.println(line);
                line = in.readLine();
                String[] info = line.split(",");
                String[] time = info[0].split(":");
                String[] id = info[1].split(":");
                Temp temp = new Temp(Long.parseLong(id[1]) % txSize, Integer.parseInt(time[0]), Integer.parseInt(time[0]) + 5, "status", info[2]);
                temps.add(temp);
                t++;
                if(t >= txSize){
                    t = 0;
                    pool.execute(new Runnable() {
                        @Override
                        public void run() {
                            tempTx(temps, db);
                        }
                    });
                    temps.clear();
                }
                count++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void importTopo(){
        File file = new File("D:\\garbage\\txId");
        if(file.exists()){
            file.delete();
        }

        DTGDatabase db = new DTGDatabase();
        db.init("192.168.1.178", 10086, "D:\\garbage");

        try {
            BufferedReader in = new BufferedReader(new FileReader("D:\\学习\\TGraph\\TGraph\\Topo.csv"));
            String line = in.readLine();
            int count = 0;
            int threshold = 10;
            int txSize = 10;
            List<Road> roads = new ArrayList<>();
            int t = 0;
            while(count < threshold){
                //System.out.println(line);
                line = in.readLine();
                String[] info = line.split(",");
                Road r = new Road(Integer.parseInt(info[0]),Integer.parseInt(info[4]),Integer.parseInt(info[2]));
                roads.add(r);
                t++;
                if(t >= txSize){
                    t = 0;
                    this.topoTx(roads, db);
                    roads = new ArrayList<>();
                }
                count++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void topoTx(List<Road> roads, DTGDatabase db) {
        try (DTGTransaction tx = db.CreateTransaction()){
            for(Road r : roads){
                NodeAgent n = db.addNode();
                n.setProperty("road_id", r.id + "");
                n.setProperty("road_length", r.length + "");
                n.setProperty("road_dir", r.dir + "");
            }

            Map<Integer, Object> map = tx.start();
            map.get(0);

        }finally {
            return;
        }
    }


    public void tempTx(List<Temp> temps, DTGDatabase db) {
        try (DTGTransaction tx = db.CreateTransaction()){
            for(Temp r : temps){
                NodeAgent n = db.getNodeById(r.id);
                n.setTemporalProperty(r.key, r.start, r.end, r.value);
            }

            Map<Integer, Object> map = tx.start();
            map.get(0);

        }finally {
            return;
        }
    }

}

class Road{
    int id;
    int length;
    int dir;

    public Road(int id, int length, int dir){
        this.id = id;
        this.length = length;
        this.dir = dir;
    }
}

class Temp{
    long id;
    int start;
    int end;
    String key;
    Object value;

    public Temp(long id, int start, int end, String key, Object value){
        this.id = id;
        this.end = end;
        this.start = start;
        this.key = key;
        this.value = value;
    }
}

import config.RelType;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.register.NeoRegister;
import sun.management.FileSystem;

import java.io.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class simple {

    @Test
    public void test() {
        int t = 1;
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


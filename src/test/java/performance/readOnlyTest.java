package performance;

import Element.NodeAgent;
import UserClient.DTGDatabase;
import UserClient.Transaction.DTGTransaction;
import org.junit.Test;
import tool.OutPutCsv;

import java.util.Map;
import java.util.Random;

public class readOnlyTest {

    @Test
    public void addTests() throws InterruptedException {
        DTGDatabase db = new DTGDatabase();
        db.init("127.0.0.1", 10086, "D:\\garbage");
        OutPutCsv output = new OutPutCsv("D:\\DTG\\test\\dtgreadOnly-5.csv", "i,start,end,cost");

        for(int i = 0; i < 1; i++){
            long start = System.currentTimeMillis();
            try (DTGTransaction tx = db.CreateTransaction()){
                for(int j = 0; j < 100; j++){
                    db.addNode();
                }
                tx.start();
            }
        }
        Thread.sleep(10000);
        long start = System.currentTimeMillis();

        output.write("this is a line");
        int count = 0;
        Random ra = new Random();
        while(count < 3000){
            try (DTGTransaction tx = db.CreateTransaction()){
                for(int j = 0; j < 1; j++){
                    NodeAgent node = db.getNodeById(ra.nextInt(100));
                }
                Thread.sleep(30);
                Map<Integer, Object> map = tx.start();
                long end = System.currentTimeMillis();
                output.write(Integer.toString(count), Long.toString(start), Long.toString(end), Long.toString(end - start));
            }catch (Exception e){
                System.out.println(e);
            }
            count++;
        }

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        output.close();
    }
}



package performance;

import UserClient.DTGDatabase;
import UserClient.Transaction.DTGTransaction;
import org.junit.Test;
import performance.tool.OutPutCsv;

import java.util.Map;

public class addConCurrency {

    @Test
    public void addTests(){
        DTGDatabase db = new DTGDatabase();
        db.init("127.0.0.1", 10086, "D:\\garbage");
        OutPutCsv output = new OutPutCsv("D:\\DTG\\test\\addInConcurrency1000-2.csv", "i,start,end,cost");

//        try (DTGTransaction tx = db.CreateTransaction()){
//            db.addNode();
//            tx.start();
//        }

        long start = System.currentTimeMillis();
        System.out.println(System.currentTimeMillis());
        for(int i = 0; i < 1000; i++){
            TxThread a = new TxThread(db, i, start, output);
            a.start();
        }

        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        output.close();
    }
}

class TxThread extends Thread{

    DTGDatabase db;
    int i;
    long start;
    OutPutCsv output;

    public TxThread(DTGDatabase db, int i, long start, OutPutCsv output){
        this.db = db;
        this.i = i;
        this.start = start;
        this.output = output;
    }

    @Override
    public void run() {
        try (DTGTransaction tx = db.CreateTransaction()){
            db.addNode();
            db.addNode();
            db.addNode();
            db.addNode();
            db.addNode();

            Map<Integer, Object> map = tx.start();
            long end = System.currentTimeMillis();
            System.out.println("end  : " + i);
            output.write(Long.toString(i), Long.toString(start), Long.toString(end), Long.toString(end - start));
        }
    }
}

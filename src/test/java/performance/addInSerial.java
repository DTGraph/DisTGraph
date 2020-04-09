package performance;

import UserClient.DTGDatabase;
import UserClient.Transaction.DTGTransaction;
import org.junit.Test;
import performance.tool.OutPutCsv;

public class addInSerial {
    @Test
    public void addTests(){
        DTGDatabase db = new DTGDatabase();
        db.init("182.129.214.222", 10086, "D:\\garbage");
        OutPutCsv output = new OutPutCsv("D:\\DTG\\test\\addInSerial-4.csv", "start,end,cost");

        for(int i = 0; i < 10; i++){
            long start = System.currentTimeMillis();
            try (DTGTransaction tx = db.CreateTransaction()){
                db.addNode();
                db.addNode();
                db.addNode();
                db.addNode();
                db.addNode();
                tx.start();
            }
            long end = System.currentTimeMillis();
            System.out.println("done! : " + i);
            output.write(Long.toString(start), Long.toString(end), Long.toString(end - start));
            try {
                Thread.sleep(1500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        output.close();
    }
}

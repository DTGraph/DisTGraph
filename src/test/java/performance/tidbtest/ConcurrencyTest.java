package performance.tidbtest;

import org.junit.Test;
import tool.OutPutCsv;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConcurrencyTest {

    @Test
    public void ConCurrTest() throws Exception {
        String url = "jdbc:mysql://49.233.217.199:4000/test?useUnicode=true&characterEncoding=utf-8&&useOldAliasMetadataBehavior=true";
        String user = "root";
        String password = "";
        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager.getConnection(url, user, password);
        Statement stmt = conn.createStatement();
        OutPutCsv output = new OutPutCsv("D:\\DTG\\test\\TidbConcurrency50-80-4.csv", "start,end,cost");

        long start = System.currentTimeMillis();
        for(int i = 0; i < 50; i++){
            TxThread4 a = new TxThread4(stmt, conn, i, start, output);
            a.start();
        }

        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        output.close();
        stmt.close();
        conn.close();
    }
}

class TxThread4 extends Thread{

    int i;
    long start;
    private ExecutorService fixedThreadPool;
    OutPutCsv output;
    Connection conn;
    Statement stmt;

    public TxThread4(Statement stmt, Connection conn, int i, long start, OutPutCsv output){
        this.conn = conn;
        this.i = i;
        this.start = start;
        fixedThreadPool = Executors.newFixedThreadPool(1);
        this.output = output;
        this.stmt = stmt;
    }

    @Override
    public void run() {
        int count = i*80;
        int max = count + 80;
        while(count < max){
            try{
                conn.setAutoCommit(false);
                String add = "insert into test_user1 values(" + count + ", 'aaa')";
                boolean rs1 = stmt.execute(add);
                conn.commit();
                long end = System.currentTimeMillis();
                //System.out.println("end  : " + count);
                fixedThreadPool.execute(new Runnable() {
                    @Override
                    public void run() {
                        output.write(Long.toString(start), Long.toString(end), Long.toString(end - start));
                    }
                });
            }catch (Exception e){
                System.out.println("rollback" + e);
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
            count++;
        }
    }
}

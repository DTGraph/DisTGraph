package performance.tidbtest;

import org.junit.Test;
import tool.OutPutCsv;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TidbAddTest {

    @Test
    public void ConCurrTest() throws Exception {
        String url = "jdbc:mysql://49.233.217.199:4000/test?useUnicode=true&characterEncoding=utf-8&&useOldAliasMetadataBehavior=true";
        String user = "root";
        String password = "";
        Class.forName("com.mysql.jdbc.Driver");
        String resultPath = "D:\\DTG\\test\\TidbConcurrency50-80-4.csv";
        int threadPoolNum = 1;
        Connection conn = DriverManager.getConnection(url, user, password);
        Statement stmt = conn.createStatement();
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(1);
        OutPutCsv output = new OutPutCsv(resultPath, "start,runtime,end,cost");

        ExecutorService threadPool = Executors.newFixedThreadPool(threadPoolNum);

        long start = System.currentTimeMillis();
        for(int i = 0; i < 50; i++){
            TidbThread thread = new TidbThread(stmt, conn, i, start, fixedThreadPool, output);
            threadPool.submit(thread);
        }

        threadPool.shutdown();
        while(true){
            if(threadPool.isTerminated()){
                System.out.println("所有的子线程都结束了！");
                break;
            }
            Thread.sleep(1000);
        }
        output.close();
        stmt.close();
        conn.close();
    }
}

class TidbThread extends Thread{

    int i;
    long start;
    private ExecutorService fixedThreadPool;
    OutPutCsv output;
    Connection conn;
    Statement stmt;

    public TidbThread(Statement stmt, Connection conn, int i, long start, ExecutorService fixedThreadPool, OutPutCsv output){
        this.conn = conn;
        this.i = i;
        this.start = start;
        this.fixedThreadPool = fixedThreadPool;
        this.output = output;
        this.stmt = stmt;
    }

    @Override
    public void run() {
        try{
            long runtime = System.currentTimeMillis();
            conn.setAutoCommit(false);
            String add = "insert into test_user1 values(" + i + ", 'aaa')";
            boolean rs1 = stmt.execute(add);
            conn.commit();
            long end = System.currentTimeMillis();
            fixedThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    output.write(Long.toString(start), Long.toString(runtime), Long.toString(end), Long.toString(end - start));
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
    }
}

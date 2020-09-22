package performance.tidbtest;

import org.junit.Test;
import tool.OutPutCsv;

import java.sql.*;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class tidbReadOnly {

    static int allCount = 5000;
    static int pool_size = 4;
    static AtomicInteger count = new AtomicInteger(0);
    static long[][] sta = new long[allCount][3];

    @Test
    public void ConCurrTest() throws Exception {
        String url = "jdbc:mysql://192.168.1.199:4000/test?useUnicode=true&characterEncoding=utf-8&&useOldAliasMetadataBehavior=true";
        String user = "root";
        String password = "";
        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager.getConnection(url, user, password);
        conn.setAutoCommit(false);
        OutPutCsv output = new OutPutCsv("D:\\distribute\\test\\TidbReadConcurrency5000-100-" + pool_size + ".csv", "start,start2,end,cost");

        ExecutorService pool = Executors.newFixedThreadPool(pool_size);

        long start = System.currentTimeMillis();
        for(int i = 0; i < allCount; i++){
            TxThread5 a = new TxThread5(conn, i, start);
//            a.run();
            pool.execute(a);
        }

        try {
            Thread.sleep(240000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("all done");
        for(int i = 0; i < allCount; i++){
            output.write(Long.toString(sta[i][0]), Long.toString(sta[i][1]), Long.toString(sta[i][2]), Long.toString(sta[i][2] - sta[i][1]));
        }
        System.out.println("all done");
        output.close();
        conn.close();
    }
}

class TxThread5 extends Thread{

    int i;
    long start;
    Connection conn;

    public TxThread5(Connection conn, int i, long start){
        this.conn = conn;
        this.i = i;
        this.start = start;
    }

    @Override
    public void run() {
        long start2 = System.currentTimeMillis();
        try{

//            PreparedStatement statement = conn.prepareStatement("insert into temp_pro values(?, ?)");
//            for(int j = 0; j < 1; j++){
//                statement.setString(1, 7 + "-" + i);
//                statement.setString(2, "aaa");
//                statement.addBatch();
//            }

            Random ra = new Random();
            Statement st = conn.createStatement();
            st.execute("select * from temp_pro where Tkey = '7-" + ra.nextInt(1000) + "'");
            st.close();
//            PreparedStatement statement = conn.prepareStatement( "select * from test_user1 where id = ?");
//            for(int j = 0; j < 1; j++){
//                statement.setString(1, 7 + "-" + ra.nextInt(1000));
//                statement.addBatch();
//            }
//            statement.executeBatch();
//            statement.close();
        }catch (Exception e){
            System.out.println("rollback" + e);
            try {
                conn.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }finally {
            try {
                conn.commit();
                //conn.setAutoCommit(true);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            long end = System.currentTimeMillis();
//            System.out.println(end - start2);
            tidbReadOnly.sta[i][0] = start;
            tidbReadOnly.sta[i][1] = start2;
            tidbReadOnly.sta[i][2] = end;
        }
    }
}



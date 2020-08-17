package performance.tidbtest;

import tool.OutPutCsv;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class tidbReadOnly {
    public static void main(String[] args) throws Exception {
        OutPutCsv output = new OutPutCsv("D:\\DTG\\test\\tidbreadOnly-3.csv", "i,start,end,cost");
        String url = "jdbc:mysql://49.233.217.199:4000/test?useUnicode=true&characterEncoding=utf-8&&useOldAliasMetadataBehavior=true";
        String user = "root";
        String password = "";
        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager.getConnection(url, user, password);
        Statement stmt = conn.createStatement();
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(1);
        int count = 0;
        Random ra = new Random();
        long start = System.currentTimeMillis();
        while(count < 3000){
            try{
                conn.setAutoCommit(false);
                String add = "select * from test_user1 where id = " + ra.nextInt(100);
                ResultSet rs = stmt.executeQuery(add);
//                while (rs.next()){
//                    System.out.println("id : " + rs.getInt(1) + ", name : " + rs.getString(2));
//                }
                conn.commit();
                long end = System.currentTimeMillis();
                fixedThreadPool.execute(new Runnable() {
                    @Override
                    public void run() {
                        output.write(Long.toString(start), Long.toString(end), Long.toString(end - start));
                    }
                });
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
        System.out.println("end");
    }
}

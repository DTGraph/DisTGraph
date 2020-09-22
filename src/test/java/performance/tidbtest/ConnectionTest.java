package performance.tidbtest;

import java.sql.*;
import java.util.Comparator;
import java.util.PriorityQueue;

public class ConnectionTest {

    public static void main(String[] args) throws Exception{
        String url = "jdbc:mysql://192.168.1.199:4000/test?useUnicode=true&characterEncoding=utf-8&&useOldAliasMetadataBehavior=true";
        String user = "root";
        String password = "";
        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager.getConnection(url, user, password);

        long start = System.currentTimeMillis();
//        try{
            conn.setAutoCommit(false);
//            String add = "insert into test_user1 values(10, 'aaa')";
//            boolean rs1 = stmt.execute(add);
//            conn.commit();
//            System.out.println(rs1);
//        }catch (Exception e){
//            System.out.println("rollback");
//            conn.rollback();
//        }

//        Statement stmt = conn.createStatement();
//        String add = " CREATE TABLE `test_user1` (Tkey1 varchar(32) NOT NULL COMMENT 'key', `val` varchar(32) NOT NULL COMMENT 'val'," +
//                "PRIMARY KEY (Tkey1)) ENGINE=RocksDB";
//        boolean rs1 = stmt.execute(add);
//        String add2 = " CREATE TABLE `temp_pro` (Tkey varchar(32) NOT NULL COMMENT 'key', `val` varchar(32) NOT NULL COMMENT 'val'," +
//        "PRIMARY KEY (Tkey)) ENGINE=RocksDB";
//        boolean rs2 = stmt.execute(add2);
//        conn.commit();
//        conn.setAutoCommit(true);
//        long end = System.currentTimeMillis();
//        System.out.println("cost : " + (end - start));
//        stmt.close();
//        conn.close();

        for(int j = 0; j < 1; j++){
            try{
                PreparedStatement statement = conn.prepareStatement("insert into test_user1 values(?, ?)");
                for(int i = 0; i < 5000; i++){//System.out.println(i);
                    statement.setString(1, i + "-i-" + j);
                    statement.setString(2, "aaaa");
                    statement.addBatch();
                }
                statement.executeBatch();
                statement.close();
            }catch (Exception e){
                //conn.rollback();
                System.out.println(e);
            }finally {
                conn.commit();
                //conn.setAutoCommit(true);
                long end = System.currentTimeMillis();
                System.out.println("cost : " + (end - start));
                //stmt.close();
            }
        }

        conn.close();



    }
}

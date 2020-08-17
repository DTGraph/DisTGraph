package performance.tidbtest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class ConnectionTest {

    public static void main(String[] args) throws Exception{
        String url = "jdbc:mysql://49.233.217.199:4000/test?useUnicode=true&characterEncoding=utf-8&&useOldAliasMetadataBehavior=true";
        String user = "root";
        String password = "";
        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager.getConnection(url, user, password);
        Statement stmt = conn.createStatement();
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
//        String add = " CREATE TABLE `test_user1` (`id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'id', `name` varchar(32) NOT NULL COMMENT 'name', " +
//                "PRIMARY KEY (`id`)) ENGINE=InnoDB";
//        boolean rs1 = stmt.execute(add);

        for(int i=0;i<100;i++){
            try{
                String add = "insert into test_user1 values("+i+", 'aaa')";
                boolean rs1 = stmt.execute(add);
                System.out.println(i);
            }catch (Exception e){
                //conn.rollback();
                System.out.println(e);
            }

        }

        //System.out.println(rs1);

//        String add = "select * from test_user1";
//        ResultSet rs = stmt.executeQuery(add);
//        while (rs.next()){
//            System.out.println("id : " + rs.getInt(1) + ", name : " + rs.getString(2));
//        }
        conn.commit();

        long end = System.currentTimeMillis();
        System.out.println("cost : " + (end - start));
        stmt.close();
        conn.close();
    }
}

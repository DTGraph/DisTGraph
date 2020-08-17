
package performance.neo4jTest;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.AuthTokens;
import tool.OutPutCsv;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class readOnlyTest implements AutoCloseable{

    private static final Driver driver = GraphDatabase.driver("bolt://139.198.19.46:8181" , AuthTokens.basic("neo4j", "123000"));;

    public readOnlyTest(String uri, String user, String password){

        //fixedThreadPool = Executors.newFixedThreadPool(1);
    }

    @Override
    public void close() throws Exception {
        driver.close();
    }

    public void printGreeting(long start, ExecutorService fixedThreadPool, OutPutCsv output){
        /**session.run()将实际创建一个事务，执行语句并提交事务。
         transaction.run()将保留事务打开状态，直到您提交它，但是仍然会发送、解释和执行语句，并返回结果。但是，实际上不会将任何更改持久化
         到数据存储中，并且事务外部的查询不会看到这些更改。您必须将事务标记为成功并提交它，否则将回滚它。**/
        //match (n) detach delete n
        int count = 0;
        Random ra = new Random();
        while (count < 3000){
            try(Session session = driver.session()) {
                String greeting = session.writeTransaction(new TransactionWork<String>(){
                    @Override
                    public String execute(Transaction tx){
                        Result result = tx.run("Match (n: Person) where n.name='a0" + ra.nextInt(100) + "' return n");
                        return null;
                    }
                });
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
    }

    public static void main(String[] args) throws Exception{
//        try(Session session = driver.session()) {
//            String greeting = session.writeTransaction(new TransactionWork<String>(){
//                @Override
//                public String execute(Transaction tx){
//                    for(int j = 0; j < 100; j++){
//                        Result result = tx.run("CREATE (a0" + j + ":Person {name: 'a0" + j+ "'})");
//                    }
//                    return null;
//                }
//            });
//        }catch (Exception e) {
//            System.out.println(e);
//        }
//        Thread.sleep(10000);
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(1);
        OutPutCsv output = new OutPutCsv("D:\\garbage\\neo4jReadonly-4.csv", "start,end,cost");
        try (readOnlyTest greeter = new readOnlyTest("bolt://localhost:8811", "neo4j", "123000")){
            long start = System.currentTimeMillis();
            greeter.printGreeting(start, fixedThreadPool, output);
            Thread.sleep(10000);
        }
    }
}



package performance.neo4jTest;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.AuthTokens;
import tool.OutPutCsv;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class neo4jDriverTest implements AutoCloseable{

    private final Driver driver;

    public neo4jDriverTest(String uri, String user, String password){
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
        //fixedThreadPool = Executors.newFixedThreadPool(1);
    }

    @Override
    public void close() throws Exception {
        driver.close();
    }

    public void printGreeting(final String message, String i, long start, ExecutorService fixedThreadPool, OutPutCsv output){
        /**session.run()将实际创建一个事务，执行语句并提交事务。
         transaction.run()将保留事务打开状态，直到您提交它，但是仍然会发送、解释和执行语句，并返回结果。但是，实际上不会将任何更改持久化
         到数据存储中，并且事务外部的查询不会看到这些更改。您必须将事务标记为成功并提交它，否则将回滚它。**/
        //match (n) detach delete n
        try(Session session = driver.session()) {

            String greeting = session.writeTransaction(new TransactionWork<String>(){
                @Override
                public String execute(Transaction tx){
                    Result result = tx.run("CREATE (a0" + i + ":Person {name: 'a0" + i+ "'})");
                    Result result2 = tx.run("CREATE (a2" + i + ":Person {name: 'a0" + i+ "'})");
                    Result result3 = tx.run("CREATE (a3" + i + ":Person {name: 'a0" + i+ "'})");
                    Result result4 = tx.run("CREATE (a4" + i + ":Person {name: 'a0" + i+ "'})");
                    Result result5 = tx.run("CREATE (a5" + i + ":Person {name: 'a0" + i+ "'})");
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
    }

    public static void main(String[] args) throws Exception{
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(1);
        OutPutCsv output = new OutPutCsv("D:\\DTG\\test\\neo4jInConcurrency50-80-6.csv", "start,end,cost");
        try (neo4jDriverTest greeter = new neo4jDriverTest("bolt://139.198.0.201:8181", "neo4j", "123000")){
            long start = System.currentTimeMillis();
            greeter.printGreeting("hello", "a1", start, fixedThreadPool, output);
            Thread.sleep(1000);
            //greeter.printGreeting("hello", 1, 250);
            for(int i = 0; i < 50; i++){
                //greeter.printGreeting("hello", 0, 1);
                TThread thread = new TThread(start,i +"b" , greeter, fixedThreadPool, output);
                thread.start();
            }
            //long end = System.currentTimeMillis();
            //System.out.println("all cost : " + (end - start));
            Thread.sleep(100000);
        }
    }
}

class TThread extends Thread{

    long start;
    String i;
    //Transaction tx;
    //Session session;
    neo4jDriverTest greeter;
    ExecutorService fixedThreadPool;
    OutPutCsv output;



    public TThread(long start, String i, neo4jDriverTest greeter, ExecutorService fixedThreadPool, OutPutCsv output){
        this.start = start;
        this.i = i;
        this.greeter = greeter;
        this.fixedThreadPool = fixedThreadPool;
        this.output = output;
        //this.tx = tx;
        //this.session = session;
    }

    @Override
    public void run() {
//        Result result = tx.run("CREATE (a2" + i + ":Person {name: 'a2" + i+ "'})");
//        System.out.println(result.hasNext());
//        long end = System.currentTimeMillis();
//        System.out.println(end - start);

//        String greeting = session.writeTransaction(new TransactionWork<String>(){
//            @Override
//            public String execute(Transaction tx){
//                Result result = tx.run("CREATE (a9" + i + ":Person {name: 'a9" + i+ "'})");
//                return null;
//            }
//        });
        int count = 0;
        while (count < 80){
            greeter.printGreeting("hello", i, start, fixedThreadPool, output);
            count ++;
        }
    }
}


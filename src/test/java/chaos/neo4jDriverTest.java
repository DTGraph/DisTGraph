package chaos;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.AuthTokens;

import static org.neo4j.driver.Values.parameters;


public class neo4jDriverTest implements AutoCloseable{

    private final Driver driver;

    public neo4jDriverTest(String uri, String user, String password){
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }

    @Override
    public void close() throws Exception {
        driver.close();
    }

    public void printGreeting(final String message, String i){
        try(Session session = driver.session()) {

            String greeting = session.writeTransaction(new TransactionWork<String>(){
                @Override
                public String execute(Transaction tx){
                    Result result = tx.run("CREATE (a0" + i + ":Person {name: 'a0" + i+ "'})");
                    return null;
                }
            });

//            long start = System.currentTimeMillis();
//            for(int j = 0; j < k; j++){
//                TThread thread = new TThread(start, i +"" + j, session);
//                thread.start();
//            }

//            String greeting = session.writeTransaction(new TransactionWork<String>(){
//                @Override
//                public String execute(Transaction tx){
//                    //long start = System.currentTimeMillis();
//
////                    for(int i = 0; i < 10; i++){
////                        //Result result = tx.run("Match (n: Person) where n.name = 'a9"+ i +"' return n");
//                        Result result = tx.run("CREATE (a2" + i + ":Person {name: 'a2" + i+ "'})");
////
////                        System.out.println(result.hasNext());
////                     }
////                    for(int j = 0; j < 5; j++){
////                        TThread thread = new TThread(start, j, tx);
////                        thread.start();
////                        long end = System.currentTimeMillis();
////                        System.out.println("tx cost: " + (end - start));
////                    }
////
////                    try {
////                        Thread.sleep(100000);
////                    } catch (InterruptedException e) {
////                        e.printStackTrace();
////                    }
//                    return null;
//                    //return result.single().get(0).asString();
//                }
//            });
//            long end = System.currentTimeMillis();
//            System.out.println("250 cost: " + (end - start));
//            Thread.sleep(10000);
//            System.out.println(greeting);
        }
//        catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }

    public static void main(String[] args) throws Exception{
        try (neo4jDriverTest greeter = new neo4jDriverTest("bolt://139.198.17.249:8181", "neo4j", "neo4j")){
            long start = System.currentTimeMillis();
            greeter.printGreeting("hello", "a1");
            Thread.sleep(1000);
            //greeter.printGreeting("hello", 1, 250);
            for(int i = 0; i < 100; i++){
                //greeter.printGreeting("hello", 0, 1);
                TThread thread = new TThread(start,i +"b" , greeter);
                thread.start();
            }
            long end = System.currentTimeMillis();
            System.out.println("all cost : " + (end - start));
            Thread.sleep(10000);
        }
    }
}

class TThread extends Thread{

    long start;
    String i;
    //Transaction tx;
    //Session session;
    neo4jDriverTest greeter;


    public TThread(long start, String i, neo4jDriverTest greeter){
        this.start = start;
        this.i = i;
        this.greeter = greeter;
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

        greeter.printGreeting("hello", i);
        long end = System.currentTimeMillis();
        System.out.println("tx cost: " + (end - start));
    }
}


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

    public void printGreeting(final String message, final int i){
        try(Session session = driver.session()) {
            //long start = System.currentTimeMillis();
            String greeting = session.writeTransaction(new TransactionWork<String>(){
                @Override
                public String execute(Transaction tx){
                    long start = System.currentTimeMillis();

//                    //for(int i = 0; i < 10; i++){
//                        //Result result = tx.run("Match (n: Person) where n.name = 'a9"+ i +"' return n");
//                        Result result = tx.run("CREATE (a15" + i + ":Person {name: 'a15" + i+ "'})");
//
//                        System.out.println(result.hasNext());
//                    //}
                    for(int j = 0; j < 10; j++){
                        TThread thread = new TThread(start, j, tx);

                        thread.start();

                        long end = System.currentTimeMillis();
                        System.out.println("tx cost: " + (end - start));
                    }

                    try {
                        Thread.sleep(100000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    return null;
                    //return result.single().get(0).asString();
                }
            });
//            long end = System.currentTimeMillis();
//            System.out.println("tx cost: " + (end - start));
//            System.out.println(greeting);
        }
    }

    public static void main(String[] args) throws Exception{
        try (neo4jDriverTest greeter = new neo4jDriverTest("bolt://localhost:8810", "neo4j", "123000")){
            long start = System.currentTimeMillis();
            for(int i = 0; i < 1; i++){
                greeter.printGreeting("hello", i);
            }
            long end = System.currentTimeMillis();
            System.out.println("all cost : " + (end - start));
        }

        Thread.sleep(10000);
    }
}

class TThread extends Thread{

    long start;
    int i;
    Transaction tx;


    public TThread(long start, int i, Transaction tx){
        this.start = start;
        this.i = i;
        this.tx = tx;
    }

    @Override
    public void run() {
        Result result = tx.run("CREATE (a18" + i + ":Person {name: 'a18" + i+ "'})");

        System.out.println(result.hasNext());
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }
}


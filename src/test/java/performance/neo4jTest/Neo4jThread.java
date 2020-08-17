package performance.neo4jTest;

import org.neo4j.driver.*;
import tool.OutPutCsv;

import java.util.concurrent.ExecutorService;

public class Neo4jThread implements Runnable {

    long start;
    int i;
    Session session;
    ExecutorService fixedThreadPool;
    OutPutCsv output;



    public Neo4jThread(Session session, long start, int i, ExecutorService fixedThreadPool, OutPutCsv output){
        this.start = start;
        this.i = i;
        this.fixedThreadPool = fixedThreadPool;
        this.output = output;
        this.session = session;
    }

    @Override
    public void run() {
        long runtime = System.currentTimeMillis();System.out.println("run2");
        String greeting = session.writeTransaction(new TransactionWork<String>(){
            @Override
            public String execute(Transaction tx){
                System.out.println("run1");
                Result result = tx.run("CREATE (a1" + i + ":Person {name: 'a1" + i+ "'})");
                Result result2 = tx.run("CREATE (a2" + i + ":Person {name: 'a2" + i+ "'})");
                Result result3 = tx.run("CREATE (a3" + i + ":Person {name: 'a3" + i+ "'})");
                Result result4 = tx.run("CREATE (a4" + i + ":Person {name: 'a4" + i+ "'})");
                Result result5 = tx.run("CREATE (a5" + i + ":Person {name: 'a5" + i+ "'})");
                return null;
            }
        });
        long end = System.currentTimeMillis();
        fixedThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                output.write(Long.toString(start), Long.toString(runtime), Long.toString(end), Long.toString(end - start));
            }
        });
    }

}

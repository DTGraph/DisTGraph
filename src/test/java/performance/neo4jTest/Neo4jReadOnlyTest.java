package performance.neo4jTest;

import org.neo4j.driver.*;
import tool.OutPutCsv;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Neo4jReadOnlyTest {

    public static void main(String[] args) throws Exception{
        String resultPath = "D:\\garbage\\neo4jInConcurrency50-80-6.csv";
        String driverPath = "bolt://localhost:8811";
        Driver driver = GraphDatabase.driver(driverPath, AuthTokens.basic("neo4j", "123000"));
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(1);
        OutPutCsv output = new OutPutCsv(resultPath, "start,runtime,end,cost");

        int count = 300;
        Random ra = new Random();

        try(Session session = driver.session()) {
            long start = System.currentTimeMillis();
            for(int i = 0; i < count; i++){
                long runtime = System.currentTimeMillis();
                String greeting = session.readTransaction(new TransactionWork<String>(){
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
                        output.write(Long.toString(start), Long.toString(runtime), Long.toString(end), Long.toString(end - start));
                    }
                });
            }
        }catch (Exception e){
            System.out.println(e);
        }
    }
}

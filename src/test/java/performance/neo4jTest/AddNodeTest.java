package performance.neo4jTest;

import org.neo4j.driver.*;
import tool.OutPutCsv;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AddNodeTest {

    public static void main(String[] args) throws Exception{
        String resultPath = "D:\\garbage\\neo4jInConcurrency50-80-6.csv";
        String driverPath = "bolt://localhost:8811";
        int threadPoolNum = 1;
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(1);
        OutPutCsv output = new OutPutCsv(resultPath, "start,runtime,end,cost");
        Driver driver = GraphDatabase.driver(driverPath, AuthTokens.basic("neo4j", "123000"));

        ExecutorService threadPool = Executors.newFixedThreadPool(threadPoolNum);
        Session session = driver.session();
        long start = System.currentTimeMillis();

        for(int i = 0; i < 50; i++){
            Neo4jThread thread = new Neo4jThread(session, start, i, fixedThreadPool, output);
            threadPool.submit(thread);
        }


        threadPool.shutdown();
        while(true){
            if(threadPool.isTerminated()){
                System.out.println("所有的子线程都结束了！");
                break;
            }
            Thread.sleep(1000);
        }
        session.close();
        driver.close();
    }

}

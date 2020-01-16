package MQ;

import com.alipay.sofa.jraft.Status;
import options.MQOptions;
import org.junit.Test;
import raft.LogStoreClosure;
import tool.ObjectAndByte;

import java.io.File;


/**
 * @author :jinkai
 * @date :Created in 2020/1/6 15:15
 * @description：
 * @modified By:
 * @version:
 */

public class addTest {

    @Test
    public void addToMQ(){
        DTGMQ dtgmq = new DTGMQ();
        MQOptions opts = new MQOptions();
        String url = "D:\\garbage\\MQTest";
        File file = new File(url);
        if  (!file.exists()  && !file .isDirectory())
        {
            file .mkdir();
        }
        opts.setLogUri(url + "\\Log");
        opts.setRockDBPath(url + "\\RockDB");
        dtgmq.init(opts);


        for(int i = 0; i < 10; i++){
            final long start = System.currentTimeMillis();
            System.out.println("START COMMIT: " + start);
            ByteTask task = new ByteTask();
            LogStoreClosure closure = new LogStoreClosure() {
                @Override
                public void run(Status status) {
                    long cost = System.currentTimeMillis() - start;
                    System.out.println("Success :" + this.getData() + ", cost : " + cost);
                }
            };
            String data = "data " + i;
            closure.setData(data);
            task.setDone(closure);
            task.setData(ObjectAndByte.toByteArray(data));
            dtgmq.apply(task);
//            if(i % 1 == 0){
//                try {
//                    Thread.sleep(200);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }

        }

        try {
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

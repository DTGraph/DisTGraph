package MQ;

import MQ.Test.TestClosure;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.rhea.serialization.Serializers;
import options.MQOptions;
import org.junit.Test;
import tool.ObjectAndByte;

import java.nio.ByteBuffer;

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
        opts.setLogUri(url + "\\Log");
        opts.setRockDBPath(url + "\\RockDB");
        dtgmq.init(opts);

        for(int i = 0; i < 10; i++){
            ByteTask task = new ByteTask();
            TestClosure closure = new TestClosure();
            String data = "data " + i;
            closure.setId(data);
            task.setDone(closure);
            task.setData(ObjectAndByte.toByteArray(data));
            dtgmq.apply(task);
        }

        try {
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

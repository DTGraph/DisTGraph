package MQ;

import DBExceptions.TxMQException;
import MQ.Test.TestClosure;
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;

import java.util.concurrent.CompletableFuture;

/**
 * @author :jinkai
 * @date :Created in 2020/1/4 16:40
 * @description：
 * @modified By:
 * @version:
 */

public class MQStateMachine{

    public void onApply(Iterator iter) {
        while (iter.hasNext()){
            final TestClosure t = (TestClosure)iter.done();
            final CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(() -> {
                System.out.println("run : " + t.getId());
                return runApplyTask(t);
            }).whenComplete((result, e) -> {
                if(result){
                    t.run(Status.OK());
                }
                else {
                    System.out.println("error!");
                }
            });
            iter.next();
        }
    }

    private boolean runApplyTask(TestClosure t){
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(t.getId());
        return true;
    }

    public void onShutdown() {

    }

    public void onError(TxMQException e) {

    }

}

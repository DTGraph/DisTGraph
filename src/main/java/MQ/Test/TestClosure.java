package MQ.Test;

import MQ.MQLogManager;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import raft.EntityStoreClosure;

/**
 * @author :jinkai
 * @date :Created in 2020/1/6 18:45
 * @description：
 * @modified By:
 * @version:
 */

public class TestClosure implements EntityStoreClosure {
    String id;
    MQLogManager logManager;
    long selfIndex;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public MQLogManager getLogManager() {
        return logManager;
    }

    public void setLogManager(MQLogManager logManager) {
        this.logManager = logManager;
    }

    public long getSelfIndex() {
        return selfIndex;
    }

    public void setSelfIndex(long selfIndex) {
        this.selfIndex = selfIndex;
    }

    @Override
    public Errors getError() {
        return null;
    }

    @Override
    public void setError(Errors error) {

    }

    @Override
    public Object getData() {
        return null;
    }

    @Override
    public void setData(Object data) {

    }

    @Override
    public void run(Status status) {
        this.logManager.addToWaitCommit(selfIndex);
        System.out.println("finish : " + id);
    }
}

package raft;

import UserClient.Transaction.TransactionLog;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.errors.Errors;

import java.io.Serializable;

/**
 * @author :jinkai
 * @date :Created in 2020/1/11 15:42
 * @description：
 * @modified By:
 * @version:
 */

public abstract class LogStoreClosure implements EntityStoreClosure, Serializable {

    private static final long serialVersionUID = 3395208635576121604L;
    private volatile Errors         error;
    private volatile Object         data;
    private volatile TransactionLog log;

    @Override
    public Errors getError() {
        return error;
    }

    @Override
    public void setError(Errors error) {
        this.error = error;
    }

    @Override
    public Object getData() {
        return data;
    }

    @Override
    public void setData(Object data) {
        this.data = data;
    }

    public TransactionLog getLog() {
        return log;
    }

    public void setLog(TransactionLog log) {
        this.log = log;
    }
}

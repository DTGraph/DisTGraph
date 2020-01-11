package raft;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.errors.Errors;

/**
 * @author :jinkai
 * @date :Created in 2020/1/11 15:42
 * @description：
 * @modified By:
 * @version:
 */

public abstract class LogStoreClosure implements EntityStoreClosure {

    private volatile Errors error;
    private volatile Object data;
    private volatile String TxId;

    public void setTxId(String txId) {
        TxId = txId;
    }

    public String getTxId() {
        return TxId;
    }

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
}

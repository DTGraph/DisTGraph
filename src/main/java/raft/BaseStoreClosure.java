package raft;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.errors.Errors;

/**
 * @author :jinkai
 * @date :Created in 2019/10/22 14:41
 * @description:
 * @modified By:
 * @version:
 */

public abstract class BaseStoreClosure implements EntityStoreClosure {

    private volatile Errors error;
    private volatile Object data;

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

package Region;

import com.alipay.sofa.jraft.rhea.errors.Errors;
import raft.EntityStoreClosure;

import java.io.Serializable;

public abstract class DTGLockClosure implements EntityStoreClosure, Serializable {

    private static final long serialVersionUID = -6799798995333393743L;

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
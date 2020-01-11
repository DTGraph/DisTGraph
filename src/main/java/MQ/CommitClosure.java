package MQ;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import raft.EntityStoreClosure;

/**
 * @author :jinkai
 * @date :Created in 2020/1/7 22:17
 * @description：
 * @modified By:
 * @version:
 */

public class CommitClosure implements EntityStoreClosure {
    private volatile Errors error;
    private volatile Object data;

    @Override
    public void run(Status status) {
        if (status.isOk()) {

        } else {

        }
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

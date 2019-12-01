package raft;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.rhea.errors.Errors;

/**
 * @author :jinkai
 * @date :Created in 2019/10/22 10:48
 * @description:
 * @modified By:
 * @version:
 */

public interface EntityStoreClosure extends Closure {

    Errors getError();

    void setError(final Errors error);

    Object getData();

    void setData(final Object data);
}

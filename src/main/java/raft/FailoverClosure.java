package raft;

import com.alipay.sofa.jraft.rhea.errors.Errors;

import java.util.concurrent.CompletableFuture;

/**
 * @author :jinkai
 * @date :Created in 2019/10/22 15:05
 * @description:
 * @modified By:
 * @version:
 */

public interface FailoverClosure<T> extends EntityStoreClosure {

    CompletableFuture<T> future();

    void success(final T result);

    void failure(final Throwable cause);

    void failure(final Errors error);

    long getReturnTxId();

    void setReturnTxId(final long returnTxId);
}

package Communication;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.rhea.options.RpcOptions;
import com.alipay.sofa.jraft.rhea.cmd.store.BaseRequest;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.util.Endpoint;
import raft.FailoverClosure;

import java.util.concurrent.CompletableFuture;

/**
 * @author :jinkai
 * @date :Created in 2019/10/22 15:13
 * @description:
 * @modified By:
 * @version:
 */

public interface DTGRpcService extends Lifecycle<RpcOptions> {

    /**
     * @see #callAsyncWithRpc(BaseRequest, FailoverClosure, Errors, boolean)
     */
    <V> CompletableFuture<V> callAsyncWithRpc(final BaseRequest request, final FailoverClosure<V> closure,
                                              final Errors lastCause);

    /**
     * Send KV requests to the remote data service nodes.
     *
     * @param request       request data
     * @param closure       callback for failover strategy
     * @param lastCause     the exception information held by the last call
     *                      failed, the initial value is null
     * @param requireLeader if true, then request to call the leader node
     * @param <V>           the type of response
     * @return a future with response
     */
    <V> CompletableFuture<V> callAsyncWithRpc(final BaseRequest request, final FailoverClosure<V> closure,
                                              final Errors lastCause, final boolean requireLeader);

    Endpoint getLeader(final long regionId, final boolean forceRefresh, final long timeoutMillis);
}

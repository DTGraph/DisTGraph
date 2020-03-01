package Communication;

import PlacementDriver.DTGPlacementDriverClient;
import PlacementDriver.DefaultPlacementDriverClient;
import com.alipay.remoting.InvokeCallback;
import com.alipay.remoting.InvokeContext;
import com.alipay.remoting.rpc.RpcClient;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.cmd.store.BaseResponse;
import com.alipay.sofa.jraft.rhea.rpc.ExtSerializerSupports;
import com.alipay.sofa.jraft.rhea.util.concurrent.DiscardOldPolicyWithReport;
import com.alipay.sofa.jraft.rhea.util.concurrent.NamedThreadFactory;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.ExecutorServiceHelper;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;
import raft.BaseStoreClosure;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

public class RpcCaller {

    private final RpcClient rpcClient;
    private ThreadPoolExecutor rpcCallbackExecutor;
    private int heartbeatRpcTimeoutMillis = 5000;
    private final DTGPlacementDriverClient pdClient;

    public RpcCaller(DTGPlacementDriverClient pdClient){
        this.pdClient = pdClient;
        this.rpcClient = ((DefaultPlacementDriverClient) pdClient).getRpcClient();
        String name = "storeEngionRpcPool";
        this.rpcCallbackExecutor = ThreadPoolUtil.newBuilder() //
                .poolName(name) //
                .enableMetric(true) //
                .coreThreads(4) //
                .maximumThreads(4) //
                .keepAliveSeconds(120L) //
                .workQueue(new ArrayBlockingQueue<>(1024)) //
                .threadFactory(new NamedThreadFactory(name, true)) //
                .rejectedHandler(new DiscardOldPolicyWithReport(name)) //
                .build();
    }

    public synchronized void shutdown() {
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.rpcCallbackExecutor);
    }

    public static abstract class RpcClosure<V> extends BaseStoreClosure {

        private volatile V result;

        public V getResult() {
            return result;
        }

        public void setResult(V result) {
            this.result = result;
        }
    }

    public  <V> void callAsyncWithRpc(final long regionId, final Object request,
                                      final BaseStoreClosure closure, boolean forceRefresh) {
        final Endpoint endpoint = this.pdClient.getLeader(regionId, forceRefresh, this.heartbeatRpcTimeoutMillis);
        callAsyncWithRpc(endpoint, request, closure);
    }

    public  <V> void callAsyncWithRpc(final Endpoint endpoint, final Object request,
                                      final BaseStoreClosure closure) {
        final String address = endpoint.toString();
        System.out.println("callAsyncWithRpc " + address);
        final InvokeContext invokeCtx = ExtSerializerSupports.getInvokeContext();
        final InvokeCallback invokeCallback = new InvokeCallback() {

            @SuppressWarnings("unchecked")
            @Override
            public void onResponse(final Object result) {
                final BaseResponse<?> response = (BaseResponse<?>) result;
                if (response.isSuccess()) {
                    if(closure instanceof RpcClosure){
                        ((RpcClosure)closure).setResult((V) response.getValue());
                    }
                    closure.setData(response.getValue());
                    closure.run(Status.OK());
                } else {
                    closure.setError(response.getError());
                    closure.run(new Status(-1, "RPC failed with address: %s, response: %s", address, response));
                }
            }

            @Override
            public void onException(final Throwable t) {
                closure.run(new Status(-1, t.getMessage()));
            }

            @Override
            public Executor getExecutor() {
                return rpcCallbackExecutor;
            }
        };
        try {
            this.rpcClient.invokeWithCallback(address, request, invokeCtx, invokeCallback,
                    this.heartbeatRpcTimeoutMillis);
        } catch (final Throwable t) {
            closure.run(new Status(-1, t.getMessage()));
        }
    }



    public int getHeartbeatRpcTimeoutMillis() {
        return heartbeatRpcTimeoutMillis;
    }

    public RpcClient getRpcClient() {
        return rpcClient;
    }
}

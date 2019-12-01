package options;

import com.alipay.sofa.jraft.rhea.options.BatchingOptions;
import com.alipay.sofa.jraft.rhea.options.PlacementDriverOptions;
import com.alipay.sofa.jraft.rhea.options.RpcOptions;
import com.alipay.sofa.jraft.rhea.options.StoreEngineOptions;
import com.alipay.sofa.jraft.rhea.options.configured.BatchingOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.RpcOptionsConfigured;

/**
 * @author :jinkai
 * @date :Created in 2019/10/20 18:48
 * @description:
 * @modified By:
 * @version:
 */

public class DTGRheaKVStoreOptions {

    // A clusterId is required to connect to the PD server, and PD server
    // use clusterId isolate different cluster.  The fake PD mode does not
    // need to be configured.
    private long                      clusterId;
    // Each store node contains one or more raft-group replication groups.
    // This field is the name prefix of all replication groups. All raft-group
    // names follow the naming rules of [clusterName-regionId].
    private String                    clusterName           = "default-group-cluster";
    private PlacementDriverOptions placementDriverOptions;
    private StoreEngineOptions        storeEngineOptions;
    // Initial server node list.
    private String                    initialServerList;
    // Whether to read data only from the leader node, reading from the
    // follower node can also ensure consistent reading, but the reading
    // delay may increase due to the delay of the follower synchronization
    // data.
    private boolean                   onlyLeaderRead        = true;
    private RpcOptions                rpcOptions            = RpcOptionsConfigured.newDefaultConfig();
    private int                       failoverRetries       = 2;
    private long                      futureTimeoutMillis   = 5000;
    private boolean                   useParallelKVExecutor = true;
    private BatchingOptions           batchingOptions       = BatchingOptionsConfigured.newDefaultConfig();

    public long getClusterId() {
        return clusterId;
    }

    public void setClusterId(long clusterId) {
        this.clusterId = clusterId;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    /**
     * Same as {@link #getClusterName()}
     */
    public String getMultiRaftGroupClusterName() {
        return clusterName;
    }

    /**
     * Same as {@link #setClusterName(String)}
     */
    public void setMultiRaftGroupClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public PlacementDriverOptions getPlacementDriverOptions() {
        return placementDriverOptions;
    }

    public void setPlacementDriverOptions(PlacementDriverOptions placementDriverOptions) {
        this.placementDriverOptions = placementDriverOptions;
    }

    public StoreEngineOptions getStoreEngineOptions() {
        return storeEngineOptions;
    }

    public void setStoreEngineOptions(StoreEngineOptions storeEngineOptions) {
        this.storeEngineOptions = storeEngineOptions;
    }

    public String getInitialServerList() {
        return initialServerList;
    }

    public void setInitialServerList(String initialServerList) {
        this.initialServerList = initialServerList;
    }

    public RpcOptions getRpcOptions() {
        return rpcOptions;
    }

    public void setRpcOptions(RpcOptions rpcOptions) {
        this.rpcOptions = rpcOptions;
    }

    public boolean isOnlyLeaderRead() {
        return onlyLeaderRead;
    }

    public void setOnlyLeaderRead(boolean onlyLeaderRead) {
        this.onlyLeaderRead = onlyLeaderRead;
    }

    public int getFailoverRetries() {
        return failoverRetries;
    }

    public void setFailoverRetries(int failoverRetries) {
        this.failoverRetries = failoverRetries;
    }

    public long getFutureTimeoutMillis() {
        return futureTimeoutMillis;
    }

    public void setFutureTimeoutMillis(long futureTimeoutMillis) {
        this.futureTimeoutMillis = futureTimeoutMillis;
    }

    public boolean isUseParallelKVExecutor() {
        return useParallelKVExecutor;
    }

    public void setUseParallelKVExecutor(boolean useParallelKVExecutor) {
        this.useParallelKVExecutor = useParallelKVExecutor;
    }

    public BatchingOptions getBatchingOptions() {
        return batchingOptions;
    }

    public void setBatchingOptions(BatchingOptions batchingOptions) {
        this.batchingOptions = batchingOptions;
    }

    @Override
    public String toString() {
        return "RheaKVStoreOptions{" + "clusterId=" + clusterId + ", clusterName='" + clusterName + '\''
                + ", placementDriverOptions=" + placementDriverOptions + ", storeEngineOptions=" + storeEngineOptions
                + ", initialServerList='" + initialServerList + '\'' + ", onlyLeaderRead=" + onlyLeaderRead
                + ", rpcOptions=" + rpcOptions + ", failoverRetries=" + failoverRetries + ", futureTimeoutMillis="
                + futureTimeoutMillis + ", useParallelKVExecutor=" + useParallelKVExecutor + ", batchingOptions="
                + batchingOptions + '}';
    }
}

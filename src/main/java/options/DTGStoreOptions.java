/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package options;

import com.alipay.sofa.jraft.rhea.options.BatchingOptions;
import com.alipay.sofa.jraft.rhea.options.RpcOptions;
import com.alipay.sofa.jraft.rhea.options.StoreEngineOptions;
import com.alipay.sofa.jraft.rhea.options.configured.BatchingOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.RpcOptionsConfigured;

/**
 * @author :jinkai
 * @date :Created in 2019/10/15 22:19
 * @description:
 * @modified By:
 * @version: 1.0
 */

public class DTGStoreOptions {

    private long clusterId;
    // Each store node contains one or more raft-group replication groups.
    // This field is the name prefix of all replication groups. All raft-group
    // names follow the naming rules of [clusterName-regionId].
    private String clusterName = "default-group-cluster";
    private boolean isRemotePd;
    private boolean onlyLeaderRead = true;
    private String initialServerList;
    private DTGStoreEngineOptions storeEngineOptions;
    private int failoverRetries;
    private long futureTimeoutMillis = 5000;
    private boolean useParallelExecutor = true;

    private RpcOptions rpcOptions = RpcOptionsConfigured.newDefaultConfig();
    private BatchingOptions batchingOptions = BatchingOptionsConfigured.newDefaultConfig();
    private DTGPlacementDriverOptions placementDriverOptions;

    public RpcOptions getRpcOptions() {
        return rpcOptions;
    }

    public void setRpcOptions(RpcOptions rpcOptions) {
        this.rpcOptions = rpcOptions;
    }

    public BatchingOptions getBatchingOptions() {
        return batchingOptions;
    }

    public void setBatchingOptions(BatchingOptions batchingOptions) {
        this.batchingOptions = batchingOptions;
    }

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

    public boolean isRemotePd() {
        return isRemotePd;
    }

    public void setRemotePd(boolean remotePd) {
        isRemotePd = remotePd;
    }

    public boolean isOnlyLeaderRead() {
        return onlyLeaderRead;
    }

    public void setOnlyLeaderRead(boolean onlyLeaderRead) {
        this.onlyLeaderRead = onlyLeaderRead;
    }

    public DTGPlacementDriverOptions getPlacementDriverOptions() {
        return placementDriverOptions;
    }

    public void setPlacementDriverOptions(DTGPlacementDriverOptions placementDriverOptions) {
        this.placementDriverOptions = placementDriverOptions;
    }

    public String getInitialServerList() {
        return initialServerList;
    }

    public void setInitialServerList(String initialServerList) {
        this.initialServerList = initialServerList;
    }

    public DTGStoreEngineOptions getStoreEngineOptions() {
        return storeEngineOptions;
    }

    public void setStoreEngineOptions(DTGStoreEngineOptions storeEngineOptions) {
        this.storeEngineOptions = storeEngineOptions;
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

    public boolean isUseParallelExecutor() {
        return useParallelExecutor;
    }

    public void setUseParallelExecutor(boolean useParallelExecutor) {
        this.useParallelExecutor = useParallelExecutor;
    }
}

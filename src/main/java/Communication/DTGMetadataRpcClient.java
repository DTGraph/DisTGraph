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
package Communication;

import Communication.RequestAndResponse.CreateRegionRequest;
import Communication.RequestAndResponse.GetVersionRequest;
import Communication.RequestAndResponse.SetDTGStoreInfoRequest;
import DBExceptions.TransactionException;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rhea.client.FutureHelper;
import com.alipay.sofa.jraft.rhea.client.failover.FailoverClosure;
import com.alipay.sofa.jraft.rhea.client.failover.RetryRunner;
import com.alipay.sofa.jraft.rhea.client.failover.impl.FailoverClosureImpl;
import com.alipay.sofa.jraft.rhea.client.pd.PlacementDriverRpcService;
import com.alipay.sofa.jraft.rhea.cmd.pd.*;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.util.Endpoint;
import config.DTGConstants;
import storage.DTGCluster;
import storage.DTGStore;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 *
 * @author jiachun.fjc
 */
public class DTGMetadataRpcClient {

    private final PlacementDriverRpcService pdRpcService;
    private final int                       failoverRetries;

    public DTGMetadataRpcClient(PlacementDriverRpcService pdRpcService, int failoverRetries) {
        this.pdRpcService = pdRpcService;
        this.failoverRetries = failoverRetries;
    }

    public List<Long> getIds(byte type){
        final CompletableFuture<List<Long>> future = new CompletableFuture<>();
        internalGetIds(type, future, this.failoverRetries, null);
        return FutureHelper.get(future);
    }

    private void internalGetIds(byte type, final CompletableFuture<List<Long>> future, final int retriesLeft, final Errors lastCause){
        final RetryRunner retryRunner = retryCause -> internalGetIds(type, future, retriesLeft - 1, retryCause);
        final FailoverClosure<List<Long>> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        final GetIdsRequest request = new GetIdsRequest();
        request.setIdType(type);
        this.pdRpcService.callPdServerWithRpc(request, closure, lastCause);
    }

    public long getVersion(){
        final CompletableFuture<Long> future = new CompletableFuture<>();
        internalGetVersion(future, this.failoverRetries, null);
        return FutureHelper.get(future);
    }

    public void internalGetVersion(final CompletableFuture<Long> future, final int retriesLeft, final Errors lastCause){
        final RetryRunner retryRunner = retryCause -> internalGetVersion(future, retriesLeft - 1, retryCause);
        final FailoverClosure<Long> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner, DTGConstants.RETRIYRUNNERWAIT);
        CompletableFuture<Long> future1 = new CompletableFuture<>();
        sendGetVersion(future1, retriesLeft, null);
        try{
            long version = FutureHelper.get(future1);
            closure.setData(version);
            closure.run(Status.OK());
        }catch (Exception e){
            closure.setError(Errors.NOT_LEADER);
            closure.run(new Status(RaftError.ENEWLEADER.getNumber(), "get version failed"));
        }
    }

    private void sendGetVersion(final CompletableFuture<Long> future, final int retriesLeft, final Errors lastCause){
        final RetryRunner retryRunner = retryCause -> sendGetVersion(future, retriesLeft - 1, retryCause);
        final FailoverClosure<Long> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        final GetVersionRequest request = new GetVersionRequest();
        this.pdRpcService.callPdServerWithRpc(request, closure, lastCause);
    }

    public boolean returnIds(List<Long> ids, byte type){
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        internalReturnIds(type, ids, future, this.failoverRetries, null);
        return FutureHelper.get(future);
    }

    private void internalReturnIds(byte type, List<Long> ids, final CompletableFuture<Boolean> future, final int retriesLeft, final Errors lastCause){
        final RetryRunner retryRunner = retryCause -> internalReturnIds(type, ids, future, retriesLeft - 1, retryCause);
        final FailoverClosure<Boolean> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        final ReturnIdsRequest request = new ReturnIdsRequest();
        request.setIdList(ids);
        request.setIdType(type);
        this.pdRpcService.callPdServerWithRpc(request, closure, lastCause);
    }

    /**
     * Returns the specified cluster information.
     */
    public DTGCluster getClusterInfo(final long clusterId) {
        final CompletableFuture<DTGCluster> future = new CompletableFuture<>();
        internalGetClusterInfo(clusterId, future, this.failoverRetries, null);
        DTGCluster cluster = FutureHelper.get(future);
        return cluster;
    }

    private void internalGetClusterInfo(final long clusterId, final CompletableFuture<DTGCluster> future,
                                        final int retriesLeft, final Errors lastCause) {
        final RetryRunner retryRunner = retryCause -> internalGetClusterInfo(clusterId, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<DTGCluster> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        final GetClusterInfoRequest request = new GetClusterInfoRequest();
        request.setClusterId(clusterId);
        this.pdRpcService.callPdServerWithRpc(request, closure, lastCause);
    }

    /**
     * The pd server stores the storeIds of all nodes.
     * This method provides a lookup for the storeId according
     * to the host.  If there is no value, then a globally
     * unique storeId is created.
     */
    public Long getOrCreateStoreId(final long clusterId, final Endpoint endpoint) {
        final CompletableFuture<Long> future = new CompletableFuture<>();
        internalGetOrCreateStoreId(clusterId, endpoint, future, this.failoverRetries, null);
        return FutureHelper.get(future);
    }

    private void internalGetOrCreateStoreId(final long clusterId, final Endpoint endpoint,
                                            final CompletableFuture<Long> future, final int retriesLeft,
                                            final Errors lastCause) {
        final RetryRunner retryRunner = retryCause -> internalGetOrCreateStoreId(clusterId, endpoint, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<Long> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        final GetStoreIdRequest request = new GetStoreIdRequest();
        request.setClusterId(clusterId);
        request.setEndpoint(endpoint);
        this.pdRpcService.callPdServerWithRpc(request, closure, lastCause);
    }

    /**
     * Query the store information by the host.  If the result
     * is a empty instance, the caller needs to use its own local
     * configuration.
     */
    public DTGStore getStoreInfo(final long clusterId, final Endpoint selfEndpoint) {
        final CompletableFuture<DTGStore> future = new CompletableFuture<>();
        internalGetStoreInfo(clusterId, selfEndpoint, future, this.failoverRetries, null);
        return FutureHelper.get(future);
    }

    private void internalGetStoreInfo(final long clusterId, final Endpoint selfEndpoint,
                                      final CompletableFuture<DTGStore> future, final int retriesLeft,
                                      final Errors lastCause) {
        final RetryRunner retryRunner = retryCause -> internalGetStoreInfo(clusterId, selfEndpoint, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<DTGStore> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        final GetStoreInfoRequest request = new GetStoreInfoRequest();
        request.setClusterId(clusterId);
        request.setEndpoint(selfEndpoint);
        this.pdRpcService.callPdServerWithRpc(request, closure, lastCause);
    }

    /**
     * Update the store information by the storeId.
     */
    public DTGStore updateStoreInfo(final long clusterId, final DTGStore store) {
        final CompletableFuture<DTGStore> future = new CompletableFuture<>();
        internalUpdateStoreInfo(clusterId, store, future, 1, null);
        return FutureHelper.get(future);
    }

    private void internalUpdateStoreInfo(final long clusterId, final DTGStore store, final CompletableFuture<DTGStore> future,
                                         final int retriesLeft, final Errors lastCause) {
        final RetryRunner retryRunner = retryCause -> internalUpdateStoreInfo(clusterId, store, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<DTGStore> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        final SetDTGStoreInfoRequest request = new SetDTGStoreInfoRequest();
        request.setClusterId(clusterId);
        request.setStore(store);
        this.pdRpcService.callPdServerWithRpc(request, closure, lastCause);
    }

    /**
     * Create a globally unique regionId.
     */
    public Long createRegionId(final long clusterId, final Endpoint endpoint) {
        final CompletableFuture<Long> future = new CompletableFuture<>();
        internalCreateRegionId(clusterId, endpoint, future, this.failoverRetries, null);
        return FutureHelper.get(future);
    }

    private void internalCreateRegionId(final long clusterId, final Endpoint endpoint,
                                        final CompletableFuture<Long> future, final int retriesLeft,
                                        final Errors lastCause) {
        final RetryRunner retryRunner = retryCause -> internalCreateRegionId(clusterId, endpoint, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<Long> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        final CreateRegionIdRequest request = new CreateRegionIdRequest();
        request.setClusterId(clusterId);
        request.setEndpoint(endpoint);
        this.pdRpcService.callPdServerWithRpc(request, closure, lastCause);
    }

    public void createRegion(final long clusterId, final byte type, final long maxId){
        final CompletableFuture future = new CompletableFuture();
        internalCreateRegion(clusterId, type, maxId, future, this.failoverRetries, null);
    }

    private void internalCreateRegion(final long clusterId, final byte type, final long maxId, final CompletableFuture future, final int retriesLeft, final Errors lastCause){
        final RetryRunner retryRunner = retryCause -> internalCreateRegion(clusterId, type, maxId, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<Long> closure = new FailoverClosureImpl<>(future, retriesLeft, retryRunner);
        final CreateRegionRequest request = new CreateRegionRequest();
        request.setClusterId(clusterId);
        request.setIdType(type);
        request.setMaxIdNeed(maxId);
        this.pdRpcService.callPdServerWithRpc(request, closure, lastCause);
    }
}

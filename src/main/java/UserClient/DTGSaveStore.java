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
package UserClient;

import Communication.DTGRpcService;
import Communication.DefaultDTGRpcService;
import Communication.RequestAndResponse.*;
import DBExceptions.TypeDoesnotExistException;
import Element.DTGOperation;
import Element.EntityEntry;
import Element.OperationName;
import PlacementDriver.DTGPlacementDriverClient;
import PlacementDriver.DefaultPlacementDriverClient;
import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.rhea.*;
import com.alipay.sofa.jraft.rhea.client.FutureGroup;
import com.alipay.sofa.jraft.rhea.client.FutureHelper;
import com.alipay.sofa.jraft.rhea.client.failover.RetryCallable;
import com.alipay.sofa.jraft.rhea.client.failover.RetryRunner;
import com.alipay.sofa.jraft.rhea.errors.ApiExceptionHelper;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.errors.RheaRuntimeException;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metrics.KVMetricNames;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import com.alipay.sofa.jraft.rhea.options.BatchingOptions;
import com.alipay.sofa.jraft.rhea.options.HeartbeatOptions;
import com.alipay.sofa.jraft.rhea.options.RpcOptions;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.storage.KVStoreClosure;
import com.alipay.sofa.jraft.rhea.util.Constants;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.Strings;
import com.alipay.sofa.jraft.rhea.util.concurrent.AffinityNamedThreadFactory;
import com.alipay.sofa.jraft.rhea.util.concurrent.NamedThreadFactory;
import com.alipay.sofa.jraft.rhea.util.concurrent.disruptor.Dispatcher;
import com.alipay.sofa.jraft.rhea.util.concurrent.disruptor.TaskDispatcher;
import com.alipay.sofa.jraft.rhea.util.concurrent.disruptor.WaitStrategyType;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;
import com.codahale.metrics.Histogram;
import com.lmax.disruptor.EventHandler;
import config.DTGConstants;
import options.DTGPlacementDriverOptions;
import options.DTGStoreEngineOptions;
import options.DTGStoreOptions;
import Region.DTGRegionEngine;

import java.util.*;
import java.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.DTGRawStore;
import raft.EntityStoreClosure;
import raft.FailoverClosure;
import raft.FailoverClosureImpl;
import storage.DTGStoreEngine;

import static config.MainType.*;
import static tool.ObjectAndByte.toByteArray;
import static tool.ObjectAndByte.toObject;

import Region.DTGRegion;

/**
 * @author :jinkai
 * @date :Created in 2019/10/15 22:07
 * @description:
 * @modified By:
 * @version: 1.0
 */

public class DTGSaveStore implements Lifecycle<DTGStoreOptions> {

    private static final Logger LOG = LoggerFactory.getLogger(DTGSaveStore.class);

    private DTGPlacementDriverClient pdClient;
    private DTGStoreEngine storeEngine;
    private Dispatcher kvDispatcher;
    private DTGRpcService dtgRpcService;
    private BatchingOptions batchingOpts;
    private Map<String, List<DTGRegion>> waitCommitMap;
    private DTGStoreOptions opts;
    private boolean onlyLeaderRead;
    private volatile boolean started;


    @Override
    public boolean init(DTGStoreOptions opts) {
        if (this.started) {
            LOG.info("[DefaultRheaKVStore] already started.");
            return true;
        }
        this.opts = opts;
        final String clusterName = opts.getClusterName();
        DTGPlacementDriverOptions pdopts = opts.getPlacementDriverOptions();
        Requires.requireNonNull(pdopts, "opts.placementDriverOptions");
        Requires.requireNonNull(clusterName, "opts.clusterName");
        if (Strings.isBlank(pdopts.getInitialServerList())) {
            // if blank, extends parent's value
            pdopts.setInitialServerList(opts.getInitialServerList());
        }
        pdClient = new DefaultPlacementDriverClient(opts.getClusterId(), clusterName, opts.isRemotePd());
        if (!this.pdClient.init(pdopts)) {
            LOG.error("Fail to init [PlacementDriverClient].");
            return false;
        }
        if(!opts.getPlacementDriverOptions().isLocalClient()){
            final DTGStoreEngineOptions stOpts = opts.getStoreEngineOptions();
            stOpts.setSaveStore(this);
            if (stOpts != null) {
                stOpts.setInitialServerList(opts.getInitialServerList());
                this.storeEngine = new DTGStoreEngine(this.pdClient);
                if (!this.storeEngine.init(stOpts)) {
                    LOG.error("Fail to init [StoreEngine].");
                    return false;
                }
            }
        }else {
            pdClient.refreshRouteTable(true);
        }

        final Endpoint selfEndpoint = this.storeEngine == null ? null : this.storeEngine.getSelfEndpoint();
        final RpcOptions rpcOpts = opts.getRpcOptions();
        this.dtgRpcService = new DefaultDTGRpcService(this.pdClient, selfEndpoint) {

            @Override
            public Endpoint getLeader(final long regionId, final boolean forceRefresh, final long timeoutMillis) {
                final Endpoint leader = getLeaderByRegionEngine(regionId);
                if (leader != null) {
                    return leader;
                }
                return super.getLeader(regionId, forceRefresh, timeoutMillis);
            }
        };
        if (!this.dtgRpcService.init(rpcOpts)) {
            LOG.error("Fail to init [RheaKVRpcService].");
            return false;
        }
        if(!opts.getPlacementDriverOptions().isLocalClient()){
            this.storeEngine.setRpcService(dtgRpcService);
        }
        this.onlyLeaderRead = opts.isOnlyLeaderRead();
        if (opts.isUseParallelExecutor()) {
            final int numWorkers = Utils.cpus();
            final int bufSize = numWorkers << 4;
            final String name = "parallel-kv-executor";
            final ThreadFactory threadFactory = Constants.THREAD_AFFINITY_ENABLED
                    ? new AffinityNamedThreadFactory(name, true) : new NamedThreadFactory(name, true);
            this.kvDispatcher = new TaskDispatcher(bufSize, numWorkers, WaitStrategyType.LITE_BLOCKING_WAIT, threadFactory);
        }
        waitCommitMap = new HashMap<>();
        return this.started = true;
    }

    @Override
    public void shutdown() {
        if (!this.started) {
            return;
        }
        this.started = false;
        if (this.pdClient != null) {
            this.pdClient.shutdown();
        }
        if (this.storeEngine != null) {
            this.storeEngine.shutdown();
        }
        if (this.dtgRpcService != null) {
            this.dtgRpcService.shutdown();
        }
        if (this.kvDispatcher != null) {
            this.kvDispatcher.shutdown();
        }
        LOG.info("[DefaultRheaKVStore] shutdown successfully.");
    }

    public Map<Integer, Object> applyRequest(final List<EntityEntry> entries, String txId, boolean isReadOnly, boolean fromClient,
                                             final int retriesLeft, boolean tryBatching, long version, byte[] firstGetMap){
        final FutureGroup<Map<Integer, Object>> futureGroup = runApply(entries, txId, fromClient, isReadOnly,
                1, retriesLeft,  tryBatching, version, firstGetMap);
        return FutureHelper.get(FutureHelper.joinMap(futureGroup), Long.MAX_VALUE);
    }


    public FutureGroup<Map<Integer, Object>> runApply(final List<EntityEntry> entries, final String txId, final boolean fromClient, final boolean isReadOnly,
                                                      final int repeate, final int retriesLeft, boolean tryBatching, long version, byte[] firstGetMap){
        List<DTGRegion> regionList = new LinkedList<>();System.out.println("runApply");
        Requires.requireNonNull(entries, "entries");
        Requires.requireTrue(!entries.isEmpty(), "entries empty");
        Map<DTGRegion, List<EntityEntry>> distributeMap = dirtributeEntity(entries, null);
        Requires.requireNonNull(distributeMap, "distributeMap");
        final List<CompletableFuture<Map<Integer, Object>>> futures = Lists.newArrayListWithCapacity(distributeMap.size());
        long mainRegionId = distributeMap.keySet().iterator().next().getId();
        List<Long> regionIds = new ArrayList<>();
        for(DTGRegion region : distributeMap.keySet()){
            regionIds.add(region.getId());
        }
        for (final Map.Entry<DTGRegion, List<EntityEntry>> entry : distributeMap.entrySet()) {
            final DTGRegion region = entry.getKey();
            final List<EntityEntry> subEntries = entry.getValue();
            DTGOperation op = new DTGOperation(subEntries, OperationName.TRANSACTIONOP);
            if(region.getId() == mainRegionId){
                op.setAllEntityEntries(entries);
                op.setRegionIds(regionIds);
            }
            op.setTxId(txId);
            System.out.println(txId);
            op.setVersion(version);
            op.setMainRegionId(mainRegionId);
            op.setReadOnly(isReadOnly);
            if(firstGetMap != null){
                op.setType(OperationName.SCEONDREAD);
                op.setIsolateRead(true);
                op.setOpData(firstGetMap) ;
            }
            CompletableFuture<Map<Integer, Object>> future = new CompletableFuture();
            applyOperation(op,  region, future, fromClient, repeate, retriesLeft, null);
            futures.add(future);
            regionList.add(region);
        }
        return new FutureGroup<>(futures);
    }


    public void applyOperation(final DTGOperation op, final DTGRegion region, final CompletableFuture<Map<Integer, Object>> future,
                               boolean fromClient,int repeate, final int retriesLeft, final Errors lastCause){
        CompletableFuture<byte[]> future2 = new CompletableFuture<>();
        internalRegionPut(region, op, fromClient,repeate, future2, retriesLeft, lastCause);
        try {
            byte[] res = FutureHelper.get(future2, Long.MAX_VALUE);
            future.complete((Map<Integer, Object>)toObject(res));
        }
        catch (Exception e){
            System.out.println(e);
        }
    }


    private Map<DTGRegion, List<EntityEntry>> dirtributeEntity(final List<EntityEntry> entityEntryList, final Throwable lastCause){
        if(lastCause != null){
            this.pdClient.refreshRouteTable(true);
        }
        LinkedList<EntityEntry> NodeEntityEntryList = new LinkedList<>();
        LinkedList<EntityEntry> RelaEntityEntryList = new LinkedList<>();
        LinkedList<EntityEntry> TempProEntityEntryList = new LinkedList<>();
        for(EntityEntry entityEntry : entityEntryList){
            switch (entityEntry.getType()){
                case NODETYPE:{
                    NodeEntityEntryList.add(entityEntry);
                    break;
                }
                case RELATIONTYPE:{
                    RelaEntityEntryList.add(entityEntry);
                    break;
                }
                case TEMPORALPROPERTYTYPE:{
                    TempProEntityEntryList.add(entityEntry);
                    break;
                }
                default:{
                    try {
                        throw new TypeDoesnotExistException(entityEntry.getType(), "entity type");
                    } catch (TypeDoesnotExistException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        Map[] distributeMap =
                pdClient.findRegionsByEntityEntries(NodeEntityEntryList, ApiExceptionHelper.isInvalidEpoch(lastCause), NODETYPE);
        pdClient.findRegionsByEntityEntries(RelaEntityEntryList, ApiExceptionHelper.isInvalidEpoch(lastCause), RELATIONTYPE, distributeMap);
        pdClient.findRegionsByEntityEntries(TempProEntityEntryList, ApiExceptionHelper.isInvalidEpoch(lastCause), TEMPORALPROPERTYTYPE, distributeMap);
        HashMap<DTGRegion, List<EntityEntry>> regionMap = (HashMap<DTGRegion, List<EntityEntry>>)distributeMap[0];
        for(DTGRegion region : regionMap.keySet()){
            Collections.sort(regionMap.get(region));
        }
        return regionMap;
    }

    public <T> void internalRegionPut(final DTGRegion region, final DTGOperation op, boolean fromClient, int repeate,
                                      final CompletableFuture<T> future, final int retriesLeft, final Errors lastCause) {
        final DTGRegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalRegionPut(region, op, fromClient, repeate, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<T> closure = new FailoverClosureImpl<T>(future, false, retriesLeft,
                retryRunner, DTGConstants.RETRIYRUNNERWAIT);
        if (regionEngine != null && regionEngine.isLeader()) {
            this.storeEngine.getRegionService(region.getId()).internalFirstPhase(op, closure);
        } else {
            final DTGBaseRequest request = getRequest(op.getType());
            request.setDTGOpreration(op);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
            if(request instanceof FirstPhaseRequest){
                ((FirstPhaseRequest) request).setRepeate(repeate);
                if(fromClient){
                    ((FirstPhaseRequest) request).setFromClient();
                }
            }
            this.dtgRpcService.callAsyncWithRpc(request, closure, lastCause);
        }
    }

    private DTGRegionEngine getRegionEngine(final long regionId) {
        if (this.storeEngine == null) {
            return null;
        }
        return this.storeEngine.getRegionEngine(regionId);
    }

    private DTGRegionEngine getRegionEngine(final long regionId, final boolean requireLeader) {
        final DTGRegionEngine engine = getRegionEngine(regionId);
        if (engine == null) {
            return null;
        }
        if (requireLeader && !engine.isLeader()) {
            return null;
        }
        return engine;
    }

    private static boolean ensureOnValidEpoch(final DTGRegion region, final DTGRegionEngine engine,
                                              final EntityStoreClosure closure) {
        if (isValidEpoch(region, engine)) {
            return true;
        }
        // will retry on this error and status
        closure.setError(Errors.INVALID_REGION_EPOCH);
        closure.run(new Status(-1, "Invalid region epoch: %s", region));
        return false;
    }

    private static boolean isValidEpoch(final DTGRegion region, final DTGRegionEngine engine) {
        return region.getRegionEpoch().equals(engine.getRegion().getRegionEpoch());
    }

    private Endpoint getLeaderByRegionEngine(final long regionId) {
        final DTGRegionEngine regionEngine = getRegionEngine(regionId);
        if (regionEngine != null) {
            final PeerId leader = regionEngine.getLeaderId();
            if (leader != null) {
                final String raftGroupId = JRaftHelper.getJRaftGroupId(this.pdClient.getClusterName(), regionId);
                RouteTable.getInstance().updateLeader(raftGroupId, leader);
                return leader.getEndpoint();
            }
        }
        return null;
    }

    public DTGBaseRequest getRequest(byte type){
        switch (type){
            case OperationName.TRANSACTIONOP:return new FirstPhaseRequest();
            case OperationName.COMMITTRANS:
            case OperationName.ROLLBACK:return new SecondPhaseRequest();
        }
        return null;
    }

    public DTGPlacementDriverClient getPlacementDriverClient() {
        return pdClient;
    }

    public long getClusterId() {
        return this.opts.getClusterId();
    }

    public DTGStoreEngine getStoreEngine() {
        return storeEngine;
    }

    public boolean isLeader(final long regionId) {
        checkState();
        final DTGRegionEngine regionEngine = getRegionEngine(regionId);
        return regionEngine != null && regionEngine.isLeader();
    }

    private void checkState() {
        // Not a strict state check, more is to use a read volatile operation to make
        // a happen-before, because the init() method finally wrote 'this.started'
        if (!this.started) {
            throw new RheaRuntimeException("rhea kv is not started or shutdown");
        }
    }

    private static boolean ensureOnValidEpoch(final Region region, final RegionEngine engine,
                                              final KVStoreClosure closure) {
        if (isValidEpoch(region, engine)) {
            return true;
        }
        // will retry on this error and status
        closure.setError(Errors.INVALID_REGION_EPOCH);
        closure.run(new Status(-1, "Invalid region epoch: %s", region));
        return false;
    }

    private static boolean isValidEpoch(final Region region, final RegionEngine engine) {
        return region.getRegionEpoch().equals(engine.getRegion().getRegionEpoch());
    }

    private DTGRegion getRegion(List<EntityEntry> entityEntryList){
        Map<DTGRegion, List<EntityEntry>> distributeMap = dirtributeEntity(entityEntryList, null);
        DTGRegion returnRegion = null;
        for(DTGRegion region : distributeMap.keySet()){
            if(returnRegion == null || region.getTransactionCount() < returnRegion.getTransactionCount()){
                returnRegion = region;
            }
        }
        return returnRegion;
    }

    public DTGDatabase getDB(){
        DTGDatabase db = new DTGDatabase(this);
        Endpoint endpoint = this.opts.getStoreEngineOptions().getServerAddress();
        db.init(endpoint.getIp(), endpoint.getPort(), "D:\\garbage");
        return db;
    }
}

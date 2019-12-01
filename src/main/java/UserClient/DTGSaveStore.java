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
import Communication.RequestAndResponse.CommitRequest;
import Communication.RequestAndResponse.DTGBaseRequest;
import DBExceptions.TypeDoesnotExistException;
import Element.DTGOpreration;
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
import com.alipay.sofa.jraft.rhea.client.failover.impl.BoolFailoverFuture;
import Communication.RequestAndResponse.TransactionRequest;
import com.alipay.sofa.jraft.rhea.client.pd.PlacementDriverClient;
import com.alipay.sofa.jraft.rhea.cmd.store.BaseRequest;
import com.alipay.sofa.jraft.rhea.errors.ApiExceptionHelper;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.errors.RheaRuntimeException;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metrics.KVMetricNames;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import com.alipay.sofa.jraft.rhea.options.BatchingOptions;
import com.alipay.sofa.jraft.rhea.options.RpcOptions;
import com.alipay.sofa.jraft.rhea.options.StoreEngineOptions;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.storage.KVStoreClosure;
import com.alipay.sofa.jraft.rhea.storage.RawKVStore;
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
import options.DTGPlacementDriverOptions;
import options.DTGPlacementDriverServerOptions;
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
    //private RheaKVRpcService rheaKVRpcService;
    private DTGRpcService dtgRpcService;
    //private DTGBatching applyBatching;
    private BatchingOptions batchingOpts;
    private Map<String, List<DTGRegion>> waitCommitMap;
    private DTGStoreOptions opts;
    private boolean onlyLeaderRead;
    private volatile boolean started;
    private int failoverRetries;
    private long futureTimeoutMillis;


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
        final DTGStoreEngineOptions stOpts = opts.getStoreEngineOptions();
        if (stOpts != null) {
            stOpts.setInitialServerList(opts.getInitialServerList());
            this.storeEngine = new DTGStoreEngine(this.pdClient);
            if (!this.storeEngine.init(stOpts)) {
                LOG.error("Fail to init [StoreEngine].");
                return false;
            }
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
        this.failoverRetries = opts.getFailoverRetries();
        this.futureTimeoutMillis = opts.getFutureTimeoutMillis();
        this.onlyLeaderRead = opts.isOnlyLeaderRead();
        if (opts.isUseParallelExecutor()) {
            final int numWorkers = Utils.cpus();
            final int bufSize = numWorkers << 4;
            final String name = "parallel-kv-executor";
            final ThreadFactory threadFactory = Constants.THREAD_AFFINITY_ENABLED
                    ? new AffinityNamedThreadFactory(name, true) : new NamedThreadFactory(name, true);
            this.kvDispatcher = new TaskDispatcher(bufSize, numWorkers, WaitStrategyType.LITE_BLOCKING_WAIT, threadFactory);
        }
//        final int numWorkers = Utils.cpus();
//        final int bufSize = numWorkers << 4;
//        final String name = "parallel-kv-executor";
//        final ThreadFactory threadFactory = Constants.THREAD_AFFINITY_ENABLED
//                ? new AffinityNamedThreadFactory(name, true) : new NamedThreadFactory(name, true);
//        this.kvDispatcher = new TaskDispatcher(bufSize, numWorkers, WaitStrategyType.LITE_BLOCKING_WAIT, threadFactory);
//        this.batchingOpts = opts.getBatchingOptions();
        waitCommitMap = new HashMap<>();
//        this.applyBatching = new DTGBatching(EntityEvent::new, "put_batching",
//                new PutBatchingHandler("put"));
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
//        if (this.getBatching != null) {
//            this.getBatching.shutdown();
//        }
//        if (this.getBatchingOnlySafe != null) {
//            this.getBatchingOnlySafe.shutdown();
//        }
//        if (this.putBatching != null) {
//            this.putBatching.shutdown();
//        }
        LOG.info("[DefaultRheaKVStore] shutdown successfully.");
    }

//    public CompletableFuture<Boolean> applyCommitRequest(String txId, boolean shouoldCommit, final int retriesLeft){
//         return FutureHelper.joinBooleans(internalCommit(shouoldCommit, txId, retriesLeft,null));
//    }

    public boolean applyCommitRequest(String txId, boolean shouoldCommit, final int retriesLeft){
        final FutureGroup<Boolean> futureGroup = internalCommit(shouoldCommit, txId, retriesLeft,null);
        return FutureHelper.get(FutureHelper.joinBooleans(futureGroup));
    }

    public FutureGroup<Boolean> internalCommit(boolean shouoldCommit, String txId, final int retriesLeft,
                                               final Errors lastCause){
        final List<DTGRegion> regionList = waitCommitMap.get(txId);
        DTGOpreration op = null;
        if(shouoldCommit)op = new DTGOpreration(null, OperationName.COMMITTRANS);
        else op = new DTGOpreration(null, OperationName.ROLLBACK);
        op.setTxId(txId);
        final List<CompletableFuture<Boolean>> futures = Lists.newArrayListWithCapacity(regionList.size());
        for(DTGRegion region : regionList){
            final RetryCallable<Boolean> retryCallable = retryCause -> internalCommit(shouoldCommit, txId,retriesLeft - 1,
                    Errors.forException(retryCause));
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            //final BoolFailoverFuture future = new BoolFailoverFuture(retriesLeft, retryCallable);
            applyCommit(op, region, future, retriesLeft, lastCause);
            //internalRegionPut(region, op, future, retriesLeft, lastCause);
            futures.add(future);
        }
        return new FutureGroup<>(futures);
    }

    public Map<Integer, Object> applyRequest(final List<EntityEntry> entries, String txId,
                                             final int retriesLeft, final Throwable lastCause, boolean tryBatching){
        final FutureGroup<Map<Integer, Object>> futureGroup = runApply(entries, txId, retriesLeft, lastCause, tryBatching);
        return FutureHelper.get(FutureHelper.joinMap(futureGroup));
    }

    public FutureGroup<Map<Integer, Object>> runApply(final List<EntityEntry> entries, String txId, final int retriesLeft, final Throwable lastCause, boolean tryBatching){
//        CompletableFuture future = new CompletableFuture<>();
//        if (tryBatching) {
//            final DTGBatching applyBatching = this.batching;
//            if (putBatching != null && putBatching.apply(new KVEntry(key, value), future)) {
//                return future;
//            }
//        }
        List<DTGRegion> regionList = new LinkedList<>();
        Requires.requireNonNull(entries, "entries");
        Requires.requireTrue(!entries.isEmpty(), "entries empty");
        Map<DTGRegion, List<EntityEntry>> distributeMap = dirtributeEntity(entries, lastCause);
        Requires.requireNonNull(distributeMap, "distributeMap");
        final List<CompletableFuture<Map<Integer, Object>>> futures = Lists.newArrayListWithCapacity(distributeMap.size());
        final Errors lastError = lastCause == null ? null : Errors.forException(lastCause);
        for (final Map.Entry<DTGRegion, List<EntityEntry>> entry : distributeMap.entrySet()) {
            final DTGRegion region = entry.getKey();
            final List<EntityEntry> subEntries = entry.getValue();
            DTGOpreration op = new DTGOpreration(subEntries, OperationName.TRANSACTIONOP);
            op.setTxId(txId);
            CompletableFuture<Map<Integer, Object>> future = new CompletableFuture();
            applyOperation(op, region, future, retriesLeft, lastError);
//            final RetryCallable<byte[]> retryCallable = retryCause -> runApply(subEntries, retriesLeft - 1,
//                    retryCause, tryBatching);
//            CompletableFuture<byte[]> future = new CompletableFuture<>();
//            internalRegionPut(region, op, future, retriesLeft, lastError);
            futures.add(future);
            regionList.add(region);System.out.println("add future");
        }
        waitCommitMap.put(txId, regionList);
        return new FutureGroup<>(futures);
    }

    public void applyOperation(final DTGOpreration op, final DTGRegion region,final CompletableFuture<Map<Integer, Object>> future,
                                               final int retriesLeft, final Errors lastCause){
        final RetryRunner retryRunner = retryCause -> applyOperation(op, region, future, retriesLeft - 1,
                retryCause);
        final FailoverClosure<Map<Integer, Object>> closure = new FailoverClosureImpl<>(future, false, retriesLeft, retryRunner);
        CompletableFuture<byte[]> future2 = new CompletableFuture<>();
        internalRegionPut(region, op, future2, retriesLeft, lastCause);
        try {
            byte[] res = future2.get(FutureHelper.DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            closure.setData(toObject(res));
            closure.run(Status.OK());
        }
        catch (InterruptedException | ExecutionException | TimeoutException e){
            closure.setError(Errors.forException(e));
            closure.run(new Status(-1, "request lock failed with region ID: %s, transaction op id: %s", region.getId(), op.getTxId()));
        }
    }

    public void applyCommit(final DTGOpreration op, final DTGRegion region,final CompletableFuture<Boolean> future,
                               final int retriesLeft, final Errors lastCause){
        final RetryRunner retryRunner = retryCause -> applyCommit(op, region, future, retriesLeft - 1,
                retryCause);
        final FailoverClosure<Boolean> closure = new FailoverClosureImpl<>(future, false, retriesLeft, retryRunner);
        CompletableFuture<Boolean> future2 = new CompletableFuture<>();
        internalRegionPut(region, op, future2, retriesLeft, lastCause);
        try {
            boolean res = future2.get(FutureHelper.DEFAULT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            closure.setData(res);
            closure.run(Status.OK());
        }
        catch (InterruptedException | ExecutionException | TimeoutException e){
            closure.setError(Errors.forException(e));
            closure.run(new Status(-1, "request lock failed with region ID: %s, transaction op id: %s", region.getId(), op.getTxId()));
        }

    }

    private Map<DTGRegion, List<EntityEntry>> dirtributeEntity(final List<EntityEntry> entityEntryList, final Throwable lastCause){
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

    private <T> void internalRegionPut(final DTGRegion region, final DTGOpreration op,
                                   final CompletableFuture<T> future, final int retriesLeft,
                                   final Errors lastCause) {
        final DTGRegionEngine regionEngine = getRegionEngine(region.getId(), true);
        final RetryRunner retryRunner = retryCause -> internalRegionPut(region, op, future,
                retriesLeft - 1, retryCause);
        final FailoverClosure<T> closure = new FailoverClosureImpl<>(future, false, retriesLeft,
                retryRunner);
        if (regionEngine != null) {
            if (ensureOnValidEpoch(region, regionEngine, closure)) {
                final DTGRawStore rawStore = getRawStore(regionEngine);
                if (this.kvDispatcher == null) {
                    //rawKVStore.put(subEntries, closure);
                    rawStore.ApplyEntityEntries(op, closure);
                } else {
                    this.kvDispatcher.execute(() -> rawStore.ApplyEntityEntries(op, closure));
                }
            }
        } else {
            final DTGBaseRequest request = getRequest(op.getType());
            System.out.println("send request..." + request.TypeString());//System.out.println(op.getSize());
            request.setDTGOpreration(op);
            request.setRegionId(region.getId());
            request.setRegionEpoch(region.getRegionEpoch());
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

    private DTGRawStore getRawStore(final DTGRegionEngine engine) {
        return engine.getMetricsRawStore();
    }

    private List<KVEntry> EntityEntry2KVEntry(List<EntityEntry> entityEntries){
        List<KVEntry> kvEntries = new LinkedList<>();
        for(EntityEntry entry : entityEntries){
            kvEntries.add(new KVEntry(toByteArray(entry.getTransactionNum()), toByteArray(entry)));
        }
        return kvEntries;
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

//    private class DTGBatching extends Batching<EntityEvent, KVEntry, Boolean> {
//
//        public DTGBatching(EventFactory<KVEvent> factory, String name, PutBatchingHandler handler) {
//            super(factory, batchingOpts.getBufSize(), name, handler);
//        }
//
//        @Override
//        public boolean apply(final KVEntry message, final CompletableFuture<Boolean> future) {
//            return this.ringBuffer.tryPublishEvent((event, sequence) -> {
//                event.reset();
//                event.kvEntry = message;
//                event.future = future;
//            });
//        }
//    }
//
//    private static abstract class Batching<T, E, F> {
//
//        protected final String        name;
//        protected final Disruptor<T> disruptor;
//        protected final RingBuffer<T> ringBuffer;
//
//        @SuppressWarnings("unchecked")
//        public Batching(EventFactory<T> factory, int bufSize, String name, EventHandler<T> handler) {
//            this.name = name;
//            this.disruptor = new Disruptor<>(factory, bufSize, new NamedThreadFactory(name, true));
//            this.disruptor.handleEventsWith(handler);
//            this.disruptor.setDefaultExceptionHandler(new LogExceptionHandler<Object>(name));
//            this.ringBuffer = this.disruptor.start();
//        }
//
//        public abstract boolean apply(final E message, final CompletableFuture<F> future);
//
//        public void shutdown() {
//            try {
//                this.disruptor.shutdown(3L, TimeUnit.SECONDS);
//            } catch (final Exception e) {
//                LOG.error("Fail to shutdown {}, {}.", toString(), StackTraceUtil.stackTrace(e));
//            }
//        }
//
//        @Override
//        public String toString() {
//            return "Batching{" + "name='" + name + '\'' + ", disruptor=" + disruptor + '}';
//        }
//    }
//
//    private class PutBatchingHandler extends AbstractBatchingHandler<EntityEvent> {
//
//        public PutBatchingHandler(String metricsName) {
//            super(metricsName);
//        }
//
//        @SuppressWarnings("unchecked")
//        @Override
//        public void onEvent(final EntityEvent event, final long sequence, final boolean endOfBatch) throws Exception {
//            this.events.add(event);
//            this.cachedBytes += event.kvEntry.length();
//            final int size = this.events.size();
//            if (!endOfBatch && size < batchingOpts.getBatchSize() && this.cachedBytes < batchingOpts.getMaxWriteBytes()) {
//                return;
//            }
//
//            if (size == 1) {
//                reset();
//                final KVEntry kv = event.kvEntry;
//                try {
//                    put(kv.getKey(), kv.getValue(), event.future, false);
//                } catch (final Throwable t) {
//                    exceptionally(t, event.future);
//                }
//            } else {
//                final List<KVEntry> entries = Lists.newArrayListWithCapacity(size);
//                final CompletableFuture<Boolean>[] futures = new CompletableFuture[size];
//                for (int i = 0; i < size; i++) {
//                    final KVEvent e = this.events.get(i);
//                    entries.add(e.kvEntry);
//                    futures[i] = e.future;
//                }
//                reset();
//                try {
//                    put(entries).whenComplete((result, throwable) -> {
//                        if (throwable == null) {
//                            for (int i = 0; i < futures.length; i++) {
//                                futures[i].complete(result);
//                            }
//                            return;
//                        }
//                        exceptionally(throwable, futures);
//                    });
//                } catch (final Throwable t) {
//                    exceptionally(t, futures);
//                }
//            }
//        }
//    }

    private abstract class AbstractBatchingHandler<T> implements EventHandler<T> {

        protected final Histogram histogramWithKeys;
        protected final Histogram histogramWithBytes;

        protected final List<T>   events      = Lists.newArrayListWithCapacity(batchingOpts.getBatchSize());
        protected int             cachedBytes = 0;

        public AbstractBatchingHandler(String metricsName) {
            this.histogramWithKeys = KVMetrics.histogram(KVMetricNames.SEND_BATCHING, metricsName + "_keys");
            this.histogramWithBytes = KVMetrics.histogram(KVMetricNames.SEND_BATCHING, metricsName + "_bytes");
        }

        public void exceptionally(final Throwable t, final CompletableFuture<?>... futures) {
            for (int i = 0; i < futures.length; i++) {
                futures[i].completeExceptionally(t);
            }
        }

        public void reset() {
            this.histogramWithKeys.update(this.events.size());
            this.histogramWithBytes.update(this.cachedBytes);

            this.events.clear();
            this.cachedBytes = 0;
        }
    }

    private static class EntityEvent {

        private EntityEntry                entityEntry;
        private CompletableFuture<Boolean> future;

        public void reset() {
            this.entityEntry = null;
            this.future = null;
        }
    }

    public DTGBaseRequest getRequest(byte type){
        switch (type){
            case OperationName.TRANSACTIONOP:return new TransactionRequest();
            case OperationName.COMMITTRANS:
            case OperationName.ROLLBACK:return new CommitRequest();
        }
        return null;
    }

    public DTGPlacementDriverClient getPlacementDriverClient() {
        return pdClient;
    }

    public void addLeaderStateListener(final long regionId, final LeaderStateListener listener) {
        addStateListener(regionId, listener);
    }

    public void addFollowerStateListener(final long regionId, final FollowerStateListener listener) {
        addStateListener(regionId, listener);
    }

    public void addStateListener(final long regionId, final StateListener listener) {
        checkState();
        if (this.storeEngine == null) {
            throw new IllegalStateException("current node do not have store engine");
        }
        final DTGRegionEngine regionEngine = this.storeEngine.getRegionEngine(regionId);
        if (regionEngine == null) {
            throw new IllegalStateException("current node do not have this region engine[" + regionId + "]");
        }
        regionEngine.getFsm().addStateListener(listener);
    }

    public long getClusterId() {
        return this.opts.getClusterId();
    }

    public DTGStoreEngine getStoreEngine() {
        return storeEngine;
    }

    public boolean isOnlyLeaderRead() {
        return onlyLeaderRead;
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

//    private DTGRegionEngine getRegionEngine(final long regionId) {
//        if (this.storeEngine == null) {
//            return null;
//        }
//        return this.storeEngine.getRegionEngine(regionId);
//    }

//    private RegionEngine getRegionEngine(final long regionId, final boolean requireLeader) {
//        final RegionEngine engine = getRegionEngine(regionId);
//        if (engine == null) {
//            return null;
//        }
//        if (requireLeader && !engine.isLeader()) {
//            return null;
//        }
//        return engine;
//    }

//    private Endpoint getLeaderByRegionEngine(final long regionId) {
//        final DTGRegionEngine regionEngine = getRegionEngine(regionId);
//        if (regionEngine != null) {
//            final PeerId leader = regionEngine.getLeaderId();
//            if (leader != null) {
//                final String raftGroupId = JRaftHelper.getJRaftGroupId(this.pdClient.getClusterName(), regionId);
//                RouteTable.getInstance().updateLeader(raftGroupId, leader);
//                return leader.getEndpoint();
//            }
//        }
//        return null;
//    }

    private DTGRawStore getDTGRawStore(final DTGRegionEngine engine) {
        return engine.getMetricsRawStore();
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

}

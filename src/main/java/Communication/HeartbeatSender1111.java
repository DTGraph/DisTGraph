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

import Communication.RequestAndResponse.DTGRegionHeartbeatRequest;
import Communication.instructions.DTGInstruction;
import PlacementDriver.DTGPlacementDriverClient;
import PlacementDriver.DefaultPlacementDriverClient;
import PlacementDriver.PD.DTGInstructionProcessor;
import PlacementDriver.PD.StatsCollector;
import com.alipay.remoting.InvokeCallback;
import com.alipay.remoting.InvokeContext;
import com.alipay.remoting.rpc.RpcClient;
import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.cmd.pd.BaseRequest;
import com.alipay.sofa.jraft.rhea.cmd.pd.BaseResponse;
import com.alipay.sofa.jraft.rhea.cmd.pd.StoreHeartbeatRequest;
import com.alipay.sofa.jraft.rhea.errors.ErrorsHelper;
import com.alipay.sofa.jraft.rhea.metadata.*;
import com.alipay.sofa.jraft.rhea.options.HeartbeatOptions;
import com.alipay.sofa.jraft.rhea.rpc.ExtSerializerSupports;
import com.alipay.sofa.jraft.rhea.storage.BaseKVStoreClosure;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.rhea.util.concurrent.DiscardOldPolicyWithReport;
import com.alipay.sofa.jraft.rhea.util.concurrent.NamedThreadFactory;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.ExecutorServiceHelper;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;
import com.alipay.sofa.jraft.util.timer.HashedWheelTimer;
import com.alipay.sofa.jraft.util.timer.Timeout;
import com.alipay.sofa.jraft.util.timer.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.DTGStoreEngine;
import Region.DTGRegion;
import Region.DTGRegionStats;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author jiachun.fjc
 */
public class HeartbeatSender1111 implements Lifecycle<HeartbeatOptions> {

    private static final Logger            LOG = LoggerFactory.getLogger(HeartbeatSender1111.class);

    private final DTGStoreEngine           storeEngine;
    private final DTGPlacementDriverClient pdClient;
    private final RpcCaller                rpcCaller;

    private StatsCollector                 statsCollector;
    private DTGInstructionProcessor        instructionProcessor;
    private HashedWheelTimer               heartbeatTimer;

    private boolean                        started;

    public HeartbeatSender1111(DTGStoreEngine storeEngine) {
        this.storeEngine = storeEngine;
        this.pdClient = storeEngine.getPlacementDriverClient();
        this.rpcCaller = storeEngine.getRpcCaller();
    }

    @Override
    public synchronized boolean init(final HeartbeatOptions opts) {
        if (this.started) {
            LOG.info("[HeartbeatSender] already started.");
            return true;
        }
        this.statsCollector = new StatsCollector(this.storeEngine);
        this.instructionProcessor = new DTGInstructionProcessor(this.storeEngine);
        this.heartbeatTimer = new HashedWheelTimer(new NamedThreadFactory("heartbeat-timer", true), 50,
            TimeUnit.MILLISECONDS, 4096);
        final long storeHeartbeatIntervalSeconds = opts.getStoreHeartbeatIntervalSeconds();
        final long regionHeartbeatIntervalSeconds = opts.getRegionHeartbeatIntervalSeconds();
        if (storeHeartbeatIntervalSeconds <= 0) {
            throw new IllegalArgumentException("Store heartbeat interval seconds must > 0, "
                                               + storeHeartbeatIntervalSeconds);
        }
        if (regionHeartbeatIntervalSeconds <= 0) {
            throw new IllegalArgumentException("Region heartbeat interval seconds must > 0, "
                                               + regionHeartbeatIntervalSeconds);
        }
        final long now = System.currentTimeMillis();
        final StoreHeartbeatTask storeHeartbeatTask = new StoreHeartbeatTask(storeHeartbeatIntervalSeconds, now, false);
        final RegionHeartbeatTask regionHeartbeatTask = new RegionHeartbeatTask(regionHeartbeatIntervalSeconds, now,
            false);
        this.heartbeatTimer.newTimeout(storeHeartbeatTask, storeHeartbeatTask.getNextDelay(), TimeUnit.SECONDS);
        this.heartbeatTimer.newTimeout(regionHeartbeatTask, regionHeartbeatTask.getNextDelay(), TimeUnit.SECONDS);
        LOG.info("[HeartbeatSender] start successfully, options: {}.", opts);
        return this.started = true;
    }

    @Override
    public synchronized void shutdown() {
        if (this.heartbeatTimer != null) {
            this.heartbeatTimer.stop();
        }
    }

    private void sendStoreHeartbeat(final long nextDelay, final boolean forceRefreshLeader, final long lastTime) {
        final long now = System.currentTimeMillis();//System.out.println("send store heartbeat");
        final StoreHeartbeatRequest request = new StoreHeartbeatRequest();
        request.setClusterId(this.storeEngine.getClusterId());
        final TimeInterval timeInterval = new TimeInterval(lastTime, now);
        final StoreStats stats = this.statsCollector.collectStoreStats(timeInterval);
        request.setStats(stats);
        final RpcCaller.RpcClosure<Object> closure = new RpcCaller.RpcClosure<Object>() {

            @Override
            public void run(final Status status) {
                final boolean forceRefresh = !status.isOk() && ErrorsHelper.isInvalidPeer(getError());
                final StoreHeartbeatTask nexTask = new StoreHeartbeatTask(nextDelay, now, forceRefresh);
                heartbeatTimer.newTimeout(nexTask, nexTask.getNextDelay(), TimeUnit.SECONDS);
            }
        };
        final Endpoint endpoint = this.pdClient.getPdLeader(forceRefreshLeader, rpcCaller.getHeartbeatRpcTimeoutMillis());
        rpcCaller.callAsyncWithRpc(endpoint, request, closure);
    }

    private void sendRegionHeartbeat(final long nextDelay, final long lastTime, final boolean forceRefreshLeader) {
        final long now = System.currentTimeMillis();
        final DTGRegionHeartbeatRequest request = new DTGRegionHeartbeatRequest();
        request.setClusterId(this.storeEngine.getClusterId());
        request.setStoreId(this.storeEngine.getStoreId());
        request.setLeastKeysOnSplit(this.storeEngine.getStoreOpts().getLeastKeysOnSplit());
        final List<Long> regionIdList = this.storeEngine.getLeaderRegionIds();
        if (regionIdList.isEmpty()) {
            // So sad, there is no even a region leader :(
            final RegionHeartbeatTask nextTask = new RegionHeartbeatTask(nextDelay, now, false);
            this.heartbeatTimer.newTimeout(nextTask, nextTask.getNextDelay(), TimeUnit.SECONDS);
            if (LOG.isInfoEnabled()) {
                LOG.info("So sad, there is no even a region leader on [clusterId:{}, storeId: {}, endpoint:{}].",
                    this.storeEngine.getClusterId(), this.storeEngine.getStoreId(), this.storeEngine.getSelfEndpoint());
            }
            return;
        }
        final List<Pair<DTGRegion, DTGRegionStats>> regionStatsList = Lists.newArrayListWithCapacity(regionIdList.size());
        final TimeInterval timeInterval = new TimeInterval(lastTime, now);
        for (final Long regionId : regionIdList) {
            final DTGRegion region = this.pdClient.getRegionByRegionId(regionId);
            final DTGRegionStats stats = this.statsCollector.collectRegionStats(region, timeInterval);
            if (stats == null) {
                continue;
            }
            regionStatsList.add(Pair.of(region, stats));
        }
        request.setRegionStatsList(regionStatsList);
        final RpcCaller.RpcClosure<List<DTGInstruction>> closure = new RpcCaller.RpcClosure<List<DTGInstruction>>() {

            @Override
            public void run(final Status status) {
                final boolean isOk = status.isOk();
                if (isOk) {
                    final List<DTGInstruction> instructions = getResult();
                    if (instructions != null && !instructions.isEmpty()) {
                        instructionProcessor.process(instructions);
                    }
                }
                final boolean forceRefresh = !isOk && ErrorsHelper.isInvalidPeer(getError());
                final RegionHeartbeatTask nextTask = new RegionHeartbeatTask(nextDelay, now, forceRefresh);
                heartbeatTimer.newTimeout(nextTask, nextTask.getNextDelay(), TimeUnit.SECONDS);
            }
        };
        final Endpoint endpoint = this.pdClient.getPdLeader(forceRefreshLeader, rpcCaller.getHeartbeatRpcTimeoutMillis());
        rpcCaller.callAsyncWithRpc(endpoint, request, closure);//System.out.println("send region heartbeat");
    }

//    private <V> void callAsyncWithRpc(final Endpoint endpoint, final BaseRequest request,
//                                      final HeartbeatClosure<V> closure) {
//        final String address = endpoint.toString();
//        final InvokeContext invokeCtx = ExtSerializerSupports.getInvokeContext();
//        final InvokeCallback invokeCallback = new InvokeCallback() {
//
//            @SuppressWarnings("unchecked")
//            @Override
//            public void onResponse(final Object result) {
//                final BaseResponse<?> response = (BaseResponse<?>) result;
//                if (response.isSuccess()) {
//                    closure.setResult((V) response.getValue());
//                    closure.run(Status.OK());
//                } else {
//                    closure.setError(response.getError());
//                    closure.run(new Status(-1, "RPC failed with address: %s, response: %s", address, response));
//                }
//            }
//
//            @Override
//            public void onException(final Throwable t) {
//                closure.run(new Status(-1, t.getMessage()));
//            }
//
//            @Override
//            public Executor getExecutor() {
//                return heartbeatRpcCallbackExecutor;
//            }
//        };
//        try {
//            this.rpcClient.invokeWithCallback(address, request, invokeCtx, invokeCallback,
//                this.heartbeatRpcTimeoutMillis);
//        } catch (final Throwable t) {
//            closure.run(new Status(-1, t.getMessage()));
//        }
//    }

//    private static abstract class HeartbeatClosure<V> extends BaseKVStoreClosure {
//
//        private volatile V result;
//
//        public V getResult() {
//            return result;
//        }
//
//        public void setResult(V result) {
//            this.result = result;
//        }
//    }

    private final class StoreHeartbeatTask implements TimerTask {

        private final long    nextDelay;
        private final long    lastTime;
        private final boolean forceRefreshLeader;

        private StoreHeartbeatTask(long nextDelay, long lastTime, boolean forceRefreshLeader) {
            this.nextDelay = nextDelay;
            this.lastTime = lastTime;
            this.forceRefreshLeader = forceRefreshLeader;
        }

        @Override
        public void run(final Timeout timeout) throws Exception {
            try {
                sendStoreHeartbeat(this.nextDelay, this.forceRefreshLeader, this.lastTime);
            } catch (final Throwable t) {
                LOG.error("Caught a error on sending [StoreHeartbeat]: {}.", StackTraceUtil.stackTrace(t));
            }
        }

        public long getNextDelay() {
            return nextDelay;
        }
    }

    private final class RegionHeartbeatTask implements TimerTask {

        private final long    nextDelay;
        private final long    lastTime;
        private final boolean forceRefreshLeader;

        private RegionHeartbeatTask(long nextDelay, long lastTime, boolean forceRefreshLeader) {
            this.nextDelay = nextDelay;
            this.lastTime = lastTime;
            this.forceRefreshLeader = forceRefreshLeader;
        }

        @Override
        public void run(final Timeout timeout) throws Exception {
            try {
                sendRegionHeartbeat(this.nextDelay, this.lastTime, this.forceRefreshLeader);
            } catch (final Throwable t) {
                LOG.error("Caught a error on sending [RegionHeartbeat]: {}.", StackTraceUtil.stackTrace(t));
            }
        }

        public long getNextDelay() {
            return nextDelay;
        }
    }
}

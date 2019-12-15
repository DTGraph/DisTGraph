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
package PlacementDriver;

/**
 * @author jinkai
 * **/


import Communication.instructions.AddRegionInfo;
import Communication.instructions.DTGInstruction;
import Communication.RequestAndResponse.*;
import Communication.pd.pipeline.event.DTGRegionPingEvent;
import Communication.pd.pipeline.event.DTGStorePingEvent;
import Communication.pd.pipeline.handler.DTGLogHandler;
import Communication.pd.pipeline.handler.DTGPlacementDriverTailHandler;
import Communication.util.pipeline.*;
import Communication.util.pipeline.future.DTGPipelineFuture;
import DBExceptions.TypeDoesnotExistException;
import PlacementDriver.IdManage.IdGenerator;
import PlacementDriver.PD.DTGMetadataStore;
import PlacementDriver.PD.DTGMetadataStoreImpl;
import Region.DTGRegion;
import Region.DTGRegionStats;
import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.rhea.*;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.cmd.pd.*;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.metadata.Instruction;
import com.alipay.sofa.jraft.rhea.metadata.Peer;
import com.alipay.sofa.jraft.rhea.options.PlacementDriverServerOptions;
import com.alipay.sofa.jraft.rhea.util.Constants;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.rhea.util.concurrent.CallerRunsPolicyWithReport;
import com.alipay.sofa.jraft.rhea.util.concurrent.NamedThreadFactory;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.JRaftServiceLoader;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.ThreadPoolUtil;
import config.DefaultOptions;
import options.DTGPlacementDriverServerOptions;
import options.IdGeneratorOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.DTGCluster;
import storage.DTGStore;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;

import static config.MainType.NODETYPE;
import static config.MainType.RELATIONTYPE;


public class DTGPlacementDriverService implements LeaderStateListener, Lifecycle<DTGPlacementDriverServerOptions> {
    private static final Logger LOG = LoggerFactory.getLogger(DTGPlacementDriverService.class);

    private volatile boolean isLeader;

    private DTGMetadataStore metadataStore;
    private DTGPipeline pipeline;
    private boolean started;
    private DTGHandlerInvoker pipelineInvoker;
    private final RheaKVStore rheaKVStore;
    private static IdGenerator nodeIdGenerator;
    private static IdGenerator relationIdGenerator;
    private int idBatchSize;
    private List<DTGInstruction> instructionList;
    private DTGStore lazyStore;
    private int lazyStoreLeaderRegionNum = 99999;


    public DTGPlacementDriverService(RheaKVStore rheaKVStore) {
        this.rheaKVStore = rheaKVStore;
        instructionList = new ArrayList<>();
    }

    @Override
    public synchronized boolean init(final DTGPlacementDriverServerOptions opts) {
        if (this.started) {
            LOG.info("[DefaultPlacementDriverService] already started.");
            return true;
        }
        Requires.requireNonNull(opts, "placementDriverServerOptions");
        this.metadataStore = new DTGMetadataStoreImpl(this.rheaKVStore);
        final ThreadPoolExecutor threadPool = createPipelineExecutor(opts);
        if (threadPool != null) {
            this.pipelineInvoker = new DTGDefaultHandlerInvoker(threadPool);
        }
        this.pipeline = new DTGDefaultPipeline(); //
        initPipeline(this.pipeline);
        LOG.info("[DefaultPlacementDriverService] start successfully, options: {}.", opts);
        nodeIdGenerator =createIdGenerator(opts.getIdGeneratorOptions(), "nodeIdGenerator");
        relationIdGenerator = createIdGenerator(opts.getIdGeneratorOptions(), "relationIdGenerator");
        this.idBatchSize = opts.getIdGeneratorOptions().getBatchSize();
        return this.started = true;
    }

    public void shutdown() {
        if (!this.started) {
            return;
        }
        try {
            if (this.pipelineInvoker != null) {
                this.pipelineInvoker.shutdown();
            }
            invalidLocalCache();
        } finally {
            this.started = false;
            LOG.info("[DefaultPlacementDriverService] shutdown successfully.");
        }
    }

    public void onLeaderStart(final long newTerm) {
        this.isLeader = true;
        invalidLocalCache();
    }

    public void onLeaderStop(final long oldTerm) {
        this.isLeader = false;
        invalidLocalCache();
    }

    public void handleStoreHeartbeatRequest(final StoreHeartbeatRequest request, final RequestProcessClosure<BaseRequest, BaseResponse> closure) {
        final StoreHeartbeatResponse response = new StoreHeartbeatResponse();
        response.setClusterId(request.getClusterId());
        if (!this.isLeader) {
            response.setError(Errors.NOT_LEADER);
            closure.sendResponse(response);
            return;
        }
        try {
            // Only save the data
            final DTGStorePingEvent storePingEvent = new DTGStorePingEvent(request, this.metadataStore);
            final DTGPipelineFuture<Object> future = this.pipeline.invoke(storePingEvent);
            future.whenComplete((ignored, throwable) -> {
                if (throwable != null) {
                    LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(throwable));
                    response.setError(Errors.forException(throwable));
                }
                closure.sendResponse(response);
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

    public void handleRegionHeartbeatRequest(final DTGRegionHeartbeatRequest request, final RequestProcessClosure<BaseRequest, BaseResponse> closure) {
        final RegionHeartbeatResponse response = new RegionHeartbeatResponse();
        final long clusterId = request.getClusterId();
        response.setClusterId(clusterId);
        if (!this.isLeader) {
            response.setError(Errors.NOT_LEADER);
            closure.sendResponse(response);
            return;
        }
        try {
            // 1. First, save the data
            // 2. Second, check if need to send a dispatch instruction
            final DTGRegionPingEvent regionPingEvent = new DTGRegionPingEvent(request, this.metadataStore);
            DTGRegionPingEvent regionPingEvent1 = regionPingEvent;
            long storeId = request.getStoreId();

            //DTGStore store = this.metadataStore.getStoreInfo(clusterId, storeId);
            List<Pair<DTGRegion, DTGRegionStats>> pairList = request.getRegionStatsList();

            if(lazyStore == null || pairList.size() < lazyStoreLeaderRegionNum){
                lazyStore = this.metadataStore.getStoreInfo(clusterId, storeId);
                lazyStoreLeaderRegionNum = pairList.size();
            }

            if(lazyStore.getId() == storeId){
                lazyStoreLeaderRegionNum = pairList.size();
            }



            for(Pair pair : pairList){
                DTGRegion region = (DTGRegion) pair.getKey();
                if(region.getId() == Constants.DEFAULT_REGION_ID){
                    DTGInstruction instruction;
                    if(instructionList.size() == 0 && this.metadataStore.getNeedUpdateDefaultRegionLeader()){
                        DTGStore[] lazyStores = this.metadataStore.findLazyWorkStores(clusterId);
                        DTGStore lazyStore = lazyStores[0];
                        final DTGInstruction.TransferLeader transferLeader = new DTGInstruction.TransferLeader();
                        //this.lazyStoreId = lazyStore.getId();
                        transferLeader.setMoveToStoreId(lazyStore.getId());
                        transferLeader.setMoveToEndpoint(lazyStore.getEndpoint());
                        instruction = new DTGInstruction();
                        instruction.setRegion(region);
                        instruction.setTransferLeader(transferLeader);
                        regionPingEvent1.addInstruction(instruction);
                        this.metadataStore.updateNeedUpdateDefaultRegionLeader(false);
                        //this.lazyStoreEndPoint = lazyStore.getEndpoint();
                        System.out.println("transferLeader!");
                        //if(lazyStore.getEndpoint() == null){System.out.println("null endpoint");}
                    }
                    while (instructionList.size() > 0 && (instruction = instructionList.get(0)) != null){
                        AddRegionInfo add = instruction.getAddRegion();
                        Peer lazyPeer = new Peer(add.getNewRegionId(), storeId, lazyStore.getEndpoint());
                        List<Peer> peers = add.getPeers();
                        int i = 0;
                        for(i = 0; i < Constants.REGION_COPY_NUMBER && lazyPeer.getEndpoint() != null ; i++){
                            Peer peer = peers.get(i);
                            if(peer.getEndpoint().toString().equals(lazyPeer.getEndpoint().toString())){//System.out.println("same peer!!!!!!!!!!!!!");
                                break;
                            }
                        }
                        if(i == Constants.REGION_COPY_NUMBER && lazyPeer.getEndpoint() != null){
                            peers.set(Constants.REGION_COPY_NUMBER - 1, lazyPeer);
                        }
                        //System.out.println("new region id = " + add.getNewRegionId() + ", peer size : " + add.getPeers().size());
                        add.setFullRegionId(region.getId());
                        instruction.setRegion(region);
                        instruction.setStoreId(storeId);
                        regionPingEvent1.addInstruction(instruction);
                        instructionList.remove(0);
                        System.out.println("add instruction, add new region id = " + add.getNewRegionId());
                        this.metadataStore.updateNeedUpdateDefaultRegionLeader(true);
                    }
                }
            }


//            DTGRegion initRegion = store.getRegions().get(0);
//            DTGInstruction instruction;
//            while (instructionList.size() > 0 && (instruction = instructionList.get(0)) != null){
//                DTGInstruction.AddRegion add = instruction.getAddRegion();
//                add.setFullRegionId(initRegion.getId());
//                instruction.setRegion(initRegion);
//                instruction.setStoreId(storeId);
//                regionPingEvent1.addInstruction(instruction);
//                instructionList.remove(0);
//                System.out.println("add instruction");
//            }

//            for(DTGInstruction instruction : instructionList){//System.out.println("add instruction , store id = " + storeId + ", instruction store id = " + instruction.getStoreId());
//                DTGInstruction.AddRegion add = instruction.getAddRegion();
//                add.setFullRegionId(initRegion.getId());
//                instruction.setRegion(initRegion);
//                instruction.setStoreId(storeId);
//                regionPingEvent1.addInstruction(instruction);
//                instructionList.remove(instruction);
//                System.out.println("add instruction");
//            }
            final DTGPipelineFuture<List<Instruction>> future = this.pipeline.invoke(regionPingEvent1);
            future.whenComplete((instructions, throwable) -> {
                if (throwable == null) {
                    response.setValue(instructions);
                } else {
                    LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(throwable));
                    response.setError(Errors.forException(throwable));
                }
                closure.sendResponse(response);
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

    public void handleGetClusterInfoRequest(final GetClusterInfoRequest request, final RequestProcessClosure<BaseRequest, BaseResponse> closure) {
        final long clusterId = request.getClusterId();System.out.println("get cluster info" + clusterId);
        final GetDTGClusterInfoResponse response = new GetDTGClusterInfoResponse();
        response.setClusterId(clusterId);
        if (!this.isLeader) {
            response.setError(Errors.NOT_LEADER);
            closure.sendResponse(response);
            return;
        }
        try {
            final DTGCluster cluster = this.metadataStore.getClusterInfo(clusterId);
            //response.setCluster(cluster);
            response.setValue(cluster);
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
        }
        closure.sendResponse(response);
    }

    public void handleGetStoreInfoRequest(final GetStoreInfoRequest request, final RequestProcessClosure<BaseRequest, BaseResponse> closure) {
        final long clusterId = request.getClusterId();
        final GetDTGStoreInfoResponse response = new GetDTGStoreInfoResponse();
        response.setClusterId(clusterId);
        LOG.info("Handling {}.", request);
        if (!this.isLeader) {
            response.setError(Errors.NOT_LEADER);
            closure.sendResponse(response);
            return;
        }
        try {
            final DTGStore store = this.metadataStore.getStoreInfo(clusterId, request.getEndpoint());
            response.setValue(store);
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
        }
        closure.sendResponse(response);
    }

    public void handleSetStoreInfoRequest(final SetDTGStoreInfoRequest request, final RequestProcessClosure<BaseRequest, BaseResponse> closure) {
        //System.out.println("getSetStoreInfoRequest!");
        final long clusterId = request.getClusterId();//System.out.println("cluster id : " + clusterId);
        final SetDTGStoreInfoResponse response = new SetDTGStoreInfoResponse();
        response.setClusterId(clusterId);
        LOG.info("Handling {}.", request);
        if (!this.isLeader) {
            response.setError(Errors.NOT_LEADER);
            closure.sendResponse(response);
            return;
        }
        try {
            final CompletableFuture<DTGStore> future = this.metadataStore.updateStoreInfo(clusterId, request.getStore());
            future.whenComplete((prevStore, throwable) -> {
                if (throwable == null) {
                    response.setValue(prevStore);
                } else {
                    LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(throwable));
                    response.setError(Errors.forException(throwable));
                }
                closure.sendResponse(response);
            });
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

    public void handleGetStoreIdRequest(final GetStoreIdRequest request, final RequestProcessClosure<BaseRequest, BaseResponse> closure) {
        final long clusterId = request.getClusterId();
        final GetStoreIdResponse response = new GetStoreIdResponse();
        response.setClusterId(clusterId);
        LOG.info("Handling {}.", request);
        if (!this.isLeader) {
            response.setError(Errors.NOT_LEADER);
            closure.sendResponse(response);
            return;
        }
        try {
            final Long storeId = this.metadataStore.getOrCreateStoreId(clusterId, request.getEndpoint());
            response.setValue(storeId);
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
        }
        closure.sendResponse(response);
    }

    public void handleCreateRegionIdRequest(final CreateRegionIdRequest request, final RequestProcessClosure<BaseRequest, BaseResponse> closure) {
        final long clusterId = request.getClusterId();
        final CreateRegionIdResponse response = new CreateRegionIdResponse();
        response.setClusterId(clusterId);
        LOG.info("Handling {}.", request);
        if (!this.isLeader) {
            response.setError(Errors.NOT_LEADER);
            closure.sendResponse(response);
            return;
        }
        try {
            final Long newRegionId = this.metadataStore.createRegionId(clusterId);
            response.setValue(newRegionId);
        } catch (final Throwable t) {
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
        }
        closure.sendResponse(response);
    }

    public void handleCreateRegionRequest(final CreateRegionRequest request, final RequestProcessClosure<BaseRequest, BaseResponse> closure) {
        System.out.println("add region request...");
        final CreateRegionResponse response = new CreateRegionResponse();
        final long clusterId = request.getClusterId();
        response.setClusterId(request.getClusterId());
        final byte type = request.getIdType();
        final long maxId = request.getMaxIdNeed();
        LOG.info("Handling {}.", request);
        if (!this.isLeader) {
            response.setError(Errors.NOT_LEADER);
            closure.sendResponse(response);
            return;
        }
        final CompletableFuture<Long> future = CompletableFuture.supplyAsync(()->{
            long nowMaxId = 0;
            do{
                DTGStore[] lazyStores =  this.metadataStore.findLazyWorkStores(clusterId);

                List<DTGRegion> regions = lazyStores[0].getRegions();


                long newRgionId = metadataStore.createRegionId(clusterId);
                List<Peer> peers = new ArrayList<>();
                for(DTGStore store : lazyStores){
                    Peer peer = new Peer(newRgionId, store.getId(), store.getEndpoint());
                    peers.add(peer);//System.out.println("add peer :" + peer.toString());
                }

                DTGInstruction instruction = new DTGInstruction();
                AddRegionInfo add = new AddRegionInfo();
                //add.setFullRegionId(region.getId());
                add.setNewRegionId(newRgionId);
                add.setStartNodeId(metadataStore.updateRegionNodeStartId(clusterId));
                add.setStartRelationId(metadataStore.updateRegionRelationStartId(clusterId));
                add.setStartTempProId(DefaultOptions.DEFAULTSTARTTIME);
                add.setPeers(peers);
                //System.out.println("store id = " + lazyStores[0].getId() );
                instruction.setAddRegion(add);
                instructionList.add(instruction);
                if(type == NODETYPE){
                    nowMaxId = metadataStore.getNewRegionNodeStartId(clusterId) - DefaultOptions.DEFAULTREGIONRELATIONSIZE;
                }else if(type == RELATIONTYPE){
                    nowMaxId = metadataStore.updateRegionRelationStartId(clusterId) - DefaultOptions.DEFAULTREGIONNODESIZE;
                }
            }while (nowMaxId < maxId);
            long newRegionId = this.metadataStore.createRegionId(clusterId);
            return newRegionId;
        });
        future.whenComplete((v, e) -> {
            if(e != null){
                LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(e));
                response.setError(Errors.forException(e));
                return;
            }
            response.setValue(v);
        });
        closure.sendResponse(response);
    }

    public void handleGetIdsRequest(final GetIdsRequest request, final RequestProcessClosure<BaseRequest, BaseResponse> closure){
        System.out.println("get id request....");
        final GetIdsResponse response = new GetIdsResponse();
        try {
            if (!this.isLeader) {
                response.setError(Errors.NOT_LEADER);
                closure.sendResponse(response);
                return;
            }
            IdGenerator idg = getIdGenerator(request.getIdType());
            CompletableFuture<List<Long>> future = CompletableFuture.supplyAsync(() -> {
                List<Long> returnId = new LinkedList();
                for(int i = 0; i < idBatchSize; i++){
                    returnId.add(idg.nextId());
                }
                return returnId;
            }).whenComplete((returnId, e) ->{
                if(e != null){
                    LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(e));
                    response.setError(Errors.forException(e));
                    closure.sendResponse(response);
                    return;
                }
                response.setValue(returnId);
                closure.sendResponse(response);
            });
        }catch (final Throwable t){
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
    }

    public void handleReturnIdsRequest(final ReturnIdsRequest request, final RequestProcessClosure<BaseRequest, BaseResponse> closure){
        System.out.println("get return id request....");
        List<Long> ids = request.getIdList();
        final ReturnIdsResponse response = new ReturnIdsResponse();
        try {
            if (!this.isLeader) {
                response.setError(Errors.NOT_LEADER);
                closure.sendResponse(response);
                return;
            }
            IdGenerator idg = getIdGenerator(request.getIdType());
            final CompletableFuture future = idg.returnIds(ids).whenComplete((ignored, returnThrow) -> {
                if(returnThrow != null){
                    LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace((Throwable) returnThrow));
                    response.setError(Errors.forException((Throwable) returnThrow));
                    closure.sendResponse(response);
                    return;
                }
                response.setValue(true);
                closure.sendResponse(response);
            });
        }catch (final Throwable t){
            LOG.error("Failed to handle: {}, {}.", request, StackTraceUtil.stackTrace(t));
            response.setError(Errors.forException(t));
            closure.sendResponse(response);
        }
        System.out.println("finish return id");
    }

    private void invalidLocalCache() {
        if (this.metadataStore != null) {
            this.metadataStore.invalidCache();
        }
        ClusterStatsManager.invalidCache();
    }

    protected void initPipeline(final DTGPipeline pipeline) {
        final List<DTGHandler> sortedHandlers = JRaftServiceLoader.load(DTGHandler.class)
                .sort();
        // default handlers and order:
        //
        // 1. logHandler
        // 2. storeStatsValidator
        // 3. regionStatsValidator
        // 4. storeStatsPersistence
        // 5. regionStatsPersistence
        // 6. regionLeaderBalance
        // 7. splittingJudgeByApproximateKeys
        // 8: placementDriverTail
        for (final DTGHandler h : sortedHandlers) {
            pipeline.addLast(h);
        }

        // first handler
        pipeline.addFirst(this.pipelineInvoker, "logHandler", new DTGLogHandler());
        // last handler
        pipeline.addLast("placementDriverTail", new DTGPlacementDriverTailHandler());
    }

    private ThreadPoolExecutor createPipelineExecutor(final PlacementDriverServerOptions opts) {
        final int corePoolSize = opts.getPipelineCorePoolSize();
        final int maximumPoolSize = opts.getPipelineMaximumPoolSize();
        if (corePoolSize <= 0 || maximumPoolSize <= 0) {
            return null;
        }
        final String name = "rheakv-pipeline-executor";
        return ThreadPoolUtil.newBuilder() //
                .poolName(name) //
                .enableMetric(false) //
                .coreThreads(corePoolSize) //
                .maximumThreads(maximumPoolSize) //
                .keepAliveSeconds(120L) //
                .workQueue(new ArrayBlockingQueue<>(1024)) //
                .threadFactory(new NamedThreadFactory(name, true)) //
                .rejectedHandler(new CallerRunsPolicyWithReport(name)) //
                .build();
    }

    private IdGenerator createIdGenerator(IdGeneratorOptions opts, String type){
        File file = new File(opts.getIdGeneratorPath() + "\\" + type);
        if(!file.exists()){
            IdGenerator.createGenerator(file, 0, false);
        }
        IdGenerator IdG = new IdGenerator(file, opts.getGrabSize(), Long.MAX_VALUE, 0);
        return IdG;
    }

    private IdGenerator getIdGenerator(byte type) throws TypeDoesnotExistException {
        switch (type){
            case NODETYPE: return nodeIdGenerator;
            case RELATIONTYPE: return relationIdGenerator;
            default:
                throw new TypeDoesnotExistException(type, "idGenerator");
        }
    }

    private DTGStore getNotNullStore(long clusterId){
        DTGCluster cluster = this.metadataStore.getClusterInfo(clusterId);

        if(cluster == null){
            return null;
        }

        List<DTGStore> stores = cluster.getStores();


        if(stores == null && stores.size() == 0){
            return null;
        }

        for(DTGStore store : stores){
            System.out.println("store : " + store.getId() + ", region size = " + store.getRegions().size());
        }

        for(DTGStore store : stores){
            if(store.getRegions().size() > 0){
                return store;
            }
        }
        return null;
    }

}

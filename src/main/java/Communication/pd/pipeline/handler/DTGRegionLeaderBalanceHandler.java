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
package Communication.pd.pipeline.handler;

import Communication.DTGInstruction;
import Communication.RequestAndResponse.DTGRegionHeartbeatRequest;
import Communication.pd.DTGClusterStatsManager;
import Communication.pd.pipeline.event.DTGRegionPingEvent;
import Communication.util.pipeline.DTGHandler;
import Communication.util.pipeline.DTGHandlerContext;
import Communication.util.pipeline.DTGInboundHandlerAdapter;
import PlacementDriver.PD.DTGMetadataStore;
import com.alipay.sofa.jraft.rhea.metadata.*;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.SPI;
import config.DefaultOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import Region.DTGRegion;
import Region.DTGRegionStats;
import storage.DTGCluster;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Trying to balance the number of leaders in each store.
 *
 * @author jiachun.fjc
 */
@SPI(name = "DTGRegionLeaderBalance", priority = 60)
@DTGHandler.Sharable
public class DTGRegionLeaderBalanceHandler extends DTGInboundHandlerAdapter<DTGRegionPingEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(DTGRegionLeaderBalanceHandler.class);

    @Override
    public void readMessage(final DTGHandlerContext ctx, final DTGRegionPingEvent event) throws Exception {
        if (event.isReady()) {
            return;
        }
        final DTGMetadataStore metadataStore = (DTGMetadataStore) event.getMetadataStore();
        final DTGRegionHeartbeatRequest request = event.getMessage();
        final long clusterId = request.getClusterId();
        final long storeId = request.getStoreId();
        final DTGClusterStatsManager clusterStatsManager = DTGClusterStatsManager.getInstance(clusterId);
        final List<Pair<DTGRegion, DTGRegionStats>> regionStatsList = request.getRegionStatsList();
        for (final Pair<DTGRegion, DTGRegionStats> stats : regionStatsList) {
            final DTGRegion region = stats.getKey();
            clusterStatsManager.addOrUpdateLeader(storeId, region.getId());
        }

        long maxNodeId = metadataStore.getNewRegionNodeStartId(clusterId);
        long maxRelationId = metadataStore.getNewRegionRelationStartId(clusterId);
        // check if the modelWorker
//        final Pair<Set<Long>, Integer> modelWorkers = clusterStatsManager.findModelWorkerStores(1);
//        final Set<Long> modelWorkerStoreIds = modelWorkers.getKey();
//        final int modelWorkerLeaders = modelWorkers.getValue();
//        if (!modelWorkerStoreIds.contains(storeId)) {
//            return;
//        }
//
//        LOG.info("[Cluster: {}] model worker stores is: {}, it has {} leaders.", clusterId, modelWorkerStoreIds, modelWorkerLeaders);

        //System.out.println("need send instruction!!!!!!!!!!!!!!!!!!!!!!!!!");

        for(final Pair<DTGRegion, DTGRegionStats> pair : regionStatsList){
            final DTGRegion region = pair.getKey();
            final List<Peer> peers = region.getPeers();
            if (peers == null) {
                continue;
            }
            final List<Endpoint> endpoints = Lists.transform(peers, Peer::getEndpoint);
            final Map<Long, Endpoint> storeIds = metadataStore.unsafeGetStoreIdsByEndpoints(clusterId, endpoints);
            if(region.getNodecount() == 0 && region.getRelationcount() == 0)continue;
            if(region.getMaxNodeId() >= maxNodeId - DefaultOptions.DEFAULTREGIONNODESIZE ||region.getMaxRelationId() >= maxRelationId - DefaultOptions.DEFAULTREGIONRELATIONSIZE){
                System.out.println("region " + region.getId() + " is full! add new region!");
                final DTGInstruction instruction = new DTGInstruction();
                DTGInstruction.AddRegion add = new DTGInstruction.AddRegion();
                add.setFullRegionId(region.getId());
                add.setNewRegionId(metadataStore.createRegionId(clusterId));
                add.setStartNodeId(metadataStore.updateRegionNodeStartId(clusterId));
                add.setStartRelationId(metadataStore.updateRegionRelationStartId(clusterId));
                add.setStartTempProId(DefaultOptions.DEFAULTSTARTTIME);
                instruction.setAddRegion(add);
                instruction.setRegion(region);
                event.addInstruction(instruction);
                //System.out.println("add instruction success");
            }
        }

//        for (final Pair<DTGRegion, RegionStats> pair : regionStatsList) {
//            final DTGRegion region = pair.getKey();
//            final List<Peer> peers = region.getPeers();
//            if (peers == null) {
//                continue;
//            }
//            final List<Endpoint> endpoints = Lists.transform(peers, Peer::getEndpoint);
//            final Map<Long, Endpoint> storeIds = metadataStore.unsafeGetStoreIdsByEndpoints(clusterId, endpoints);
//            // find lazyWorkers
//            final List<Pair<Long, Integer>> lazyWorkers = clusterStatsManager.findLazyWorkerStores(storeIds.keySet());
//            if (lazyWorkers.isEmpty()) {
//                return;
//            }
//            for (int i = lazyWorkers.size() - 1; i >= 0; i--) {
//                final Pair<Long, Integer> worker = lazyWorkers.get(i);
//                if (modelWorkerLeaders - worker.getValue() <= 1) { // no need to transfer
//                    lazyWorkers.remove(i);
//                }
//            }
//            if (lazyWorkers.isEmpty()) {
//                continue;
//            }
//            final Pair<Long, Integer> laziestWorker = tryToFindLaziestWorker(clusterId, metadataStore, lazyWorkers);
//            if (laziestWorker == null) {
//                continue;
//            }
//            final Long lazyWorkerStoreId = laziestWorker.getKey();
//            LOG.info("[Cluster: {}], lazy worker store is: {}, it has {} leaders.", clusterId, lazyWorkerStoreId,
//                    laziestWorker.getValue());
//            final Instruction.TransferLeader transferLeader = new Instruction.TransferLeader();
//            transferLeader.setMoveToStoreId(lazyWorkerStoreId);
//            transferLeader.setMoveToEndpoint(storeIds.get(lazyWorkerStoreId));
//            final Instruction instruction = new Instruction();
//            instruction.setRegion(region.copy());
//            instruction.setTransferLeader(transferLeader);
//            event.addInstruction(instruction);
//            LOG.info("[Cluster: {}], send 'instruction.transferLeader': {} to region: {}.", clusterId, instruction, region);
//            break; // Only do one thing at a time
//        }
    }

    private Pair<Long, Integer> tryToFindLaziestWorker(final long clusterId, final DTGMetadataStore metadataStore,
                                                       final List<Pair<Long, Integer>> lazyWorkers) {
        final List<Pair<Pair<Long, Integer>, StoreStats>> storeStatsList = Lists.newArrayList();
        for (final Pair<Long, Integer> worker : lazyWorkers) {
            final StoreStats stats = metadataStore.getStoreStats(clusterId, worker.getKey());
            if (stats != null) {
                // TODO check timeInterval
                storeStatsList.add(Pair.of(worker, stats));
            }
        }
        if (storeStatsList.isEmpty()) {
            return null;
        }
        if (storeStatsList.size() == 1) {
            return storeStatsList.get(0).getKey();
        }
        final Pair<Pair<Long, Integer>, StoreStats> min = Collections.min(storeStatsList, (o1, o2) -> {
            final StoreStats s1 = o1.getValue();
            final StoreStats s2 = o2.getValue();
            int val = Boolean.compare(s1.isBusy(), s2.isBusy());
            if (val != 0) {
                return val;
            }
            val = Integer.compare(s1.getRegionCount(), s2.getRegionCount());
            if (val != 0) {
                return val;
            }
            val = Long.compare(s1.getBytesWritten(), s2.getBytesWritten());
            if (val != 0) {
                return val;
            }
            val = Long.compare(s1.getBytesRead(), s2.getBytesRead());
            if (val != 0) {
                return val;
            }
            val = Long.compare(s1.getKeysWritten(), s2.getKeysWritten());
            if (val != 0) {
                return val;
            }
            val = Long.compare(s1.getKeysRead(), s2.getKeysRead());
            if (val != 0) {
                return val;
            }
            return Long.compare(-s1.getAvailable(), -s2.getAvailable());
        });
        return min.getKey();
    }
}

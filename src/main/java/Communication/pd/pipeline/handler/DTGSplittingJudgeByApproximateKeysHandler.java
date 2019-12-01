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

import Communication.RequestAndResponse.DTGRegionHeartbeatRequest;
import Communication.pd.DTGClusterStatsManager;
import Communication.pd.pipeline.event.DTGRegionPingEvent;
import Communication.util.pipeline.DTGHandler;
import Communication.util.pipeline.DTGHandlerContext;
import Communication.util.pipeline.DTGInboundHandlerAdapter;
import PlacementDriver.PD.DTGMetadataStore;
import Region.DTGRegion;
import Region.DTGRegionStats;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.util.SPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

/**
 * Range split judge, the reference indicator for splitting is the
 * region's approximate keys.
 *
 * @author jiachun.fjc
 */
@SPI(name = "DTGSplittingJudgeByApproximateKeys", priority = 50)
@DTGHandler.Sharable
public class DTGSplittingJudgeByApproximateKeysHandler extends DTGInboundHandlerAdapter<DTGRegionPingEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(DTGSplittingJudgeByApproximateKeysHandler.class);

    @Override
    public void readMessage(final DTGHandlerContext ctx, final DTGRegionPingEvent event) throws Exception {
        if (event.isReady()) {
            return;
        }
        final DTGMetadataStore metadataStore = event.getMetadataStore();
        final DTGRegionHeartbeatRequest request = event.getMessage();
        final long clusterId = request.getClusterId();
        final DTGClusterStatsManager clusterStatsManager = DTGClusterStatsManager.getInstance(clusterId);
        clusterStatsManager.addOrUpdateRegionStats(request.getRegionStatsList());
        final Set<Long> stores = metadataStore.unsafeGetStoreIds(clusterId);
        if (stores == null || stores.isEmpty()) {
            return;
        }
        if (clusterStatsManager.regionSize() >= stores.size()) {
            // one store one region is perfect
            return;
        }
        final Pair<DTGRegion, DTGRegionStats> modelWorker = clusterStatsManager.findModelWorkerRegion();
        if (!isSplitNeeded(request, modelWorker)) {
            return;
        }

        LOG.info("[Cluster: {}] model worker region is: {}.", clusterId, modelWorker);

        System.out.println("need instruction!!!!!!!!!!!!");

//        final Long newRegionId = metadataStore.createRegionId(clusterId);
//        final Instruction.RangeSplit rangeSplit = new Instruction.RangeSplit();
//        rangeSplit.setNewRegionId(newRegionId);
//        final Instruction instruction = new Instruction();
//        instruction.setRegion(modelWorker.getKey().copy());
//        instruction.setRangeSplit(rangeSplit);
//        event.addInstruction(instruction);
    }

    private boolean isSplitNeeded(final DTGRegionHeartbeatRequest request, final Pair<DTGRegion, DTGRegionStats> modelWorker) {
        if (modelWorker == null) {
            return false;
        }
        final long modelApproximateKeys = modelWorker.getValue().getApproximateKeys();
        if (request.getLeastKeysOnSplit() > modelApproximateKeys) {
            return false;
        }
        final DTGRegion modelRegion = modelWorker.getKey();
        final List<Pair<DTGRegion, DTGRegionStats>> regionStatsList = request.getRegionStatsList();
        for (final Pair<DTGRegion, DTGRegionStats> p : regionStatsList) {
            if (modelRegion.equals(p.getKey())) {
                return true;
            }
        }
        return false;
    }
}

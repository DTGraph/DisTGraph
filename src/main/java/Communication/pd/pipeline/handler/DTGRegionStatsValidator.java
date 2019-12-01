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
import Communication.pd.pipeline.event.DTGRegionPingEvent;
import Communication.util.pipeline.DTGHandler;
import Communication.util.pipeline.DTGHandlerContext;
import Communication.util.pipeline.DTGInboundHandlerAdapter;
import PlacementDriver.PD.DTGMetadataStore;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;
import com.alipay.sofa.jraft.rhea.metadata.TimeInterval;
import com.alipay.sofa.jraft.rhea.util.Pair;
import com.alipay.sofa.jraft.util.SPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import Region.DTGRegion;
import Region.DTGRegionStats;

import java.util.List;

/**
 *
 * @author jiachun.fjc
 */
@SPI(name = "DTGRegionStatsValidator", priority = 90)
@DTGHandler.Sharable
public class DTGRegionStatsValidator extends DTGInboundHandlerAdapter<DTGRegionPingEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(DTGRegionStatsValidator.class);

    @Override
    public void readMessage(final DTGHandlerContext ctx, final DTGRegionPingEvent event) throws Exception {
        final DTGMetadataStore metadataStore = event.getMetadataStore();
        final DTGRegionHeartbeatRequest request = event.getMessage();
        final List<Pair<DTGRegion, DTGRegionStats>> regionStatsList = request.getRegionStatsList();
        if (regionStatsList == null || regionStatsList.isEmpty()) {
            LOG.error("Empty [RegionStatsList] by event: {}.", event);
            throw Errors.INVALID_REGION_STATS.exception();
        }
        for (final Pair<DTGRegion, DTGRegionStats> pair : regionStatsList) {
            final DTGRegion region = pair.getKey();
            if (region == null) {
                LOG.error("Empty [Region] by event: {}.", event);
                throw Errors.INVALID_REGION_STATS.exception();
            }
            final RegionEpoch regionEpoch = region.getRegionEpoch();
            if (regionEpoch == null) {
                LOG.error("Empty [RegionEpoch] by event: {}.", event);
                throw Errors.INVALID_REGION_STATS.exception();
            }
            final DTGRegionStats regionStats = pair.getValue();
            if (regionStats == null) {
                LOG.error("Empty [RegionStats] by event: {}.", event);
                throw Errors.INVALID_REGION_STATS.exception();
            }
            final Pair<DTGRegion, DTGRegionStats> currentRegionInfo = metadataStore.getRegionStats(request.getClusterId(),
                region);
            if (currentRegionInfo == null) {
                return; // new data
            }
            final DTGRegion currentRegion = currentRegionInfo.getKey();
            if (regionEpoch.compareTo(currentRegion.getRegionEpoch()) < 0) {
                LOG.error("The region epoch is out of date: {}.", event);
                throw Errors.REGION_HEARTBEAT_OUT_OF_DATE.exception();
            }
            final TimeInterval interval = regionStats.getInterval();
            if (interval == null) {
                LOG.error("Empty [TimeInterval] by event: {}.", event);
                throw Errors.INVALID_REGION_STATS.exception();
            }
            final TimeInterval currentInterval = currentRegionInfo.getValue().getInterval();
            if (interval.getEndTimestamp() < currentInterval.getEndTimestamp()) {
                LOG.error("The [TimeInterval] is out of date: {}.", event);
                throw Errors.REGION_HEARTBEAT_OUT_OF_DATE.exception();
            }

        }
    }
}

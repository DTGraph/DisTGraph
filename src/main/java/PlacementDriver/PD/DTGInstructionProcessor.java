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
package PlacementDriver.PD;

import Communication.instructions.AddRegionInfo;
import Communication.instructions.DTGInstruction;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.util.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.BaseStoreClosure;
import storage.DTGStoreEngine;
import Region.DTGRegionEngine;
import Region.DTGRegion;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Processing the instructions from the placement driver server.
 *
 * @author jiachun.fjc
 */
public class DTGInstructionProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(DTGInstructionProcessor.class);

    private final DTGStoreEngine storeEngine;

    public DTGInstructionProcessor(DTGStoreEngine storeEngine) {
        this.storeEngine = storeEngine;
    }

    public void process(final List<DTGInstruction> instructions) {
        LOG.info("Received instructions: {}.", instructions);
        for (final DTGInstruction instruction : instructions) {
            if (!checkInstruction(instruction)) {
                continue;
            }
            //processSplit(instruction);
            processAddRegion(instruction);
            processTransferLeader(instruction);
        }
    }

//    private boolean processSplit(final Instruction instruction) {
//        try {
//            final Instruction.RangeSplit rangeSplit = instruction.getRangeSplit();
//            if (rangeSplit == null) {
//                return false;
//            }
//            final Long newRegionId = rangeSplit.getNewRegionId();
//            if (newRegionId == null) {
//                LOG.error("RangeSplit#newRegionId must not be null, {}.", instruction);
//                return false;
//            }
//            final Region region = instruction.getRegion();
//            final long regionId = region.getId();
//            final DTGRegionEngine engine = this.storeEngine.getRegionEngine(regionId);
//            if (engine == null) {
//                LOG.error("Could not found regionEngine, {}.", instruction);
//                return false;
//            }
//            if (!region.equals(engine.getRegion())) {
//                LOG.warn("Instruction [{}] is out of date.", instruction);
//                return false;
//            }
//            final CompletableFuture<Status> future = new CompletableFuture<>();
//            this.storeEngine.applySplit(regionId, newRegionId, new BaseKVStoreClosure() {
//
//                @Override
//                public void run(Status status) {
//                    future.complete(status);
//                }
//            });
//            final Status status = future.get(20, TimeUnit.SECONDS);
//            final boolean ret = status.isOk();
//            if (ret) {
//                LOG.info("Range-split succeeded, instruction: {}.", instruction);
//            } else {
//                LOG.warn("Range-split failed: {}, instruction: {}.", status, instruction);
//            }
//            return ret;
//        } catch (final Throwable t) {
//            LOG.error("Caught an exception on #processSplit: {}.", StackTraceUtil.stackTrace(t));
//            return false;
//        }
//    }

    private boolean processAddRegion(final DTGInstruction instruction){
        try{System.out.println("prepare add region...");
            final AddRegionInfo newRegionInfo = instruction.getAddRegion();
            if(newRegionInfo == null)return false;
            final Long newRegionId = newRegionInfo.getNewRegionId();
            if (newRegionId == null) {
                LOG.error("RangeSplit#newRegionId must not be null, {}.", instruction);
                return false;
            }
            final DTGRegionEngine engine = this.storeEngine.getRegionEngine(newRegionInfo.getFullRegionId());
            if (engine == null) {
                LOG.error("Could not found regionEngine, {}.", instruction);
                return false;
            }
            final CompletableFuture<Status> future = new CompletableFuture<>();
//            this.storeEngine.addRegion(newRegionInfo.getFullRegionId(), newRegionInfo.getNewRegionId(), newRegionInfo.getPeers(),
//                    newRegionInfo.getStartNodeId(), newRegionInfo.getStartRelationId(), newRegionInfo.getStartTempProId(), new BaseStoreClosure() {
            this.storeEngine.addRegion(newRegionInfo, new BaseStoreClosure() {
                        @Override
                        public void run(Status status) {
                            future.complete(status);
                        }
                    });
            final Status status = future.get(20, TimeUnit.SECONDS);
            final boolean ret = status.isOk();
            if (ret) {
                LOG.info("ADD REGION succeeded, instruction: {}.", instruction);
            } else {
                LOG.warn("ADD REGION failed: {}, instruction: {}.", status, instruction);
            }System.out.println("success add region");
            return ret;
        }catch (final Throwable t) {
            LOG.error("Caught an exception on #processADDREGION: {}.", StackTraceUtil.stackTrace(t));
            return false;
        }
    }

    private boolean processTransferLeader(final DTGInstruction instruction) {
        try {
            final DTGInstruction.TransferLeader transferLeader = instruction.getTransferLeader();
            if (transferLeader == null) {
                return false;
            }
            final Endpoint toEndpoint = transferLeader.getMoveToEndpoint();
            if (toEndpoint == null) {
                LOG.error("TransferLeader#toEndpoint must not be null, {}.", instruction);
                return false;
            }
            final DTGRegion region = instruction.getRegion();
            final long regionId = region.getId();
            final DTGRegionEngine engine = this.storeEngine.getRegionEngine(regionId);
            if (engine == null) {
                LOG.error("Could not found regionEngine, {}.", instruction);
                return false;
            }
            if (!region.equals(engine.getRegion())) {
                LOG.warn("Instruction [{}] is out of date.", instruction);
                return false;
            }
            return engine.transferLeadershipTo(toEndpoint);
        } catch (final Throwable t) {
            LOG.error("Caught an exception on #processTransferLeader: {}.", StackTraceUtil.stackTrace(t));
            return false;
        }
    }

    private boolean checkInstruction(final DTGInstruction instruction) {
        if (instruction == null) {
            LOG.warn("Null instructions element.");
            return false;
        }
        if (instruction.getRegion() == null) {
            LOG.warn("Null region with instruction: {}.", instruction);
            return false;
        }
        return true;
    }
}

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
package Region;

import Communication.DTGRpcService;
import Communication.RpcCaller;
import LocalDBMachine.DTGStateMachine;
import UserClient.DTGSaveStore;
import com.alipay.remoting.rpc.RpcServer;
import com.alipay.sofa.jraft.*;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rhea.JRaftHelper;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.options.RegionEngineOptions;
import com.alipay.sofa.jraft.rhea.storage.MetricsRawKVStore;
import com.alipay.sofa.jraft.rhea.storage.RaftRawKVStore;
import com.alipay.sofa.jraft.rhea.storage.RawKVStore;
import com.alipay.sofa.jraft.rhea.util.Strings;
import com.alipay.sofa.jraft.rhea.util.ThrowUtil;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Requires;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Slf4jReporter;
import options.DTGMetricsRawStoreOptions;
import options.DTGRegionEngineOptions;
import org.apache.commons.io.FileUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.DTGMetricsRawStore;
import raft.DTGRaftRawStore;
import raft.DTGRawStore;
import storage.DTGStoreEngine;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Minimum execution/copy unit of RheaKVStore.
 *
 * @author jiachun.fjc
 */
public class  DTGRegionEngine implements Lifecycle<DTGRegionEngineOptions> {

    private static final Logger LOG = LoggerFactory.getLogger(DTGRegionEngine.class);

    private final DTGRegion        region;
    private final DTGStoreEngine   storeEngine;

    //private RaftRawKVStore       raftRawKVStore;
    private DTGRaftRawStore        raftRawStore;
    //private MetricsRawKVStore    metricsRawKVStore;
    private DTGMetricsRawStore     metricsRawStore;
    private RaftGroupService       raftGroupService;
    private Node                   node;
    private DTGStateMachine        fsm;
    private DTGRegionEngineOptions regionOpts;
    private RpcCaller              rpcCaller;

    //private GraphDatabaseService db;

    private ScheduledReporter      regionMetricsReporter;

    private boolean                started;

    public DTGRegionEngine(DTGRegion region, DTGStoreEngine storeEngine) {
        this.region = region;
        this.storeEngine = storeEngine;
        this.rpcCaller = storeEngine.getRpcCaller();
    }

    @Override
    public synchronized boolean init(final DTGRegionEngineOptions opts) {
        if (this.started) {
            LOG.info("[RegionEngine: {}] already started.", this.region);
            return true;
        }
        this.regionOpts = Requires.requireNonNull(opts, "opts");
        this.fsm = new DTGStateMachine(this.region, this.storeEngine);

//        db = new GraphDatabaseFactory()
//                .newEmbeddedDatabaseBuilder(opts.getDbPath())
//                .loadPropertiesFromFile("")
//                .newGraphDatabase();

        // node options
        NodeOptions nodeOpts = opts.getNodeOptions();
        if (nodeOpts == null) {
            nodeOpts = new NodeOptions();
        }
        final long metricsReportPeriod = opts.getMetricsReportPeriod();
        if (metricsReportPeriod > 0) {
            // metricsReportPeriod > 0 means enable metrics
            nodeOpts.setEnableMetrics(true);
        }
        nodeOpts.setInitialConf(new Configuration(JRaftHelper.toJRaftPeerIdList(this.region.getPeers())));
        nodeOpts.setFsm(this.fsm);
        final String raftDataPath = opts.getRaftDataPath();
        try {
            FileUtils.forceMkdir(new File(raftDataPath));
        } catch (final Throwable t) {
            LOG.error("Fail to make dir for raftDataPath {}.", raftDataPath);
            return false;
        }
        if (Strings.isBlank(nodeOpts.getLogUri())) {
            final Path logUri = Paths.get(raftDataPath, "log");
            nodeOpts.setLogUri(logUri.toString());
        }
        if (Strings.isBlank(nodeOpts.getRaftMetaUri())) {
            final Path meteUri = Paths.get(raftDataPath, "meta");
            nodeOpts.setRaftMetaUri(meteUri.toString());
        }
        if (Strings.isBlank(nodeOpts.getSnapshotUri())) {
            final Path snapshotUri = Paths.get(raftDataPath, "snapshot");
            nodeOpts.setSnapshotUri(snapshotUri.toString());
        }
        LOG.info("[RegionEngine: {}], log uri: {}, raft meta uri: {}, snapshot uri: {}.", this.region,
            nodeOpts.getLogUri(), nodeOpts.getRaftMetaUri(), nodeOpts.getSnapshotUri());
        final Endpoint serverAddress = opts.getServerAddress();
        final PeerId serverId = new PeerId(serverAddress, 0);
        final RpcServer rpcServer = this.storeEngine.getRpcServer();
        this.raftGroupService = new RaftGroupService(opts.getRaftGroupId(), serverId, nodeOpts, rpcServer, true);
        this.node = this.raftGroupService.start(false);
        RouteTable.getInstance().updateConfiguration(this.raftGroupService.getGroupId(), nodeOpts.getInitialConf());
        if (this.node != null) {
            final DTGRawStore rawStore = this.storeEngine.getlocalDB();
            //final Executor readIndexExecutor = this.storeEngine.getReadIndexExecutor();
            this.raftRawStore = new DTGRaftRawStore(this.node, rawStore);
            //this.raftRawKVStore = new RaftRawKVStore(this.node, rawKVStore, readIndexExecutor);
            this.metricsRawStore = new DTGMetricsRawStore(this.region.getId(), this.raftRawStore);
            if(!initDTGMetricsRawStore(opts)){
                return false;
            }
            // metrics config
            if (this.regionMetricsReporter == null && metricsReportPeriod > 0) {
                final MetricRegistry metricRegistry = this.node.getNodeMetrics().getMetricRegistry();
                if (metricRegistry != null) {
                    final ScheduledExecutorService scheduler = this.storeEngine.getMetricsScheduler();
                    // start raft node metrics reporter
                    this.regionMetricsReporter = Slf4jReporter.forRegistry(metricRegistry) //
                        .prefixedWith("region_" + this.region.getId()) //
                        .withLoggingLevel(Slf4jReporter.LoggingLevel.INFO) //
                        .outputTo(LOG) //
                        .scheduleOn(scheduler) //
                        .shutdownExecutorOnStop(scheduler != null) //
                        .build();
                    this.regionMetricsReporter.start(metricsReportPeriod, TimeUnit.SECONDS);
                }
            }
            this.started = true;
            LOG.info("[RegionEngine] start successfully: {}.", this);
        }
        return this.started;
    }

    public void startOther(DTGSaveStore store){
        this.metricsRawStore.startOther(store);
    }

    private boolean initDTGMetricsRawStore(DTGRegionEngineOptions opts){
        DTGMetricsRawStoreOptions mrsOptions = new DTGMetricsRawStoreOptions();
        mrsOptions.setUrl(opts.getDbPath()+ region.getId());
        mrsOptions.setSaveStore(opts.getSaveStore());
        if(!this.metricsRawStore.init(mrsOptions)){
            return false;
        }
        return true;
    }

    @Override
    public synchronized void shutdown() {
        if (!this.started) {
            return;
        }
        if (this.raftGroupService != null) {
            this.raftGroupService.shutdown();
            try {
                this.raftGroupService.join();
            } catch (final InterruptedException e) {
                ThrowUtil.throwException(e);
            }
        }
        if (this.regionMetricsReporter != null) {
            this.regionMetricsReporter.stop();
        }
        this.started = false;
        LOG.info("[RegionEngine] shutdown successfully: {}.", this);
    }

    public boolean transferLeadershipTo(final Endpoint endpoint) {
        final PeerId peerId = new PeerId(endpoint, 0);
        final Status status = this.node.transferLeadershipTo(peerId);System.out.println("transfer leader to :" + endpoint.toString());
        final boolean isOk = status.isOk();
        if (isOk) {
            LOG.info("Transfer-leadership succeeded: [{} --> {}].", this.storeEngine.getSelfEndpoint(), endpoint);
        } else {
            LOG.error("Transfer-leadership failed: {}, [{} --> {}].", status, this.storeEngine.getSelfEndpoint(),
                endpoint);
        }
        return isOk;
    }

//    public GraphDatabaseService getDb() {
//        return db;
//    }

    public DTGRegion getRegion() {
        return region;
    }

    public DTGStoreEngine getStoreEngine() {
        return storeEngine;
    }

    public boolean isLeader() {
        return this.node.isLeader();
    }

    public PeerId getLeaderId() {
        return this.node.getLeaderId();
    }

//    public RaftRawKVStore getRaftRawKVStore() {
//        return raftRawKVStore;
//    }


    public DTGRaftRawStore getRaftRawStore() {
        return raftRawStore;
    }

    public DTGMetricsRawStore getMetricsRawStore() {
        return metricsRawStore;
    }

//    public MetricsRawKVStore getMetricsRawKVStore() {
//        return metricsRawKVStore;
//    }

    public Node getNode() {
        return node;
    }

    public DTGStateMachine getFsm() {
        return fsm;
    }

    public RegionEngineOptions copyRegionOpts() {
        return Requires.requireNonNull(this.regionOpts, "opts").copy();
    }

    public DTGRegionEngineOptions copyNullRegionOpts() {
        return Requires.requireNonNull(this.regionOpts, "opts").copyNull();
    }

    public RpcCaller getRpcCaller() {
        return rpcCaller;
    }

    @Override
    public String toString() {
        return "RegionEngine{" + "region=" + region + ", regionOpts=" + regionOpts + '}';
    }
}

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

import Communication.DTGMetadataRpcClient;
import DBExceptions.IdDistributeException;
import DBExceptions.TypeDoesnotExistException;
import Element.EntityEntry;
import com.alipay.remoting.rpc.RpcClient;
import com.alipay.sofa.jraft.CliService;
import com.alipay.sofa.jraft.RaftServiceFactory;
import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.CliServiceImpl;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rhea.JRaftHelper;
import com.alipay.sofa.jraft.rhea.client.RoundRobinLoadBalancer;
import com.alipay.sofa.jraft.rhea.client.pd.PlacementDriverRpcService;
import com.alipay.sofa.jraft.rhea.errors.RouteTableException;
import com.alipay.sofa.jraft.rhea.metadata.*;
import com.alipay.sofa.jraft.rhea.options.RegionEngineOptions;
import com.alipay.sofa.jraft.rhea.options.RegionRouteTableOptions;
import com.alipay.sofa.jraft.rhea.options.configured.RpcOptionsConfigured;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.rhea.util.Strings;
import com.alipay.sofa.jraft.rhea.util.ThrowUtil;
import com.alipay.sofa.jraft.rpc.CliClientService;
import com.alipay.sofa.jraft.rpc.impl.AbstractBoltClientService;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.Requires;
import config.DTGConstants;
import config.DefaultOptions;
import options.DTGPlacementDriverOptions;
import options.DTGRegionEngineOptions;
import options.DTGStoreEngineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import Region.DTGRegionRouteTable;
import com.alipay.sofa.jraft.rhea.options.RpcOptions;
import Region.DTGRegion;
import storage.DTGCluster;
import storage.DTGStore;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static config.MainType.*;

/**
 * @author :jinkai
 * @date :Created in 2019/10/14 19:24
 * @description:
 * @modified By:
 * @version: 1.0
 */

public class DefaultPlacementDriverClient implements DTGPlacementDriverClient{

    private static final Logger LOG = LoggerFactory.getLogger(DefaultPlacementDriverClient.class);

    private String  pdGroupId;
    private boolean started;
    private boolean isRemotePd;
    private LinkedList<Long> nodeIdList;
    private LinkedList<Long> relationIdList;


    protected DTGRegionRouteTable          RelationRegionRouteTable;
    protected DTGRegionRouteTable          NodeRegionRouteTable;
    protected DTGRegionRouteTable          TemProRegionRouteTable;
    protected final long                   clusterId;
    protected final String                 clusterName;
    protected RpcClient                    rpcClient;
    protected CliClientService             cliClientService;
    private DTGMetadataRpcClient           metadataRpcClient;
    protected CliService                   cliService;
    protected DTGPlacementDriverRpcService pdRpcService;
    private int minIdBatchSize;

    public DefaultPlacementDriverClient(long clusterId, String clusterName, boolean isRemotePd){
        this.clusterId = clusterId;
        this.clusterName = clusterName;
        this.isRemotePd = isRemotePd;
        this.nodeIdList = new LinkedList<>();
        this.relationIdList = new LinkedList<>();
    }

    @Override
    public boolean init(final DTGPlacementDriverOptions opts) {
        if (this.started) {
            LOG.info("[RemotePlacementDriverClient] already started.");
            return true;
        }
        this.minIdBatchSize = opts.getMinIdBatchSize();
        RelationRegionRouteTable = new DTGRegionRouteTable(RELATIONTYPE, this);
        NodeRegionRouteTable = new DTGRegionRouteTable(NODETYPE, this);
        TemProRegionRouteTable = new DTGRegionRouteTable(TEMPORALPROPERTYTYPE, this);
        initCli(opts.getCliOptions());
        this.pdRpcService = new DTGPlacementDriverRpcService(this);
        RpcOptions rpcOpts = opts.getPdRpcOptions();
        if (rpcOpts == null) {
            rpcOpts = RpcOptionsConfigured.newDefaultConfig();
            rpcOpts.setCallbackExecutorCorePoolSize(0);
            rpcOpts.setCallbackExecutorMaximumPoolSize(0);
        }
        if (!this.pdRpcService.init(rpcOpts)) {
            LOG.error("Fail to init [PlacementDriverRpcService].");
            return false;
        }
        final List<RegionRouteTableOptions> regionRouteTableOptionsList = opts.getRegionRouteTableOptionsList();
        if (regionRouteTableOptionsList != null) {
            regionRouteTableOptionsList.add(0, DefaultOptions.DefaultRegionRouteTableOptions());
            final String initialServerList = opts.getInitialServerList();
            for (final RegionRouteTableOptions regionRouteTableOpts : regionRouteTableOptionsList) {
                if (Strings.isBlank(regionRouteTableOpts.getInitialServerList())) {
                    // if blank, extends parent's value
                    regionRouteTableOpts.setInitialServerList(initialServerList);
                }
                initRouteTableByRegion(regionRouteTableOpts);
            }
        }
        if(!opts.isRemotePd()){
            this.pdGroupId = opts.getPdGroupId();
            if (Strings.isBlank(this.pdGroupId)) {
                throw new IllegalArgumentException("opts.pdGroup id must not be blank");
            }
            final String initialPdServers = opts.getInitialPdServerList();
            if (Strings.isBlank(initialPdServers)) {
                throw new IllegalArgumentException("opts.initialPdServerList must not be blank");
            }
            RouteTable.getInstance().updateConfiguration(this.pdGroupId, initialPdServers);
            this.metadataRpcClient = new DTGMetadataRpcClient(pdRpcService, 3);
            //refreshRouteTable();
            //int a = 0;
            LOG.info("[DTGPlacementDriverClient] start successfully, options: {}.", opts);
        }
//        if(opts.isLocalClient()){
//            this.getIds(NODETYPE);
//            this.getIds(RELATIONTYPE);
//            while (true){
//                if(this.relationIdList.size()>0&&this.nodeIdList.size()>0)break;
//            }
//        }
        return this.started = true;
    }

    @Override
    public void shutdown() {
        if(this.nodeIdList.size()>0){
            this.returnIds(NODETYPE);
        }
        if(this.relationIdList.size()>0){
            this.returnIds(RELATIONTYPE);
        }
        if (this.cliService != null) {
            this.cliService.shutdown();
        }
        if (this.pdRpcService != null) {
            this.pdRpcService.shutdown();
        }
    }

    @Override
    public long getClusterId() {
        return this.clusterId;
    }

    @Override
    public DTGRegion getRegionByRegionId(final long regionId) {
        DTGRegion region = NodeRegionRouteTable.getRegionById(regionId);
        if(region != null){
            return region;
        }
        region = RelationRegionRouteTable.getRegionById(regionId);
        if(region != null){
            return region;
        }
        region = TemProRegionRouteTable.getRegionById(regionId);
        if(region != null){
            return region;
        }
        return null;
    }

    @Override
    public DTGRegion findRegionById(final long id, final boolean forceRefresh, final byte type) {
        if (forceRefresh) {
            refreshRouteTable(true);
        }
        DTGRegionRouteTable regionRouteTable = returnRegionRouteTable(type);
        return regionRouteTable.findRegionByKey(id);
    }

    @Override
    public Map<DTGRegion, List<Long>> findRegionsByIds(final long[] ids, final boolean forceRefresh, final byte type) {
        if (forceRefresh) {
            refreshRouteTable(true);
        }
        DTGRegionRouteTable regionRouteTable = returnRegionRouteTable(type);
        return regionRouteTable.findRegionsByKeys(ids);
    }

    @Override
    public Map[] findRegionsByEntityEntries(final List<EntityEntry> entityEntries, final boolean forceRefresh, final byte type) {
        if (forceRefresh) {
            refreshRouteTable(true);
        }
        DTGRegionRouteTable regionRouteTable = returnRegionRouteTable(type);
        return regionRouteTable.findRegionsByEntityEntries(entityEntries);
    }

    @Override
    public Map[] findRegionsByEntityEntries(final List<EntityEntry> entityEntries, final boolean forceRefresh, final byte type, final Map[] map) {
        if (forceRefresh) {
            refreshRouteTable(true);
        }
        DTGRegionRouteTable regionRouteTable = returnRegionRouteTable(type);
        return regionRouteTable.findRegionsByEntityEntries(entityEntries, map);
    }

    @Override
    public List<DTGRegion> findRegionsByIdRange(final long startId, final long endId, final boolean forceRefresh, final byte type) {
        if (forceRefresh) {
            refreshRouteTable(true);
        }
        DTGRegionRouteTable regionRouteTable = returnRegionRouteTable(type);
        return regionRouteTable.findRegionsByKeyRange(startId, endId);
    }

    @Override
    public long findStartIdOfNextRegion(final long Id, final boolean forceRefresh, final byte type) {
        if (forceRefresh) {
            refreshRouteTable(true);
        }
        DTGRegionRouteTable regionRouteTable = returnRegionRouteTable(type);
        return regionRouteTable.findStartKeyOfNextRegion(Id);
    }

    @Override
    public DTGRegionRouteTable getRegionRouteTable(final byte type) {
        return returnRegionRouteTable(type);
    }

    @Override
    public DTGStore getStoreMetadata(final DTGStoreEngineOptions opts) {
        if(this.isRemotePd){
            return pdGetStoreMetadata(opts);
        }
        final Endpoint selfEndpoint = opts.getServerAddress();
        // remote conf is the preferred
        final DTGStore remoteStore = this.metadataRpcClient.getStoreInfo(this.clusterId, selfEndpoint);
        if (!remoteStore.isEmpty()) {
            final List<DTGRegion> regions = remoteStore.getRegions();
            for (final DTGRegion region : regions) {
                addOrUpdateRegion(region, true);
            }
            return remoteStore;
        }
        // local conf
        final DTGStore localStore = new DTGStore();
        final List<DTGRegionEngineOptions> rOptsList = opts.getRegionEngineOptionsList();
        final List<DTGRegion> regionList = Lists.newArrayListWithCapacity(rOptsList.size());
        localStore.setId(remoteStore.getId());
        localStore.setEndpoint(selfEndpoint);
        for (final DTGRegionEngineOptions rOpts : rOptsList) {
            regionList.add(getLocalRegionMetadata(rOpts));
        }
        localStore.setRegions(regionList);
        this.metadataRpcClient.updateStoreInfo(this.clusterId, localStore);
        return localStore;
    }

    @Override
    public long getStoreId(DTGStoreEngineOptions opts) {
        if(this.isRemotePd){
            return pdGetStoreMetadata(opts).getId();
        }
        final Endpoint selfEndpoint = opts.getServerAddress();
        // remote conf is the preferred
        final DTGStore remoteStore = this.metadataRpcClient.getStoreInfo(this.clusterId, selfEndpoint);
        if(remoteStore != null){
            return remoteStore.getId();
        }
        return DTGConstants.NULL_STORE;
    }

    public DTGStore pdGetStoreMetadata(final DTGStoreEngineOptions opts){
        final DTGStore store = new DTGStore();
        final List<DTGRegionEngineOptions> rOptsList = opts.getRegionEngineOptionsList();
        final List<DTGRegion> regionList = Lists.newArrayListWithCapacity(rOptsList.size());
        store.setId(-1);
        store.setEndpoint(opts.getServerAddress());
        for (final RegionEngineOptions rOpts : rOptsList) {
            regionList.add(getLocalRegionMetadata(rOpts));
        }
        store.setRegions(regionList);
        return store;
    }

    @Override
    public Endpoint getLeader(final long regionId, final boolean forceRefresh, final long timeoutMillis) {
        final String raftGroupId = JRaftHelper.getJRaftGroupId(this.clusterName, regionId);
        PeerId leader = getLeader(raftGroupId, forceRefresh, timeoutMillis);
        if (leader == null && !forceRefresh) {
            // Could not found leader from cache, try again and force refresh cache
            leader = getLeader(raftGroupId, true, timeoutMillis);
        }
        if (leader == null) {
            throw new RouteTableException("no leader in group: " + raftGroupId);
        }
        return leader.getEndpoint();
    }

    protected PeerId getLeader(final String raftGroupId, final boolean forceRefresh, final long timeoutMillis) {
        final RouteTable routeTable = RouteTable.getInstance();
        if (forceRefresh) {
            final long deadline = System.currentTimeMillis() + timeoutMillis;
            final StringBuilder error = new StringBuilder();
            // A newly launched raft group may not have been successful in the election,
            // or in the 'leader-transfer' state, it needs to be re-tried
            Throwable lastCause = null;
            for (;;) {
                try {
                    final Status st = routeTable.refreshLeader(this.cliClientService, raftGroupId, 2000);
                    if (st.isOk()) {
                        break;
                    }
                    error.append(st.toString());
                } catch (final InterruptedException e) {
                    ThrowUtil.throwException(e);
                } catch (final Throwable t) {
                    lastCause = t;
                    error.append(t.getMessage());
                }
                if (System.currentTimeMillis() < deadline) {
                    LOG.debug("Fail to find leader, retry again, {}.", error);
                    error.append(", ");
                    try {
                        Thread.sleep(10);
                    } catch (final InterruptedException e) {
                        ThrowUtil.throwException(e);
                    }
                } else {
                    throw lastCause != null ? new RouteTableException(error.toString(), lastCause)
                            : new RouteTableException(error.toString());
                }
            }
        }
        return routeTable.selectLeader(raftGroupId);
    }

    @Override
    public Endpoint getLuckyPeer(final long regionId, final boolean forceRefresh, final long timeoutMillis, final Endpoint unExpect) {
        final String raftGroupId = JRaftHelper.getJRaftGroupId(this.clusterName, regionId);
        final RouteTable routeTable = RouteTable.getInstance();
        if (forceRefresh) {
            final long deadline = System.currentTimeMillis() + timeoutMillis;
            final StringBuilder error = new StringBuilder();
            // A newly launched raft group may not have been successful in the election,
            // or in the 'leader-transfer' state, it needs to be re-tried
            for (;;) {
                try {
                    final Status st = routeTable.refreshConfiguration(this.cliClientService, raftGroupId, 5000);
                    if (st.isOk()) {
                        break;
                    }
                    error.append(st.toString());
                } catch (final InterruptedException e) {
                    ThrowUtil.throwException(e);
                } catch (final TimeoutException e) {
                    error.append(e.getMessage());
                }
                if (System.currentTimeMillis() < deadline) {
                    LOG.debug("Fail to get peers, retry again, {}.", error);
                    error.append(", ");
                    try {
                        Thread.sleep(5);
                    } catch (final InterruptedException e) {
                        ThrowUtil.throwException(e);
                    }
                } else {
                    throw new RouteTableException(error.toString());
                }
            }
        }
        final Configuration configs = routeTable.getConfiguration(raftGroupId);
        if (configs == null) {
            throw new RouteTableException("empty configs in group: " + raftGroupId);
        }
        final List<PeerId> peerList = configs.getPeers();
        if (peerList == null || peerList.isEmpty()) {
            throw new RouteTableException("empty peers in group: " + raftGroupId);
        }
        final int size = peerList.size();
        if (size == 1) {
            return peerList.get(0).getEndpoint();
        }
        final RoundRobinLoadBalancer balancer = RoundRobinLoadBalancer.getInstance(regionId);
        for (int i = 0; i < size; i++) {
            final PeerId candidate = balancer.select(peerList);
            final Endpoint luckyOne = candidate.getEndpoint();
            if (!luckyOne.equals(unExpect)) {
                return luckyOne;
            }
        }
        throw new RouteTableException("have no choice in group(peers): " + raftGroupId);
    }

    @Override
    public void refreshRouteConfiguration(final long regionId) {
        final String raftGroupId = JRaftHelper.getJRaftGroupId(this.clusterName, regionId);
        try {
            getLeader(raftGroupId, true, 5000);
            RouteTable.getInstance().refreshConfiguration(this.cliClientService, raftGroupId, 5000);
        } catch (final Exception e) {
            LOG.error("Fail to refresh route configuration for {}, {}.", regionId, StackTraceUtil.stackTrace(e));
        }
    }

    @Override
    public boolean transferLeader(final long regionId, Peer peer, final boolean refreshConf) {
        Requires.requireNonNull(peer, "peer");
        Requires.requireNonNull(peer.getEndpoint(), "peer.endpoint");
        final String raftGroupId = JRaftHelper.getJRaftGroupId(this.clusterName, regionId);
        final Configuration conf = RouteTable.getInstance().getConfiguration(raftGroupId);
        final Status status = this.cliService.transferLeader(raftGroupId, conf, JRaftHelper.toJRaftPeerId(peer));
        if (status.isOk()) {
            if (refreshConf) {
                refreshRouteConfiguration(regionId);
            }
            return true;
        }
        LOG.error("Fail to [transferLeader], [regionId: {}, peer: {}], status: {}.", regionId, peer, status);
        return false;
    }

    @Override
    public boolean addReplica(final long regionId, Peer peer, final boolean refreshConf) {
        Requires.requireNonNull(peer, "peer");
        Requires.requireNonNull(peer.getEndpoint(), "peer.endpoint");
        final String raftGroupId = JRaftHelper.getJRaftGroupId(this.clusterName, regionId);
        final Configuration conf = RouteTable.getInstance().getConfiguration(raftGroupId);
        final Status status = this.cliService.addPeer(raftGroupId, conf, JRaftHelper.toJRaftPeerId(peer));
        if (status.isOk()) {
            if (refreshConf) {
                refreshRouteConfiguration(regionId);
            }
            return true;
        }
        LOG.error("Fail to [addReplica], [regionId: {}, peer: {}], status: {}.", regionId, peer, status);
        return false;
    }

    @Override
    public boolean removeReplica(final long regionId, Peer peer, final boolean refreshConf) {
        Requires.requireNonNull(peer, "peer");
        Requires.requireNonNull(peer.getEndpoint(), "peer.endpoint");
        final String raftGroupId = JRaftHelper.getJRaftGroupId(this.clusterName, regionId);
        final Configuration conf = RouteTable.getInstance().getConfiguration(raftGroupId);
        final Status status = this.cliService.removePeer(raftGroupId, conf, JRaftHelper.toJRaftPeerId(peer));
        if (status.isOk()) {
            if (refreshConf) {
                refreshRouteConfiguration(regionId);
            }
            return true;
        }
        LOG.error("Fail to [removeReplica], [regionId: {}, peer: {}], status: {}.", regionId, peer, status);
        return false;
    }

    @Override
    public String getClusterName() {
        return clusterName;
    }

    @Override
    public Endpoint getPdLeader(final boolean forceRefresh, final long timeoutMillis) {
        PeerId leader = getLeader(this.pdGroupId, forceRefresh, timeoutMillis);
        if (leader == null && !forceRefresh) {
            leader = getLeader(this.pdGroupId, true, timeoutMillis);
        }
        if (leader == null) {
            throw new RouteTableException("no placement driver leader in group: " + this.pdGroupId);
        }
        return new Endpoint(leader.getIp(), leader.getPort());
    }

    @Override
    public PlacementDriverRpcService getPdRpcService() {
        return pdRpcService;
    }

    @Override
    public void refreshRouteTable(boolean needLock) {//System.out.println("start refresh..");
        final DTGCluster cluster = this.metadataRpcClient.getClusterInfo(this.clusterId);
        if (cluster == null) {
            LOG.warn("Cluster info is empty: {}.", this.clusterId);
            return;
        }
        final List<DTGStore> stores = cluster.getStores();
        if (stores == null || stores.isEmpty()) {
            LOG.error("Stores info is empty: {}.", this.clusterId);
            return;
        }
        for (final DTGStore store : stores) {
            final List<DTGRegion> regions = store.getRegions();
            if (regions == null || regions.isEmpty()) {
                LOG.error("Regions info is empty: {} - {}.", this.clusterId, store.getId());
                continue;
            }
            for (final DTGRegion region : regions) {
                String serverList = "";
                for(Peer peer : region.getPeers()){
                    serverList = serverList + peer.getEndpoint().toString() + ",";
                }
                final Configuration conf = new Configuration();
                conf.parse(serverList);
                // update raft route table
                RouteTable.getInstance().updateConfiguration(JRaftHelper.getJRaftGroupId(this.getClusterName(), region.getId()), conf);
                addOrUpdateRegion(region, needLock);
            }
        }
        int a = 1;
        returnRegionRouteTable(NODETYPE).updataTop(cluster.getNodeIdTopRestrict());
        returnRegionRouteTable(RELATIONTYPE).updataTop(cluster.getRelationIdTopRestrict());//System.out.println("refresh done!");
    }

    private DTGRegionRouteTable returnRegionRouteTable(final byte type){
        try {
            switch (type){
                case NODETYPE: return this.NodeRegionRouteTable;
                case RELATIONTYPE: return this.RelationRegionRouteTable;
                case TEMPORALPROPERTYTYPE: return this.TemProRegionRouteTable;
                default:{
                    LOG.error("can not find region route tabale of type");
                    throw new TypeDoesnotExistException(type, "region route tabale of type");
                }
            }
        }catch (Throwable e){
            System.out.println(e);
        }
        return null;
    }

    public RpcClient getRpcClient() {
        return rpcClient;
    }

    protected DTGRegion getLocalRegionMetadata(final RegionEngineOptions opts) {
        final long regionId = Requires.requireNonNull(opts.getRegionId(), "opts.regionId");
        Requires.requireTrue(regionId >= Region.MIN_ID_WITH_MANUAL_CONF, "opts.regionId must >= "
                + Region.MIN_ID_WITH_MANUAL_CONF);
        Requires.requireTrue(regionId < Region.MAX_ID_WITH_MANUAL_CONF, "opts.regionId must < "
                + Region.MAX_ID_WITH_MANUAL_CONF);

        final String initialServerList = opts.getInitialServerList();
        final DTGRegion region = new DTGRegion(opts.getInitNodeId(), opts.getInitRelationId(), opts.getInitTempProId());
        final Configuration conf = new Configuration();
        // region
        region.setId(regionId);
        if(opts.getRelationIdRangeList() != null){
            region.setRelationIdRangeList(opts.getRelationIdRangeList());
        }
        if(opts.getNodeIdRangeList() != null){
            region.setNodeIdRangeList(opts.getNodeIdRangeList());
        }
        if(opts.getTemporalPropertyTimeRangeList() != null){
            region.setTemporalPropertyTimeRangeList(opts.getTemporalPropertyTimeRangeList());
        }
        region.setRegionEpoch(new RegionEpoch(-1, -1));
        // peers
        Requires.requireTrue(Strings.isNotBlank(initialServerList), "opts.initialServerList is blank");
        conf.parse(initialServerList);
        region.setPeers(JRaftHelper.toPeerList(conf.listPeers()));
        addOrUpdateRegion(region, true);
        return region;
    }

    public void addOrUpdateRegion(final DTGRegion region, boolean needLock){//System.out.println("addOrUpdateRegion : " + region.getId());
        NodeRegionRouteTable.addOrUpdateRegion(region, needLock);
        RelationRegionRouteTable.addOrUpdateRegion(region, needLock);
        TemProRegionRouteTable.addOrUpdateRegion(region, needLock);
    }

    protected void initCli(CliOptions cliOpts) {
        if (cliOpts == null) {
            cliOpts = new CliOptions();
            cliOpts.setTimeoutMs(50000);
            cliOpts.setMaxRetry(3);
        }
        this.cliService = RaftServiceFactory.createAndInitCliService(cliOpts);
        this.cliClientService = ((CliServiceImpl) this.cliService).getCliClientService();
        Requires.requireNonNull(this.cliClientService, "cliClientService");
        this.rpcClient = ((AbstractBoltClientService) this.cliClientService).getRpcClient();
    }



    protected void initRouteTableByRegion(final RegionRouteTableOptions opts) {
        final long regionId = Requires.requireNonNull(opts.getRegionId(), "opts.regionId");
        LOG.info("init region, region id = " + regionId);
        final String initialServerList = opts.getInitialServerList();
        final DTGRegion region = new DTGRegion(opts.getInitNodeId(), opts.getInitRelationId(), opts.getInitTempProId());
        final Configuration conf = new Configuration();
        // region
        region.setId(regionId);
        if(opts.getRelationIdRangeList() != null){
            region.setRelationIdRangeList(opts.getRelationIdRangeList());
        }
        if(opts.getNodeIdRangeList() != null){
            region.setNodeIdRangeList(opts.getNodeIdRangeList());
        }
        if(opts.getTemporalPropertyTimeRangeList() != null){
            region.setTemporalPropertyTimeRangeList(opts.getTemporalPropertyTimeRangeList());
        }
        //region.setNodeIdRegionList(opts.getNodeIdRangeList());
        //region.setRelationIdRegionList(opts.getRelationIdRangeList());
//        region.setTemporalPropertyTimeRegionList(opts.getTemporalPropertyTimeRangeList());
        region.setRegionEpoch(new RegionEpoch(-1, -1));
        // peers
        Requires.requireTrue(Strings.isNotBlank(initialServerList), "opts.initialServerList is blank");
        conf.parse(initialServerList);
        region.setPeers(JRaftHelper.toPeerList(conf.listPeers()));
        // update raft route table
        RouteTable.getInstance().updateConfiguration(JRaftHelper.getJRaftGroupId(clusterName, regionId), conf);
        addOrUpdateRegion(region, true);
    }

    @Override
    public boolean isRemotePd() {
        return isRemotePd;
    }

    @Override
    public void getIds(byte type) {
        if(type == TEMPORALPROPERTYTYPE)return;
        List<Long> list = this.metadataRpcClient.getIds(type);
        System.out.println("get id from pd, the size is : " + list.size());
        getIdList(type).addAll(list);
    }

    @Override
    public void initIds() {
        this.getIds(NODETYPE);
        this.getIds(RELATIONTYPE);
        while (true){
            if(this.relationIdList.size()>0&&this.nodeIdList.size()>0)break;
        }
    }

    @Override
    public synchronized long getId(byte type) {
        LinkedList<Long> list = getIdList(type);
        long id = list.poll();
        if(list.size() < minIdBatchSize){
            CompletableFuture.runAsync(() -> {
                getIds(type);
            });
        }
        return id;
    }

    @Override
    public long getVersion() {
        //System.out.println("get version :" + System.currentTimeMillis());
        return this.metadataRpcClient.getVersion();
    }

    @Override
    public DTGMetadataRpcClient getDTGMetadataRpcClient() {
        return this.metadataRpcClient;
    }

    @Override
    public void returnIds(byte type) {
        if(type == TEMPORALPROPERTYTYPE)return;
        if(! this.metadataRpcClient.returnIds(getIdList(type), type)){
            try {
                throw new IdDistributeException("can not return id to PD");
            } catch (IdDistributeException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void removeId(byte type, long id){
        if(type == TEMPORALPROPERTYTYPE)return;
        getIdList(type).add(id);
    }

    private LinkedList<Long> getIdList(byte type){
        if (type == NODETYPE){
            return nodeIdList;
        }
        else return relationIdList;
    }

}

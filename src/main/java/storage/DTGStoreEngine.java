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
package storage;

import Communication.DTGRpcService;
import Communication.HeartbeatSender;
import Communication.RpcCaller;
import Communication.instructions.AddRegionInfo;
import DBExceptions.RegionStoreException;
import Element.DTGOperation;
import Element.EntityEntry;
import Element.OperationName;
import LocalDBMachine.LocalDB;
import PlacementDriver.DTGPlacementDriverClient;
import PlacementDriver.DefaultPlacementDriverClient;
import UserClient.DTGSaveStore;
import com.alipay.remoting.rpc.RpcServer;
import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rhea.*;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.errors.RheaRuntimeException;
import com.alipay.sofa.jraft.rhea.metadata.Peer;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import com.alipay.sofa.jraft.rhea.options.*;
import com.alipay.sofa.jraft.rhea.rpc.ExtSerializerSupports;
import com.alipay.sofa.jraft.rhea.serialization.Serializers;
import com.alipay.sofa.jraft.rhea.storage.*;
import com.alipay.sofa.jraft.rhea.util.*;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.util.*;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Slf4jReporter;
import config.DTGConstants;
import config.DefaultOptions;
import options.DTGRegionEngineOptions;
import options.DTGStoreEngineOptions;
import options.LocalDBOption;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import Region.*;
import raft.EntityEntryClosureAdapter;
import raft.EntityStoreClosure;
import tool.ObjectAndByte;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static config.MainType.*;

/**
 * Storage engine, there is only one instance in a node,
 * containing one or more {@link RegionEngine}.
 *
 * @author jiachun.fjc
 */
public class DTGStoreEngine implements Lifecycle<DTGStoreEngineOptions> {

    private static final Logger                          LOG                  = LoggerFactory
                                                                                  .getLogger(DTGStoreEngine.class);

    static {
        ExtSerializerSupports.init();
    }

    protected final ConcurrentMap<Long, DTGRegionService>   regionServiceTable = Maps.newConcurrentMapLong();
    protected final ConcurrentMap<Long, DTGRegionEngine>    regionEngineTable  = Maps.newConcurrentMapLong();
    private final DTGPlacementDriverClient                  pdClient;
    protected final long                                    clusterId;

    protected Long                                          storeId;
    protected final AtomicBoolean                           splitting          = new AtomicBoolean(false);
    // When the store is started (unix timestamp in milliseconds)
    protected long                                          startTime          = System.currentTimeMillis();
    protected File                                          dbPath;
    protected RpcServer                                     rpcServer;
    //protected BatchRawKVStore<?>                          rawKVStore;

    protected LocalDB                                       localDB;
    protected BatchRawKVStore<?>                            rawKVStore;
    private   DTGStore                                      store;
    private   DTGSaveStore                                  saveStore;

    protected HeartbeatSender                               heartbeatSender;
    protected DTGStoreEngineOptions                         storeOpts;
    private   RpcCaller                                     rpcCaller;

    // Shared executor services
    protected ExecutorService                            readIndexExecutor;
    protected ExecutorService                            raftStateTrigger;
    protected ExecutorService                            snapshotExecutor;
    protected ExecutorService                            cliRpcExecutor;
    protected ExecutorService                            raftRpcExecutor;
    protected ExecutorService                            kvRpcExecutor;

    protected ScheduledExecutorService                   metricsScheduler;
    protected ScheduledReporter                          kvMetricsReporter;
    protected ScheduledReporter                          threadPoolMetricsReporter;

    private   Endpoint                                   endpoint;

    protected boolean                                    started;

    private   String                                     filePath;

    public DTGStoreEngine(DTGPlacementDriverClient pdClient) {
        this.pdClient = pdClient;
        this.clusterId = pdClient.getClusterId();
    }

    @Override
    public synchronized boolean init(final DTGStoreEngineOptions opts) {
        if (this.started) {
            LOG.info("[StoreEngine] already started.");
            return true;
        }

        rpcCaller = new RpcCaller(this.pdClient);
        this.saveStore = opts.getSaveStore();
        this.filePath = opts.getRaftDataPath();
        this.storeOpts = Requires.requireNonNull(opts, "opts");
        Endpoint serverAddress = Requires.requireNonNull(opts.getServerAddress(), "opts.serverAddress");
        this.endpoint = serverAddress;
        final int port = serverAddress.getPort();
        final String ip = serverAddress.getIp();
        if (ip == null || Utils.IP_ANY.equals(ip)) {
            serverAddress = new Endpoint(NetUtil.getLocalCanonicalHostName(), port);
            opts.setServerAddress(serverAddress);
        }
        final long metricsReportPeriod = opts.getMetricsReportPeriod();

        // init region options
        List<DTGRegionEngineOptions> rOptsList = opts.getRegionEngineOptionsList();

//        final RegionEngineOptions initOpts = new RegionEngineOptions();
//        initOpts.setRegionId(Constants.DEFAULT_REGION_ID);
//        initOpts.setInitialServerList(DefaultOptions.ALL_NODE_ADDRESSES);
//        initOpts.setInitNodeId(-2);
//        initOpts.setInitRelationId(-2);
//        initOpts.setInitTempProId(-2);
//        rOptsList.add(0,initOpts);
        DTGRegionEngineOptions rOpt = new DTGRegionEngineOptions();
        rOpt.setRegionId(DTGConstants.DEFAULT_INIT_REGION);
        rOptsList.add(0, rOpt);
        storeId = this.pdClient.getStoreId(opts);
        List<Long> regionsId = readConfig();
        if(regionsId != null){
            rOptsList = new ArrayList<>();
            for(long id : regionsId){
                DTGRegionEngineOptions rOp = new DTGRegionEngineOptions();
                rOp.setRegionId(id);
                rOptsList.add(rOp);
            }
            opts.setRegionEngineOptionsList(rOptsList);
        }
        if (rOptsList == null || rOptsList.isEmpty()) {
            // -1 region
//            final RegionEngineOptions rOpts = new RegionEngineOptions();
//            rOpts.setRegionId(Constants.DEFAULT_REGION_ID);
//            rOptsList = Lists.newArrayList();
//            rOptsList.add(rOpts);
            opts.setRegionEngineOptionsList(rOptsList);
        }
        final String clusterName = this.pdClient.getClusterName();
        for (final DTGRegionEngineOptions rOpts : rOptsList) {
            rOpts.setRaftGroupId(JRaftHelper.getJRaftGroupId(clusterName, rOpts.getRegionId()));
            //System.out.println("group id is : " + JRaftHelper.getJRaftGroupId(clusterName, rOpts.getRegionId()));
            rOpts.setServerAddress(serverAddress);
            rOpts.setInitialServerList(opts.getInitialServerList());
            rOpts.setSaveStore(opts.getSaveStore());
            if (rOpts.getNodeOptions() == null) {
                // copy common node options
                rOpts.setNodeOptions(opts.getCommonNodeOptions() == null ? new NodeOptions() : opts
                    .getCommonNodeOptions().copy());
            }
            if (rOpts.getMetricsReportPeriod() <= 0 && metricsReportPeriod > 0) {
                // extends store opts
                rOpts.setMetricsReportPeriod(metricsReportPeriod);
            }
        }
        // init store
        store = this.pdClient.getStoreMetadata(opts);
        if (store == null || store.getRegions() == null || store.getRegions().isEmpty()) {
            LOG.error("Empty store metadata: {}.", store);
            return false;
        }
        this.storeId = store.getId();
        // init executors
        if (this.readIndexExecutor == null) {
            this.readIndexExecutor = StoreEngineHelper.createReadIndexExecutor(opts.getReadIndexCoreThreads());
        }
        if (this.raftStateTrigger == null) {
            this.raftStateTrigger = StoreEngineHelper.createRaftStateTrigger(opts.getLeaderStateTriggerCoreThreads());
        }
        if (this.snapshotExecutor == null) {
            this.snapshotExecutor = StoreEngineHelper.createSnapshotExecutor(opts.getSnapshotCoreThreads());
        }
        // init rpc executors
        final boolean useSharedRpcExecutor = opts.isUseSharedRpcExecutor();
        if (!useSharedRpcExecutor) {
            if (this.cliRpcExecutor == null) {
                this.cliRpcExecutor = StoreEngineHelper.createCliRpcExecutor(opts.getCliRpcCoreThreads());
            }
            if (this.raftRpcExecutor == null) {
                this.raftRpcExecutor = StoreEngineHelper.createRaftRpcExecutor(opts.getRaftRpcCoreThreads());
            }
            if (this.kvRpcExecutor == null) {
                if(opts.getMaxKvRpcCoreThreads() > 0){
                    this.kvRpcExecutor = StoreEngineHelper.createKvRpcExecutor(opts.getKvRpcCoreThreads(), opts.getMaxKvRpcCoreThreads());
                }
                else {
                    this.kvRpcExecutor = StoreEngineHelper.createKvRpcExecutor(opts.getKvRpcCoreThreads());
                }

            }
        }
        // init metrics
        startMetricReporters(metricsReportPeriod);
        // init rpc server
        this.rpcServer = new RpcServer(port, true, true);
        RaftRpcServerFactory.addRaftRequestProcessors(this.rpcServer, this.raftRpcExecutor, this.cliRpcExecutor);
        StoreEngineHelper.addKvStoreRequestProcessor(this.rpcServer, this);
        if (!this.rpcServer.start()) {
            LOG.error("Fail to init [RpcServer].");
            return false;
        }
        // init db store
//        if (!initRawKVStore(opts)) {
//            return false;
//        }
        if(!initDBStore(opts)){
            return false;
        }
        // init all region engine
        if (!initAllRegionEngine(opts, store)) {
            LOG.error("Fail to init all [RegionEngine].");
            return false;
        }
        // heartbeat sender
        if (!this.pdClient.isRemotePd()) {
            this.pdClient.refreshRouteTable(true);
            HeartbeatOptions heartbeatOpts = opts.getHeartbeatOptions();
            if (heartbeatOpts == null) {
                heartbeatOpts = new HeartbeatOptions();
            }
            this.heartbeatSender = new HeartbeatSender(this);
            if (!this.heartbeatSender.init(heartbeatOpts)) {
                LOG.error("Fail to init [HeartbeatSender].");
                return false;
            }
        }
        this.startTime = System.currentTimeMillis();
        LOG.info("[StoreEngine] start successfully: {}.", this);
        return this.started = true;
    }

    @Override
    public synchronized void shutdown() {
        if (!this.started) {
            return;
        }
        if (this.rpcServer != null) {
            this.rpcServer.stop();
        }
        saveConfig();
        if (!this.regionEngineTable.isEmpty()) {
            for (final DTGRegionEngine engine : this.regionEngineTable.values()) {
                engine.shutdown();
            }
            this.regionEngineTable.clear();
        }
        if(localDB != null){
            localDB.shutdown();
        }
        if (this.rawKVStore != null) {
            this.rawKVStore.shutdown();
        }
        if (this.heartbeatSender != null) {
            this.heartbeatSender.shutdown();
        }
        this.regionServiceTable.clear();
        if (this.kvMetricsReporter != null) {
            this.kvMetricsReporter.stop();
        }
        if (this.threadPoolMetricsReporter != null) {
            this.threadPoolMetricsReporter.stop();
        }
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.readIndexExecutor);
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.raftStateTrigger);
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.snapshotExecutor);
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.cliRpcExecutor);
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.raftRpcExecutor);
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.kvRpcExecutor);
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.metricsScheduler);
        this.started = false;
        LOG.info("[StoreEngine] shutdown successfully.");
    }

    public DTGPlacementDriverClient getPlacementDriverClient() {
        return pdClient;
    }

    public long getClusterId() {
        return clusterId;
    }

    public Long getStoreId() {
        return storeId;
    }

    public DTGStoreEngineOptions getStoreOpts() {
        return storeOpts;
    }

    public long getStartTime() {
        return startTime;
    }

    public RpcServer getRpcServer() {
        return rpcServer;
    }

//    public BatchRawKVStore<?> getRawKVStore() {
//        return rawKVStore;
//    }
    public LocalDB getlocalDB(){
        return this.localDB;
    }

    public DTGRegionService getRegionKVService(final long regionId) {
        return this.regionServiceTable.get(regionId);
    }

    public long getTotalSpace() {
        if (this.dbPath == null || !this.dbPath.exists()) {
            return 0;
        }
        return this.dbPath.getTotalSpace();
    }

    public long getUsableSpace() {
        if (this.dbPath == null || !this.dbPath.exists()) {
            return 0;
        }
        return this.dbPath.getUsableSpace();
    }

    public long getStoreUsedSpace() {
        if (this.dbPath == null || !this.dbPath.exists()) {
            return 0;
        }
        return FileUtils.sizeOf(this.dbPath);
    }

    public Endpoint getSelfEndpoint() {
        return this.storeOpts == null ? null : this.storeOpts.getServerAddress();
    }

    public DTGRegionEngine getRegionEngine(final long regionId) {
        return this.regionEngineTable.get(regionId);
    }

    public List<DTGRegionEngine> getAllRegionEngines() {
        return Lists.newArrayList(this.regionEngineTable.values());
    }

    public ExecutorService getReadIndexExecutor() {
        return readIndexExecutor;
    }

    public void setReadIndexExecutor(ExecutorService readIndexExecutor) {
        this.readIndexExecutor = readIndexExecutor;
    }

    public ExecutorService getRaftStateTrigger() {
        return raftStateTrigger;
    }

    public void setRaftStateTrigger(ExecutorService raftStateTrigger) {
        this.raftStateTrigger = raftStateTrigger;
    }

    public ExecutorService getSnapshotExecutor() {
        return snapshotExecutor;
    }

    public void setSnapshotExecutor(ExecutorService snapshotExecutor) {
        this.snapshotExecutor = snapshotExecutor;
    }

    public ExecutorService getCliRpcExecutor() {
        return cliRpcExecutor;
    }

    public void setCliRpcExecutor(ExecutorService cliRpcExecutor) {
        this.cliRpcExecutor = cliRpcExecutor;
    }

    public ExecutorService getRaftRpcExecutor() {
        return raftRpcExecutor;
    }

    public void setRaftRpcExecutor(ExecutorService raftRpcExecutor) {
        this.raftRpcExecutor = raftRpcExecutor;
    }

    public ExecutorService getKvRpcExecutor() {
        return kvRpcExecutor;
    }

    public void setKvRpcExecutor(ExecutorService kvRpcExecutor) {
        this.kvRpcExecutor = kvRpcExecutor;
    }

    public ScheduledExecutorService getMetricsScheduler() {
        return metricsScheduler;
    }

    public void setMetricsScheduler(ScheduledExecutorService metricsScheduler) {
        this.metricsScheduler = metricsScheduler;
    }

    public ScheduledReporter getKvMetricsReporter() {
        return kvMetricsReporter;
    }

    public void setKvMetricsReporter(ScheduledReporter kvMetricsReporter) {
        this.kvMetricsReporter = kvMetricsReporter;
    }

    public ScheduledReporter getThreadPoolMetricsReporter() {
        return threadPoolMetricsReporter;
    }

    public void setThreadPoolMetricsReporter(ScheduledReporter threadPoolMetricsReporter) {
        this.threadPoolMetricsReporter = threadPoolMetricsReporter;
    }

    public boolean removeAndStopRegionEngine(final long regionId) {
        final DTGRegionEngine engine = this.regionEngineTable.get(regionId);
        if (engine != null) {
            engine.shutdown();
            return true;
        }
        return false;
    }

    public List<Long> getLeaderRegionIds() {
        final List<Long> regionIds = Lists.newArrayListWithCapacity(this.regionEngineTable.size());
        for (final DTGRegionEngine regionEngine : this.regionEngineTable.values()) {
            if (regionEngine.isLeader()) {
                regionIds.add(regionEngine.getRegion().getId());
            }
        }
        return regionIds;
    }

    public int getRegionCount() {
        return this.regionEngineTable.size();
    }

    public int getLeaderRegionCount() {
        int count = 0;
        for (final DTGRegionEngine regionEngine : this.regionEngineTable.values()) {
            if (regionEngine.isLeader()) {
                count++;
            }
        }
        return count;
    }

    public boolean isBusy() {
        // Need more info
        return splitting.get();
    }

    public void setRpcService(DTGRpcService rpcService){
        for(long regionId : regionServiceTable.keySet()){
            regionServiceTable.get(regionId).setDtgRpcService(rpcService);
        }
    }

    //    public void applySplit(final Long regionId, final Long newRegionId, final KVStoreClosure closure) {
//        Requires.requireNonNull(regionId, "regionId");
//        Requires.requireNonNull(newRegionId, "newRegionId");
//        if (this.regionEngineTable.containsKey(newRegionId)) {
//            closure.setError(Errors.CONFLICT_REGION_ID);
//            closure.run(new Status(-1, "Conflict region id %d", newRegionId));
//            return;
//        }
//        if (!this.splitting.compareAndSet(false, true)) {
//            closure.setError(Errors.SERVER_BUSY);
//            closure.run(new Status(-1, "Server is busy now"));
//            return;
//        }
//        final DTGRegionEngine parentEngine = getRegionEngine(regionId);
//        if (parentEngine == null) {
//            closure.setError(Errors.NO_REGION_FOUND);
//            closure.run(new Status(-1, "RegionEngine[%s] not found", regionId));
//            this.splitting.set(false);
//            return;
//        }
//        if (!parentEngine.isLeader()) {
//            closure.setError(Errors.NOT_LEADER);
//            closure.run(new Status(-1, "RegionEngine[%s] not leader", regionId));
//            this.splitting.set(false);
//            return;
//        }
//        final Region parentRegion = parentEngine.getRegion();
//        final byte[] startKey = BytesUtil.nullToEmpty(parentRegion.getStartKey());
//        final byte[] endKey = parentRegion.getEndKey();
//        final long approximateKeys = this.rawKVStore.getApproximateKeysInRange(startKey, endKey);
//        final long leastKeysOnSplit = this.storeOpts.getLeastKeysOnSplit();
//        if (approximateKeys < leastKeysOnSplit) {
//            closure.setError(Errors.TOO_SMALL_TO_SPLIT);
//            closure.run(new Status(-1, "RegionEngine[%s]'s keys less than %d", regionId, leastKeysOnSplit));
//            this.splitting.set(false);
//            return;
//        }
//        final byte[] splitKey = this.rawKVStore.jumpOver(startKey, approximateKeys >> 1);
//        if (splitKey == null) {
//            closure.setError(Errors.STORAGE_ERROR);
//            closure.run(new Status(-1, "Fail to scan split key"));
//            this.splitting.set(false);
//            return;
//        }
//        final KVOperation op = KVOperation.createRangeSplit(splitKey, regionId, newRegionId);
//        final Task task = new Task();
//        task.setData(ByteBuffer.wrap(Serializers.getDefault().writeObject(op)));
//        task.setDone(new KVClosureAdapter(closure, op));
//        parentEngine.getNode().apply(task);
//    }

//    public void doSplit(final Long regionId, final Long newRegionId, final byte[] splitKey, byte splitType) {
//        try {
//            Requires.requireNonNull(regionId, "regionId");
//            Requires.requireNonNull(newRegionId, "newRegionId");
//            final DTGRegionEngine parent = getRegionEngine(regionId);
//            final DTGRegion region = parent.getRegion().copy();
//            final RegionEngineOptions rOpts = parent.copyRegionOpts();
//            region.setId(newRegionId);
//            region.setStartKey(splitKey);
//            region.setRegionEpoch(new RegionEpoch(-1, -1));
//
//            rOpts.setRegionId(newRegionId);
//            rOpts.setStartKeyBytes(region.getStartKey());
//            rOpts.setEndKeyBytes(region.getEndKey());
//            rOpts.setRaftGroupId(JRaftHelper.getJRaftGroupId(this.pdClient.getClusterName(), newRegionId));
//            rOpts.setRaftDataPath(null);
//
//            String baseRaftDataPath = this.storeOpts.getRaftDataPath();
//            if (Strings.isBlank(baseRaftDataPath)) {
//                baseRaftDataPath = "";
//            }
//            rOpts.setRaftDataPath(baseRaftDataPath + "raft_data_region_" + region.getId() + "_"
//                                  + getSelfEndpoint().getPort());
//            final DTGRegionEngine engine = new DTGRegionEngine(region, this);
//            if (!engine.init(rOpts)) {
//                LOG.error("Fail to init [RegionEngine: {}].", region);
//                throw Errors.REGION_ENGINE_FAIL.exception();
//            }
//
//            // update parent conf
//            final DTGRegion pRegion = parent.getRegion();
//            final RegionEpoch pEpoch = pRegion.getRegionEpoch();
//            final long version = pEpoch.getVersion();
//            pEpoch.setVersion(version + 1); // version + 1
//            pRegion.setEndKey(splitKey); // update endKey
//
//            // the following two lines of code can make a relation of 'happens-before' for
//            // read 'pRegion', because that a write to a ConcurrentMap happens-before every
//            // subsequent read of that ConcurrentMap.
//            this.regionEngineTable.put(region.getId(), engine);
//            registerRegionService(new DTGRegionService(engine));
//
//            // update local regionRouteTable
//            this.pdClient.getRegionRouteTable(splitType).splitRegion(pRegion.getId(), region);
//        } finally {
//            this.splitting.set(false);
//        }
//    }


    public DTGPlacementDriverClient getPdClient() {
        return pdClient;
    }

    protected void startMetricReporters(final long metricsReportPeriod) {
        if (metricsReportPeriod <= 0) {
            return;
        }
        if (this.kvMetricsReporter == null) {
            if (this.metricsScheduler == null) {
                // will sharing with all regionEngines
                this.metricsScheduler = StoreEngineHelper.createMetricsScheduler();
            }
            // start kv store metrics reporter
            this.kvMetricsReporter = Slf4jReporter.forRegistry(KVMetrics.metricRegistry()) //
                .prefixedWith("store_" + this.storeId) //
                .withLoggingLevel(Slf4jReporter.LoggingLevel.INFO) //
                .outputTo(LOG) //
                .scheduleOn(this.metricsScheduler) //
                .shutdownExecutorOnStop(false) //
                .build();
            this.kvMetricsReporter.start(metricsReportPeriod, TimeUnit.SECONDS);
        }
        if (this.threadPoolMetricsReporter == null) {
            if (this.metricsScheduler == null) {
                // will sharing with all regionEngines
                this.metricsScheduler = StoreEngineHelper.createMetricsScheduler();
            }
            // start threadPool metrics reporter
            this.threadPoolMetricsReporter = Slf4jReporter.forRegistry(MetricThreadPoolExecutor.metricRegistry()) //
                .withLoggingLevel(Slf4jReporter.LoggingLevel.INFO) //
                .outputTo(LOG) //
                .scheduleOn(this.metricsScheduler) //
                .shutdownExecutorOnStop(false) //
                .build();
            this.threadPoolMetricsReporter.start(metricsReportPeriod, TimeUnit.SECONDS);
        }
    }

    protected boolean initDBStore(final DTGStoreEngineOptions opts) {
        final StorageType storageType = opts.getStorageType();
        switch (storageType) {
            case RocksDB:
                return initRocksDB(opts);
            case LocalDB:
                return initLocalDB(opts);
            default:
                throw new UnsupportedOperationException("unsupported storage type: " + storageType);
        }
    }

    private boolean initLocalDB(final DTGStoreEngineOptions opts) {
        LocalDBOption loOpts = opts.getLocalDBOption();
        if(loOpts == null){
            LOG.error("LocalDB path is null!");
            return false;
        }
        String dbPath = loOpts.getDbPath();
        if (Strings.isNotBlank(dbPath)) {
            try {
                FileUtils.forceMkdir(new File(dbPath));
            } catch (final Throwable t) {
                LOG.error("Fail to make dir for dbPath {}.", dbPath);
                return false;
            }
        } else {
            LOG.error("LocalDB path is null!");
            return false;
        }
        this.dbPath = new File(loOpts.getDbPath());
        final LocalDB local = new LocalDB();
        if (!local.init(loOpts)) {
            LOG.error("Fail to init [RocksRawKVStore].");
            return false;
        }
        this.localDB = local;
        return true;
    }

    private boolean initRocksDB(final DTGStoreEngineOptions opts) {
        RocksDBOptions rocksOpts = opts.getRocksDBOptions();
        if (rocksOpts == null) {
            rocksOpts = new RocksDBOptions();
            opts.setRocksDBOptions(rocksOpts);
        }
        String dbPath = rocksOpts.getDbPath();
        if (Strings.isNotBlank(dbPath)) {
            try {
                FileUtils.forceMkdir(new File(dbPath));
            } catch (final Throwable t) {
                LOG.error("Fail to make dir for dbPath {}.", dbPath);
                return false;
            }
        } else {
            dbPath = "";
        }
        final String childPath = "db_" + this.storeId + "_" + opts.getServerAddress().getPort();
        rocksOpts.setDbPath(Paths.get(dbPath, childPath).toString());
        this.dbPath = new File(rocksOpts.getDbPath());
        System.out.println(rocksOpts.getDbPath());
        final RocksRawKVStore rocksRawKVStore = new RocksRawKVStore();
        if (!rocksRawKVStore.init(rocksOpts)) {
            LOG.error("Fail to init [RocksRawKVStore].");
            return false;
        }
        this.rawKVStore = rocksRawKVStore;
        return true;
    }
//
//    private boolean initMemoryDB(final StoreEngineOptions opts) {
//        MemoryDBOptions memoryOpts = opts.getMemoryDBOptions();
//        if (memoryOpts == null) {
//            memoryOpts = new MemoryDBOptions();
//            opts.setMemoryDBOptions(memoryOpts);
//        }
//        final MemoryRawKVStore memoryRawKVStore = new MemoryRawKVStore();
//        if (!memoryRawKVStore.init(memoryOpts)) {
//            LOG.error("Fail to init [MemoryRawKVStore].");
//            return false;
//        }
//        this.rawKVStore = memoryRawKVStore;
//        return true;
//    }

//    public void addRegion(final long fullRegionid, final long newRegionId, List<Peer> peers, final long nodeIdStart, final long relationIdStart, final long tempProStart, final EntityStoreClosure closure){
    public void addRegion(final AddRegionInfo addRegionInfo, final EntityStoreClosure closure){
        System.out.println("DTGStoreEngine\\addRegion\\   receive add region");
        long newRegionId = addRegionInfo.getNewRegionId();
        long fullRegionid = addRegionInfo.getFullRegionId();
        if (this.regionEngineTable.containsKey(newRegionId)) {
            closure.setError(Errors.CONFLICT_REGION_ID);
            closure.run(new Status(-1, "Conflict region id %d", newRegionId));
            return;
        }
        final DTGRegionEngine parentEngine = getRegionEngine(fullRegionid);
        if (parentEngine == null) {
            closure.setError(Errors.NO_REGION_FOUND);
            closure.run(new Status(-1, "RegionEngine[%s] not found", fullRegionid));
            this.splitting.set(false);
            return;
        }
        if (!parentEngine.isLeader()) {
            closure.setError(Errors.NOT_LEADER);
            closure.run(new Status(-1, "RegionEngine[%s] not leader", fullRegionid));
            this.splitting.set(false);
            return;
        }
        final DTGOperation op = new DTGOperation(new ArrayList<EntityEntry>(), OperationName.ADDREGION);
        op.setOpData(ObjectAndByte.toByteArray(addRegionInfo));
//        op.setRegionId(fullRegionid);
//        op.setStartNodeId(nodeIdStart);
//        op.setStartRelationId(relationIdStart);
//        op.setNewRegionId(newRegionId);
//        op.setStartTempProId(tempProStart);
        final Task task = new Task();
        task.setData(ByteBuffer.wrap(Serializers.getDefault().writeObject(op)));
        task.setDone(new EntityEntryClosureAdapter(closure, op));
        parentEngine.getNode().apply(task);
    }

//    public void doAddRegion(final long fullRegionid, final long newRegionId, final long nodeIdStart, final long relationIdStart, final long tempProStart){
    public void doAddRegion(AddRegionInfo addRegionInfo){
        Requires.requireNonNull(addRegionInfo.getFullRegionId(), "regionId");
        Requires.requireNonNull(addRegionInfo.getNewRegionId(), "newRegionId");
        //System.out.println("add new region : " + nodeIdStart + ", " + relationIdStart);
        List<Peer> peers = addRegionInfo.getPeers();
        //System.out.println("add new region : " + addRegionInfo.getNewRegionId());
        for(Peer peer : peers){
            //System.out.println("self endpoint :" + this.endpoint.toString());
            //System.out.println("peer endpoint :" + peer.getEndpoint().toString());
            if(this.endpoint.toString().equals(peer.getEndpoint().toString())){
                internalAddRegion(addRegionInfo);
                break;
            }
        }
    }

    private void internalAddRegion(AddRegionInfo addRegionInfo){
        long newRegionId = addRegionInfo.getNewRegionId();
        final DTGRegionEngine parent = getRegionEngine(addRegionInfo.getFullRegionId());
        final DTGRegion region = parent.getRegion().copyNull(newRegionId,
                addRegionInfo.getStartNodeId(), addRegionInfo.getStartRelationId(),addRegionInfo.getStartTempProId());
        final DTGRegionEngineOptions rOpts = parent.copyNullRegionOpts();
        region.setRegionEpoch(new RegionEpoch(-1, -1));
        rOpts.setRegionId(newRegionId);
        rOpts.setRaftGroupId(JRaftHelper.getJRaftGroupId(this.pdClient.getClusterName(), newRegionId));
        rOpts.setRaftDataPath(null);
        rOpts.setDbPath(this.dbPath.getPath());
        String baseRaftDataPath = this.storeOpts.getRaftDataPath();
        if (Strings.isBlank(baseRaftDataPath)) {
            baseRaftDataPath = "";
        }
        rOpts.setRaftDataPath(baseRaftDataPath + "raft_data_region_" + region.getId() + "_"
                + getSelfEndpoint().getPort());
        rOpts.setSaveStore(storeOpts.getSaveStore());
        final DTGRegionEngine engine = new DTGRegionEngine(region, this);
        if (!engine.init(rOpts)) {
            LOG.error("Fail to init [RegionEngine: {}].", region);
            throw Errors.REGION_ENGINE_FAIL.exception();
        }
        this.storeOpts.getRegionEngineOptionsList().add(rOpts);
        // update parent conf
        final DTGRegion pRegion = parent.getRegion();
        final RegionEpoch pEpoch = pRegion.getRegionEpoch();
        final long version = pEpoch.getVersion();
        pEpoch.setVersion(version); // version + 1

        // the following two lines of code can make a relation of 'happens-before' for
        // read 'pRegion', because that a write to a ConcurrentMap happens-before every
        // subsequent read of that ConcurrentMap.
        this.regionEngineTable.put(region.getId(), engine);
        registerRegionService(new DTGRegionService(engine));

        // update local regionRouteTable

        String serverList = "";
        for(Peer peer : region.getPeers()){
            serverList = serverList + peer.getEndpoint().toString() + ",";
        }
        final Configuration conf = new Configuration();
        conf.parse(serverList);
        // update raft route table
        RouteTable.getInstance().updateConfiguration(JRaftHelper.getJRaftGroupId(this.pdClient.getClusterName(), region.getId()), conf);

        this.pdClient.getRegionRouteTable(NODETYPE).addOrUpdateRegion(region, true);
        this.pdClient.getRegionRouteTable(RELATIONTYPE).addOrUpdateRegion(region, true);
        this.pdClient.getRegionRouteTable(TEMPORALPROPERTYTYPE).addOrUpdateRegion(region, true);
        store.getRegions().add(region);
        this.pdClient.getDTGMetadataRpcClient().updateStoreInfo(clusterId, store);

        System.out.println("add region success, id = " + region.getId());
    }

    protected boolean initAllRegionEngine(final DTGStoreEngineOptions opts, final DTGStore store) {
        Requires.requireNonNull(opts, "opts");
        Requires.requireNonNull(store, "store");
        String baseRaftDataPath = opts.getRaftDataPath();
        if (Strings.isNotBlank(baseRaftDataPath)) {
            try {
                FileUtils.forceMkdir(new File(baseRaftDataPath));
            } catch (final Throwable t) {
                LOG.error("Fail to make dir for raftDataPath: {}.", baseRaftDataPath);
                return false;
            }
        } else {
            baseRaftDataPath = "";
        }
        final Endpoint serverAddress = opts.getServerAddress();
        final List<DTGRegionEngineOptions> rOptsList = opts.getRegionEngineOptionsList();
//        RegionEngineOptions rOpt = new RegionEngineOptions();
//        rOpt.setRegionId(Constants.DEFAULT_REGION_ID);
//        rOptsList.add(0, rOpt);
        final List<DTGRegion> regionList = store.getRegions();
        //System.out.println("rOptsList size = " + rOptsList.size() + "   , regionList size = " + regionList.size());
        Requires.requireTrue(rOptsList.size() == regionList.size());

        for (int i = 0; i < rOptsList.size(); i++) {
            final DTGRegionEngineOptions rOpts = rOptsList.get(i);
            final DTGRegion region = regionList.get(i);//System.out.println("OPTS REGION ID : " + region.getId());
            if (Strings.isBlank(rOpts.getRaftDataPath())) {
                final String childPath = "raft_data_region_" + region.getId() + "_" + serverAddress.getPort();
                rOpts.setRaftDataPath(Paths.get(baseRaftDataPath, childPath).toString());
                rOpts.setDbPath(this.dbPath.getPath());
                rOpts.setSaveStore(storeOpts.getSaveStore());
            }

            Requires.requireNonNull(region.getRegionEpoch(), "regionEpoch");
            final DTGRegionEngine engine = new DTGRegionEngine(region, this);
            if (engine.init(rOpts)) {
                final DTGRegionService regionService = new DTGRegionService(engine);
                registerRegionService(regionService);
                this.regionEngineTable.put(region.getId(), engine);
            } else {
                LOG.error("Fail to init [RegionEngine: {}].", region);
                return false;
            }

        }
        return true;
    }

    private void registerRegionService(final DTGRegionService regionService) {
        final DTGRegionService preService = this.regionServiceTable.putIfAbsent(regionService.getRegionId(),
            regionService);
        if (preService != null) {
            throw new RheaRuntimeException("RegionKVService[region=" + regionService.getRegionId()
                                           + "] has already been registered, can not register again!");
        }
    }

    private void saveConfig(){
        List<DTGRegion> regions = this.store.getRegions();
        try {
            FileChannel fileChannel = (new RandomAccessFile(filePath + "\\RegionInfo", "rw")).getChannel();
            fileChannel.truncate(0);
            ByteBuffer buffer = ByteBuffer.allocate(DTGConstants.STORE_REGION_HEADER_SIZE);
            buffer.put(DTGConstants.STICKY_GENERATOR).putLong(store.getId()).putInt(regions.size()).flip();
            fileChannel.write(buffer);
            fileChannel.force(false);
//            for(DTGRegion region : regions){
//                byte[] regionByte = ObjectAndByte.toByteArray(region);
//                buffer.clear();
//                buffer = ByteBuffer.allocate(regionByte.length + 4);
//                buffer.putInt(regionByte.length).put(regionByte).flip();
//                fileChannel.write(buffer);
//                fileChannel.force(false);
//            }
            buffer.clear();
            buffer = ByteBuffer.allocate(8*regions.size());
            for(DTGRegion region : regions){
                buffer.putLong(region.getId());
            }
            buffer.flip();
            fileChannel.write(buffer);
            fileChannel.force(false);

            buffer.clear();
            buffer.put( DTGConstants.CLEAN_GENERATOR );
            buffer.limit( 1 );
            buffer.flip();
            fileChannel.position( 0 );
            fileChannel.write( buffer );
            fileChannel.force( false );
            fileChannel.close();
            fileChannel = null;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    private List<DTGRegion> readConfig(){
//        List<DTGRegion> regions = new ArrayList<DTGRegion>();
//        try {
//            FileChannel fileChannel = (new RandomAccessFile(filePath + "\\RegionInfo", "rw")).getChannel();
//            ByteBuffer buffer = ByteBuffer.allocate(DTGConstants.STORE_REGION_HEADER_SIZE);
//            int read = fileChannel.read(buffer);
//            if(read != DTGConstants.STORE_REGION_HEADER_SIZE){
//                throw new RegionStoreException("read region error, unable read header");
//            }
//            buffer.flip();
//            byte storageStatus = buffer.get();
//            if ( storageStatus != DTGConstants.CLEAN_GENERATOR ){
//                throw new RegionStoreException("Sticky file[ " +
//                        filePath + "\\RegionInfo] delete this id file and build a new one");
//            }
//            if(buffer.getLong() != storeId){
//                throw new RegionStoreException("file [ " +
//                        filePath + "\\RegionInfo] doesn't belong to this store");
//            }
//            int regionNum = buffer.getInt();
//            int readPosition = DTGConstants.STORE_REGION_HEADER_SIZE;
//            for(int i = 0; i < regionNum; i++){
//                fileChannel.position(readPosition);
//                buffer.clear();
//                buffer = ByteBuffer.allocate(4);
//                int bytesRead = fileChannel.read(buffer);
//                assert bytesRead ==4;
//                buffer.flip();
//                int regionByteSize = buffer.getInt();
//                buffer.clear();
//                buffer =ByteBuffer.allocate(regionByteSize);
//                readPosition = readPosition  + 4;
//                fileChannel.position(readPosition);
//                bytesRead = fileChannel.read(buffer);
//                assert bytesRead == regionByteSize;
//                byte[] regionByte = new byte[regionByteSize];
//                buffer.get(regionByte);
//                regions.add((DTGRegion)ObjectAndByte.toObject(regionByte));
//                readPosition = readPosition + regionByteSize;
//            }
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (RegionStoreException e) {
//            e.printStackTrace();
//        }
//        return regions;
//    }

    private List<Long> readConfig(){
        if(storeId == DTGConstants.NULL_STORE){
            return null;
        }
        File file = new File(filePath + "\\RegionInfo");
        if(!file.exists()){
            return null;
        }
        List<Long> regionsId = new ArrayList<Long>();
        try {
            FileChannel fileChannel = (new RandomAccessFile(filePath + "\\RegionInfo", "rw")).getChannel();
            ByteBuffer buffer = ByteBuffer.allocate(DTGConstants.STORE_REGION_HEADER_SIZE);
            int read = fileChannel.read(buffer);
            if(read != DTGConstants.STORE_REGION_HEADER_SIZE){
                throw new RegionStoreException("read region error, unable read header");
            }
            buffer.flip();
            byte storageStatus = buffer.get();
            if ( storageStatus != DTGConstants.CLEAN_GENERATOR ){
                throw new RegionStoreException("Sticky file[ " +
                        filePath + "\\RegionInfo] delete this id file and build a new one");
            }
            if(buffer.getLong() != storeId){
                throw new RegionStoreException("file [ " +
                        filePath + "\\RegionInfo] doesn't belong to this store");
            }
            int regionNum = buffer.getInt();
            fileChannel.position(DTGConstants.STORE_REGION_HEADER_SIZE);
            buffer.clear();
            buffer = ByteBuffer.allocate(8*regionNum);
            int bytesRead = fileChannel.read(buffer);
            assert bytesRead == 8 * regionNum;
            buffer.flip();
            for(int i = 0; i < regionNum; i++){
                long regionId = buffer.getLong();
                regionsId.add(regionId);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (RegionStoreException e) {
            e.printStackTrace();
        }
        return regionsId;
    }


    private void saveStoreOpts(){
        try {
                FileChannel fileChannel = (new RandomAccessFile(filePath + "\\StoreInfo", "rw")).getChannel();
            fileChannel.truncate(0);
            ByteBuffer buffer = ByteBuffer.allocate(DTGConstants.STORE_OPTS_HEADER_SIZE);
            byte[] optsByte = ObjectAndByte.toByteArray(this.storeOpts.getRegionEngineOptionsList());
            buffer.put(DTGConstants.STICKY_GENERATOR).putLong(store.getId()).putInt(optsByte.length).flip();
            fileChannel.write(buffer);
            fileChannel.force(false);

            buffer.clear();
            buffer = ByteBuffer.allocate(optsByte.length);
            buffer.put(optsByte).flip();
            fileChannel.write(buffer);
            fileChannel.force(false);

            buffer.clear();
            buffer.put( DTGConstants.CLEAN_GENERATOR );
            buffer.limit( 1 );
            buffer.flip();
            fileChannel.position( 0 );
            fileChannel.write( buffer );
            fileChannel.force( false );
            fileChannel.close();
            fileChannel = null;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void readStoreOpts(){
        try {
            File file = new File(filePath + "\\StoreInfo");
            if(!file.exists()){
                return;
            }
            FileChannel fileChannel = (new RandomAccessFile(filePath + "\\StoreInfo", "rw")).getChannel();
            ByteBuffer buffer = ByteBuffer.allocate(DTGConstants.STORE_OPTS_HEADER_SIZE);
            int read = fileChannel.read(buffer);
            if(read != DTGConstants.STORE_OPTS_HEADER_SIZE){
                throw new RegionStoreException("read region error, unable read header");
            }
            buffer.flip();
            byte storageStatus = buffer.get();
            if ( storageStatus != DTGConstants.CLEAN_GENERATOR ){
                throw new RegionStoreException("Sticky file[ " +
                        filePath + "\\RegionInfo] delete this id file and build a new one");
            }
            if(buffer.getLong() != storeId){
                throw new RegionStoreException("file [ " +
                        filePath + "\\RegionInfo] doesn't belong to this store");
            }
            int optsByteSize = buffer.getInt();
            buffer.clear();
            buffer =ByteBuffer.allocate(optsByteSize);
            fileChannel.position(DTGConstants.STORE_OPTS_HEADER_SIZE);
            int bytesRead = fileChannel.read(buffer);
            assert bytesRead == optsByteSize;
            byte[] optsByte = new byte[optsByteSize];
            buffer.get(optsByte);
            this.storeOpts.setRegionEngineOptionsList((List<DTGRegionEngineOptions>)ObjectAndByte.toObject(optsByte));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (RegionStoreException e) {
            e.printStackTrace();
        }
    }

    public RpcCaller getRpcCaller() {
        return rpcCaller;
    }

    public DTGSaveStore getSaveStore() {
        return saveStore;
    }

    public DTGRegionService getRegionService(long regionId){
        return this.regionServiceTable.get(regionId);
    }

    @Override
    public String toString() {
        return "StoreEngine{storeId=" + storeId + ", startTime=" + startTime + ", dbPath=" + dbPath + ", storeOpts="
               + storeOpts + ", started=" + started + '}';
    }
}

//package UserClient;
//
//import Communication.DTGRpcService;
//import Communication.DefaultDTGRpcService;
//import PlacementDriver.DTGPlacementDriverClient;
//import PlacementDriver.DefaultPlacementDriverClient;
//import Region.DTGRegion;
//import com.alipay.sofa.jraft.Lifecycle;
//import com.alipay.sofa.jraft.rhea.options.BatchingOptions;
//import com.alipay.sofa.jraft.rhea.options.RpcOptions;
//import com.alipay.sofa.jraft.rhea.util.Constants;
//import com.alipay.sofa.jraft.rhea.util.Strings;
//import com.alipay.sofa.jraft.rhea.util.concurrent.AffinityNamedThreadFactory;
//import com.alipay.sofa.jraft.rhea.util.concurrent.NamedThreadFactory;
//import com.alipay.sofa.jraft.rhea.util.concurrent.disruptor.Dispatcher;
//import com.alipay.sofa.jraft.rhea.util.concurrent.disruptor.TaskDispatcher;
//import com.alipay.sofa.jraft.rhea.util.concurrent.disruptor.WaitStrategyType;
//import com.alipay.sofa.jraft.util.Endpoint;
//import com.alipay.sofa.jraft.util.Requires;
//import com.alipay.sofa.jraft.util.Utils;
//import options.DTGPlacementDriverOptions;
//import options.DTGStoreEngineOptions;
//import options.DTGStoreOptions;
//import options.UserClientStoreOption;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import storage.DTGStoreEngine;
//
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.ThreadFactory;
//
///**
// * @author :jinkai
// * @date :Created in 2019/12/6 15:55
// * @description：
// * @modified By:
// * @version:
// */
//
//public class UserClientStore implements Lifecycle<UserClientStoreOption> {
//
//    private static final Logger LOG = LoggerFactory.getLogger(UserClientStore.class);
//
//    private DTGPlacementDriverClient pdClient;
//    private Dispatcher kvDispatcher;
//    //private RheaKVRpcService rheaKVRpcService;
//    private DTGRpcService dtgRpcService;
//    //private DTGBatching applyBatching;
//    private BatchingOptions batchingOpts;
//    private Map<String, List<DTGRegion>> waitCommitMap;
//    private UserClientStoreOption opts;
//    private boolean onlyLeaderRead;
//    private volatile boolean started;
//    private int failoverRetries;
//    private long futureTimeoutMillis;
//
//    @Override
//    public boolean init(UserClientStoreOption opts) {
//        if (this.started) {
//            LOG.info("[DefaultRheaKVStore] already started.");
//            return true;
//        }
//        this.opts = opts;
//        DTGPlacementDriverOptions pdopts = opts.getPlacementDriverOptions();
//        Requires.requireNonNull(pdopts, "opts.placementDriverOptions");
//        Requires.requireNonNull(clusterName, "opts.clusterName");
//        if (Strings.isBlank(pdopts.getInitialServerList())) {
//            // if blank, extends parent's value
//            pdopts.setInitialServerList(opts.getInitialServerList());
//        }
//        pdClient = new DefaultPlacementDriverClient(opts.getClusterId(), clusterName, opts.isRemotePd());
//        if (!this.pdClient.init(pdopts)) {
//            LOG.error("Fail to init [PlacementDriverClient].");
//            return false;
//        }
//        final DTGStoreEngineOptions stOpts = opts.getStoreEngineOptions();
//        if (stOpts != null) {
//            stOpts.setInitialServerList(opts.getInitialServerList());
//            this.storeEngine = new DTGStoreEngine(this.pdClient);
//            if (!this.storeEngine.init(stOpts)) {
//                LOG.error("Fail to init [StoreEngine].");
//                return false;
//            }
//        }
//        final Endpoint selfEndpoint = this.storeEngine == null ? null : this.storeEngine.getSelfEndpoint();
//
//        final RpcOptions rpcOpts = opts.getRpcOptions();
//        this.dtgRpcService = new DefaultDTGRpcService(this.pdClient, selfEndpoint) {
//
//            @Override
//            public Endpoint getLeader(final long regionId, final boolean forceRefresh, final long timeoutMillis) {
//                final Endpoint leader = getLeaderByRegionEngine(regionId);
//                if (leader != null) {
//                    return leader;
//                }
//                return super.getLeader(regionId, forceRefresh, timeoutMillis);
//            }
//        };
//        if (!this.dtgRpcService.init(rpcOpts)) {
//            LOG.error("Fail to init [RheaKVRpcService].");
//            return false;
//        }
//        this.failoverRetries = opts.getFailoverRetries();
//        this.futureTimeoutMillis = opts.getFutureTimeoutMillis();
//        this.onlyLeaderRead = opts.isOnlyLeaderRead();
//        if (opts.isUseParallelExecutor()) {
//            final int numWorkers = Utils.cpus();
//            final int bufSize = numWorkers << 4;
//            final String name = "parallel-kv-executor";
//            final ThreadFactory threadFactory = Constants.THREAD_AFFINITY_ENABLED
//                    ? new AffinityNamedThreadFactory(name, true) : new NamedThreadFactory(name, true);
//            this.kvDispatcher = new TaskDispatcher(bufSize, numWorkers, WaitStrategyType.LITE_BLOCKING_WAIT, threadFactory);
//        }
////        final int numWorkers = Utils.cpus();
////        final int bufSize = numWorkers << 4;
////        final String name = "parallel-kv-executor";
////        final ThreadFactory threadFactory = Constants.THREAD_AFFINITY_ENABLED
////                ? new AffinityNamedThreadFactory(name, true) : new NamedThreadFactory(name, true);
////        this.kvDispatcher = new TaskDispatcher(bufSize, numWorkers, WaitStrategyType.LITE_BLOCKING_WAIT, threadFactory);
////        this.batchingOpts = opts.getBatchingOptions();
//        waitCommitMap = new HashMap<>();
////        this.applyBatching = new DTGBatching(EntityEvent::new, "put_batching",
////                new PutBatchingHandler("put"));
//        return this.started = true;
//    }
//
//    @Override
//    public void shutdown() {
//
//    }
//}

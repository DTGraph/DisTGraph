package config;

import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rhea.options.*;
import com.alipay.sofa.jraft.rhea.options.configured.MultiRegionRouteTableOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.RocksDBOptionsConfigured;
import com.alipay.sofa.jraft.rhea.options.configured.RpcOptionsConfigured;
import com.alipay.sofa.jraft.rhea.storage.StorageType;
import com.alipay.sofa.jraft.rhea.util.Constants;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.util.Endpoint;
import options.*;
import com.alipay.sofa.jraft.rhea.pd.options.PlacementDriverServerOptions;
import org.neo4j.unsafe.impl.batchimport.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author :jinkai
 * @date :Created in 2019/10/23 20:58
 * @description:
 * @modified By:
 * @version:
 */

public class DefaultOptions {
    public static final int    TIMEOUTMS                   =  2000;
    public static final int    MAXRETRY                    =  5;
    public static final String DB_PATH                     = "DTG_DB" + File.separator;
    public static final String RAFT_DATA_PATH              = "raft_data" + File.separator;
    public static final String ALL_NODE_ADDRESSES          = "127.0.0.1:8184,127.0.0.1:8185,127.0.0.1:8186";
    public static final String CLUSTER_NAME                = "DTG_DEFAULT";
    public static final String PDGROUPID                   = "METADATA_SAVE";
    public static final String INITIALSERVERLIST           = "127.0.0.1:8184,127.0.0.1:8185,127.0.0.1:8186" ;
    public static final String INITIALPDSERVERLIST         = "127.0.0.1:8181,127.0.0.1:8182,127.0.0.1:8183";
    public static final int    MINIDBATCHSIZE              = 50;
    public static final int    IDBATCHSIZE                 = 5000;
    public static final int    CLUSTERID                   = 1;
    public static final int    PDCLUSTERID                 = 0;
    public static final int    GRABSIZE                    = 50000;
    public static final int    BATCHSIZE                   = 100;
    public static final int    PIPELINECOREPOOLSIZE        = 4;
    public static final int    PIPELINEMAXMUMPOOLSIZE      = 4;

    public static final int    DEFAULTREGIONNODESIZE       = 5000;
    public static final int    DEFAULTREGIONRELATIONSIZE   = 5000;

    public static final int    DEFAULTSTARTTIME            = 0;
    public static final int    INITREGIONNUMBER            = 3;

    public static CliOptions defaultCliOptios(){
        CliOptions opts = new CliOptions();
        opts.setMaxRetry(MAXRETRY);
        opts.setTimeoutMs(TIMEOUTMS);
        return opts;
    }

    public static RpcOptions defaultRpcOptions(){
        RpcOptions rpcOpts = RpcOptionsConfigured.newDefaultConfig();
        rpcOpts.setCallbackExecutorCorePoolSize(0);
        rpcOpts.setCallbackExecutorMaximumPoolSize(0);
        return rpcOpts;
    }

    public static DTGPlacementDriverOptions defaultDTGPlacementDriverOptions(){
        DTGPlacementDriverOptions opts = new DTGPlacementDriverOptions();
        opts.setCliOptions(defaultCliOptios());
        opts.setPdRpcOptions(defaultRpcOptions());
//        final List<RegionRouteTableOptions> regionRouteTableOptionsList = MultiRegionRouteTableOptionsConfigured
//                .newConfigured() //
//                .withInitialServerList(1L /* default id */, INITIALSERVERLIST) //
//                .withInitNodeStartId(1L, 0)
//                .withInitRelationStartId(1L, 0)
//                .config();
//        opts.setRegionRouteTableOptionsList(regionRouteTableOptionsList);
        opts.setInitialPdServerList(INITIALPDSERVERLIST);
        opts.setMinIdBatchSize(MINIDBATCHSIZE);
        opts.setPdGroupId(PDGROUPID + "-" + PDCLUSTERID);
        return opts;
    }

    public static DTGPlacementDriverOptions defaultDTGPlacementDriverOptionsWithoutRegion(){
        DTGPlacementDriverOptions opts = new DTGPlacementDriverOptions();
//        final List<RegionRouteTableOptions> regionRouteTableOptionsList = new ArrayList<>();
//        opts.setRegionRouteTableOptionsList(regionRouteTableOptionsList);
        opts.setCliOptions(defaultCliOptios());
        opts.setPdRpcOptions(defaultRpcOptions());
        opts.setInitialPdServerList(INITIALPDSERVERLIST);
        opts.setMinIdBatchSize(MINIDBATCHSIZE);
        opts.setPdGroupId(PDGROUPID + "-" + PDCLUSTERID);
        return opts;
    }

    public static DTGStoreOptions defaultDTGStoreOptions(){
        DTGStoreOptions opts = new DTGStoreOptions();
        opts.setBatchingOptions(defaultBatchingOptions());
        opts.setRpcOptions(defaultRpcOptions());
        opts.setPlacementDriverOptions(defaultDTGPlacementDriverOptions());
        opts.setFailoverRetries(MAXRETRY);
        return opts;
    }

    public static DTGStoreOptions defaultPDDTGStoreOptions(String ip, int port, String path){
        DTGStoreOptions opts = defaultDTGStoreOptions();
        opts.setClusterId(PDCLUSTERID);
        opts.setClusterName(PDGROUPID);
        opts.setInitialServerList(INITIALPDSERVERLIST);
        //opts.setStoreEngineOptions(defaultPDStoreEngineOptions(ip, port, path));
        opts.getPlacementDriverOptions().setLocalClient(false);
        opts.setRemotePd(true);
        opts.getPlacementDriverOptions().setRemotePd(true);
        return opts;
    }

    public static DTGStoreOptions defaultClusterDTGStoreOptions(String ip, int port, String path){
        DTGStoreOptions opts = defaultDTGStoreOptions();
        opts.setClusterName(CLUSTER_NAME);
        opts.setClusterId(CLUSTERID);
        opts.setInitialServerList(INITIALSERVERLIST);
        opts.setStoreEngineOptions(defaultClusterStoreEngineOptions(ip, port, path));
        opts.getPlacementDriverOptions().setLocalClient(false);
        opts.setRemotePd(false);
        opts.getPlacementDriverOptions().setRemotePd(false);
        return opts;
    }

    public static DTGStoreOptions defaultClientDTGStoreOptions(String ip, int port, String path){
        DTGStoreOptions opts = new DTGStoreOptions();
        opts.setClusterId(CLUSTERID);
        opts.setClusterName(CLUSTER_NAME);
        opts.setBatchingOptions(defaultBatchingOptions());
        opts.setRpcOptions(defaultRpcOptions());
        opts.setPlacementDriverOptions(defaultDTGPlacementDriverOptions());
        opts.setFailoverRetries(MAXRETRY);

        opts.setInitialServerList(ip + ":" + port);
        //opts.setStoreEngineOptions(defaultClusterStoreEngineOptions(ip, port, path));
        opts.getPlacementDriverOptions().setLocalClient(true);
        opts.setRemotePd(false);
        opts.getPlacementDriverOptions().setRemotePd(false);
        return opts;
    }

    public static BatchingOptions defaultBatchingOptions(){
        return new BatchingOptions();
    }

    public static DTGPlacementDriverServerOptions defaultDTGPlacementDriverServerOptions(String ip, int port, String path){
        DTGPlacementDriverServerOptions opts = new DTGPlacementDriverServerOptions();
        opts.setDtgPlacementDriverOptions(defaultDTGPlacementDriverOptions());
        opts.setDTGStoreOptions(defaultPDDTGStoreOptions(ip, port, path));
        opts.setIdGeneratorOptions(defaultIdGeneratorOptions(path));
        opts.setRheaKVStoreOptions(defaultRheaKVStoreOptions(ip, port, path));
        opts.setPipelineCorePoolSize(PIPELINECOREPOOLSIZE);
        opts.setPipelineMaximumPoolSize(PIPELINEMAXMUMPOOLSIZE);
        return opts;
    }

    public static IdGeneratorOptions defaultIdGeneratorOptions(String path){
        IdGeneratorOptions opts = new IdGeneratorOptions();
        opts.setBatchSize(IDBATCHSIZE);
        opts.setGrabSize(GRABSIZE);
        opts.setIdGeneratorPath(path);
        return opts;
    }

    public static RheaKVStoreOptions defaultRheaKVStoreOptions(String ip, int port, String localdbPath){
        RheaKVStoreOptions opts = new RheaKVStoreOptions();
        opts.setClusterId(PDCLUSTERID);
        opts.setClusterName(PDGROUPID);
        opts.setPlacementDriverOptions(defaultPlacementDriverOptions());
        opts.setInitialServerList(INITIALPDSERVERLIST);
        opts.setStoreEngineOptions(defaultStoreEngineOptions(ip, port, localdbPath));
        return opts;
    }

    public static PlacementDriverOptions defaultPlacementDriverOptions(){
        PlacementDriverOptions opts = new PlacementDriverOptions();
        final List<RegionRouteTableOptions> regionRouteTableOptionsList = MultiRegionRouteTableOptionsConfigured
                .newConfigured() //
                .withInitialServerList(0L /* default id */, INITIALPDSERVERLIST) //
                .config();
        opts.setRegionRouteTableOptionsList(regionRouteTableOptionsList);
        opts.setFake(true);
        return opts;
    }

    public static StoreEngineOptions defaultStoreEngineOptions(String ip, int port, String localdbPath){
        StoreEngineOptions opts = new StoreEngineOptions();
        opts.setStorageType(StorageType.RocksDB);
        opts.setRocksDBOptions(RocksDBOptionsConfigured.newConfigured().withDbPath(localdbPath + "\\pd" + ip + port).config());
        opts.setRaftDataPath(localdbPath+"\\"+ RAFT_DATA_PATH);
        opts.setServerAddress(new Endpoint(ip , port));

        RegionEngineOptions rOpts = new RegionEngineOptions();
        rOpts.setRegionId(0L);
        List<RegionEngineOptions> rOptsList = Lists.newArrayList();
        rOptsList.add(rOpts);
        opts.setRegionEngineOptionsList(rOptsList);

        return opts;
    }

    public static DTGStoreEngineOptions defaultPDStoreEngineOptions(String ip, int port, String localdbPath){
        DTGStoreEngineOptions opts = new DTGStoreEngineOptions();
        opts.setStorageType(StorageType.RocksDB);
        opts.setRocksDBOptions(RocksDBOptionsConfigured.newConfigured().withDbPath(localdbPath + "\\pd" + ip + port).config());
        opts.setRaftDataPath(localdbPath+"\\"+ RAFT_DATA_PATH);
        opts.setServerAddress(new Endpoint(ip , port));
        return opts;
    }

    public static DTGStoreEngineOptions defaultClusterStoreEngineOptions(String ip, int port, String localdbPath){
        DTGStoreEngineOptions opts = new DTGStoreEngineOptions();
        opts.setStorageType(StorageType.LocalDB);
        opts.setLocalDBOption(defaultLocalDBOption(localdbPath + "\\" +DB_PATH));
        opts.setRaftDataPath(localdbPath+"\\"+ RAFT_DATA_PATH);
        opts.setServerAddress(new Endpoint(ip , port));


        List<DTGRegionEngineOptions> rOptsList = Lists.newArrayList();
        for(long i = DTGConstants.FIRST_REGION_ID, t = 1; t < INITREGIONNUMBER; i++, t++){
            DTGRegionEngineOptions rOpts = new DTGRegionEngineOptions();
            rOpts.setRegionId(i);
            rOpts.setInitNodeId(t * DEFAULTREGIONNODESIZE);
            rOpts.setInitRelationId(t * DEFAULTREGIONRELATIONSIZE);
            rOptsList.add(rOpts);
        }
        opts.setRegionEngineOptionsList(rOptsList);

        return opts;
    }

    public static LocalDBOption defaultLocalDBOption(String localdbPath){
        LocalDBOption opts = new LocalDBOption();
        opts.setDbPath(localdbPath);
        return opts;
    }

    public static PlacementDriverServerOptions defaultPlacementDriverServerOptions(String ip, int port, String path){
        PlacementDriverServerOptions opts = new PlacementDriverServerOptions();
        opts.setRheaKVStoreOptions(defaultRheaKVStoreOptions(ip, port, path));
        opts.setPipelineCorePoolSize(PIPELINECOREPOOLSIZE);
        opts.setPipelineMaximumPoolSize(PIPELINEMAXMUMPOOLSIZE);
        return opts;
    }

    public static RegionRouteTableOptions DefaultRegionRouteTableOptions(){
        RegionRouteTableOptions opts = new RegionRouteTableOptions();
        opts.setRegionId(Constants.DEFAULT_REGION_ID);
        opts.setInitialServerList(INITIALPDSERVERLIST);
        opts.setInitNodeId( -2);
        opts.setInitRelationId(-2);
        return opts;
    }

}

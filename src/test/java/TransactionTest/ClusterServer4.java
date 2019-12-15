package TransactionTest;

import UserClient.DTGSaveStore;
import com.alipay.sofa.jraft.rhea.options.RegionEngineOptions;
import com.alipay.sofa.jraft.rhea.storage.StorageType;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.util.Endpoint;
import config.DefaultOptions;
import options.DTGStoreEngineOptions;
import options.DTGStoreOptions;
import storage.ClusterServer;

import java.util.List;

/**
 * @author :jinkai
 * @date :Created in 2019/10/24 14:00
 * @description:
 * @modified By:
 * @version:
 */

public class ClusterServer4 {

    public static void main(String[] args){
        DTGStoreOptions opts = new DTGStoreOptions();

        String path = "D:\\garbage\\8087";
        String ip = "127.0.0.1";
        int port = 8187;

        opts.setBatchingOptions(DefaultOptions.defaultBatchingOptions());
        opts.setRpcOptions(DefaultOptions.defaultRpcOptions());
        opts.setPlacementDriverOptions(DefaultOptions.defaultDTGPlacementDriverOptionsWithoutRegion());
        opts.setFailoverRetries(DefaultOptions.MAXRETRY);

        opts.setClusterName(DefaultOptions.CLUSTER_NAME);
        opts.setClusterId(DefaultOptions.CLUSTERID);
        opts.setInitialServerList(DefaultOptions.INITIALSERVERLIST);
        opts.getPlacementDriverOptions().setLocalClient(false);
        opts.setRemotePd(false);
        opts.getPlacementDriverOptions().setRemotePd(false);

        DTGStoreEngineOptions sopts = new DTGStoreEngineOptions();
        sopts.setStorageType(StorageType.LocalDB);
        sopts.setLocalDBOption(DefaultOptions.defaultLocalDBOption(path + "\\" +DefaultOptions.DB_PATH));
        sopts.setRaftDataPath(path+"\\"+ DefaultOptions.RAFT_DATA_PATH);
        sopts.setServerAddress(new Endpoint(ip , port));
        List<RegionEngineOptions> rOptsList = Lists.newArrayList();
        sopts.setRegionEngineOptionsList(rOptsList);

        opts.setStoreEngineOptions(sopts);

        DTGSaveStore store = new DTGSaveStore();
        store.init(opts);
        System.out.println("server start OK");
    }
}

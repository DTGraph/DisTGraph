package storage;

import UserClient.DTGDatabase;
import UserClient.DTGSaveStore;
import options.DTGStoreOptions;

import static config.DefaultOptions.defaultClusterDTGStoreOptions;

/**
 * @author :jinkai
 * @date :Created in 2019/10/24 12:53
 * @description:
 * @modified By:
 * @version:
 */

public class ClusterServer {

    final DTGStoreOptions opts;

    private DTGSaveStore store;

    public ClusterServer(String ip, int port, String path){
        opts = defaultClusterDTGStoreOptions(ip, port, path);
    }

    public void setRaftPath(String path){
        opts.getStoreEngineOptions().setRaftDataPath(path);
    }

    public void start(){
        this.store = new DTGSaveStore();
        store.init(opts);
        System.out.println("server start OK");
    }

    public void shutdown(){
        this.store.shutdown();
    }

    public DTGDatabase getDB(){
        this.start();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return this.store.getDB();
    }
}

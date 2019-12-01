package PlacementDriver;

import PlacementDriver.KVPD.DTGPDPlacementDriverServer;
import UserClient.DTGSaveStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import options.DTGPlacementDriverServerOptions;
import options.DTGStoreOptions;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CopyOnWriteArrayList;

import static config.DefaultOptions.defaultDTGPlacementDriverServerOptions;
import static config.DefaultOptions.defaultPDDTGStoreOptions;

/**
 * @author :jinkai
 * @date :Created in 2019/10/24 12:53
 * @description:
 * @modified By:
 * @version:
 */

public class PdServer {

    //private static final String[] CONF = { "/pd/pd_1.yaml", "/pd/pd_2.yaml", "/pd/pd_3.yaml" };
    private final String rootPath;
    private volatile String tempDbPath;
    private volatile String tempRaftPath;
    private DTGPDPlacementDriverServer pdServer;
    private final int port;
    private final String ip;
    private final boolean isLeader;

    public PdServer(String ip, int port, String path, boolean isLeader){
        //opts = defaultPDDTGStoreOptions(ip, port);
        this.rootPath = path;
        this.ip = ip;
        this.port = port;
        this.isLeader = isLeader;
    }

    public void start() throws IOException, InterruptedException {
        System.out.println("PlacementDriverServer init ...");
        File file = new File(rootPath + "\\pd_db");
        if (file.exists()) {
            FileUtils.forceDelete(file);
        }
        file = new File(rootPath + "\\pd_db");
        if (file.mkdir()) {
            this.tempDbPath = file.getAbsolutePath();
            System.out.println("make dir: " + this.tempDbPath);
        }
        file = new File(rootPath + "pd_raft");
        if (file.exists()) {
            FileUtils.forceDelete(file);
        }
        file = new File(rootPath + "pd_raft");
        if (file.mkdir()) {
            this.tempRaftPath = file.getAbsolutePath();
            System.out.println("make dir: " + this.tempRaftPath);
        }
        final DTGPlacementDriverServerOptions opts = defaultDTGPlacementDriverServerOptions(ip, port, rootPath);
        opts.getDtgPlacementDriverOptions().setRemotePd(true);
        this.pdServer = new DTGPDPlacementDriverServer();
        if (pdServer.init(opts)) {
            System.out.println("Pd server is ready");
        } else {
            System.err.println("Fail to init [DTGPlacementDriverServer] witch conf: ip = " + ip + " port = " + port);
        }
        if(isLeader){
            Thread.sleep(20000);
            pdServer.awaitReady(10000);
        }
    }

    public void shutdown() throws IOException {
        System.out.println("PlacementDriverServer shutdown ...");
        this.pdServer.shutdown();
        if (this.tempDbPath != null) {
            System.out.println("removing dir: " + this.tempDbPath);
            FileUtils.forceDelete(new File(this.tempDbPath));
        }
        if (this.tempRaftPath != null) {
            System.out.println("removing dir: " + this.tempRaftPath);
            FileUtils.forceDelete(new File(this.tempRaftPath));
        }
        System.out.println("PlacementDriverServer shutdown complete");
    }
}

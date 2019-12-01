package PlacementDriver;

import PlacementDriver.KVPD.DTGPDPlacementDriverServer;
import com.alipay.sofa.jraft.rhea.options.PlacementDriverServerOptions;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import options.DTGPlacementDriverServerOptions;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CopyOnWriteArrayList;

import static config.DefaultOptions.defaultDTGPlacementDriverServerOptions;

/**
 * @author :jinkai
 * @date :Created in 2019/10/25 12:12
 * @description:
 * @modified By:
 * @version:
 */

public class GroupPd {

    private final String rootPath;
    private final String nodesAddress;
    private volatile String tempDbPath;
    private volatile String tempRaftPath;
    private CopyOnWriteArrayList<DTGPDPlacementDriverServer> pdServerList = new CopyOnWriteArrayList<>();
    private static final String[]                       CONF         = { "/pd/pd_1.yaml", "/pd/pd_2.yaml",
            "/pd/pd_3.yaml"                                         };

    public GroupPd(String rootPath, String nodesAddress){
        this.rootPath = rootPath;
        this.nodesAddress = nodesAddress;
    }

    public void start() throws IOException {
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
        String[] address = nodesAddress.split(",");
        int i = 0;
        for(String add : address){
            String[] socket = add.split(":");
            final DTGPlacementDriverServerOptions opts = defaultDTGPlacementDriverServerOptions(socket[0], Integer.parseInt(socket[1]), rootPath+"\\" + socket);
            opts.getDtgPlacementDriverOptions().setRemotePd(true);

//            final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
//            final InputStream in = GroupPd.class.getResourceAsStream(CONF[i++]);
//            DTGPlacementDriverServerOptions opts = mapper.readValue(in, DTGPlacementDriverServerOptions.class);

            DTGPDPlacementDriverServer pdServer = new DTGPDPlacementDriverServer();
            if (pdServer.init(opts)) {
                pdServerList.add(pdServer);
            } else {
                System.err.println("Fail to init [DTGPlacementDriverServer] witch conf: ip = " + socket[0] + " port = " + Integer.parseInt(socket[1]));
            }
        }

        pdServerList.get(0).awaitReady(10000);
        System.out.println("Pd server is ready");
    }

    public void shutdown() throws IOException {
        System.out.println("PlacementDriverServer shutdown ...");
        for (final DTGPDPlacementDriverServer server : this.pdServerList) {
            server.shutdown();
        }
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

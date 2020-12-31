package TransactionTest;

import config.DTGConstants;
import org.junit.Test;
import storage.ClusterServer;

import java.io.IOException;

/**
 * @author :jinkai
 * @date :Created in 2019/10/24 14:00
 * @description:
 * @modified By:
 * @version:
 */

public class ClusterServer1 {

    public static void main(String[] args) {
        ClusterServer server = new ClusterServer(DTGConstants.SERVER1, DTGConstants.SERVER1PORT, "D:\\garbage\\8084");
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {System.out.println("START SHUTDOWN");
                server.shutdown();
        }));
    }
}

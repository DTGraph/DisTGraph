package TransactionTest;

import config.DTGConstants;
import org.junit.Test;
import storage.ClusterServer;

/**
 * @author :jinkai
 * @date :Created in 2019/10/24 14:00
 * @description:
 * @modified By:
 * @version:
 */

public class ClusterServer2 {

    public static void main(String[] args){
        ClusterServer server = new ClusterServer(DTGConstants.SERVER2, DTGConstants.SERVER2PORT, "D:\\garbage\\8085");
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {System.out.println("START SHUTDOWN");
                server.shutdown();
        }));
    }
}

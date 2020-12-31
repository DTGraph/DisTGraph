package TransactionTest;

import config.DTGConstants;
import storage.ClusterServer;

/**
 * @author :jinkai
 * @date :Created in 2019/10/24 14:00
 * @description:
 * @modified By:
 * @version:
 */

public class ClusterServer5 {

    public static void main(String[] args){
        ClusterServer server = new ClusterServer("127.0.0.1", 8187, "D:\\garbage\\8187");
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {System.out.println("START SHUTDOWN");
                server.shutdown();
        }));
    }
}

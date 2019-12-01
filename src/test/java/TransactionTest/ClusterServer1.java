package TransactionTest;

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
        ClusterServer server = new ClusterServer("127.0.0.1", 8184, "D:\\garbage\\8084");
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                server.shutdown();
        }));
    }
}

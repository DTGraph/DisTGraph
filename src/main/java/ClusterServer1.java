import storage.ClusterServer;

/**
 * @author :jinkai
 * @date :Created in 2019/10/24 14:00
 * @description:
 * @modified By:
 * @version:
 */

public class ClusterServer1 {
//java -jar Dis-TGraph-1.0-SNAPSHOT.jar "127.0.0.1" "D:\\garbage\\8085"
    public static void main(String[] args) {
        ClusterServer server = new ClusterServer(args[0], 8184, args[1]);
        //ClusterServer server = new ClusterServer("182.129.214.222", 8184, "D:\\garbage\\8084");

        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {System.out.println("START SHUTDOWN");
                server.shutdown();
        }));
    }
}

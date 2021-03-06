package TransactionTest;

import PlacementDriver.GroupPd;
import PlacementDriver.PdServer;
import org.junit.Test;

import java.io.IOException;

/**
 * @author :jinkai
 * @date :Created in 2019/10/24 13:50
 * @description:
 * @modified By:
 * @version:
 */

public class pdServerAll {

//    @Test
//    public void addPd() throws IOException {
//        final GroupPd server = new GroupPd("D:\\garbage","127.0.0.1:8181,127.0.0.1:8182,127.0.0.1:8183");
//        server.start();
//
//        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//            try {
//                server.shutdown();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }));
//    }

    public static void main(String[] args) throws IOException {
        GroupPd server = new GroupPd("D:\\garbage","127.0.0.1:8181,127.0.0.1:8182,127.0.0.1:8183");
        //GroupPd server = new GroupPd("/data/DTG","192.168.1.212:8181,192.168.1.212:8182,192.168.1.212:8183");
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                //System.out.println("shutdown ...");
                server.shutdown();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));
    }

}

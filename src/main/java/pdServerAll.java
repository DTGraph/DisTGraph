import PlacementDriver.GroupPd;

import java.io.File;
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
//               try {
//                server.shutdown();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }));
//    }

    public static void main(String[] args) throws IOException {
        //File dir = new File("/usr/pdCluster");
        File dir = new File(args[0]);
        if (!dir.exists()) {// 判断目录是否存在
            dir.mkdir();
        }
        GroupPd server = new GroupPd(args[0],args[1] + ":8181,"+ args[1] + ":8182," + args[1] + ":8183");
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

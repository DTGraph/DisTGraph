package TransactionTest;

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

public class pdServer1 {

    public static void main(String[] args){
        PdServer pd = new PdServer("127.0.0.1", 8081, "D:\\garbage\\8081", true);
        try {
            pd.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    pd.shutdown();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }));
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}

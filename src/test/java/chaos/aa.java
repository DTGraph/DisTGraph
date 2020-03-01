package chaos;

import chaos.sThread;
import org.junit.Test;
import tool.GetSystem;

import java.util.HashMap;

public class aa{
    @Test
    public void tests(){
        long start = System.currentTimeMillis();
        for(int i = 0; i < 50; i++){
            String s = GetSystem.getMacAddress();
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
//        for(int i = 0; i < 10; i++){
//            sThread tx = new sThread(start);
//            tx.start();
//        }
//        long end = System.currentTimeMillis();
//        System.out.println(end - start);
//        try {
//            Thread.sleep(100000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }

}

//class TxThread extends Thread{
//
//    @Override
//    public void run() {
//        try {
//            Thread.sleep(1000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
//}


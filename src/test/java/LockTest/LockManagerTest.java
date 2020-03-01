//package LockTest;
//
//import Region.LockManager;
//import config.MainType;
//import org.junit.Test;
//
//import java.util.List;
//import java.util.concurrent.CompletableFuture;
//
///**
// * @author :jinkai
// * @date :Created in 2020/1/15 18:49
// * @description：
// * @modified By:
// * @version:
// */
//
//public class LockManagerTest {
//
//    @Test
//    public void addLockTest() throws InterruptedException {
//        LockManager lockManager = new LockManager();
//        boolean res = lockManager.addLock(0, MainType.NODETYPE, "1", MainType.ADD);
//        System.out.println(res);
//        res = lockManager.addLock(0, MainType.NODETYPE, "2", MainType.ADD);
//        System.out.println(res);
//        CompletableFuture.runAsync(() -> {
//            boolean res1 = lockManager.addLock(0, MainType.NODETYPE, "3", MainType.REMOVE);
//            System.out.println("1:" + res1);
//        });
//        Thread.sleep(50);
//        CompletableFuture.runAsync(() -> {
//            System.out.println("2:");
//            boolean res2 = lockManager.addLock(0, MainType.NODETYPE, "4", MainType.REMOVE);
//            System.out.println("2:" + res2);
//        });
//        Thread.sleep(50);
//        CompletableFuture.runAsync(() -> {
//            System.out.println("3:");
//            boolean res2 = lockManager.addLock(0, MainType.NODETYPE, "5", MainType.ADD);
//            System.out.println("33:" + res2);
//        });
//        Thread.sleep(50);
//        CompletableFuture.runAsync(() -> {
//            System.out.println("4:");
//            boolean res2 = lockManager.addLock(0, MainType.NODETYPE, "6", MainType.REMOVE);
//            System.out.println("4:" + res2);
//        });
//        Thread.sleep(50);
//        CompletableFuture.runAsync(() -> {
//            System.out.println("5:");
//            boolean res2 = lockManager.addLock(0, MainType.NODETYPE, "7", MainType.REMOVE);
//            System.out.println("5:" + res2);
//        });
//        Thread.sleep(50);
//        CompletableFuture.runAsync(() -> {
//            System.out.println("3:");
//            boolean res3 = lockManager.removeLock(0, MainType.NODETYPE, "3", MainType.REMOVE);
//            System.out.println("3:" + res3);
//        });
//        Thread.sleep(50);
//        CompletableFuture.runAsync(() -> {
//            System.out.println("3:");
//            boolean res3 = lockManager.finishLock(0, MainType.NODETYPE, "4", MainType.REMOVE);
//            System.out.println("3:" + res3);
//        });
//
//        Thread.sleep(10000);
//
//    }
//
//    @Test
//    public void getBeforeTest(){
//        LockManager lockManager = new LockManager();
//        lockManager.addLock(0, MainType.NODETYPE, "1", MainType.ADD);
//        lockManager.addLock(0, MainType.NODETYPE, "2", MainType.ADD);
//        lockManager.addLock(0, MainType.NODETYPE, "3", MainType.ADD);
//        lockManager.addLock(0, MainType.NODETYPE, "4", MainType.ADD);
//        lockManager.addLock(0, MainType.NODETYPE, "5", MainType.ADD);
//        lockManager.removeLock(0, MainType.NODETYPE, "3", MainType.ADD);
//        lockManager.addLock(0, MainType.NODETYPE, "6", MainType.ADD);
//        List<String> list = lockManager.getTxBeforeNow(0, MainType.NODETYPE, "4");
//        for(String s : list){
//            System.out.println(s);
//        }
//    }
//
//
//}

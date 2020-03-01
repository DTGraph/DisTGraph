package chaos;

public class sThread extends Thread{

    long start;

    public sThread(long start){
        this.start = start;
    }

    @Override
    public void run() {
        try {
            Thread.sleep(1000);
            long end = System.currentTimeMillis();
            System.out.println(end - start);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

package performance.tool;

import java.io.*;

public class OutPutCsv {
    private String path;
    FileWriter fw = null;
    BufferedWriter out;

    public OutPutCsv(String path, String head){
        try {
            this.path = path;
            File f = new File(path);
            this.fw = new FileWriter(f);
            this.out = new BufferedWriter(fw);
            this.out.write(head + "\r\n");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized void write(String ... numbers){
        try {
            for(String s : numbers){
                this.out.write(s + ",");
            }
            this.out.write("\r\n");
            this.out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close(){
        try {
            this.out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

package chaos;

import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class LiveProcess {

    @Test
    public void process(){
        String path = "D:\\self\\video";
        String logPath = "/bilibili/live.log";
        String runVideoPath = "/home/video";
        String livePath = "\"rtmp://live-push.bilivideo.com/live-bvc/?streamname=live_20767246_75971584&key=f546701f4b56bbde0f0499f748a2a698&schedule=rtmp\"";
        File file = new File(path);
        File[] movieList = file.listFiles();
        int runTimes = 1000;

        try{
            BufferedWriter out = new BufferedWriter(new FileWriter(path + "\\run.sh"));
            out.write("#!/bin/bash\n");
            out.write("first=true\n");
            out.write("for i in {1.." + runTimes +"}\n");
            out.write("do\n");
            for (int i = 1; i <= movieList.length; i++) {
                movieList[i-1].renameTo(new File(path + "\\" + i ));
                if (movieList[i-1].isDirectory()) {
                    File[] pages = movieList[i-1].listFiles();
                    String suffix = "";
                    for (int j = 1; j <= pages.length; j++) {
                        File f = pages[j-1];
                        String filename = f.getName();
                        int lastIndexOf = filename.lastIndexOf(".");
                        suffix = filename.substring(lastIndexOf);
                        System.out.println(path + "\\" + i + "\\" + j+ suffix);
                        f.renameTo(new File(path + "\\" + i + "\\" + j + suffix));
                    }
                    out.write("if [[ \"$first\" == \"false\" ]] || [[ $1 -eq " + i + " ]] ;\n");
                    out.write("then\n");
                    out.write("for j in {1.." + pages.length +"}\n");
                    out.write("do\n");
                    out.write("if [[ \"$first\" == \"false\" ]] || [[ $2 -eq $j ]] ;\n");
                    out.write("then\n");
                    out.write("first=false\n");
                    out.write("echo Gen_Num: -$i-" + i + "-$j >>" + logPath + "\n");
                    out.write("echo live start time:$(date +'%F %T') >>" + logPath + "\n");
                    out.write("ffmpeg -re -i " + runVideoPath + "/" + i + "/$j" + suffix + " -vcodec copy -acodec aac -b:a 192k -f flv " + livePath + "\n");
                    out.write("fi\n");
                    out.write("done\n");
                    out.write("fi\n");
                }
            }
            out.write("done\n");
            out.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }
}

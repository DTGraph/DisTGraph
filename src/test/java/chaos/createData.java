package chaos;

import org.junit.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public class createData {

    @Test
    public void create(){

        FileWriter writer = null;
        try {
            writer = new FileWriter("D:\\distribute\\temp2.csv");
            writer.write( "time,id,status,carnumber,crosstime\r\n");
            Random r = new Random();
            for(int i = 0; i < 2880; i++){
                for(int j = 0; j < 5028; j++){
                    writer.write((i*5) + "," + j + ","  + r.nextInt(3) + "," + r.nextInt(20) + "," + (r.nextInt(15) + 2) + "\r\n");
                }
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void createTopo(){

        FileWriter writer = null;
        try {
            writer = new FileWriter("D:\\distribute\\topo2.csv");
            writer.write( "time,id,start,end,length\r\n");
            Random r = new Random();
            for(int i = 0; i < 1; i++){
                for(int j = 0; j < 5028; j++){
                    writer.write(i + "," + j + "," + (r.nextInt(20) + i) + "," + (r.nextInt(20) + i) + ","  + (r.nextInt(13) + 5) +  "\r\n");
                }
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}

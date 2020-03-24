package performance.tool;

import org.junit.Test;
import org.neo4j.kernel.impl.util.register.NeoRegister;

public class test1 {
    @Test
    public void inputTest(){
        OutPutCsv output = new OutPutCsv("D:\\garbage\\s.csv", "a,b");
        output.write("1","2");
        output.write("1","2");
        output.write("1","2");
        output.close();
    }
}

package Tool;

import PlacementDriver.IdManage.IdGenerator;
import config.DTGConstants;
import org.junit.Test;

import java.io.File;

public class IdGeneratorTest {

    @Test
    public void getIdsTest(){
        File file = new File("D:\\garbage\\testIds");
        IdGenerator.createGenerator(file, 0, false);
        IdGenerator idGenerator = new IdGenerator(file, DTGConstants.applyBatch, Long.MAX_VALUE, 0);
        for(int i = 0; i < 5; i++){
            idGenerator.nextId();
        }
        idGenerator.freeId(3);
        long[] res = idGenerator.getIds(4);
        System.out.println(res[0] + " - " + res[1]);
    }
}

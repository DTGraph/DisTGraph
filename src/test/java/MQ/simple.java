package MQ;

import com.alipay.sofa.jraft.rhea.serialization.Serializers;
import org.junit.Test;
import tool.ObjectAndByte;

import java.nio.ByteBuffer;

/**
 * @author :jinkai
 * @date :Created in 2020/1/7 20:21
 * @description：
 * @modified By:
 * @version:
 */

public class simple {

    @Test
    public void bytetest(){
        ByteBuffer b = ByteBuffer.wrap(Serializers.getDefault().writeObject(new TransactionLogEntry(2)));
        int a =1;
    }
}

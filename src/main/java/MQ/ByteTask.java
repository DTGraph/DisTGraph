package MQ;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.entity.Task;

import java.nio.ByteBuffer;

/**
 * @author :jinkai
 * @date :Created in 2020/1/8 13:21
 * @description：
 * @modified By:
 * @version:
 */

public class ByteTask extends Task {

    private byte[] byteData;

    public ByteTask(){
        super();
    }

    public ByteTask(byte[] data, Closure done){
        this.byteData = data;
        super.setDone(done);
    }

    @Override
    public ByteBuffer getData() {
        return ByteBuffer.wrap(this.byteData);
    }

    public byte[] getByteData() {
        return this.byteData;
    }

    public void setData(byte[] data) {
        this.byteData = data;
    }
}

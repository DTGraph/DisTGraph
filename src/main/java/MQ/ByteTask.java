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
    private long version;
    private byte txStatus;
    private long mainRegion = -1;

    public ByteTask(){
        super();
    }

    public ByteTask(byte[] data, long version, Closure done){
        this.byteData = data;
        super.setDone(done);
        this.version = version;
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

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public byte getTxStatus() {
        return txStatus;
    }

    public void setTxStatus(byte txStatus) {
        this.txStatus = txStatus;
    }

    public long getMainRegion() {
        return mainRegion;
    }

    public void setMainRegion(long mainRegion) {
        this.mainRegion = mainRegion;
    }
}

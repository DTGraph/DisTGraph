package Communication.RequestAndResponse;

import com.alipay.sofa.jraft.rhea.cmd.store.BaseResponse;

/**
 * @author :jinkai
 * @date :Created in 2019/10/17 20:30
 * @description:
 * @modified By:
 * @version:
 */

public class FirstPhaseResponse extends BaseResponse<byte[]> {
    private static final long serialVersionUID = -2649911091366848410L;//byte[]

    private long selfRegionId;

    public void setSelfRegionId(long selfRegionId) {
        this.selfRegionId = selfRegionId;
    }

    public long getSelfRegionId() {
        return selfRegionId;
    }
}

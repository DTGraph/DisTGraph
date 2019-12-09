package Communication.RequestAndResponse;

import com.alipay.sofa.jraft.rhea.cmd.pd.BaseRequest;

/**
 * @author :jinkai
 * @date :Created in 2019/12/3 20:37
 * @description：
 * @modified By:
 * @version:
 */

public class CreateRegionRequest extends BaseRequest {

    private static final long serialVersionUID = -3488763392944634684L;

    private long maxIdNeed;
    private byte idType;

    public long getMaxIdNeed() {
        return maxIdNeed;
    }

    public void setMaxIdNeed(long maxIdNeed) {
        this.maxIdNeed = maxIdNeed;
    }

    public void setIdType(byte idType) {
        this.idType = idType;
    }

    public byte getIdType() {
        return idType;
    }

    @Override
    public byte magic() {
        return CREATE_REGION;
    }
}

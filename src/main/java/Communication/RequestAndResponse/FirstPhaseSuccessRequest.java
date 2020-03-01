package Communication.RequestAndResponse;

import com.alipay.sofa.jraft.rhea.cmd.store.BaseRequest;
import config.DTGConstants;

/**
 * @author :jinkai
 * @date :Created in 2019/10/23 18:30
 * @description:
 * @modified By:
 * @version:
 */

public class FirstPhaseSuccessRequest extends DTGBaseRequest {

    private static final long serialVersionUID = 2177357963599585212L;
    private String txId;
    private long selfRegionId;
    private boolean IsSuccess;

    public boolean isSuccess() {
        return IsSuccess;
    }

    public void setSuccess(boolean success) {
        IsSuccess = success;
    }

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public long getSelfRegionId() {
        return selfRegionId;
    }

    public void setSelfRegionId(long selfRegionId) {
        this.selfRegionId = selfRegionId;
    }

    @Override
    public byte magic() {
        return DTGConstants.FIRST_PHASE_SUCCESS_REQUEST;
    }

}

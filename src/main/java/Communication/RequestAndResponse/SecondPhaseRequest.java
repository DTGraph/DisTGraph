package Communication.RequestAndResponse;

import config.DTGConstants;

/**
 * @author :jinkai
 * @date :Created in 2019/10/16 19:08
 * @description:
 * @modified By:
 * @version:
 */

public class SecondPhaseRequest extends DTGBaseRequest {


    private static final long serialVersionUID = 7400456690465502908L;

    private long version;
    private String txId;
    private boolean isSuccess;

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public void setSuccess(boolean success) {
        isSuccess = success;
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    @Override
    public byte magic() {
        return DTGConstants.SECOND_PHASE_REQUEST;
    }
}

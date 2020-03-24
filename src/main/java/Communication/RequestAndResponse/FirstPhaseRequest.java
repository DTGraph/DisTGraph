package Communication.RequestAndResponse;

import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;
import config.DTGConstants;

/**
 * @author :jinkai
 * @date :Created in 2019/10/16 19:08
 * @description:
 * @modified By:
 * @version:
 */

public class FirstPhaseRequest extends DTGBaseRequest {

    private static final long serialVersionUID = 6788331258524208116L;

    private boolean fromClient = false;

    private int repeate;

    public void setRepeate(int repeate) {
        this.repeate = repeate;
    }

    public int getRepeate() {
        return repeate;
    }

    public boolean isFromClient() {
        return fromClient;
    }

    public void setFromClient(){
        this.fromClient = true;
    }

    @Override
    public byte magic() {
        return DTGConstants.FIRST_PHASE_REQUEST;
    }
}

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

    @Override
    public byte magic() {
        return DTGConstants.SECOND_PHASE_REQUEST;
    }
}

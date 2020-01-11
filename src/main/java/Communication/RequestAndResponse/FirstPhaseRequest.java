package Communication.RequestAndResponse;

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

    @Override
    public byte magic() {
        return DTGConstants.FIRST_PHASE_REQUEST;
    }
}

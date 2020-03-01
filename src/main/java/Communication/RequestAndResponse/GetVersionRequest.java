package Communication.RequestAndResponse;

import com.alipay.sofa.jraft.rhea.cmd.pd.BaseRequest;
import config.DTGConstants;

/**
 * @author :jinkai
 * @date :Created in 2019/10/23 18:30
 * @description:
 * @modified By:
 * @version:
 */

public class GetVersionRequest extends BaseRequest {

    private static final long serialVersionUID = 2289460497725970003L;

    @Override
    public byte magic() {
        return DTGConstants.GET_VERSION_REQUEST;
    }





}

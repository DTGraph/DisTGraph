package Communication.RequestAndResponse;

import Element.DTGOpreration;
import com.alipay.sofa.jraft.rhea.cmd.store.BaseRequest;

/**
 * @author :jinkai
 * @date :Created in 2019/10/23 18:30
 * @description:
 * @modified By:
 * @version:
 */

public class CommitRequest extends DTGBaseRequest {

    private static final long serialVersionUID = 9094996319975697956L;

    private boolean shouldCommit;

    @Override
    public byte magic() {
        return COMMIT_REQUEST;
    }





}

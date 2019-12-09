package Communication.RequestAndResponse;

import Element.DTGOperation;
import com.alipay.sofa.jraft.rhea.cmd.store.BaseRequest;

/**
 * @author :jinkai
 * @date :Created in 2019/10/23 19:50
 * @description:
 * @modified By:
 * @version:
 */

public abstract class DTGBaseRequest extends BaseRequest {
    private static final long serialVersionUID = -2869864483811882624L;

    private DTGOperation op;

    public DTGOperation getDTGOpreration() {
        return op;
    }

    public void setDTGOpreration(DTGOperation op) {
        this.op = op;
    }

    public String TypeString(){
        switch (magic()){
            case TRANSACTION_REQUEST: return "transaction request";
            case COMMIT_REQUEST: return "commit transaction";
            default:return "other";
        }
    }
}

package Communication.RequestAndResponse;

import Element.DTGOperation;
import Element.EntityEntry;

import java.util.List;

/**
 * @author :jinkai
 * @date :Created in 2019/10/23 18:30
 * @description:
 * @modified By:
 * @version:
 */

public class LockRequest extends DTGBaseRequest {

    private static final long serialVersionUID = 9094996319975697956L;

    private DTGOperation op;

    public DTGOperation getDTGOpreration() {
        return op;
    }

    public void setDTGOpreration(DTGOperation op) {
        this.op = op;
    }

    @Override
    public byte magic() {
        return KEY_LOCK;
    }





}

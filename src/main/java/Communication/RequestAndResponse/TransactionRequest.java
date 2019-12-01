package Communication.RequestAndResponse;

/**
 * @author :jinkai
 * @date :Created in 2019/10/16 19:08
 * @description:
 * @modified By:
 * @version:
 */

public class TransactionRequest extends DTGBaseRequest {

    private static final long serialVersionUID = 7337402699243729392L;

    @Override
    public byte magic() {
        return TRANSACTION_REQUEST;
    }
}

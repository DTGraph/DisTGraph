package Communication.RequestAndResponse;

/**
 * @author :jinkai
 * @date :Created in 2019/10/23 18:30
 * @description:
 * @modified By:
 * @version:
 */

public class CommitRequest extends DTGBaseRequest {

    private static final long serialVersionUID = 5058637005605352471L;
    private boolean shouldCommit;

    @Override
    public byte magic() {
        return COMMIT_REQUEST;
    }





}

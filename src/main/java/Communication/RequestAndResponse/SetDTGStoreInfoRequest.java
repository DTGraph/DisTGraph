package Communication.RequestAndResponse;

import com.alipay.sofa.jraft.rhea.cmd.pd.BaseRequest;
import storage.DTGStore;

/**
 * @author :jinkai
 * @date :Created in 2019/10/22 21:35
 * @description:
 * @modified By:
 * @version:
 */

public class SetDTGStoreInfoRequest extends BaseRequest {

    private static final long serialVersionUID = 6928334353055874540L;

    private DTGStore store;

    public DTGStore getStore() {
        return store;
    }

    public void setStore(DTGStore store) {
        this.store = store;
    }

    @Override
    public byte magic() {
        return SET_STORE_INFO;
    }

    @Override
    public String toString() {
        return "SetStoreInfoRequest{" + "store=" + store + "} " + super.toString();
    }
}
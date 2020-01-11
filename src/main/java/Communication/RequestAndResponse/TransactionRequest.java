package Communication.RequestAndResponse;

import Element.EntityEntry;
import com.alipay.sofa.jraft.rhea.cmd.store.BaseRequest;

import java.util.LinkedList;

/**
 * @author :jinkai
 * @date :Created in 2019/10/16 19:08
 * @description:
 * @modified By:
 * @version:
 */

public class TransactionRequest extends BaseRequest {

    private static final long serialVersionUID = -7177581092521924953L;

    private LinkedList<EntityEntry> entries;

    private String txId;

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public LinkedList<EntityEntry> getEntries() {
        return entries;
    }

    public void setEntries(LinkedList<EntityEntry> entries) {
        this.entries = entries;
    }

    @Override
    public byte magic() {
        return TRANSACTION_REQUEST;
    }
}

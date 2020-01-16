package UserClient.Transaction;

import Element.EntityEntry;

import java.io.Serializable;
import java.util.List;

/**
 * @author :jinkai
 * @date :Created in 2020/1/11 22:28
 * @description：
 * @modified By:
 * @version:
 */

public class TransactionLog implements Serializable {

    private final String              txId;
    private final List<EntityEntry>   ops;

    public TransactionLog(String txId, List<EntityEntry> ops){
        this.ops = ops;
        this.txId = txId;
    }

    public String getTxId() {
        return txId;
    }

    public List<EntityEntry> getOps() {
        return ops;
    }
}

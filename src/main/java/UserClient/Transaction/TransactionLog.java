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
    private final boolean             isCommit;

    public TransactionLog(String txId, List<EntityEntry> ops){
        this.ops = ops;
        this.txId = txId;
        this.isCommit = false;
    }

    public TransactionLog(String txId, boolean isCommit){
        this.isCommit = isCommit;
        this.txId = txId;
        this.ops = null;
    }

    public String getTxId() {
        return txId;
    }

    public List<EntityEntry> getOps() {
        return ops;
    }

    public boolean isCommit() {
        return isCommit;
    }
}

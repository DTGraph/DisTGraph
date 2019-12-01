package LocalDBMachine.LocalTx;

import com.alipay.sofa.jraft.rhea.client.FutureHelper;

/**
 * @author :jinkai
 * @date :Created in 2019/10/27 15:52
 * @description:
 * @modified By:
 * @version:
 */

public class TransactionThreadLock {
    private boolean shouldCommit;
    private Object commitLock = new Object();
    private String txId;

    public TransactionThreadLock(String txId){
        this.txId = txId;
    }

    public void commit() throws InterruptedException {
        this.shouldCommit = true;
    }

    public void rollback() throws InterruptedException {
        this.shouldCommit = false;
    }

    public boolean isShouldCommit() {
        return shouldCommit;
    }

    public Object getCommitLock() {
        return commitLock;
    }

    public String getTxId() {
        return txId;
    }
}

package Element;

import UserClient.Transaction.DTGTransaction;
import UserClient.Transaction.TransactionManage;
import com.alipay.sofa.jraft.util.Requires;

/**
 * @author :jinkai
 * @date :Created in 2019/10/18 10:15
 * @description:
 * @modified By:
 * @version:
 */

public abstract class Agent {
    protected TransactionManage transactionManage;
    protected int TransactionObjectId;

    public Agent(TransactionManage transactionManage){
        this.transactionManage = transactionManage;
    }

    public int getTransactionObjectId() {
        return TransactionObjectId;
    }

    public void setTransactionObjectId(int transactionObjectId) {
        TransactionObjectId = transactionObjectId;
    }

    public void setProperty(String key, Object value){
        DTGTransaction transaction = transactionManage.getTransaction();
        EntityEntry entry = new EntityEntry();
        entry.setTransactionNum(transaction.getEntityNum());
        entry.setType(getType());
        entry.setOperationType(EntityEntry.SET);
        entry.setIsTemporalProperty(false);
        entry.setId(-2);
        entry.setKey(key);
        entry.setValue(value);
        Requires.requireNonNull(TransactionObjectId, "Transaction error");
        entry.setParaId(getTransactionObjectId());
        transaction.addEntityEntries(entry);
        transaction.NotReadOnly();
    }

    public void setTemporalProperty(String key, int start, int end,  Object value){
        DTGTransaction transaction = transactionManage.getTransaction();
        EntityEntry entry = new EntityEntry();
        entry.setTransactionNum(transaction.getEntityNum());
        entry.setType(getType());
        entry.setOperationType(EntityEntry.SET);
        entry.setIsTemporalProperty(true);
        entry.setId(-2);
        entry.setKey(key);
        entry.setValue(value);
        entry.setStart(start);
        entry.setOther(end);
        Requires.requireNonNull(TransactionObjectId, "Transaction error");
        entry.setParaId(getTransactionObjectId());
        transaction.addEntityEntries(entry);
        transaction.NotReadOnly();
    }

    public void setTemporalProperty(String key, int time,  Object value){
        DTGTransaction transaction = transactionManage.getTransaction();
        EntityEntry entry = new EntityEntry();
        entry.setTransactionNum(transaction.getEntityNum());
        entry.setType(getType());
        entry.setOperationType(EntityEntry.SET);
        entry.setIsTemporalProperty(true);
        entry.setId(-2);
        entry.setKey(key);
        entry.setValue(value);
        entry.setStart(time);
        entry.setOther(-1);
        Requires.requireNonNull(TransactionObjectId, "Transaction error");
        entry.setParaId(getTransactionObjectId());
        transaction.addEntityEntries(entry);
        transaction.NotReadOnly();
    }

    public void deleteself(){
        DTGTransaction transaction = transactionManage.getTransaction();
        EntityEntry entry = new EntityEntry();
        entry.setTransactionNum(transaction.getEntityNum());
        entry.setType(getType());
        entry.setOperationType(EntityEntry.REMOVE);
        entry.setIsTemporalProperty(false);
        entry.setId(-2);
        Requires.requireNonNull(TransactionObjectId, "Transaction error");
        entry.setParaId(getTransactionObjectId());
        transaction.addEntityEntries(entry);
        transaction.NotReadOnly();
    }

    public void deleteProperty(String key){
        DTGTransaction transaction = transactionManage.getTransaction();
        EntityEntry entry = new EntityEntry();
        entry.setTransactionNum(transaction.getEntityNum());
        entry.setType(getType());
        entry.setOperationType(EntityEntry.REMOVE);
        entry.setIsTemporalProperty(false);
        entry.setId(-2);
        entry.setKey(key);
        Requires.requireNonNull(TransactionObjectId, "Transaction error");
        entry.setParaId(getTransactionObjectId());
        transaction.addEntityEntries(entry);
        transaction.NotReadOnly();
    }

    public void deleteTemporalProperty(String key ){
        DTGTransaction transaction = transactionManage.getTransaction();
        EntityEntry entry = new EntityEntry();
        entry.setTransactionNum(transaction.getEntityNum());
        entry.setType(getType());
        entry.setOperationType(EntityEntry.SET);
        entry.setIsTemporalProperty(true);
        entry.setId(-2);
        entry.setKey(key);
        Requires.requireNonNull(TransactionObjectId, "Transaction error");
        entry.setParaId(getTransactionObjectId());
        transaction.addEntityEntries(entry);
        transaction.NotReadOnly();
    }

    public int getProperty(String key){
        DTGTransaction transaction = transactionManage.getTransaction();
        EntityEntry entry = new EntityEntry();
        entry.setTransactionNum(transaction.getEntityNum());
        entry.setType(getType());
        entry.setOperationType(EntityEntry.SET);
        entry.setIsTemporalProperty(true);
        entry.setId(-2);
        entry.setKey(key);
        Requires.requireNonNull(TransactionObjectId, "Transaction error");
        entry.setParaId(getTransactionObjectId());
        transaction.addEntityEntries(entry);
        return entry.getTransactionNum();
    }

    public int getNodeTemporalProperty(String key, int time){
        DTGTransaction transaction = transactionManage.getTransaction();
        EntityEntry entry = new EntityEntry();
        entry.setTransactionNum(transaction.getEntityNum());
        entry.setType(getType());
        entry.setOperationType(EntityEntry.SET);
        entry.setIsTemporalProperty(true);
        entry.setId(-2);
        entry.setKey(key);
        entry.setStart(time);
        entry.setOther(-1);
        Requires.requireNonNull(TransactionObjectId, "Transaction error");
        entry.setParaId(getTransactionObjectId());
        transaction.addEntityEntries(entry);
        transaction.NotReadOnly();
        return entry.getTransactionNum();
    }

    abstract byte getType();
}

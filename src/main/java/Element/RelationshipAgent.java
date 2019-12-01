package Element;

import UserClient.Transaction.DTGTransaction;
import UserClient.Transaction.TransactionManage;
import com.alipay.sofa.jraft.util.Requires;

import static config.MainType.RELATIONTYPE;

/**
 * @author :jinkai
 * @date :Created in 2019/10/18 10:15
 * @description:
 * @modified By:
 * @version:
 */

public class RelationshipAgent extends Agent{

    public RelationshipAgent(TransactionManage transactionManage) {
        super(transactionManage);
    }

    public int getStartNode(){
        DTGTransaction transaction = super.transactionManage.getTransaction();
        EntityEntry entry = new EntityEntry();
        entry.setTransactionNum(transaction.getEntityNum());
        entry.setType(getType());
        entry.setOperationType(EntityEntry.GET);
        entry.setIsTemporalProperty(false);
        entry.setId(-2);
        entry.setStart(1);
        entry.setOther(-1);
        Requires.requireNonNull(TransactionObjectId, "Transaction error");
        entry.setParaId(getTransactionObjectId());
        transaction.addEntityEntries(entry);
        transaction.NotReadOnly();
        return entry.getTransactionNum();
    }

    public int getEndNode(){
        DTGTransaction transaction = super.transactionManage.getTransaction();
        EntityEntry entry = new EntityEntry();
        entry.setTransactionNum(transaction.getEntityNum());
        entry.setType(getType());
        entry.setOperationType(EntityEntry.GET);
        entry.setIsTemporalProperty(false);
        entry.setId(-2);
        entry.setStart(-1);
        entry.setOther(1);
        Requires.requireNonNull(TransactionObjectId, "Transaction error");
        entry.setParaId(getTransactionObjectId());
        transaction.addEntityEntries(entry);
        transaction.NotReadOnly();
        return entry.getTransactionNum();
    }

    @Override
    byte getType() {
        return RELATIONTYPE;
    }
}

package UserClient.Transaction;

public class TransactionManage {

    private final ThreadLocal<DTGTransaction> transactionMap = new ThreadLocal<>();

    public void BindTransaction(DTGTransaction transaction){
        if ( transactionMap.get() != null )
        {
            throw new IllegalStateException( Thread.currentThread() + " already has a transaction bound" );
        }
        transactionMap.set( transaction );
    }

    public DTGTransaction getTransaction(){
        return transactionMap.get();
    }

    public void removeBound(){
        if ( transactionMap.get() == null )
        {
            throw new IllegalStateException( Thread.currentThread() + " the transaction does not bound!" );
        }
        transactionMap.remove();
    }
}

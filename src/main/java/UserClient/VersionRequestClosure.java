package UserClient;

import UserClient.Transaction.DTGTransaction;
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.errors.Errors;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

public class VersionRequestClosure implements Closure {
    private long version;
    private DTGTransaction transaction;
    private volatile Errors error;
    private CompletableFuture<Object> future;
    private Object result;
    CountDownLatch shutdownLatch;

    public Errors getError() {
        return error;
    }

    public void setError(Errors error) {
        this.error = error;
    }

//    public VersionRequestClosure(DTGTransaction transaction, CompletableFuture future){
//        this.transaction = transaction;
//        this.future = future;
//    }

    public void setVersion(long version) {
        this.version = version;
        this.transaction.setVersion(version);
    }

    public DTGTransaction getTransaction() {
        return transaction;
    }

    public void setDone(CompletableFuture<Object> future) {
        this.future = future;
    }

    public void setTransaction(DTGTransaction transaction) {
        this.transaction = transaction;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    public void reset(){
        this.transaction = null;
        this.future = null;
        this.result = null;
        this.error = null;
        this.shutdownLatch = null;
    }

    @Override
    public void run(Status status) {
        if(status.isOk()){
            this.future.complete(version);
        }else{
            this.future.complete(-1);
        }
    }
}

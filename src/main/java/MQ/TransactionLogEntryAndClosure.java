package MQ;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;

import java.util.concurrent.CountDownLatch;

/**
 * @author :jinkai
 * @date :Created in 2020/1/12 15:00
 * @description：
 * @modified By:
 * @version:
 */

public class TransactionLogEntryAndClosure implements Closure {
    private TransactionLogEntry entry;
    private Closure             done;
    CountDownLatch              shutdownLatch;

    public void setDone(Closure done) {
        this.done = done;
    }

    public Closure getDone() {
        return done;
    }

    public TransactionLogEntry getEntry() {
        return entry;
    }

    public void setEntry(TransactionLogEntry entry) {
        this.entry = entry;
    }

    public void reset() {
        this.entry = null;
        this.done = null;
        this.shutdownLatch = null;
    }

    @Override
    public void run(Status status) {
        if (this.done != null) {
            this.done.run(status);
        }
    }
}

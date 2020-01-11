package DBExceptions;

import java.util.HashMap;
import java.util.Map;

public enum TxMQError {


    /**
     * Unknown error
     */
    UNKNOWN(-2),

    /**
     * Success, no error.
     */
    SUCCESS(0),

    /**
     * <pre>
     * All Kinds of Timeout(Including Election_timeout, Timeout_now, Stepdown_timeout)
     * </pre>
     * <p>
     * <code>ERAFTTIMEDOUT = 10001;</code>
     */
    ETXTIMEDOUT(20001),

    /**
     * <pre>
     * Bad User State Machine
     * </pre>
     * <p>
     * <code>ESTATEMACHINE = 10002;</code>
     */
    ESTATEMACHINE(20002),

    /**
     * <pre>
     * Catchup Failed
     * </pre>
     * <p>
     * <code>ECATCHUP = 10003;</code>
     */
    ECATCHUP(20003),

    /**
     * <pre>
     * Shut_down
     * </pre>
     * <p>
     * <code>ENODESHUTDOWN = 20004;</code>
     */
    ENODESHUTDOWN(20004),

    /**
     * <pre>
     * Receive Higher Term Requests
     * </pre>
     * <p>
     * <code>EHIGHERTERMREQUEST = 20005;</code>
     */
    EHIGHERTERMREQUEST(20005),

    /**
     * <pre>
     * Receive Higher Term Response
     * </pre>
     * <p>
     * <code>EHIGHERTERMRESPONSE = 20006;</code>
     */
    EHIGHERTERMRESPONSE(20006),

    /**
     * <pre>
     * Node Is In Error
     * </pre>
     * <p>
     * <code>EBADNODE = 20007;</code>
     */
    EBADNODE(20007),

    /**
     * <pre>
     * The log at the given index is deleted
     * </pre>
     * <p>
     * <code>ELOGDELETED = 20008;</code>
     */
    ELOGDELETED(20008),

    /**
     * <pre>
     * No available user log to read
     * </pre>
     * <p>
     * <code>ENOMOREUSERLOG = 20009;</code>
     */
    ENOMOREUSERLOG(20009),

    /* other non-raft error codes 1000~10000 */
    /**
     * Invalid rpc request
     */
    EREQUEST(2000),

    /**
     * Task is stopped
     */
    ESTOP(2001),

    /**
     * Retry again
     */
    EAGAIN(2002),

    /**
     * Interrupted
     */
    EINTR(2003),

    /**
     * Internal exception
     */
    EINTERNAL(2004),

    /**
     * Task is canceled
     */
    ECANCELED(2005),

    /**
     * Host is down
     */
    EHOSTDOWN(2006),

    /**
     * Service is shutdown
     */
    ESHUTDOWN(2007),

    /**
     * Permission issue
     */
    EPERM(2008),

    /**
     * Server is in busy state
     */
    EBUSY(2009),

    /**
     * Timed out
     */
    ETIMEDOUT(2010),

    /**
     * Data is stale
     */
    ESTALE(2011),

    /**
     * Something not found
     */
    ENOENT(2012),

    /**
     * File/folder already exists
     */
    EEXISTS(2013),

    /**
     * IO error
     */
    EIO(2014),

    /**
     * Invalid value.
     */
    EINVAL(2015),

    /**
     * Permission denied
     */
    EACCES(2016);

    private static final Map<Integer, TxMQError> TX_MQ_ERROR_MAP = new HashMap<>();

    static {
        for (final TxMQError error : TxMQError.values()) {
            TX_MQ_ERROR_MAP.put(error.getNumber(), error);
        }
    }

    public final int getNumber() {
        return this.value;
    }

    public static TxMQError forNumber(final int value) {
        return TX_MQ_ERROR_MAP.getOrDefault(value, UNKNOWN);
    }

    public static String describeCode(final int code) {
        TxMQError e = forNumber(code);
        return e != null ? e.name() : "<Unknown:" + code + ">";
    }

    private final int value;

    TxMQError(final int value) {
        this.value = value;
    }
}

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

    SAVEFAILED(20001),

    ENODESHUTDOWN(20002),

    EBUSY(20003),

    EPERM(20004),

    COMMITERROR(20005),

    FIRSTPHASEERROR(20006),

    SCEONDPHASEERROR(20007);

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

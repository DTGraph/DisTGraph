package DBExceptions;

import java.util.HashMap;
import java.util.Map;

public enum TxError {

    /**
     * Unknown error
     */
    UNKNOWN(-2),

    /**
     * Success, no error.
     */
    SUCCESS(0),

    FAILED(4001);

    private static final Map<Integer, TxError> Lock_ERROR_MAP = new HashMap<>();

    static {
        for (final TxError error : TxError.values()) {
            Lock_ERROR_MAP.put(error.getNumber(), error);
        }
    }

    public final int getNumber() {
        return this.value;
    }

    public static TxError forNumber(final int value) {
        return Lock_ERROR_MAP.getOrDefault(value, UNKNOWN);
    }

    public static String describeCode(final int code) {
        TxError e = forNumber(code);
        return e != null ? e.name() : "<Unknown:" + code + ">";
    }

    private final int value;

    TxError(final int value) {
        this.value = value;
    }
}

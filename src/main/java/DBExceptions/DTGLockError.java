package DBExceptions;

import java.util.HashMap;
import java.util.Map;

public enum DTGLockError {

    /**
     * Unknown error
     */
    UNKNOWN(-2),

    /**
     * Success, no error.
     */
    SUCCESS(0),

    OBJECTNOTEXIST(30001),

    HASREMOVED(30002);

    private static final Map<Integer, DTGLockError> Lock_ERROR_MAP = new HashMap<>();

    static {
        for (final DTGLockError error : DTGLockError.values()) {
            Lock_ERROR_MAP.put(error.getNumber(), error);
        }
    }

    public final int getNumber() {
        return this.value;
    }

    public static DTGLockError forNumber(final int value) {
        return Lock_ERROR_MAP.getOrDefault(value, UNKNOWN);
    }

    public static String describeCode(final int code) {
        DTGLockError e = forNumber(code);
        return e != null ? e.name() : "<Unknown:" + code + ">";
    }

    private final int value;

    DTGLockError(final int value) {
        this.value = value;
    }
}

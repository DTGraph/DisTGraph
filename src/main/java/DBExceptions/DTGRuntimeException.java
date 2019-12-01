package DBExceptions;

/**
 * @author :jinkai
 * @date :Created in 2019/10/22 22:39
 * @description:
 * @modified By:
 * @version:
 */

public class DTGRuntimeException extends RuntimeException {
    private static final long serialVersionUID = 15548L;

    public DTGRuntimeException() {
    }

    public DTGRuntimeException(String message) {
        super(message);
    }

    public DTGRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public DTGRuntimeException(Throwable cause) {
        super(cause);
    }
}

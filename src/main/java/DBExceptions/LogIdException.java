package DBExceptions;

/**
 * @author :jinkai
 * @date :Created in 2020/1/6 16:39
 * @description：
 * @modified By:
 * @version:
 */

public class LogIdException  extends ArrayIndexOutOfBoundsException  {

    public LogIdException(String reason) {
        super(reason);
    }
}

package DBExceptions;

/**
 * @author :jinkai
 * @date :Created in 2019/11/25 9:59
 * @description:
 * @modified By:
 * @version:
 */

public class RangeException extends Exception {

    public RangeException(){
        super("region range process error!");
    }

    public RangeException(int length){
        super("range format should be [start, end), but it has " + length + " numbers");
    }

    public RangeException(String reason){
        super("region range exception: " + reason);
    }
}

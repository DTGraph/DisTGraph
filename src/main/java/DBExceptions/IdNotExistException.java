package DBExceptions;

/**
 * @author :jinkai
 * @date :Created in 2019/12/3 19:59
 * @description：
 * @modified By:
 * @version:
 */

public class IdNotExistException extends Exception {

    public IdNotExistException(){
        super("id doesn't exist in database");
    }

    public IdNotExistException(String type, long id){
        super(type + "id " + id + " doesn't exist in database");
    }
}

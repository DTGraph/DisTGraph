package DBExceptions;

public class ObjectExistException extends Exception {
    public ObjectExistException(){
        super("id has exist in database");
    }

    public ObjectExistException(String type, long id) {
        super(type + "id " + id + " has exist in database");
    }
}

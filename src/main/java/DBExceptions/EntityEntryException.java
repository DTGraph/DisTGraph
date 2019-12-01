package DBExceptions;

import Element.EntityEntry;

/**
 * @author :jinkai
 * @date :Created in 2019/10/21 10:30
 * @description:
 * @modified By:
 * @version:
 */

public class EntityEntryException extends Exception {

    public EntityEntryException(){
        super("EntityEntry exists some error!");
    }

    public EntityEntryException(EntityEntry entry){
        super("EntityEntry exists some error : " + entry.toString());
    }
}

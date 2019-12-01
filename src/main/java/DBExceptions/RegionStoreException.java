package DBExceptions;

/**
 * @author :jinkai
 * @date :Created in 2019/11/24 15:59
 * @description:
 * @modified By:
 * @version:
 */

public class RegionStoreException extends Exception {

    public RegionStoreException(){
        super("Region storage meet some error!");
    }

    public RegionStoreException(String type){
        super("region storage error: " + type);
    }

}

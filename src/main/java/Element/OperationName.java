package Element;

/**
 * @author :jinkai
 * @date :Created in 2019/10/22 13:48
 * @description:
 * @modified By:
 * @version:
 */

public class OperationName {
    public static final byte  TRANSACTIONOP  = 0x01;
    public static final byte  MERGE          = 0x02;
    public static final byte  SPLIT          = 0x03;
    public static final byte  READONLY       = 0x04;
    public static final byte  COMMITTRANS    = 0x05;
    public static final byte  ROLLBACK       = 0x06;
    public static final byte  ADDREGION      = 0X07;
}

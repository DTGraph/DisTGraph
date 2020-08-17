package config;

/**
 * @author :jinkai
 * @date :Created in 2019/11/18 16:06
 * @description:
 * @modified By:
 * @version:
 */

public class MainType {
    public static final byte  NODETYPE              = 0x01;
    public static final byte  RELATIONTYPE          = 0x02;
    public static final byte  TEMPORALPROPERTYTYPE  = 0x03;

    public static final byte  ADD     = 0x04;
    public static final byte  SET     = 0x05;
    public static final byte  REMOVE  = 0x06;
    public static final byte  GET     = 0x07;
    public static final byte  LOCKGET = 0x08;
}

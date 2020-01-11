package config;

/**
 * @author :jinkai
 * @date :Created in 2019/12/17 15:49
 * @description：
 * @modified By:
 * @version:
 */

public class DTGConstants {
    public static final byte   CLEAN_GENERATOR               = (byte) 0;
    public static final byte   STICKY_GENERATOR              = (byte) 1;
    public static final int    STORE_REGION_HEADER_SIZE      = 13;
    public static final int    STORE_OPTS_HEADER_SIZE        = 13;
    public static final long   NULL_STORE                    = -99999;

    public static final byte    FIRST_PHASE_REQUEST          = 0x16;
    public static final byte    SECOND_PHASE_REQUEST         = 0x17;
    public static final String  DEFAULT_TX_ID                = "NULL";

    //RAFT OPTION
    public static final int DisruptorPublishEventWaitTimeoutSecs = 10;
    public static final int maxAppendBufferSize                  = 256 * 1024;
    public static final int disruptorBufferSize                  = 16384;
    public static final int applyBatch                           = 32;

    //Config

    //if TX_LOG_SAVE_LEVEL==true, the log save with checksum;
    public static final boolean TX_LOG_SAVE_LEVEL            = false;
    public static final long    NULL_INDEX                   = -99998;
}

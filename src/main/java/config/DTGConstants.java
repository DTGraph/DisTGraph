package config;

import com.alipay.sofa.jraft.rhea.util.Constants;

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

    public static final long   DEFAULT_INIT_REGION           = Constants.DEFAULT_REGION_ID;
    public static final long   FIRST_REGION_ID               = 2;
    public static final long   DEFAULT_MAX_VERSION           = -1;


    public static final byte    FIRST_PHASE_REQUEST          = 0x16;
    public static final byte    SECOND_PHASE_REQUEST         = 0x17;
    public static final byte    GET_VERSION_REQUEST          = 0x18;
    public static final byte    FIRST_PHASE_SUCCESS_REQUEST  = 0x19;
    public static final String  DEFAULT_TX_ID                = "NULL";

    public static final byte    TXRECEIVED                   = 0x20;
    public static final byte    TXDONEFIRST                  = 0x21;
    public static final byte    TXRERUNFIRST                 = 0x22;
    public static final byte    TXFAILEDFIRST                = 0x23;
    public static final byte    TXROLLBACK                   = 0x24;
    public static final byte    TXSUCCESS                    = 0x25;
    public static final byte    SYNOP                        = 0x26;
    public static final byte    SYNOPFAILED                  = 0x27;

    //RAFT OPTION
    public static final int DisruptorPublishEventWaitTimeoutSecs = 10;
    public static final int maxAppendBufferSize                  = 256 * 1024;
    public static final int disruptorBufferSize                  = 16384;
    public static final int applyBatch                           = 32;

    //Config

    //if TX_LOG_SAVE_LEVEL==true, the log save with checksum;
    public static final boolean TX_LOG_SAVE_LEVEL            = false;
    public static final long    NULL_INDEX                   = -99998;
    public static final int     FAILOVERRETRIES              = 5;
    public static final int     RETRIYRUNNERWAIT             = 500;
    public static final int     MAXWAITTIME                  = 100;
}

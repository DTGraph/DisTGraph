package LocalDBMachine;

public class LockStatus {

    public static final byte INIT      = 0x01;
    public static final byte STARTLOCK = 0x02;
    public static final byte ENDLOCK   = 0x03;

    private byte status = INIT;

    public byte getStatus() {
        return status;
    }

    public void setStatus(byte status) {
        this.status = status;
    }
}

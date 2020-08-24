package tool.MemMvcc;

public class TimeMVCCObject extends MVCCObject {

    private long startTime;
    private long endTime;
    public TimeMVCCObject(long version, Object value, long startTime, long endTime) {
        super(version, value);
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public int getEndTime() {
        return (int)endTime;
    }

    public int getStartTime() {
        return (int)startTime;
    }
}

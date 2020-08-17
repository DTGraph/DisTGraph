package LocalDBMachine.LocalTx;

public class ObjectTimeLock {

    private byte type; //node or relation
    //private boolean isTemporal;
    private long objectId;
    private String propertyKey;
//    private long startTime;
//    private long endTime;

    public ObjectTimeLock(byte type, long objectId, String propertyKey){
        this.type = type;
        //this.isTemporal = false;
        this.objectId = objectId;
        this.propertyKey = propertyKey;
    }

//    public ObjectTimeLock(byte type, long objectId, String propertyKey, long startTime, long endTime){
//        this.type = type;
//        this.isTemporal = true;
//        this.objectId = objectId;
//        this.propertyKey = propertyKey;
//        this.startTime = startTime;
//        this.endTime = endTime;
//    }

    public byte getType() {
        return type;
    }

//    public boolean isTemporal() {
//        return isTemporal;
//    }

    public long getObjectId() {
        return objectId;
    }

    public String getPropertyKey() {
        return propertyKey;
    }

//    public long getStartTime() {
//        return startTime;
//    }
//
//    public long getEndTime() {
//        return endTime;
//    }

    @Override
    public boolean equals(Object o){
        if(o == null){
            return false;
        }
        if(this == o){
            return true;
        }
        if(!(o instanceof ObjectTimeLock)){
            return false;
        }
        if(this.type != ((ObjectTimeLock) o).getType()){
            return false;
        }
//        if(this.isTemporal != ((ObjectTimeLock) o).isTemporal()){
//            return false;
//        }
        if(this.objectId != ((ObjectTimeLock) o).getObjectId()){
            return false;
        }
        if(this.propertyKey != ((ObjectTimeLock) o).getPropertyKey()){
            return false;
        }
//        if(this.startTime != ((ObjectTimeLock) o).getStartTime()){
//            return false;
//        }
//        if(this.endTime != ((ObjectTimeLock) o).getEndTime()){
//            return false;
//        }
        return true;
    }

    @Override
    public int hashCode(){
        int code = 0;
        code = code + Byte.hashCode(type);
        code = code*31 + Long.hashCode(objectId);
        code = code*31 + propertyKey.hashCode();
        return code;
    }
}

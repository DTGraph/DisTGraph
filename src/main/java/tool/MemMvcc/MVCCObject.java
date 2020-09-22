package tool.MemMvcc;

import org.jetbrains.annotations.NotNull;

public class MVCCObject implements Comparable<MVCCObject>{

    private long version;
    private long commitVersion = -1;
    private Object value;
    private boolean isMaxVerion = false;

    public MVCCObject(long version, Object value){
        this.version = version;
        this.value = value;
    }

    public long getVersion() {
        return version;
    }

    public void setCommitVersion(long version){
        this.commitVersion = version;
    }

    public long getCommitVersion() {
        return commitVersion;
    }

    public Object getValue() {
        return value;
    }

    public boolean isMaxVerion() {
        return isMaxVerion;
    }

    public void setMaxVerion(boolean maxVerion) {
        isMaxVerion = maxVerion;
    }

    @Override
    public int compareTo(@NotNull MVCCObject o) {
        if(o instanceof MVCCObject){
            if(this.version > o.version){
                return 1;
            }else{
                return -1;
            }
        }
        return -1;
    }

    public int commitCompareTo(@NotNull MVCCObject o) {
        if(o instanceof MVCCObject){
            if(this.commitVersion > o.commitVersion){
                return 1;
            }else{
                return -1;
            }
        }
        return -1;
    }
}


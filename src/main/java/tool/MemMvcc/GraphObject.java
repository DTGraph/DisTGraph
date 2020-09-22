package tool.MemMvcc;

import java.util.HashMap;
import java.util.Map;

import static config.DTGConstants.DELETESELF;

public abstract class GraphObject {
    private long id;
    Map<String, Object> properties = new HashMap<>();
    Map<String, Object> needUpdate = new HashMap<>();
    private byte type;
    Object realObjectInDB;
    private long version;

    public GraphObject(long id, long version){
        this.id = id;
        this.version = version;
    }

    public long getId(){
        return id;
    }

    public void setProperty(String key, Object value){
        Object oldv = this.properties.get(key);
        if(oldv == null || !oldv.equals(value)){
            this.needUpdate.put(key, value);
        }
        this.properties.put(key, value);
    }

    public void initAllProperty(Map<String, Object> properties){
        this.properties = properties;
    }

    public void removeProperty(String key){
        Object oldv = this.properties.get(key);
        if(oldv != null){
            this.needUpdate.put(key, DELETESELF);
        }
        this.properties.remove(key);
    }

    public void cleanUpdateMap(){
        this.needUpdate = new HashMap<>();
    }

    public Map<String, Object> getNeedUpdate() {
        return needUpdate;
    }

    public Object getProperty(String key){
        return this.properties.get(key);
    }

    public byte getType() {
        return type;
    }

    void setType(byte type){
        this.type = type;
    }

    public void setRealObjectInDB(Object o){
        this.realObjectInDB = o;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public abstract Object getRealObjectInDB();

    public abstract GraphObject copy();
}

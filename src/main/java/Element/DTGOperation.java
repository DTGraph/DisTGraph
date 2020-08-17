package Element;

import java.io.Serializable;
import java.util.List;

/**
 * @author :jinkai
 * @date :Created in 2019/10/22 16:23
 * @description:
 * @modified By:
 * @version:
 */

public class DTGOperation implements Serializable {
    private static final long serialVersionUID = -1252903871896272256L;
    private List<EntityEntry> entityEntries;
    private List<EntityEntry> AllEntityEntries;
    private List<Long> regionIds;
    private byte type;
    private int size;
    private String txId;
    private long version;
    private long mainRegionId;
    private boolean highA = true;
    private boolean isReadOnly = false;
    private boolean isolateRead = false;

    private byte[] OpData;

//    private long regionId;
//    private long newRegionId;
//    private long startNodeId;
//    private long startRelationId;
//    private long startTempProId;

    public DTGOperation(){
        size = 0;
    }

    public DTGOperation(List<EntityEntry> entityEntries, byte type){
        this.type = type;
        if(entityEntries != null){
            this.entityEntries = entityEntries;
            size = entityEntries.size();
        }
        else size = 0;
    }

    public DTGOperation(byte type){
        this.type = type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public byte getType() {
        return type;
    }

    public List<EntityEntry> getEntityEntries() {
        return entityEntries;
    }

    public int getSize() {
        return size;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public String getTxId() {
        return txId;
    }

    public byte[] getOpData() {
        return OpData;
    }

    public void setOpData(byte[] opData) {
        OpData = opData;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public long getVersion() {
        return version;
    }

    public long getMainRegionId() {
        return mainRegionId;
    }

    public void setMainRegionId(long mainRegionId) {
        this.mainRegionId = mainRegionId;
    }

    public List<EntityEntry> getAllEntityEntries() {
        return AllEntityEntries;
    }

    public void setEntityEntries(List<EntityEntry> entityEntries) {
        this.entityEntries = entityEntries;
    }

    public void setAllEntityEntries(List<EntityEntry> allEntityEntries) {
        AllEntityEntries = allEntityEntries;
    }

    public List<Long> getRegionIds() {
        return regionIds;
    }

    public void setRegionIds(List<Long> regionIds) {
        this.regionIds = regionIds;
    }

    public boolean isHighA() {
        return highA;
    }

    public void setHighA(boolean highA) {
        this.highA = highA;
    }

    public void setReadOnly(boolean readOnly) {
        isReadOnly = readOnly;
    }

    public boolean isReadOnly() {
        return isReadOnly;
    }

    public boolean isIsolateRead() {
        return isolateRead;
    }

    public void setIsolateRead(boolean isolateRead) {
        this.isolateRead = true;
    }

    @Override
    public String toString(){
        String s = "op id : " + txId + ", op size : " + size + " op type : " + type;
        return s;
    }

}

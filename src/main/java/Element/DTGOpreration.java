package Element;

import Communication.DTGInstruction;

import java.io.Serializable;
import java.util.List;

/**
 * @author :jinkai
 * @date :Created in 2019/10/22 16:23
 * @description:
 * @modified By:
 * @version:
 */

public class DTGOpreration implements Serializable {
    private static final long serialVersionUID = -1252903871896272256L;
    private List<EntityEntry> entityEntries;
    private final byte type;
    private int size;
    private String txId;

    private long regionId;
    private long newRegionId;
    private long startNodeId;
    private long startRelationId;

    public DTGOpreration(List<EntityEntry> entityEntries, byte type){
        this.type = type;
        if(entityEntries != null){
            this.entityEntries = entityEntries;
            size = entityEntries.size();
        }
        else size = 0;
    }

    public DTGOpreration(byte type){
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

    public void setStartRelationId(long startRelationId) {
        this.startRelationId = startRelationId;
    }

    public void setStartNodeId(long startNodeId) {
        this.startNodeId = startNodeId;
    }

    public void setRegionId(long regionId) {
        this.regionId = regionId;
    }

    public long getStartRelationId() {
        return startRelationId;
    }

    public long getStartNodeId() {
        return startNodeId;
    }

    public long getRegionId() {
        return regionId;
    }

    public void setNewRegionId(long newRegionId) {
        this.newRegionId = newRegionId;
    }

    public long getNewRegionId() {
        return newRegionId;
    }
}

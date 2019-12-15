package Communication.instructions;

import com.alipay.sofa.jraft.rhea.metadata.Peer;

import java.io.Serializable;
import java.util.List;

/**
 * @author :jinkai
 * @date :Created in 2019/12/10 9:32
 * @description：
 * @modified By:
 * @version:
 */

public class AddRegionInfo implements Serializable {

    private static final long serialVersionUID = -8168089550369170893L;

    private long fullRegionId;
    private long newRegionId;
    private long startNodeId;
    private long startRelationId;
    private long startTempProId;
    private List<Peer> peers;

    public long getNewRegionId() {
        return newRegionId;
    }

    public long getStartNodeId() {
        return startNodeId;
    }

    public long getStartRelationId() {
        return startRelationId;
    }

    public void setNewRegionId(long newRegionId) {
        this.newRegionId = newRegionId;
    }

    public void setStartNodeId(long startNodeId) {
        this.startNodeId = startNodeId;
    }

    public void setStartRelationId(long startRelationId) {
        this.startRelationId = startRelationId;
    }

    public long getFullRegionId() {
        return fullRegionId;
    }

    public void setFullRegionId(long fullRegionId) {
        this.fullRegionId = fullRegionId;
    }

    public long getStartTempProId() {
        return startTempProId;
    }

    public void setStartTempProId(long startTempProId) {
        this.startTempProId = startTempProId;
    }

    public List<Peer> getPeers() {
        return peers;
    }

    public void setPeers(List<Peer> peers) {
        this.peers = peers;
    }

    @Override
    public String toString(){
        return "add region { id = " + newRegionId + ", start node id = " + startNodeId + ", start relation id = " + startRelationId;
    }
}

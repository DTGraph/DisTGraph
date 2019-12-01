package Region;

import com.alipay.sofa.jraft.rhea.metadata.Peer;
import com.alipay.sofa.jraft.rhea.metadata.PeerStats;
import com.alipay.sofa.jraft.rhea.metadata.TimeInterval;

import java.io.Serializable;
import java.util.List;

/**
 * @author :jinkai
 * @date :Created in 2019/11/30 19:07
 * @description：
 * @modified By:
 * @version:
 */

public class DTGRegionStats implements Serializable {

    private static final long serialVersionUID = -8330267414061927327L;

    private long              regionId;
    // Leader Peer sending the heartbeat
    private Peer              leader;
    // Leader considers that these peers are down
    private List<PeerStats>   downPeers;
    // Pending peers are the peers that the leader can't consider as working followers
    private List<PeerStats>   pendingPeers;
    // Bytes written for the region during this period
    private long              bytesWritten;
    // Bytes read for the region during this period
    private long              bytesRead;
    // Keys written for the region during this period
    private long              keysWritten;
    // Keys read for the region during this period
    private long              keysRead;
    // Approximate region size
    private long              approximateSize;
    // Approximate number of keys
    private long              approximateKeys;
    // Actually reported time interval
    private TimeInterval      interval;

//    private List<long[]>      NodeIdRangeList;
//    private List<long[]>      RelationIdRangeList;
//    private List<long[]>      TemporalPropertyTimeRangeList;

    public long getRegionId() {
        return regionId;
    }

    public void setRegionId(long regionId) {
        this.regionId = regionId;
    }

    public Peer getLeader() {
        return leader;
    }

    public void setLeader(Peer leader) {
        this.leader = leader;
    }

    public List<PeerStats> getDownPeers() {
        return downPeers;
    }

    public void setDownPeers(List<PeerStats> downPeers) {
        this.downPeers = downPeers;
    }

    public List<PeerStats> getPendingPeers() {
        return pendingPeers;
    }

    public void setPendingPeers(List<PeerStats> pendingPeers) {
        this.pendingPeers = pendingPeers;
    }

    public long getBytesWritten() {
        return bytesWritten;
    }

    public void setBytesWritten(long bytesWritten) {
        this.bytesWritten = bytesWritten;
    }

    public long getBytesRead() {
        return bytesRead;
    }

    public void setBytesRead(long bytesRead) {
        this.bytesRead = bytesRead;
    }

    public long getKeysWritten() {
        return keysWritten;
    }

    public void setKeysWritten(long keysWritten) {
        this.keysWritten = keysWritten;
    }

    public long getKeysRead() {
        return keysRead;
    }

    public void setKeysRead(long keysRead) {
        this.keysRead = keysRead;
    }

    public long getApproximateSize() {
        return approximateSize;
    }

    public void setApproximateSize(long approximateSize) {
        this.approximateSize = approximateSize;
    }

    public long getApproximateKeys() {
        return approximateKeys;
    }

    public void setApproximateKeys(long approximateKeys) {
        this.approximateKeys = approximateKeys;
    }

    public TimeInterval getInterval() {
        return interval;
    }

    public void setInterval(TimeInterval interval) {
        this.interval = interval;
    }

//    public List<long[]> getNodeIdRangeList() {
//        return NodeIdRangeList;
//    }
//
//    public void setNodeIdRangeList(List<long[]> nodeIdRangeList) {
//        NodeIdRangeList = nodeIdRangeList;
//    }
//
//    public List<long[]> getRelationIdRangeList() {
//        return RelationIdRangeList;
//    }
//
//    public void setRelationIdRangeList(List<long[]> relationIdRangeList) {
//        RelationIdRangeList = relationIdRangeList;
//    }
//
//    public List<long[]> getTemporalPropertyTimeRangeList() {
//        return TemporalPropertyTimeRangeList;
//    }
//
//    public void setTemporalPropertyTimeRangeList(List<long[]> temporalPropertyTimeRangeList) {
//        TemporalPropertyTimeRangeList = temporalPropertyTimeRangeList;
//    }

    @Override
    public String toString() {
        return "RegionStats{" + "regionId=" + regionId + ", leader=" + leader + ", downPeers=" + downPeers
                + ", pendingPeers=" + pendingPeers + ", bytesWritten=" + bytesWritten + ", bytesRead=" + bytesRead
                + ", keysWritten=" + keysWritten + ", keysRead=" + keysRead + ", approximateSize=" + approximateSize
                + ", approximateKeys=" + approximateKeys + ", interval=" + interval + '}';
    }
}

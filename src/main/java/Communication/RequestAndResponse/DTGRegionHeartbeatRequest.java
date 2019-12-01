package Communication.RequestAndResponse;

import Region.DTGRegionStats;
import com.alipay.sofa.jraft.rhea.cmd.pd.BaseRequest;
import Region.DTGRegion;
import com.alipay.sofa.jraft.rhea.util.Pair;

import java.util.List;


/**
 * @author :jinkai
 * @date :Created in 2019/10/24 16:19
 * @description:
 * @modified By:
 * @version:
 */

public class DTGRegionHeartbeatRequest extends BaseRequest {

    private static final long serialVersionUID = -4702161955816691656L;
    private long                            storeId;
    private long                            leastKeysOnSplit;
    private List<Pair<DTGRegion, DTGRegionStats>> regionStatsList;

    public long getStoreId() {
        return storeId;
    }

    public void setStoreId(long storeId) {
        this.storeId = storeId;
    }

    public long getLeastKeysOnSplit() {
        return leastKeysOnSplit;
    }

    public void setLeastKeysOnSplit(long leastKeysOnSplit) {
        this.leastKeysOnSplit = leastKeysOnSplit;
    }

    public List<Pair<DTGRegion, DTGRegionStats>> getRegionStatsList() {
        return regionStatsList;
    }

    public void setRegionStatsList(List<Pair<DTGRegion, DTGRegionStats>> regionStatsList) {
        this.regionStatsList = regionStatsList;
    }

    @Override
    public byte magic() {
        return DTGREGION_HEARTBEAT;
    }

    @Override
    public String toString() {
        return "RegionHeartbeatRequest{" + "storeId=" + storeId + ", leastKeysOnSplit=" + leastKeysOnSplit
                + ", regionStatsList=" + regionStatsList + "} " + super.toString();
    }
}


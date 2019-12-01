package Communication.RequestAndResponse;

import com.alipay.sofa.jraft.rhea.cmd.pd.BaseResponse;
import storage.DTGCluster;

/**
 * @author :jinkai
 * @date :Created in 2019/10/25 15:15
 * @description:
 * @modified By:
 * @version:
 */

public class GetDTGClusterInfoResponse extends BaseResponse {

    private static final long serialVersionUID = 839199900816543650L;
    private DTGCluster           cluster;

    public DTGCluster getCluster() {
        return cluster;
    }

    public void setCluster(DTGCluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public String toString() {
        return "GetClusterInfoResponse{" + "cluster=" + cluster + "} " + super.toString();
    }
}

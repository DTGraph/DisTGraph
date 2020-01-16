package options;

import UserClient.DTGSaveStore;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rhea.options.RegionEngineOptions;

/**
 * @author :jinkai
 * @date :Created in 2020/1/13 16:04
 * @description：
 * @modified By:
 * @version:
 */

public class DTGRegionEngineOptions extends RegionEngineOptions {

    private DTGSaveStore saveStore;

    public void setSaveStore(DTGSaveStore saveStore) {
        this.saveStore = saveStore;
    }

    public DTGSaveStore getSaveStore() {
        return saveStore;
    }

    @Override
    public DTGRegionEngineOptions copyNull(){
        final DTGRegionEngineOptions copy = new DTGRegionEngineOptions();
        copy.setNodeOptions(super.getNodeOptions() == null ? new NodeOptions() : super.getNodeOptions().copy());
        copy.setRaftGroupId(super.getRaftGroupId());
        copy.setRaftDataPath(super.getRaftDataPath());
        copy.setServerAddress(super.getServerAddress());
        copy.setInitialServerList(super.getInitialServerList());
        copy.setMetricsReportPeriod(super.getMetricsReportPeriod());
        copy.setDbPath(super.getDbPath());
        copy.setSaveStore(this.saveStore);
        return copy;
    }
}

package options;

import com.alipay.sofa.jraft.rhea.pd.options.PlacementDriverServerOptions;
import com.alipay.sofa.jraft.rhea.options.RheaKVStoreOptions;

/**
 * @author :jinkai
 * @date :Created in 2019/10/20 15:27
 * @description:
 * @modified By:
 * @version:
 */

public class DTGPlacementDriverServerOptions extends PlacementDriverServerOptions {

    private IdGeneratorOptions        idGeneratorOptions;
    private DTGStoreOptions           DTGStoreOptionsOpts;
    private int                       pipelineCorePoolSize;
    private int                       pipelineMaximumPoolSize;
    private boolean                   isPDServer;
    private DTGPlacementDriverOptions dtgPlacementDriverOptions;
    private long                      NewRegionNodeStartId;
    private long                      NewRegionRelationStartId;

    public DTGStoreOptions getDTGStoreOptions() {
        return DTGStoreOptionsOpts;
    }

    public void setDTGStoreOptions(DTGStoreOptions DTGStoreOptionsOpts) {
        this.DTGStoreOptionsOpts = DTGStoreOptionsOpts;
    }

    public int getPipelineCorePoolSize() {
        return pipelineCorePoolSize;
    }

    public void setPipelineCorePoolSize(int pipelineCorePoolSize) {
        this.pipelineCorePoolSize = pipelineCorePoolSize;
    }

    public int getPipelineMaximumPoolSize() {
        return pipelineMaximumPoolSize;
    }

    public void setPipelineMaximumPoolSize(int pipelineMaximumPoolSize) {
        this.pipelineMaximumPoolSize = pipelineMaximumPoolSize;
    }

    public void setIdGeneratorOptions(IdGeneratorOptions idGeneratorOptions) {
        this.idGeneratorOptions = idGeneratorOptions;
    }

    public IdGeneratorOptions getIdGeneratorOptions() {
        return idGeneratorOptions;
    }

    public boolean isPDServer() {
        return isPDServer;
    }

    public void setPDServer(boolean PDServer) {
        isPDServer = PDServer;
    }

    public DTGPlacementDriverOptions getDtgPlacementDriverOptions() {
        return dtgPlacementDriverOptions;
    }

    public void setDtgPlacementDriverOptions(DTGPlacementDriverOptions dtgPlacementDriverOptions) {
        this.dtgPlacementDriverOptions = dtgPlacementDriverOptions;
    }

    public long getNewRegionNodeStartId() {
        return NewRegionNodeStartId;
    }

    public long getNewRegionRelationStartId() {
        return NewRegionRelationStartId;
    }

    public void setNewRegionNodeStartId(long newRegionNodeStartId) {
        NewRegionNodeStartId = newRegionNodeStartId;
    }

    public void setNewRegionRelationStartId(long newRegionRelationStartId) {
        NewRegionRelationStartId = newRegionRelationStartId;
    }

    @Override
    public String toString() {
        return "PlacementDriverServerOptions{" + "DTGStoreOptionsOpts=" + DTGStoreOptionsOpts + ", pipelineCorePoolSize="
                + pipelineCorePoolSize + ", pipelineMaximumPoolSize=" + pipelineMaximumPoolSize + '}';
    }

}

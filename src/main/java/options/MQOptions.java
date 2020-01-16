package options;

import UserClient.DTGSaveStore;
import com.alipay.sofa.jraft.util.Utils;

/**
 * @author :jinkai
 * @date :Created in 2019/12/28 20:38
 * @description：
 * @modified By:
 * @version:
 */

public class MQOptions {

    private String                 RockDBPath;
    //private MQStateMachine         fsm;
    private String                 suffix;
    private String                 logUri;
    private DTGSaveStore           saveStore;



    public String getRockDBPath() {
        return RockDBPath;
    }

    public void setRockDBPath(String rockDBPath) {
        RockDBPath = rockDBPath;
    }

//    public MQStateMachine getFsm() {
//        return fsm;
//    }

//    public void setFsm(MQStateMachine fsm) {
//        this.fsm = fsm;
//    }


    public DTGSaveStore getSaveStore() {
        return saveStore;
    }

    public void setSaveStore(DTGSaveStore saveStore) {
        this.saveStore = saveStore;
    }

    public String getSuffix() {
        return suffix;
    }

    public void setSuffix(String suffix) {
        this.suffix = suffix;
    }

    public String getLogUri() {
        return logUri;
    }

    public void setLogUri(String logUri) {
        this.logUri = logUri;
    }

}

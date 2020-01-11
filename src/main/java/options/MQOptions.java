package options;

import MQ.MQStateMachine;
import com.alipay.sofa.jraft.util.Utils;
import com.codahale.metrics.MetricRegistry;

/**
 * @author :jinkai
 * @date :Created in 2019/12/28 20:38
 * @description：
 * @modified By:
 * @version:
 */

public class MQOptions {

    private String                 RockDBPath;
    private MQStateMachine         fsm;
    private String                 suffix;
    private String                 logUri;

    private boolean                enableMetrics          = false;
    private int                    timerPoolSize          = Utils.cpus() * 3 > 20 ? 20 : Utils.cpus() * 3;
    private int                    snapshotIntervalSecs   = 3600;



    public String getRockDBPath() {
        return RockDBPath;
    }

    public void setRockDBPath(String rockDBPath) {
        RockDBPath = rockDBPath;
    }

    public MQStateMachine getFsm() {
        return fsm;
    }

    public boolean isEnableMetrics() {
        return enableMetrics;
    }

    public void setFsm(MQStateMachine fsm) {
        this.fsm = fsm;
    }

    public int getTimerPoolSize() {
        return this.timerPoolSize;
    }

    public void setTimerPoolSize(final int timerPoolSize) {
        this.timerPoolSize = timerPoolSize;
    }

    public String getSuffix() {
        return suffix;
    }

    public void setSuffix(String suffix) {
        this.suffix = suffix;
    }

    public int getSnapshotIntervalSecs() {
        return this.snapshotIntervalSecs;
    }

    public void setSnapshotIntervalSecs(final int snapshotIntervalSecs) {
        this.snapshotIntervalSecs = snapshotIntervalSecs;
    }

    public String getLogUri() {
        return logUri;
    }

    public void setLogUri(String logUri) {
        this.logUri = logUri;
    }

}

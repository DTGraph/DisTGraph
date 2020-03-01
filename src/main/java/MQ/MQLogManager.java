package MQ;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.storage.LogStorage;

/**
 * @author :jinkai
 * @date :Created in 2019/12/29 20:45
 * @description：
 * @modified By:
 * @version:
 */

public class MQLogManager implements Lifecycle {

    private LogStorage logStorage;

    @Override
    public boolean init(Object opts) {
        return false;
    }

    @Override
    public void shutdown() {

    }
}

package raft;

import Element.DTGOperation;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author :jinkai
 * @date :Created in 2019/10/22 11:06
 * @description:
 * @modified By:
 * @version:
 */

public class EntityEntryClosureAdapter implements EntityStoreClosure{

    private static final Logger LOG = LoggerFactory.getLogger(EntityEntryClosureAdapter.class);

    private final EntityStoreClosure done;
    private final DTGOperation op;

    public EntityEntryClosureAdapter(final EntityStoreClosure done, final DTGOperation op){
        this.done = done;
        this.op = op;
    }

    public DTGOperation getOperation() {
        return op;
    }

    public EntityStoreClosure getDone() {
        return done;
    }

    @Override
    public Errors getError() {
        if (this.done != null) {
            return this.done.getError();
        }
        return null;
    }

    @Override
    public void setError(Errors error) {
        if (this.done != null) {
            this.done.setError(error);
        }
    }

    @Override
    public Object getData() {
        if (this.done != null) {
            return this.done.getData();
        }
        return null;
    }

    @Override
    public void setData(Object data) {
        if (this.done != null) {
            this.done.setData(data);
        }
    }

    @Override
    public void run(Status status) {
        if (status.isOk()) {
            setError(Errors.NONE);
        } else {
            LOG.error("Fail status: {}.", status);
            if (getError() == null) {
                switch (status.getRaftError()) {
                    case SUCCESS:
                        setError(Errors.NONE);
                        break;
                    case EINVAL:
                        setError(Errors.INVALID_REQUEST);
                        break;
                    case EIO:
                        setError(Errors.STORAGE_ERROR);
                        break;
                    default:
                        setError(Errors.LEADER_NOT_AVAILABLE);
                        break;
                }
            }
        }
        if (done != null) {
            done.run(status);
        }
    }
}

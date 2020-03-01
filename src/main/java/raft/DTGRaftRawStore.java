package raft;

import Element.DTGOperation;
import Region.DTGLockClosure;
import Region.DTGRegion;
import Region.FirstPhaseClosure;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.serialization.Serializers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;

import java.nio.ByteBuffer;

/**
 * @author :jinkai
 * @date :Created in 2019/10/22 10:39
 * @description:
 * @modified By:
 * @version:
 */

public class DTGRaftRawStore implements DTGRawStore {

    private static final Logger LOG = LoggerFactory.getLogger(DTGRaftRawStore.class);

    private Node node;
    private DTGRawStore dtgRawStore;

    public DTGRaftRawStore(Node node, DTGRawStore dtgRawStore){
        this.node = node;
        this.dtgRawStore = dtgRawStore;
    }

    @Override
    public Iterator localIterator() {
        return null;
    }

    @Override
    public void saveLog(LogStoreClosure closure) {

    }

    @Override
    public void setLock(final DTGOperation op, final DTGLockClosure closure, DTGRegion region) {

    }

    @Override
    public void sendLock(DTGOperation op, EntityStoreClosure closure) {

    }

    @Override
    public void commitSuccess(long version) {

    }

    @Override
    public void firstPhaseProcessor(DTGOperation op, final FirstPhaseClosure closure, DTGRegion region) {

    }

    @Override
    public void ApplyEntityEntries(DTGOperation op, EntityStoreClosure closure) {
        if(!isLeader()){
            closure.setError(Errors.NOT_LEADER);
            closure.run(new Status(RaftError.EPERM, "Not leader"));
            return;
        }
        final Task task = new Task();
        task.setData(ByteBuffer.wrap(Serializers.getDefault().writeObject(op)));
        task.setDone(new EntityEntryClosureAdapter(closure, op));
        this.node.apply(task);
    }

    @Override
    public void readOnlyEntityEntries(DTGOperation op, EntityStoreClosure closure) {

    }

    @Override
    public void merge() {

    }

    @Override
    public void split() {

    }

    private boolean isLeader() {
        return this.node.isLeader();
    }

    public DTGRawStore getDtgRawStore() {
        return dtgRawStore;
    }
}

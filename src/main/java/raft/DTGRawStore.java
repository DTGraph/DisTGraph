package raft;

import Element.DTGOperation;
import Region.DTGLockClosure;
import Region.DTGRegion;
import Region.FirstPhaseClosure;
import scala.collection.Iterator;

/**
 * @author :jinkai
 * @date :Created in 2019/10/22 10:40
 * @description:
 * @modified By:
 * @version:
 */

public interface DTGRawStore {

    void saveLog(LogStoreClosure closure);

    void setLock(final DTGOperation op, final DTGLockClosure closure, DTGRegion region);

    void commitSuccess(final long version);

    void firstPhaseProcessor(final DTGOperation op, final FirstPhaseClosure closure, DTGRegion region);

    void ApplyEntityEntries(final DTGOperation op, final EntityStoreClosure closure);

    void readOnlyEntityEntries(final DTGOperation op, final EntityStoreClosure closure);

    void secondRead(final DTGOperation op, final EntityStoreClosure closure, DTGRegion region);

    void merge();

    void split();
}

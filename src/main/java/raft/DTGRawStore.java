package raft;

import Element.DTGOperation;
import scala.collection.Iterator;

/**
 * @author :jinkai
 * @date :Created in 2019/10/22 10:40
 * @description:
 * @modified By:
 * @version:
 */

public interface DTGRawStore  {

    Iterator localIterator();

    void saveLog(LogStoreClosure closure);

    void ApplyEntityEntries(final DTGOperation op, final EntityStoreClosure closure);

    void readOnlyEntityEntries(final DTGOperation op, final EntityStoreClosure closure);

    void merge();

    void split();
}

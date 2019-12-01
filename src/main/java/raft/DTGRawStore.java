package raft;

import Element.DTGOpreration;
import Element.EntityEntry;
import scala.collection.Iterator;

import java.util.List;

/**
 * @author :jinkai
 * @date :Created in 2019/10/22 10:40
 * @description:
 * @modified By:
 * @version:
 */

public interface DTGRawStore  {

    Iterator localIterator();

    void ApplyEntityEntries(final DTGOpreration op, final EntityStoreClosure closure);

    void readOnlyEntityEntries(final DTGOpreration op, final EntityStoreClosure closure);

    void merge();

    void split();
}

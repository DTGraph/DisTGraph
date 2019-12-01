package storage;

/**
 * @author :jinkai
 * @date :Created in 2019/10/16 13:31
 * @description:
 * @modified By:
 * @version:
 */

public interface RawEntityStore {

    /**
     * Returns a heap-allocated iterator over the contents of the
     * database.
     *
     * Caller should close the iterator when it is no longer needed.
     * The returned iterator should be closed before this db is closed.
     *
     * <pre>
     *     KVIterator it = unsafeLocalIterator();
     *     try {
     *         // do something
     *     } finally {
     *         it.close();
     *     }
     * <pre/>
     */
    EntityIterator localIterator();


}

package storage;

/**
 * @author :jinkai
 * @date :Created in 2019/10/16 13:33
 * @description:
 * @modified By:
 * @version:
 */

public interface EntityIterator extends AutoCloseable {

    /**
     * An iterator is either positioned at an entry, or not valid.
     * This method returns true if the iterator is valid.
     *
     * @return true if iterator is valid.
     */
    boolean isValid();

    /**
     * Position at the first entry in the source.  The iterator is Valid()
     * after this call if the source is not empty.
     */
    void seekToFirst();

    /**
     * Position at the last entry in the source.  The iterator is
     * valid after this call if the source is not empty.
     */
    void seekToLast();

    /**
     * Position at the first entry in the source whose key is that or
     * past target.
     *
     * The iterator is valid after this call if the source contains
     * a key that comes at or past target.
     *
     * @param target byte array describing a key or a
     *               key prefix to seek for.
     */
    void seek(final byte[] target);

    /**
     * Position at the first entry in the source whose key is that or
     * before target.
     *
     * The iterator is valid after this call if the source contains
     * a key that comes at or before target.
     *
     * @param target byte array describing a key or a
     *               key prefix to seek for.
     */
    void seekForPrev(final byte[] target);

    /**
     * Moves to the next entry in the source.  After this call, Valid() is
     * true if the iterator was not positioned at the last entry in the source.
     *
     * REQUIRES: {@link #isValid()}
     */
    void next();

    /**
     * Moves to the previous entry in the source.  After this call, Valid() is
     * true if the iterator was not positioned at the first entry in source.
     *
     * REQUIRES: {@link #isValid()}
     */
    void prev();
}

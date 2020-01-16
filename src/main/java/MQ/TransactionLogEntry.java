package MQ;

import com.alipay.sofa.jraft.entity.Checksum;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.util.CrcUtil;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * @author :jinkai
 * @date :Created in 2020/1/2 15:07
 * @description：
 * @modified By:
 * @version:
 */

public class TransactionLogEntry implements Checksum, Serializable {
    private static final long serialVersionUID = -8724248381600438846L;
    /** entry type */
    private EnumOutter.EntryType type;
    /** log id with index/term */
    private MQLogId id;
    /** entry data */
    private byte[] data;
    /** checksum for log entry*/
    private long                 checksum;
    /** true when the log has checksum **/
    private boolean              hasChecksum;

    public TransactionLogEntry(long index) {
        super();
        id = new MQLogId(index);
    }

    public TransactionLogEntry(final EnumOutter.EntryType type, long index) {
        super();
        this.type = type;
        id = new MQLogId(index);
    }

    @Override
    public long checksum() {
        long c = checksum(this.type.getNumber(), this.id.checksum());
        if (this.data != null && data.length > 0) {
            c = checksum(c, CrcUtil.crc64(this.data));
        }
        return c;
    }

//    @SuppressWarnings("DeprecatedIsStillUsed")
//    @Deprecated
//    public byte[] encode() {
//        return MQV1Encoder.INSTANCE.encode(this);
//    }


    @SuppressWarnings("DeprecatedIsStillUsed")
//    @Deprecated
//    public boolean decode(final byte[] content) {
//        if (content == null || content.length == 0) {
//            return false;
//        }
//        if (content[0] != MQV1LogEntryCodecFactory.MAGIC) {
//            // Corrupted log
//            return false;
//        }
//        MQV1Decoder.INSTANCE.decode(this, content);
//        return true;
//    }

    /**
     * Returns whether the log entry has a checksum.
     * @return true when the log entry has checksum, otherwise returns false.
     * @since 1.2.26
     */
    public boolean hasChecksum() {
        return this.hasChecksum;
    }

    /**
     * Returns true when the log entry is corrupted, it means that the checksum is mismatch.
     * @since 1.2.6
     * @return true when the log entry is corrupted, otherwise returns false
     */
    public boolean isCorrupted() {
        return this.hasChecksum && this.checksum != checksum();
    }

    /**
     * Returns the checksum of the log entry. You should use {@link #hasChecksum} to check if
     * it has checksum.
     * @return checksum value
     */
    public long getChecksum() {
        return this.checksum;
    }

    public void setChecksum(final long checksum) {
        this.checksum = checksum;
        this.hasChecksum = true;
    }

    public EnumOutter.EntryType getType() {
        return this.type;
    }

    public void setType(final EnumOutter.EntryType type) {
        this.type = type;
    }

    public MQLogId getId() {
        return this.id;
    }

    public void setId(final MQLogId id) {
        this.id = id;
    }

    public ByteBuffer getData() {
        if(data == null){
            return null;
        }
        return ByteBuffer.wrap(this.data);
    }

    public void setData(final byte[] data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "TransactionLogEntry [type=" + this.type + ", id=" + this.id + ", data="
                + (this.data != null ? data.length > 0 : 0) + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.data == null) ? 0 : this.data.hashCode());
        result = prime * result + ((this.id == null) ? 0 : this.id.hashCode());
        result = prime * result + ((this.type == null) ? 0 : this.type.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        TransactionLogEntry other = (TransactionLogEntry) obj;
        if (this.data == null) {
            if (other.data != null) {
                return false;
            }
        } else if (!this.data.equals(other.data)) {
            return false;
        }
        if (this.id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!this.id.equals(other.id)) {
            return false;
        }
        return this.type == other.type;
    }
}

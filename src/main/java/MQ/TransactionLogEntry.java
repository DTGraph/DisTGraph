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
    private long version = -1;
    /** entry data */
    private byte[] data;
    /** checksum for log entry*/
    private long                 checksum;
    /** true when the log has checksum **/
    private boolean              hasChecksum;

    private long mainRegion;

    private byte status;

    public TransactionLogEntry(long version) {
        super();
       this.version = version;
    }

    public TransactionLogEntry(final EnumOutter.EntryType type, long version) {
        super();
        this.type = type;
        this.version = version;
    }

    @Override
    public long checksum() {
        long c = checksum(this.type.getNumber(), this.version);
        if (this.data != null && data.length > 0) {
            c = checksum(c, CrcUtil.crc64(this.data));
        }
        return c;
    }

    public byte getStatus() {
        return status;
    }

    public void setStatus(byte status) {
        this.status = status;
    }

    public long getMainRegion() {
        return mainRegion;
    }

    public void setMainRegion(long mainRegion) {
        this.mainRegion = mainRegion;
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

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public ByteBuffer getData() {
        if(data == null){
            return null;
        }
        return ByteBuffer.wrap(this.data);
    }

    public byte[] getByteData() {
        return this.data;
    }

    public void setData(final byte[] data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "TransactionLogEntry [type=" + this.type + ", id=" + this.version + ", data="
                + (this.data != null ? data.length > 0 : 0) + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.data == null) ? 0 : this.data.hashCode());
        result = prime * result + (int) (this.version ^ (this.version >>> 32));
        //result = prime * result + ((this.version < 0) ? 0 : this.id.hashCode());
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
        if (this.version < 0) {
            if (other.version >= 0) {
                return false;
            }
        } else if (this.version != other.version) {
            return false;
        }
        return this.type == other.type;
    }
}

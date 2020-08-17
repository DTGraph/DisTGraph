package MQ;

import DBExceptions.LogIdException;
import MQ.codec.MQLogEntryDecoder;
import MQ.codec.MQLogEntryEncoder;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.util.*;
import options.MQLogStorageOptions;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tool.ObjectAndByte;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author :jinkai
 * @date :Created in 2019/12/29 21:38
 * @description：
 * @modified By:
 * @version:
 */

public class MQRocksDBLogStorage implements MQLogStorage, Describer {

    private static final Logger LOG = LoggerFactory.getLogger(MQRocksDBLogStorage.class);

    public static final byte[] FIRST_LOG_IDX_KEY  = Utils.getBytes("meta/0firstLogIndex");
//    public static final byte[] COMMIT_LOG_IDX_KEY = Utils.getBytes("meta/1commitLogIndex");
//    public static final byte[] MAX_VERSION        = Utils.getBytes("meta/2maxVersion");

    static {
        RocksDB.loadLibrary();
    }

    private interface WriteBatchTemplate {

        void execute(WriteBatch batch) throws RocksDBException, IOException;
    }

    private final String                    path;
    //private final boolean                   sync;
    //private final boolean                   openStatistics;
    private RocksDB                         db;
    private DBOptions                       dbOptions;
    private WriteOptions                    writeOptions;
    private final List<ColumnFamilyOptions> cfOptions     = new ArrayList<>();
    private ColumnFamilyHandle              defaultHandle;
    private ColumnFamilyHandle              confHandle;
    private ReadOptions                     totalOrderReadOptions;
    private final ReadWriteLock             readWriteLock = new ReentrantReadWriteLock();
    private final Lock                      readLock      = this.readWriteLock.readLock();
    private final Lock                      writeLock     = this.readWriteLock.writeLock();

    private volatile long                   firstLogIndex  = 1;
    private volatile long                   commitLogIndex = 0;

    private volatile boolean                hasLoadFirstLogIndex;

    public MQRocksDBLogStorage(final String path) {
        super();
        this.path = path;
    }

    public static DBOptions createDBOptions() {
        return StorageOptionsFactory.getRocksDBOptions(MQRocksDBLogStorage.class);
    }

    public static ColumnFamilyOptions createColumnFamilyOptions() {
        final BlockBasedTableConfig tConfig = StorageOptionsFactory
                .getRocksDBTableFormatConfig(MQRocksDBLogStorage.class);
        return StorageOptionsFactory.getRocksDBColumnFamilyOptions(MQRocksDBLogStorage.class) //
                .useFixedLengthPrefixExtractor(8) //
                .setTableFormatConfig(tConfig) //
                .setMergeOperator(new StringAppendOperator());
    }

    @Override
    public boolean init(MQLogStorageOptions opts) {
        Requires.requireNonNull(opts.getLogEntryCodecFactory(), "Null log entry codec factory");
        this.writeLock.lock();
        try {
            if (this.db != null) {
                LOG.warn("MQRocksDBLogStorage init() already.");
                return true;
            }
            this.dbOptions = createDBOptions();
            this.totalOrderReadOptions = new ReadOptions();
            this.totalOrderReadOptions.setTotalOrderSeek(true);

            this.writeOptions = new WriteOptions();
            this.writeOptions.setSync(true);
            this.totalOrderReadOptions = new ReadOptions();
            this.totalOrderReadOptions.setTotalOrderSeek(true);
            return initAndLoad();
        } catch (RocksDBException e) {
            e.printStackTrace();
            return false;
        }finally {
            this.writeLock.unlock();
        }

    }

    @Override
    public long getFirstLogIndex() {
        this.readLock.lock();
        RocksIterator it = null;
        try {
            if (this.hasLoadFirstLogIndex) {
                return this.firstLogIndex;
            }
            checkState();
            it = this.db.newIterator(this.defaultHandle, this.totalOrderReadOptions);
            it.seekToFirst();
            if (it.isValid()) {
                final long ret = Bits.getLong(it.key(), 0);
                saveFirstLogIndex(ret);
                setFirstLogIndex(ret);
                return ret;
            }
            return 0L;
        } finally {
            if (it != null) {
                it.close();
            }
            this.readLock.unlock();
        }
    }

    @Override
    public long getLastLogIndex() {
        this.readLock.lock();
        checkState();
        try (final RocksIterator it = this.db.newIterator(this.defaultHandle, this.totalOrderReadOptions)) {
            it.seekToLast();
            if (it.isValid()) {
                return Bits.getLong(it.key(), 0);
            }
            return 0L;
        } finally {
            this.readLock.unlock();
        }
    }

//    public long getCommitLogIndex() {
//        return getCommitLogIndex(false);
//    }

//    @Override
//    public long getCommitLogIndex(boolean flush) {
//        this.readLock.lock();
//        RocksIterator it = null;
//        try {
//            if(!flush){
//                return this.commitLogIndex;
//            }
//            checkState();
//            final byte[] bs = this.db.get(this.confHandle, this.COMMIT_LOG_IDX_KEY);
//            if(bs != null){
//                return Bits.getLong(bs, 0);
//            }
//            return 0L;
//        } catch (RocksDBException e) {
//            e.printStackTrace();
//        } finally {
//            if (it != null) {
//                it.close();
//            }
//            this.readLock.unlock();
//        }
//        return 0L;
//    }
//
//    @Override
//    public void setCommitLogIndex(long index) {
//        if(index < this.firstLogIndex || index < this.commitLogIndex){
//            throw new LogIdException("commit index out of bound : index = " + index + ", firstLogIndex :" + firstLogIndex + ", commitLogIndex" + commitLogIndex);
//        }
//        saveCommitLogIndex(index);
//
//    }

    @Override
    public TransactionLogEntry getEntry(long version) {
        this.readLock.lock();
        try {
            if (this.hasLoadFirstLogIndex && version < this.firstLogIndex) {
                return null;
            }
            final byte[] keyBytes = getKeyBytes(version);
            final byte[] bs = onDataGet(getValueFromRocksDB(keyBytes));
            if (bs != null) {
                final TransactionLogEntry entry = (TransactionLogEntry)ObjectAndByte.toObject(bs);
                if (entry != null) {
                    return entry;
                } else {
                    LOG.error("Bad log entry format for version={}, the log data is: {}.", version, BytesUtil.toHex(bs));
                    // invalid data remove? TODO
                    return null;
                }
            }
        } catch (final RocksDBException | IOException e) {
            LOG.error("Fail to get log entry at version {}.", version, e);
        } finally {
            this.readLock.unlock();
        }
        return null;
    }

    @Override
    public List<TransactionLogEntry> getEntries(long start, long end){
        List<TransactionLogEntry> list = new ArrayList<>();
        this.readLock.lock();
        try{
            if(start > end){
                return null;
            }
            RocksIterator it = this.db.newIterator(this.defaultHandle, this.totalOrderReadOptions);
            it.seekToFirst();
            while (it.isValid()){
                if(it.key().length == 8){
                    long index = Bits.getLong(it.key(), 0);
                    if(index >= start && index <= end){
                        TransactionLogEntry entry = (TransactionLogEntry)ObjectAndByte.toObject(it.value());
                        list.add(entry);
                    }
                }
                it.next();
            }
            return list;
        }finally {
            this.readLock.unlock();
        }
    }

//    @Override
//    public int getMaxVersion() {
//        this.readLock.lock();
//        RocksIterator it = null;
//        try {
//            checkState();
//            final byte[] bs = this.db.get(this.confHandle, this.MAX_VERSION);
//            if(bs != null){
//                return Bits.getInt(bs, 0);
//            }
//            return 1;
//        } catch (RocksDBException e) {
//            e.printStackTrace();
//        } finally {
//            if (it != null) {
//                it.close();
//            }
//            this.readLock.unlock();
//        }
//        return 1;
//    }
//
//    @Override
//    public void setMaxVersion(int maxVersion) {
//        this.readLock.lock();
//        try {
//            final byte[] vs = new byte[4];
//            Bits.putInt( vs, 0, maxVersion);
//            checkState();
//            this.db.put(this.confHandle, this.writeOptions, MAX_VERSION, vs);
//        } catch (final RocksDBException e) {
//            LOG.error("Fail to save commit log index {}.", maxVersion, e);
//        } finally {
//            this.readLock.unlock();
//        }
//    }


    @Override
    public boolean appendEntry(TransactionLogEntry entry) {
        //System.out.println("save log :" + System.currentTimeMillis());
        this.readLock.lock();
        try {
            if (this.db == null) {
                LOG.warn("DB not initialized or destroyed.");
                return false;
            }
            final long logIndex = entry.getVersion();
            final byte[] valueBytes = ObjectAndByte.toByteArray(entry);
            final byte[] newValueBytes = onDataAppend(logIndex, valueBytes);
            this.db.put(this.defaultHandle, this.writeOptions, getKeyBytes(logIndex), newValueBytes);
            System.out.println("end save log :" + System.currentTimeMillis());
            return true;
        } catch (final RocksDBException | IOException e) {
            LOG.error("Fail to append entry.", e);
            return false;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public int appendEntries(List<TransactionLogEntry> entries) {
        //System.out.println("save batch log :" + System.currentTimeMillis());
        if (entries == null || entries.isEmpty()) {
            return 0;
        }
        final int entriesCount = entries.size();
        final boolean ret = executeBatch(batch -> {
            for (int i = 0; i < entriesCount; i++) {
                final TransactionLogEntry entry = entries.get(i);
                addDataBatch(entry, batch);
            }
        });
        if (ret) {
            //System.out.println("end save log :" + System.currentTimeMillis());
            return entriesCount;
        } else {
            return 0;
        }
    }

    public boolean removeEntry(long version){
        this.readLock.lock();
        try{
            this.db.delete(this.defaultHandle, getKeyBytes(version));
        } catch (RocksDBException e) {
            e.printStackTrace();
        } finally {
            this.readLock.unlock();
        }
        return true;
    }

    @Override
    public boolean truncatePrefix(long firstIndexKept) {
        this.readLock.lock();
        try {
            final long startIndex = getFirstLogIndex();
            final boolean ret = saveFirstLogIndex(firstIndexKept);
            if (ret) {
                setFirstLogIndex(firstIndexKept);
            }
            truncatePrefixInBackground(startIndex, firstIndexKept);
            return ret;
        } finally {
            this.readLock.unlock();
        }
    }

    private void truncatePrefixInBackground(final long startIndex, final long firstIndexKept) {
        // delete logs in background.
        Utils.runInThread(() -> {
            this.readLock.lock();
            try {
                if (this.db == null) {
                    return;
                }
                this.db.deleteRange(this.defaultHandle, getKeyBytes(startIndex), getKeyBytes(firstIndexKept));
                this.db.deleteRange(this.confHandle, getKeyBytes(startIndex), getKeyBytes(firstIndexKept));
            } catch (RocksDBException e) {
                e.printStackTrace();
            } finally {
                this.readLock.unlock();
            }
        });
    }

    @Override
    public boolean truncateSuffix(long lastIndexKept) {
        this.readLock.lock();
        try {
            try {
                onTruncateSuffix(lastIndexKept);
            } finally {
                this.db.deleteRange(this.defaultHandle, this.writeOptions, getKeyBytes(lastIndexKept + 1),
                        getKeyBytes(getLastLogIndex() + 1));
                this.db.deleteRange(this.confHandle, this.writeOptions, getKeyBytes(lastIndexKept + 1),
                        getKeyBytes(getLastLogIndex() + 1));
            }
            return true;
        } catch (final RocksDBException | IOException e) {
            LOG.error("Fail to truncateSuffix {}.", lastIndexKept, e);
        } finally {
            this.readLock.unlock();
        }
        return false;
    }

    protected void onTruncateSuffix(final long lastIndexKept) throws RocksDBException, IOException {
    }

    @Override
    public boolean reset(long nextLogIndex) {
        if (nextLogIndex <= 0) {
            throw new IllegalArgumentException("Invalid next log index.");
        }
        this.writeLock.lock();
        try (final Options opt = new Options()) {
            TransactionLogEntry entry = getEntry(nextLogIndex);
            closeDB();
            try {
                RocksDB.destroyDB(this.path, opt);
                if (initAndLoad()) {
                    if (entry == null) {
                        entry = new TransactionLogEntry(0);
                        entry.setType(EnumOutter.EntryType.ENTRY_TYPE_NO_OP);
                        entry.setVersion(nextLogIndex);
                        //entry.setId(new MQLogId(nextLogIndex, 0));
                        LOG.warn("Entry not found for nextLogIndex {} when reset.", nextLogIndex);
                    }
                    return appendEntry(entry);
                } else {
                    return false;
                }
            } catch (final RocksDBException e) {
                LOG.error("Fail to reset next log index.", e);
                return false;
            }
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void shutdown() {
        this.writeLock.lock();
        try {
            // The shutdown order is matter.
            // 1. close column family handles
            closeDB();
            // 2. close column family options.
            for (final ColumnFamilyOptions opt : this.cfOptions) {
                opt.close();
            }
            // 3. close options
            this.dbOptions.close();
            this.writeOptions.close();
            this.totalOrderReadOptions.close();
            // 4. help gc.
            this.cfOptions.clear();
            this.dbOptions = null;
            this.writeOptions = null;
            this.totalOrderReadOptions = null;
            this.defaultHandle = null;
            this.confHandle = null;
            this.db = null;
            LOG.info("DB destroyed, the db path is: {}.", this.path);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void describe(Printer out) {
        this.readLock.lock();
        try {
            if (this.db != null) {
                out.println(this.db.getProperty("rocksdb.stats"));
            }
            out.println("");
        } catch (final RocksDBException e) {
            out.println(e);
        } finally {
            this.readLock.unlock();
        }
    }

    private boolean initAndLoad() throws RocksDBException {
        this.hasLoadFirstLogIndex = false;
        this.firstLogIndex = 0;
        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        final ColumnFamilyOptions cfOption = createColumnFamilyOptions();
        this.cfOptions.add(cfOption);
        // Column family to store configuration log entry.
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor("Configuration".getBytes(), cfOption));
        // Default column family to store user data log entry.
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOption));

        openDB(columnFamilyDescriptors);
        load();
        return onInitLoaded();
    }

    private void openDB(final List<ColumnFamilyDescriptor> columnFamilyDescriptors) throws RocksDBException {
        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

        final File dir = new File(this.path);
        if (dir.exists() && !dir.isDirectory()) {
            throw new IllegalStateException("Invalid log path, it's a regular file: " + this.path);
        }
        this.db = RocksDB.open(this.dbOptions, this.path, columnFamilyDescriptors, columnFamilyHandles);

        assert (columnFamilyHandles.size() == 2);
        this.confHandle = columnFamilyHandles.get(0);
        this.defaultHandle = columnFamilyHandles.get(1);
    }

    private void load() {
        checkState();
        try (final RocksIterator it = this.db.newIterator(this.confHandle, this.totalOrderReadOptions)) {
            it.seekToFirst();
            while (it.isValid()) {
                final byte[] ks = it.key();
                final byte[] bs = it.value();
                if (Arrays.equals(FIRST_LOG_IDX_KEY, ks)) {
                    setFirstLogIndex(Bits.getLong(bs, 0));
                    truncatePrefixInBackground(0L, this.firstLogIndex);
                } else {
                    LOG.warn("Unknown entry in configuration storage key={}, value={}.", BytesUtil.toHex(ks),
                            BytesUtil.toHex(bs));
                }
                it.next();
            }
        }
//        this.commitLogIndex = getCommitLogIndex(true);
//        System.out.println("init : commitLogIndex : " + commitLogIndex);
    }

    @Override
    public List<TransactionLogEntry> getUnCommitLog(){
        List<TransactionLogEntry> list = new ArrayList<>();
        this.readLock.lock();
        try{
            RocksIterator it = this.db.newIterator(this.defaultHandle, this.totalOrderReadOptions);
            it.seekToFirst();
            while (it.isValid()){
                if(it.key().length == 8){
                    long index = Bits.getLong(it.key(), 0);
                    TransactionLogEntry entry = (TransactionLogEntry)ObjectAndByte.toObject(it.value());
                    list.add(entry);
                }
                it.next();
            }
            return list;
        }finally {
            this.readLock.unlock();
        }
    }

    private void checkState() {
        Requires.requireNonNull(this.db, "DB not initialized or destroyed");
    }

    protected boolean onInitLoaded() {
        return true;
    }

    private void setFirstLogIndex(final long index) {
        this.firstLogIndex = index;
        this.hasLoadFirstLogIndex = true;
    }

    private boolean saveFirstLogIndex(final long firstLogIndex) {
        this.readLock.lock();
        try {
            final byte[] vs = new byte[8];
            Bits.putLong(vs, 0, firstLogIndex);
            checkState();
            this.db.put(this.confHandle, this.writeOptions, FIRST_LOG_IDX_KEY, vs);
            return true;
        } catch (final RocksDBException e) {
            LOG.error("Fail to save first log index {}.", firstLogIndex, e);
            return false;
        } finally {
            this.readLock.unlock();
        }
    }

//    private boolean saveCommitLogIndex(final long CommitLogIndex) {
//        this.readLock.lock();
//        try {
//            this.commitLogIndex = CommitLogIndex;
//            final byte[] vs = new byte[8];
//            Bits.putLong(vs, 0, CommitLogIndex);
//            checkState();
//            this.db.put(this.confHandle, this.writeOptions, COMMIT_LOG_IDX_KEY, vs);
//            //System.out.println("save commitIndex: " + CommitLogIndex);
//            return true;
//        } catch (final RocksDBException e) {
//            LOG.error("Fail to save commit log index {}.", CommitLogIndex, e);
//            return false;
//        } finally {
//            this.readLock.unlock();
//        }
//    }



    /**
     * Execute write batch template.
     *
     * @param template write batch template
     */
    private boolean executeBatch(final WriteBatchTemplate template) {
        this.readLock.lock();
        if (this.db == null) {
            LOG.warn("DB not initialized or destroyed.");
            this.readLock.unlock();
            return false;
        }
        try (final WriteBatch batch = new WriteBatch()) {
            template.execute(batch);
            this.db.write(this.writeOptions, batch);
        } catch (final RocksDBException e) {
            LOG.error("Execute batch failed with rocksdb exception.", e);
            return false;
        } catch (final IOException e) {
            LOG.error("Execute batch failed with io exception.", e);
            return false;
        } finally {
            this.readLock.unlock();
        }
        return true;
    }

    private void closeDB() {
        this.confHandle.close();
        this.defaultHandle.close();
        this.db.close();
    }

    protected byte[] getKeyBytes(final long index) {
        final byte[] ks = new byte[8];
        Bits.putLong(ks, 0, index);
        return ks;
    }

    protected byte[] onDataGet(final byte[] value) throws IOException {
        return value;
    }

    protected byte[] getValueFromRocksDB(final byte[] keyBytes) throws RocksDBException {
        checkState();
        return this.db.get(this.defaultHandle, keyBytes);
    }

//    private void addConfBatch(final TransactionLogEntry entry, final WriteBatch batch) throws RocksDBException {
//        final byte[] ks = getKeyBytes(entry.getVersion());
//        final byte[] content = this.logEntryEncoder.encode(entry);
//        batch.put(this.defaultHandle, ks, content);
//        batch.put(this.confHandle, ks, content);
//    }

    private void addDataBatch(final TransactionLogEntry entry, final WriteBatch batch) throws RocksDBException, IOException {
        final long logIndex = entry.getVersion();
        //final byte[] s = ObjectAndByte.toByteArray(new TransactionLogEntry(1));

        byte[] content = ObjectAndByte.toByteArray(entry);
        //final byte[] content = this.logEntryEncoder.encode(entry);
        batch.put(this.defaultHandle, getKeyBytes(logIndex), onDataAppend(logIndex, content));
        //System.out.println("add entry " + logIndex);
    }

    /**
     * Called before appending data entry.
     *
     * @param logIndex the log index
     * @param value    the data value in log entry.
     * @return the new value
     */
    protected byte[] onDataAppend(final long logIndex, final byte[] value) throws IOException {
        return value;
    }

}

package Region;

import com.alipay.sofa.jraft.util.*;
import options.MQLogStorageOptions;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tool.ObjectAndByte;

import java.io.File;
import java.io.IOException;
import java.util.*;
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

public class RegionSet2 implements Describer {

    private static final Logger LOG = LoggerFactory.getLogger(RegionSet2.class);

    public static final byte[] FIRST_LOG_IDX_KEY  = Utils.getBytes("meta/0firstLogIndex");
    public static final byte[] LOG_STATUS_IDX_KEY = Utils.getBytes("meta/1statusLogIndex");
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
    private ColumnFamilyHandle              statusHandle;
    private ReadOptions                     totalOrderReadOptions;
    private final ReadWriteLock             readWriteLock = new ReentrantReadWriteLock();
    private final Lock                      readLock      = this.readWriteLock.readLock();
    private final Lock                      writeLock     = this.readWriteLock.writeLock();

    private volatile long                   firstLogIndex  = 0;
    private volatile long                   firstLogStatusIndex  = 0;
    private volatile long                   commitLogIndex = 0;

    private volatile boolean                hasLoadFirstLogIndex = false;
    private volatile boolean                hasLoadFirstStatusLogIndex = false;
    private volatile boolean                batchOption = false;

    public RegionSet2(final String path) {
        super();
        this.path = path;
    }

    public static DBOptions createDBOptions() {
        return StorageOptionsFactory.getRocksDBOptions(RegionSet2.class);
    }

    public static ColumnFamilyOptions createColumnFamilyOptions() {
        final BlockBasedTableConfig tConfig = StorageOptionsFactory
                .getRocksDBTableFormatConfig(RegionSet2.class);
        return StorageOptionsFactory.getRocksDBColumnFamilyOptions(RegionSet2.class) //
                .useFixedLengthPrefixExtractor(8) //
                .setTableFormatConfig(tConfig) //
                .setMergeOperator(new StringAppendOperator());
    }

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


    private LinkedList<BatchEntry> batchList = new LinkedList();

    public void setBatch(){
        this.batchOption = true;
    }

    public void closeBatch(){
        this.batchOption = false;
        final int entriesCount = batchList.size();
        final boolean ret = executeBatch(batch -> {
            for (int i = 0; i < entriesCount; i++) {
                addDataBatch(batchList.poll(), batch);
            }
        });
    }

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

    private void addDataBatch(final BatchEntry entry, final WriteBatch batch) throws RocksDBException, IOException {
        final byte[] valueBytes = ObjectAndByte.toByteArray(entry.value);
        final byte[] keyBytes = ObjectAndByte.toByteArray(entry.key);
        batch.put(this.defaultHandle, keyBytes, valueBytes);
    }

    public Object getEntry(String key) {
        this.readLock.lock();
        try {
            byte[] value =  getValueFromRocksDB(ObjectAndByte.toByteArray(key), this.defaultHandle);
            if(value == null)return null;
            return ObjectAndByte.toObject(value);
        } catch (final RocksDBException e) {
            LOG.error("Fail to get log entry at version {}.", e);
        } finally {
            this.readLock.unlock();
        }
        return null;
    }



    public boolean appendEntry(String key, Object value) {
        //System.out.println("appendEntry :" + key);
        if(batchOption){
            this.batchList.offer(new BatchEntry(key, value));
            return true;
        }
        this.readLock.lock();
        try {
            if (this.db == null) {
                LOG.warn("DB not initialized or destroyed.");
                return false;
            }

            this.db.put(this.defaultHandle, this.writeOptions, ObjectAndByte.toByteArray(key), ObjectAndByte.toByteArray(value));//do not break

            return true;
        } catch (final RocksDBException e) {
            LOG.error("Fail to append entry.", e);
            return false;
        } finally {
            this.readLock.unlock();
        }
    }


    public boolean removeEntry(String key){
        this.readLock.lock();
        try{
            this.db.delete(this.defaultHandle, ObjectAndByte.toByteArray(key));
        } catch (RocksDBException e) {
            e.printStackTrace();
        } finally {
            this.readLock.unlock();
        }
        return true;
    }

    private void truncatePrefixInBackground(final long startIndex, final long firstIndexKept, ColumnFamilyHandle handle) {
        // delete logs in background.
        Utils.runInThread(() -> {
            this.readLock.lock();
            try {
                if (this.db == null) {
                    return;
                }
                this.db.deleteRange(handle, getKeyBytes(startIndex), getKeyBytes(firstIndexKept));
            } catch (RocksDBException e) {
                e.printStackTrace();
            } finally {
                this.readLock.unlock();
            }
        });
    }


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
            this.statusHandle = null;
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

        columnFamilyDescriptors.add(new ColumnFamilyDescriptor("tx_status".getBytes(), cfOption));

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
        this.statusHandle = columnFamilyHandles.get(2);
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
                    truncatePrefixInBackground(0L, this.firstLogIndex, this.defaultHandle);
                    truncatePrefixInBackground(0L, this.firstLogStatusIndex, this.statusHandle);
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

    private void setFirstLogStatusIndex(final long index) {
        this.firstLogStatusIndex = index;
        this.hasLoadFirstStatusLogIndex = true;
    }

    private boolean saveFirstLogIndex(final long firstLogIndex, final byte[] key) {
        this.readLock.lock();
        try {
            final byte[] vs = new byte[8];
            Bits.putLong(vs, 0, firstLogIndex);
            checkState();
            this.db.put(this.confHandle, this.writeOptions, key, vs);
            return true;
        } catch (final RocksDBException e) {
            LOG.error("Fail to save first log index {}.", firstLogIndex, e);
            return false;
        } finally {
            this.readLock.unlock();
        }
    }


    private void closeDB() {
        this.confHandle.close();
        this.defaultHandle.close();
        this.statusHandle.close();
        this.db.close();
    }

    protected byte[] getKeyBytes(final long index) {
        final byte[] ks = new byte[8];
        Bits.putLong(ks, 0, index);
        return ks;
    }

    protected byte[] getValueFromRocksDB(final byte[] keyBytes, ColumnFamilyHandle handle) throws RocksDBException {
        return this.db.get(handle, keyBytes);
    }
    class BatchEntry{
        String key;
        Object value;
        public BatchEntry(String key, Object value){
            this.key = key;
            this.value = value;
        }
    }

}

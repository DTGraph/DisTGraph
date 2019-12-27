/**
 * create by Jiangjinkai on 2019-9-12 (The day before Mid-Autumn Festival)
 * idGenerator can generate unique ID.
 * this class is modified from org.neo4j.kernel.impl.store.id.IdGeneratorImpl
 **/

package PlacementDriver.IdManage;

import org.neo4j.kernel.impl.store.InvalidIdGeneratorException;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.max;

public class IdGenerator {
    public static final int HEADER_SIZE = 9;
    private final long max;
    private final AtomicLong highId = new AtomicLong(-1);
    private int grabSize = -1;
    public static final long INTEGER_MINUS_ONE = 0xFFFFFFFFL;  // 4294967295L;
    // total bytes read from file, used in writeIdBatch() and close()
    private long readPosition;
    // marks how much this session is allowed to read from previously released id batches.
    private long maxReadPosition = HEADER_SIZE;
    // used to calculate number of ids actually in use
    private long defraggedIdCount = -1;
    private final File IdGeneratorFile;
    private FileChannel fileChannel = null;
    // if sticky the id generator wasn't closed properly so it has to be
    // rebuilt (go through the node, relationship, property, rel type etc files)
    private static final byte CLEAN_GENERATOR = (byte) 0;
    private static final byte STICKY_GENERATOR = (byte) 1;
    // defragged ids read from file (freed in a previous session).
    private final LinkedList<Long> idsReadFromFile = new LinkedList<Long>();
    // ids freed in this session that haven't been flushed to disk yet
    private final LinkedList<Long> releasedIdList = new LinkedList<Long>();




    public IdGenerator(File file, int grabSize, long max, long highId )
    {
        //this.aggressiveReuse = aggressiveReuse;
        if ( grabSize < 1 )
        {
            throw new IllegalArgumentException( "Illegal grabSize: " + grabSize );
        }
        this.max = max;
        this.IdGeneratorFile = file;
        this.grabSize = grabSize;
        initGenerator();
        this.highId.set( max( this.highId.get(), highId ) );
    }

    public synchronized long nextId()
    {
        assertStillOpen();
        long nextDefragId = nextIdFromDefragList();
        if ( nextDefragId != -1 )
        {
            return nextDefragId;
        }

        long id = highId.get();
        if ( id == INTEGER_MINUS_ONE )
        {
            // Skip the integer -1 (0xFFFFFFFF) because it represents
            // special values, f.ex. the end of a relationships/property chain.
            id = highId.incrementAndGet();
        }
        assertIdWithinCapacity( id );
        highId.incrementAndGet();
        return id;
    }

    // initialize the id generator and performs a simple validation
    private synchronized void initGenerator()
    {
        try
        {
            fileChannel = new RandomAccessFile(IdGeneratorFile.getAbsolutePath(),"rw").getChannel();
            ByteBuffer buffer = readHeader();
            markAsSticky( fileChannel, buffer );

            fileChannel.position( HEADER_SIZE );
            maxReadPosition = fileChannel.size();
            defraggedIdCount = (int) (maxReadPosition - HEADER_SIZE) / 8;
            readIdBatch();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException(
                    "Unable to init id generator " + IdGeneratorFile, e );
        }
    }

    public static void createGenerator( File fileName, long highId,
                                        boolean throwIfFileExists )
    {
        if ( fileName == null )
        {
            throw new IllegalArgumentException( "Null filename" );
        }
        if ( throwIfFileExists && fileName.exists())
        {
            throw new IllegalStateException( "Can't create IdGeneratorFile["
                    + fileName + "], file already exists" );
        }
        try
        {
            FileChannel channel = (new RandomAccessFile(fileName.getAbsolutePath(),"rw")).getChannel();
            // write the header
            channel.truncate( 0 );
            ByteBuffer buffer = ByteBuffer.allocate( HEADER_SIZE );
            buffer.put( CLEAN_GENERATOR ).putLong( highId ).flip();
            channel.write( buffer );
            channel.force( false );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException(
                    "Unable to create id generator" + fileName, e );
        }
    }

    public synchronized void freeId( long id )
    {
        if ( id == INTEGER_MINUS_ONE )
        {
            return;
        }

        if ( fileChannel == null )
        {
            throw new IllegalStateException( "Generator closed " + IdGeneratorFile );
        }
        if ( id < 0 || id >= highId.get() )
        {
            throw new IllegalArgumentException( "Illegal id[" + id + "], highId is " + highId.get() );
        }
        releasedIdList.add( id );
        defraggedIdCount++;
        if ( releasedIdList.size() >= grabSize )
        {
            writeIdBatch( ByteBuffer.allocate( grabSize*8 ) );
        }
    }

    public synchronized void close()
    {
        if ( isClosed() )
        {
            return;
        }

        // write out lists
        ByteBuffer writeBuffer = ByteBuffer.allocate( grabSize*8 );
        if ( !releasedIdList.isEmpty() )
        {
            writeIdBatch( writeBuffer );
        }
        if ( !idsReadFromFile.isEmpty() )
        {
            while ( !idsReadFromFile.isEmpty() )
            {
                releasedIdList.add( idsReadFromFile.removeFirst() );
            }
            writeIdBatch( writeBuffer );
        }

        try
        {
            ByteBuffer buffer = ByteBuffer.allocate( HEADER_SIZE );
            writeHeader( buffer );
            defragReusableIdsInFile( writeBuffer );

            fileChannel.force( false );

            markAsCleanlyClosed( buffer );

            closeChannel();
            System.out.println("close id generator...");
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException(
                    "Unable to close id generator " + IdGeneratorFile, e );
        }
    }

    private void closeChannel() throws IOException
    {
        // flush and close
        fileChannel.force( false );
        fileChannel.close();
        fileChannel = null;
        // make this generator unusable
        highId.set( -1 );
    }

    private void markAsCleanlyClosed( ByteBuffer buffer ) throws IOException
    {
        // remove sticky
        buffer.clear();
        buffer.put( CLEAN_GENERATOR );
        buffer.limit( 1 );
        buffer.flip();
        fileChannel.position( 0 );
        fileChannel.write( buffer );//System.out.println("markAsCleanlyClosed");
    }

    private void defragReusableIdsInFile( ByteBuffer writeBuffer ) throws IOException
    {
        if ( readPosition > HEADER_SIZE )
        {
            long writePosition = HEADER_SIZE;
            long position = Math.min( readPosition, maxReadPosition );
            int bytesRead;
            do
            {
                writeBuffer.clear();
                fileChannel.position( position );
                bytesRead = fileChannel.read( writeBuffer );
                position += bytesRead;
                writeBuffer.flip();
                fileChannel.position( writePosition );
                writePosition += fileChannel.write( writeBuffer );
            }
            while ( bytesRead > 0 );
            // truncate
            fileChannel.truncate( writePosition );
        }
    }

    private boolean isClosed()
    {
        return highId.get() == -1;
    }

    private void writeIdBatch( ByteBuffer writeBuffer )
    {
        // position at end
        try
        {
            fileChannel.position( fileChannel.size() );
            writeBuffer.clear();
            while ( !releasedIdList.isEmpty() )
            {
                long id = releasedIdList.removeFirst();
                if ( id == INTEGER_MINUS_ONE )
                {
                    continue;
                }
                writeBuffer.putLong( id );
                if ( writeBuffer.position() == writeBuffer.capacity() )
                {
                    writeBuffer.flip();
                    while ( writeBuffer.hasRemaining() )
                    {
                        fileChannel.write( writeBuffer );
                    }
                    writeBuffer.clear();
                }
            }
            writeBuffer.flip();
            while ( writeBuffer.hasRemaining() )
            {
                fileChannel.write( writeBuffer );
            }
            // position for next readIdBatch
            fileChannel.position( readPosition );
            maxReadPosition = fileChannel.size();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException(
                    "Unable to write defragged id " + " batch", e );
        }
    }

    private long nextIdFromDefragList()
    {
        if(!releasedIdList.isEmpty()){
            Long id = releasedIdList.poll();
            if ( id != null )
            {
                defraggedIdCount--;
                return id;
            }
        }

        if ( !idsReadFromFile.isEmpty() || canReadMoreIdBatches() )
        {
            if ( idsReadFromFile.isEmpty() )
            {
                readIdBatch();
            }
            long id = idsReadFromFile.removeFirst();
            defraggedIdCount--;
            return id;
        }
        return -1;
    }

    private void assertIdWithinCapacity( long id )
    {
        if ( id > max || id < 0  )
        {
            throw new UnderlyingStorageException(
                    "Id capacity exceeded: " + id + " is not within bounds [0; " + max + "] for " + idsReadFromFile );
        }
    }

    private void assertStillOpen()
    {
        if ( fileChannel == null )
        {
            throw new IllegalStateException( "Closed id generator " + IdGeneratorFile );
        }
    }

    private static ByteBuffer readHighIdFromHeader( FileChannel channel, File fileName ) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocate( HEADER_SIZE );
        int read = channel.read( buffer );
        if ( read != HEADER_SIZE )
        {
            throw new InvalidIdGeneratorException(
                    "Unable to read header, bytes read: " + read );
        }
        buffer.flip();
        byte storageStatus = buffer.get();
        if ( storageStatus != CLEAN_GENERATOR )
        {
            throw new InvalidIdGeneratorException( "Sticky generator[ " +
                    fileName + "] delete this id file and build a new one" );
        }
        return buffer;
    }

    private ByteBuffer readHeader() throws IOException
    {
        try
        {
            ByteBuffer buffer = readHighIdFromHeader( fileChannel, IdGeneratorFile );
            readPosition = HEADER_SIZE;
            this.highId.set( buffer.getLong() );
            return buffer;
        }
        catch ( InvalidIdGeneratorException e )
        {
            fileChannel.close();
            throw e;
        }
    }

    /**
     * Made available for testing purposes.
     * Marks an id generator as sticky, i.e. not cleanly shut down.
     */
    public static void markAsSticky( FileChannel fileChannel, ByteBuffer buffer ) throws IOException
    {
        buffer.clear();
        buffer.put( STICKY_GENERATOR ).limit( 1 ).flip();
        fileChannel.position( 0 );
        fileChannel.write( buffer );
        fileChannel.force( false );
    }

    private void readIdBatch()
    {
        if ( !canReadMoreIdBatches() )
        {
            return;
        }

        try
        {
            int howMuchToRead = (int) Math.min( grabSize*8, maxReadPosition-readPosition );
            ByteBuffer readBuffer = ByteBuffer.allocate( howMuchToRead );

            fileChannel.position( readPosition );
            int bytesRead = fileChannel.read( readBuffer );
            assert fileChannel.position() <= maxReadPosition;
            readPosition += bytesRead;
            readBuffer.flip();
            assert (bytesRead % 8) == 0;
            int idsRead = bytesRead / 8;
            for ( int i = 0; i < idsRead; i++ )
            {
                long id = readBuffer.getLong();
                if ( id != INTEGER_MINUS_ONE )
                {
                    idsReadFromFile.add( id );
                }
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException(
                    "Failed reading defragged id batch", e );
        }
    }

    private boolean canReadMoreIdBatches()
    {
        return readPosition < maxReadPosition;
    }

    public void setHighId( long id )
    {
        assertIdWithinCapacity( id );
        highId.set( id );
    }

    public long getHighId()
    {
        return highId.get();
    }

    private void writeHeader( ByteBuffer buffer ) throws IOException
    {
        fileChannel.position( 0 );
        buffer.put( STICKY_GENERATOR ).putLong( highId.get() );
        buffer.flip();
        fileChannel.write( buffer );
    }

    public static long readHighId(File file ) throws IOException
    {
        try {
            FileChannel channel = (new RandomAccessFile(file.getAbsolutePath(), "rw")).getChannel();
            return readHighIdFromHeader(channel, file).getLong();
        } finally {

        }
    }

    public CompletableFuture returnIds(final List<Long> ids){
        CompletableFuture future =  CompletableFuture.runAsync(() -> {
            for(long id : ids){
                freeId(id);
            }
        });
        return future;
    }

}

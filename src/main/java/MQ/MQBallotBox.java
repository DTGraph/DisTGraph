/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package MQ;

import DBExceptions.LogIdException;
import DBExceptions.TxMQException;
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.FSMCaller;
import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.closure.ClosureQueue;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.Ballot;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.BallotBoxOptions;
import com.alipay.sofa.jraft.util.ArrayDeque;
import com.alipay.sofa.jraft.util.Describer;
import com.alipay.sofa.jraft.util.OnlyForTest;
import com.alipay.sofa.jraft.util.Requires;
import config.DTGConstants;
import options.MQBallotBoxOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.locks.StampedLock;

/**
 * Ballot box for voting.
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-04 2:32:10 PM
 */
@ThreadSafe
public class MQBallotBox implements Lifecycle<MQBallotBoxOptions>, Describer {

    private static final Logger      LOG                = LoggerFactory.getLogger(MQBallotBox.class);

    private MQFSMCaller              waiter;
    private ClosureQueue             closureQueue;
    private final StampedLock        stampedLock        = new StampedLock();
    //private long                     lastCommittedIndex = 0;
    private long                     pendingIndex;
    //private final ArrayDeque<Ballot> pendingMetaQueue   = new ArrayDeque<>();

    //private int                      addIndex           = 0;

//    @OnlyForTest
    long getPendingIndex() {
        return this.pendingIndex;
    }

//    @OnlyForTest
//    ArrayDeque<Ballot> getPendingMetaQueue() {
//        return this.pendingMetaQueue;
//    }

//    public long getLastCommittedIndex() {
//        long stamp = this.stampedLock.tryOptimisticRead();
//        final long optimisticVal = this.lastCommittedIndex;
//        if (this.stampedLock.validate(stamp)) {
//            return optimisticVal;
//        }
//        stamp = this.stampedLock.readLock();
//        try {
//            return this.lastCommittedIndex;
//        } finally {
//            this.stampedLock.unlockRead(stamp);
//        }
//    }

    @Override
    public boolean init(final MQBallotBoxOptions opts) {
        if (opts.getWaiter() == null || opts.getClosureQueue() == null) {
            LOG.error("waiter or closure queue is null.");
            return false;
        }
        this.waiter = opts.getWaiter();
        this.closureQueue = opts.getClosureQueue();
        return true;
    }

    /**
     * Called by leader, otherwise the behavior is undefined
     * Set logs in [first_log_index, last_log_index] are stable at |peer|.
     */
    public boolean commitAt( final long lastLogIndex) {
        //System.out.println("lastLogIndex: " + lastLogIndex + ", this.pendingIndex" + this.pendingIndex);
        if(!checkIndex(lastLogIndex)){
            return false;
        }
        // TODO  use lock-free algorithm here?
        final long stamp = stampedLock.writeLock();
        try {
            if (this.pendingIndex == 0) {
                return false;
            }
            if (lastLogIndex > this.pendingIndex) {
                throw new ArrayIndexOutOfBoundsException();
            }
        } finally {
            this.stampedLock.unlockWrite(stamp);
        }
        this.waiter.onCommitted(lastLogIndex);
        return true;
    }

    /**
     * Called when the leader steps down, otherwise the behavior is undefined
     * When a leader steps down, the uncommitted user applications should
     * fail immediately, which the new leader will deal whether to commit or
     * truncate.
     */
    public void clearPendingTasks() {
        final long stamp = this.stampedLock.writeLock();
        try {
            this.pendingIndex = 0;
            //this.lastCommittedIndex = 0;
            this.closureQueue.clear();
        } finally {
            this.stampedLock.unlockWrite(stamp);
        }
    }

    /**
     * Called when a candidate becomes the new leader, otherwise the behavior is
     * undefined.
     * According the the raft algorithm, the logs from previous terms can't be
     * committed until a log at the new term becomes committed, so
     * |newPendingIndex| should be |last_log_index| + 1.
     * @param newPendingIndex pending index of new leader
     * @return returns true if reset success
     */
//    public boolean resetPendingIndex(final long newPendingIndex) {
//        final long stamp = this.stampedLock.writeLock();
//        try {
//            if (newPendingIndex <= this.lastCommittedIndex) {
//                LOG.error("resetPendingIndex fail, newPendingIndex={}, lastCommittedIndex={}.", newPendingIndex,
//                    this.lastCommittedIndex);
//                return false;
//            }
//            this.pendingIndex = newPendingIndex;
//            this.closureQueue.resetFirstIndex(newPendingIndex);
//            return true;
//        } finally {
//            this.stampedLock.unlockWrite(stamp);
//        }
//    }

    /**
     * Called by leader, otherwise the behavior is undefined
     * Store application context before replication.
     *
     * @param done      callback
     * @return          returns true on success
     */
    public boolean appendPendingTask(final Closure done, long closureIndex) {
        final long stamp = this.stampedLock.writeLock();
        try {
            this.closureQueue.appendPendingClosure(done);
            //long closureIndex = ((DTGMQ.TransactionLogEntryAndClosure)done).entry.getId().getIndex();
            if(!checkIndex(closureIndex)){
                return false;
            }

            if(closureIndex > pendingIndex)pendingIndex = closureIndex;
            return true;
        } finally {
            this.stampedLock.unlockWrite(stamp);
        }
    }

//    /**
//     * Called by follower, otherwise the behavior is undefined.
//     * Set committed index received from leader
//     *
//     * @param lastCommittedIndex last committed index
//     * @return returns true if set success
//     */
//    public boolean setLastCommittedIndex(final long lastCommittedIndex) {
//        if(!checkIndex(lastCommittedIndex)){
//            return false;
//        }
//        boolean doUnlock = true;
//        final long stamp = this.stampedLock.writeLock();
//        try {
//            if (lastCommittedIndex < this.lastCommittedIndex) {
//                return false;
//            }
//            if (lastCommittedIndex > this.lastCommittedIndex) {
//                this.lastCommittedIndex = lastCommittedIndex;
//                this.stampedLock.unlockWrite(stamp);
//                doUnlock = false;
//                this.waiter.onCommitted(lastCommittedIndex);
//            }
//        } finally {
//            if (doUnlock) {
//                this.stampedLock.unlockWrite(stamp);
//            }
//        }
//        return true;
//    }

    @Override
    public void shutdown() {
        clearPendingTasks();
    }

    @Override
    public void describe(final Printer out) {
        long _lastCommittedIndex;
        long _pendingIndex;
        long _pendingMetaQueueSize;
        long stamp = this.stampedLock.tryOptimisticRead();
        if (this.stampedLock.validate(stamp)) {
            //_lastCommittedIndex = this.lastCommittedIndex;
            _pendingIndex = this.pendingIndex;
        } else {
            stamp = this.stampedLock.readLock();
            try {
                //_lastCommittedIndex = this.lastCommittedIndex;
                _pendingIndex = this.pendingIndex;
            } finally {
                this.stampedLock.unlockRead(stamp);
            }
        }
//        out.print("  lastCommittedIndex: ") //
//            .println(_lastCommittedIndex);
        out.print("  pendingIndex: ") //
            .println(_pendingIndex);
    }

    public boolean checkIndex(long index) throws LogIdException {
        if(index < 0){
            if(index == DTGConstants.NULL_INDEX){
                throw new LogIdException("the Log id is null");
            }
            else throw new LogIdException("the Log id is incorrect!");
        }
        return true;
    }
}

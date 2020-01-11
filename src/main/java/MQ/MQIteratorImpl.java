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

import DBExceptions.TxMQException;
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.StateMachine;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.error.LogEntryCorruptedException;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.LogManager;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.util.Utils;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The iterator implementation.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-03 3:28:37 PM
 */
public class MQIteratorImpl implements Iterator {

    private final MQStateMachine  fsm;
    private final MQLogManager    logManager;
    private final List<Closure>   closures;
    private final long            firstClosureIndex;
    private long                  currentIndex;
    private final long            applyingIndex;
    private TransactionLogEntry   currEntry = new TransactionLogEntry(0); // blank entry
    private final AtomicLong      committedIndex;
    private TxMQException         error;

    public MQIteratorImpl(final MQStateMachine fsm, final MQLogManager logManager, final List<Closure> closures,
                          final long firstClosureIndex, final AtomicLong committedIndex,
                          final long applyingIndex) {
        super();
        this.fsm = fsm;
        this.logManager = logManager;
        this.closures = closures;
        this.firstClosureIndex = firstClosureIndex;
        if(committedIndex.get() >= firstClosureIndex){
            this.currentIndex = committedIndex.get() + 1;
        }
        else {
            currentIndex = firstClosureIndex;
        }
        this.committedIndex = committedIndex;
        this.applyingIndex = applyingIndex;
        this.currEntry = getEntryByIndex(currentIndex);
    }

    @Override
    public String toString() {
        return "IteratorImpl [fsm=" + this.fsm + ", logManager=" + this.logManager + ", closures=" + this.closures
               + ", firstClosureIndex=" + this.firstClosureIndex + ", currentIndex=" + this.currentIndex
               + ", committedIndex=" + this.committedIndex + ", currEntry=" + this.currEntry + ", applyingIndex="
               + this.applyingIndex + ", error=" + this.error + "]";
    }

    public TransactionLogEntry entry() {
        return this.currEntry;
    }

    public TxMQException getError() {
        return this.error;
    }

    public boolean isGood() {//System.out.println("currentIndex:" + currentIndex + ", applyingIndex:" + applyingIndex);
        return this.currentIndex <= this.applyingIndex && this.currEntry != null && !hasError();
    }

    public boolean hasError() {
        return this.error != null;
    }

    /**
     * Move to next
     */
    public ByteBuffer next() {
        final ByteBuffer data = getData();
//        if(!hasNext()){
//            return null;
//        }
        this.currEntry = null; //release current entry
        //get next entry
        if (this.currentIndex <= this.committedIndex.get()) {
            this.currentIndex++;
        }
        if (this.currentIndex > this.committedIndex.get() && this.currentIndex < this.applyingIndex) {
            this.committedIndex.set(this.currentIndex);
            currentIndex++;
            this.currEntry = getEntryByIndex(currentIndex);
        }
        return data;
    }

    public long getIndex() {
        return this.currentIndex;
    }

    @Override
    public long getTerm() {
        return 0;
    }

    public Closure done() {
        if (this.currentIndex < this.firstClosureIndex) {
            return null;
        }
        return this.closures.get((int) (this.currentIndex - this.firstClosureIndex));
    }

    protected void runTheRestClosureWithError() {
        for (long i = Math.max(this.currentIndex, this.firstClosureIndex); i <= this.applyingIndex; i++) {
            final Closure done = this.closures.get((int) (i - this.firstClosureIndex));
            if (done != null) {
                Requires.requireNonNull(this.error, "error");
                Requires.requireNonNull(this.error.getStatus(), "error.status");
                final Status status = this.error.getStatus();
                Utils.runClosureInThread(done, status);
            }
        }
    }

    public void setErrorAndRollback(final long ntail, final Status st) {
        Requires.requireTrue(ntail > 0, "Invalid ntail=" + ntail);
        if (this.currEntry == null || this.currEntry.getType() != EnumOutter.EntryType.ENTRY_TYPE_DATA) {
            this.currentIndex -= ntail;
        } else {
            this.currentIndex -= ntail - 1;
        }
        this.currEntry = null;
        getOrCreateError().setType(EnumOutter.ErrorType.ERROR_TYPE_STATE_MACHINE);
        getOrCreateError().getStatus().setError(RaftError.ESTATEMACHINE,
            "StateMachine meet critical error when applying one or more tasks since index=%d, %s", this.currentIndex,
            st != null ? st.toString() : "none");

    }

    private TxMQException getOrCreateError() {
        if (this.error == null) {
            this.error = new TxMQException();
        }
        return this.error;
    }

    public boolean hasNext() {
        return this.isGood() && this.entry().getType() == EnumOutter.EntryType.ENTRY_TYPE_DATA;
    }

    public ByteBuffer getData() {
        final TransactionLogEntry entry = this.entry();
        return entry != null ? entry.getData() : null;
    }

//    public ByteBuffer getNextData() {
//        final ByteBuffer data = getData();
//        if (hasNext()) {
//            this.next();
//        }
//        return data;
//    }

    private TransactionLogEntry getEntryByIndex(long index){
        TransactionLogEntry entry = null;
        try {
            entry = this.logManager.getEntry(index);
            if (entry == null) {
                getOrCreateError().setType(EnumOutter.ErrorType.ERROR_TYPE_LOG);
                getOrCreateError().getStatus().setError(-1,
                        "Fail to get entry at index=%d while committed_index=%d", index,
                        this.committedIndex.get());
            }
            return entry;
        } catch (final LogEntryCorruptedException e) {
            getOrCreateError().setType(EnumOutter.ErrorType.ERROR_TYPE_LOG);
            getOrCreateError().getStatus().setError(RaftError.EINVAL, e.getMessage());
        }
        return entry;
    }
}

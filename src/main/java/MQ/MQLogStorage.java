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

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.storage.Storage;
import options.MQLogStorageOptions;

import java.util.List;

/**
 * Log entry storage service.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-12 3:43:54 PM
 */
public interface MQLogStorage extends Lifecycle<MQLogStorageOptions>, Storage {

    /**
     * Returns first log index in log.
     */
    long getFirstLogIndex();

    /**
     * Returns last log index in log.
     */
    long getLastLogIndex();

    long getCommitLogIndex(boolean flush);

    void setCommitLogIndex(long index);

    /**
     * Get logEntry by index.
     */
    TransactionLogEntry getEntry(final long index);

    /**
     * Append entries to log.
     */
    boolean appendEntry(final TransactionLogEntry entry);

    /**
     * Append entries to log, return append success number.
     */
    int appendEntries(final List<TransactionLogEntry> entries);

    /**
     * Delete logs from storage's head, [first_log_index, first_index_kept) will
     * be discarded.
     */
    boolean truncatePrefix(final long firstIndexKept);

    /**
     * Delete uncommitted logs from storage's tail, (last_index_kept, last_log_index]
     * will be discarded.
     */
    boolean truncateSuffix(final long lastIndexKept);

    /**
     * Drop all the existing logs and reset next log index to |next_log_index|.
     * This function is called after installing snapshot from leader.
     */
    boolean reset(final long nextLogIndex);

    List<TransactionLogEntry> getEntries(long start, long end);

}

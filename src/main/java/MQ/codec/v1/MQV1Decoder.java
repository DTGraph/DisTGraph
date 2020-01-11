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
package MQ.codec.v1;

import MQ.MQLogId;
import MQ.TransactionLogEntry;
import MQ.codec.MQLogEntryDecoder;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.util.Bits;
import config.DTGConstants;

import java.nio.ByteBuffer;

/**
 * V1 log entry decoder
 * @author boyan(boyan@antfin.com)
 *
 */
@Deprecated
public final class MQV1Decoder implements MQLogEntryDecoder {

    private MQV1Decoder() {
    }

    public static final MQV1Decoder INSTANCE = new MQV1Decoder();

    @Override
    public TransactionLogEntry decode(final byte[] content) {
        if (content == null || content.length == 0) {
            return null;
        }
        if (content[0] != MQV1LogEntryCodecFactory.MAGIC) {
            // Corrupted log
            return null;
        }
        TransactionLogEntry log = new TransactionLogEntry(DTGConstants.NULL_INDEX);
        decode(log, content);

        return log;
    }

    public void decode(final TransactionLogEntry log, final byte[] content) {
        // 1-5 type
        final int iType = Bits.getInt(content, 1);
        log.setType(EnumOutter.EntryType.forNumber(iType));
        // 5-13 index
        final long index = Bits.getLong(content, 5);
        log.setId(new MQLogId(index));
        int pos = 13;

        // data
        if (content.length > pos) {
            final int len = content.length - pos;
            ByteBuffer data = ByteBuffer.allocate(len);
            data.put(content, pos, len);
            data.flip();
            log.setData(data.array());
        }
    }
}
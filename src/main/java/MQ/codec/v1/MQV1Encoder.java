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
import MQ.codec.MQLogEntryEncoder;
import com.alipay.sofa.jraft.entity.EnumOutter.EntryType;
import com.alipay.sofa.jraft.util.Bits;

import java.nio.ByteBuffer;

/**
 * V1 log entry encoder
 * @author boyan(boyan@antfin.com)
 *
 */
@Deprecated
public final class MQV1Encoder implements MQLogEntryEncoder {

    private MQV1Encoder() {
    }

    public static final MQLogEntryEncoder INSTANCE = new MQV1Encoder();

    @Override
    public byte[] encode(final TransactionLogEntry log) {

        EntryType type = log.getType();
        MQLogId id = log.getId();
        ByteBuffer data = log.getData();

        // magic number 1 byte
        int totalLen = 1;
        final int iType = type.getNumber();
        final long index = id.getIndex();
        // type(4) + index(8)
        totalLen += 4 + 8;

        final int bodyLen = data != null ? data.remaining() : 0;
        totalLen += bodyLen;

        final byte[] content = new byte[totalLen];
        // {0} magic
        content[0] = MQV1LogEntryCodecFactory.MAGIC;
        // 1-5 type
        Bits.putInt(content, 1, iType);
        // 5-13 index
        Bits.putLong(content, 5, index);
        int pos = 13;
        // data
        if (data != null) {
            System.arraycopy(data.array(), data.position(), content, pos, data.remaining());
        }

        return content;
    }
}
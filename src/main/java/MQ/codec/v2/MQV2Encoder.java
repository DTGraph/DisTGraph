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
package MQ.codec.v2;

import MQ.MQLogId;
import MQ.TransactionLogEntry;
import MQ.codec.MQLogEntryEncoder;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.codec.v2.LogOutter.PBLogEntry;
import com.alipay.sofa.jraft.error.LogEntryCorruptedException;
import com.alipay.sofa.jraft.util.Requires;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.ZeroByteStringHelper;
import tool.ObjectAndByte;

import java.io.IOException;

/**
 * V2 log entry encoder based on protobuf, see src/main/resources/log.proto
 *
 * @author boyan(boyan@antfin.com)
 */
public class MQV2Encoder implements MQLogEntryEncoder {

    public static final MQV2Encoder INSTANCE = new MQV2Encoder();

    @Override
    public byte[] encode(final TransactionLogEntry log) {System.out.println("encode");
        Requires.requireNonNull(log, "Null log");

        byte[] res = ObjectAndByte.toByteArray(log);
        System.out.println(" END encode");
        return res;
//        final MQLogId logId = log.getId();
//        final PBLogEntry.Builder builder = PBLogEntry.newBuilder() //
//            .setType(log.getType()) //
//            .setIndex(logId.getIndex());
//        System.out.println("encode 2");
//        if (log.hasChecksum()) {
//            builder.setChecksum(log.getChecksum());
//        }
//        System.out.println("encode 3");
//        builder.setData(log.getData() != null ? ZeroByteStringHelper.wrap(log.getData()) : ByteString.EMPTY);
//
//        final PBLogEntry pbLogEntry = builder.build();
//        final int bodyLen = pbLogEntry.getSerializedSize();
//        final byte[] ret = new byte[MQV2LogEntryCodecFactory.HEADER_SIZE + bodyLen];
//
//        System.out.println("encode 4");
//        // write header
//        int i = 0;
//        for (; i < MQV2LogEntryCodecFactory.MAGIC_BYTES.length; i++) {
//            ret[i] = MQV2LogEntryCodecFactory.MAGIC_BYTES[i];
//        }
//        ret[i++] = MQV2LogEntryCodecFactory.VERSION;
//        // avoid memory copy for only 3 bytes
//        for (; i < MQV2LogEntryCodecFactory.HEADER_SIZE; i++) {
//            ret[i] = MQV2LogEntryCodecFactory.RESERVED[i - MQV2LogEntryCodecFactory.MAGIC_BYTES.length - 1];
//        }
//        System.out.println("encode 5");
//        // write body
//        writeToByteArray(pbLogEntry, ret, i, bodyLen);
//        System.out.println("encode 6");
//        return ret;
    }

//    private void writeToByteArray(final PBLogEntry pbLogEntry, final byte[] array, final int offset, final int len) {
//        final CodedOutputStream output = CodedOutputStream.newInstance(array, offset, len);
//        try {
//            pbLogEntry.writeTo(output);
//            output.checkNoSpaceLeft();
//        } catch (final IOException e) {
//            throw new LogEntryCorruptedException(
//                "Serializing PBLogEntry to a byte array threw an IOException (should never happen).", e);
//        }
//    }

    private MQV2Encoder() {
    }
}

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

import MQ.TransactionLogEntry;
import MQ.codec.MQLogEntryDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tool.ObjectAndByte;

/**
 * V2 log entry decoder based on protobuf, see src/main/resources/log.proto
 *
 * @author boyan(boyan@antfin.com)
 */
public class MQV2Decoder implements MQLogEntryDecoder {

    private static final Logger   LOG      = LoggerFactory.getLogger(MQV2Decoder.class);

    public static final MQV2Decoder INSTANCE = new MQV2Decoder();

    @Override
    public TransactionLogEntry decode(final byte[] bs) {
        TransactionLogEntry log = (TransactionLogEntry) ObjectAndByte.toObject(bs);
        return log;
//        if (bs == null || bs.length < MQV2LogEntryCodecFactory.HEADER_SIZE) {
//            return null;
//        }
//
//        int i = 0;
//        for (; i < MQV2LogEntryCodecFactory.MAGIC_BYTES.length; i++) {
//            if (bs[i] != MQV2LogEntryCodecFactory.MAGIC_BYTES[i]) {
//                return null;
//            }
//        }
//
//        if (bs[i++] != MQV2LogEntryCodecFactory.VERSION) {
//            return null;
//        }
//        // Ignored reserved
//        i += MQV2LogEntryCodecFactory.RESERVED.length;
//        try {
//            final PBLogEntry entry = PBLogEntry.parseFrom(ZeroByteStringHelper.wrap(bs, i, bs.length - i));
//
//            final TransactionLogEntry log = new TransactionLogEntry(entry.getIndex());
//            log.setType(entry.getType());
//            log.getId().setIndex(entry.getIndex());
//
//            if (entry.hasChecksum()) {
//                log.setChecksum(entry.getChecksum());
//            }
//
//
//            final ByteString data = entry.getData();
//            if (!data.isEmpty()) {
//                log.setData(ByteBuffer.wrap(ZeroByteStringHelper.getByteArray(data)));
//            }
//
//            return log;
//        } catch (final InvalidProtocolBufferException e) {
//            LOG.error("Fail to decode pb log entry", e);
//            return null;
//        }
    }

    private MQV2Decoder() {
    }
}

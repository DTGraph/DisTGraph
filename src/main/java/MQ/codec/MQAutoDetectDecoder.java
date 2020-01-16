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
package MQ.codec;

import MQ.TransactionLogEntry;
import MQ.codec.v2.MQV2Decoder;
import MQ.codec.v2.MQV2LogEntryCodecFactory;

/**
 * Decoder that supports both v1 and v2 log entry codec protocol.
 * @author boyan(boyan@antfin.com)
 *
 */
public class MQAutoDetectDecoder implements MQLogEntryDecoder {

    private MQAutoDetectDecoder() {

    }

    public static final MQAutoDetectDecoder INSTANCE = new MQAutoDetectDecoder();

    @Override
    public TransactionLogEntry decode(final byte[] bs) {
        if (bs == null || bs.length < 1) {
            return null;
        }

        if (bs[0] == MQV2LogEntryCodecFactory.MAGIC_BYTES[0]) {
            return MQV2Decoder.INSTANCE.decode(bs);
        } else {
            //return MQV1Decoder.INSTANCE.decode(bs);
            return MQV2Decoder.INSTANCE.decode(bs);
        }
    }

}

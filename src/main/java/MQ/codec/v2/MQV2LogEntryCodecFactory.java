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

import MQ.codec.MQAutoDetectDecoder;
import MQ.codec.MQLogEntryCodecFactory;
import MQ.codec.MQLogEntryDecoder;
import MQ.codec.MQLogEntryEncoder;

/**
 * V2(Now) log entry codec implementation, header format:
 *
 *   0  1     2    3  4  5
 *  +-+-+-+-+-+-+-+-++-+-+-+
 *  |Magic|Version|Reserved|
 *  +-+-+-+-+-+-+-+-++-+-+-+
 *
 * @author boyan(boyan@antfin.com)
 * @since 1.2.6
 *
 */
public class MQV2LogEntryCodecFactory implements MQLogEntryCodecFactory {

    private static final MQV2LogEntryCodecFactory INSTANCE = new MQV2LogEntryCodecFactory();

    public static MQV2LogEntryCodecFactory getInstance() {
        return INSTANCE;
    }

    // BB-8 and R2D2 are good friends.
    public static final byte[] MAGIC_BYTES = new byte[] { (byte) 0xBB, (byte) 0xD2 };
    // Codec version
    public static final byte   VERSION     = 1;

    public static final byte[] RESERVED    = new byte[3];

    public static final int    HEADER_SIZE = MAGIC_BYTES.length + 1 + RESERVED.length;

    @Override
    public MQLogEntryEncoder encoder() {
        return MQV2Encoder.INSTANCE;
    }

    @Override
    public MQLogEntryDecoder decoder() {
        return MQAutoDetectDecoder.INSTANCE;
    }

    private MQV2LogEntryCodecFactory() {
    }
}

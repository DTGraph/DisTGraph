///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package MQ.codec;
//
//import MQ.TransactionLogEntry;
//import config.DTGConstants;
//
///**
// * Default log entry codec factory
// * @author boyan(boyan@antfin.com)
// *
// */
//public class DefaultMQLogEntryCodecFactory implements MQLogEntryCodecFactory {
//
//    private DefaultMQLogEntryCodecFactory() {
//    }
//
//    private static DefaultMQLogEntryCodecFactory INSTANCE = new DefaultMQLogEntryCodecFactory();
//
//    /**
//     * Returns a singleton instance of DefaultLogEntryCodecFactory.
//     * @return a singleton instance
//     */
//    public static DefaultMQLogEntryCodecFactory getInstance() {
//        return INSTANCE;
//    }
//
//    @SuppressWarnings("deprecation")
//    private static MQLogEntryEncoder ENCODER = TransactionLogEntry::encode;
//
//    @SuppressWarnings("deprecation")
//    private static MQLogEntryDecoder DECODER = bs -> {
//        final TransactionLogEntry log = new TransactionLogEntry(DTGConstants.NULL_INDEX);
//        if (log.decode(bs)) {
//            return log;
//        }
//        return null;
//    };
//
//    @Override
//    public MQLogEntryEncoder encoder() {
//        return ENCODER;
//    }
//
//    @Override
//    public MQLogEntryDecoder decoder() {
//        return DECODER;
//    }
//
//}

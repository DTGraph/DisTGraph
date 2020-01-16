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
//package options;
//
//import MQ.MQFSMCaller;
//import MQ.MQLogStorage;
//import MQ.codec.MQLogEntryCodecFactory;
//import MQ.codec.v2.MQV2LogEntryCodecFactory;
//import com.alipay.sofa.jraft.FSMCaller;
//import com.alipay.sofa.jraft.core.NodeMetrics;
//
///**
// * Options for log manager.
// *
// * @author boyan (boyan@alibaba-inc.com)
// *
// * 2018-Mar-13 5:15:15 PM
// */
//public class MQLogManagerOptions {
//
//    private MQLogStorage           logStorage;
//    //private ConfigurationManager   configurationManager;
//    private MQFSMCaller            fsmCaller;
//    private int                    disruptorBufferSize  = 1024;
//    //private RaftOptions          raftOptions;
//    private NodeMetrics            nodeMetrics;
//    private MQLogEntryCodecFactory logEntryCodecFactory = MQV2LogEntryCodecFactory.getInstance();
//
//    public MQLogEntryCodecFactory getLogEntryCodecFactory() {
//        return this.logEntryCodecFactory;
//    }
//
//    public void setLogEntryCodecFactory(final MQLogEntryCodecFactory logEntryCodecFactory) {
//        this.logEntryCodecFactory = logEntryCodecFactory;
//    }
//
//    public NodeMetrics getNodeMetrics() {
//        return this.nodeMetrics;
//    }
//
//    public void setNodeMetrics(final NodeMetrics nodeMetrics) {
//        this.nodeMetrics = nodeMetrics;
//    }
//
////    public RaftOptions getRaftOptions() {
////        return this.raftOptions;
////    }
////
////    public void setRaftOptions(final RaftOptions raftOptions) {
////        this.raftOptions = raftOptions;
////    }
//
//    public int getDisruptorBufferSize() {
//        return this.disruptorBufferSize;
//    }
//
//    public void setDisruptorBufferSize(final int disruptorBufferSize) {
//        this.disruptorBufferSize = disruptorBufferSize;
//    }
//
//    public MQLogStorage getLogStorage() {
//        return this.logStorage;
//    }
//
//    public void setLogStorage(final MQLogStorage logStorage) {
//        this.logStorage = logStorage;
//    }
//
////    public ConfigurationManager getConfigurationManager() {
////        return this.configurationManager;
////    }
////
////    public void setConfigurationManager(final ConfigurationManager configurationManager) {
////        this.configurationManager = configurationManager;
////    }
//
//    public MQFSMCaller getFsmCaller() {
//        return this.fsmCaller;
//    }
//
//    public void setFsmCaller(final MQFSMCaller fsmCaller) {
//        this.fsmCaller = fsmCaller;
//    }
//
//}

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
//package MQ.conf;
//
//import com.alipay.sofa.jraft.entity.PeerId;
//import com.alipay.sofa.jraft.util.Copiable;
//import com.alipay.sofa.jraft.util.Requires;
//import org.apache.commons.lang.StringUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.Serializable;
//import java.util.*;
//
///**
// * A configuration with a set of peers.
// * @author boyan (boyan@alibaba-inc.com)
// *
// * 2018-Mar-15 11:00:26 AM
// */
//public class MQConfiguration implements Iterable<PeerId>, Copiable<MQConfiguration>, Serializable {
//
//    private static final Logger   LOG             = LoggerFactory.getLogger(MQConfiguration.class);
//
//    private static final String   LEARNER_POSTFIX = "/learner";
//
//    public MQConfiguration() {
//        super();
//    }
//
//    @Override
//    public MQConfiguration copy() {
//        return new MQConfiguration(this.peers, this.learners);
//    }
//
//    /**
//     * Returns true when the configuration is valid.
//     *
//     * @return true if the configuration is valid.
//     */
//    public boolean isValid() {
//        final Set<PeerId> intersection = new HashSet<>(this.peers);
//        intersection.retainAll(this.learners);
//        return !this.peers.isEmpty() && intersection.isEmpty();
//    }
//
//    public void reset() {
//        this.peers.clear();
//        this.learners.clear();
//    }
//
//    public boolean isEmpty() {
//        return this.peers.isEmpty();
//    }
//
//    /**
//     * Returns the peers total number.
//     *
//     * @return total num of peers
//     */
//    public int size() {
//        return this.peers.size();
//    }
//
//    @Override
//    public Iterator<PeerId> iterator() {
//        return this.peers.iterator();
//    }
//
//    public Set<PeerId> getPeerSet() {
//        return new HashSet<>(this.peers);
//    }
//
//    public List<PeerId> listPeers() {
//        return new ArrayList<>(this.peers);
//    }
//
//    public List<PeerId> getPeers() {
//        return this.peers;
//    }
//
//    public void setPeers(final List<PeerId> peers) {
//        this.peers.clear();
//        for (final PeerId peer : peers) {
//            this.peers.add(peer.copy());
//        }
//    }
//
//    public void appendPeers(final Collection<PeerId> set) {
//        this.peers.addAll(set);
//    }
//
//    public boolean addPeer(final PeerId peer) {
//        return this.peers.add(peer);
//    }
//
//    public boolean removePeer(final PeerId peer) {
//        return this.peers.remove(peer);
//    }
//
//    public boolean contains(final PeerId peer) {
//        return this.peers.contains(peer);
//    }
//
//    @Override
//    public int hashCode() {
//        final int prime = 31;
//        int result = 1;
//        result = prime * result + ((this.learners == null) ? 0 : this.learners.hashCode());
//        result = prime * result + ((this.peers == null) ? 0 : this.peers.hashCode());
//        return result;
//    }
//
//    @Override
//    public boolean equals(final Object obj) {
//        if (this == obj) {
//            return true;
//        }
//        if (obj == null) {
//            return false;
//        }
//        if (getClass() != obj.getClass()) {
//            return false;
//        }
//        MQConfiguration other = (MQConfiguration) obj;
//        if (this.learners == null) {
//            if (other.learners != null) {
//                return false;
//            }
//        } else if (!this.learners.equals(other.learners)) {
//            return false;
//        }
//        if (this.peers == null) {
//            return other.peers == null;
//        } else {
//            return this.peers.equals(other.peers);
//        }
//    }
//
//    @Override
//    public String toString() {
//        final StringBuilder sb = new StringBuilder();
//        final List<PeerId> peers = listPeers();
//        int i = 0;
//        int size = peers.size();
//        for (final PeerId peer : peers) {
//            sb.append(peer);
//            if (i < size - 1 || !this.learners.isEmpty()) {
//                sb.append(",");
//            }
//            i++;
//        }
//
//        size = this.learners.size();
//        i = 0;
//        for (final PeerId peer : this.learners) {
//            sb.append(peer).append(LEARNER_POSTFIX);
//            if (i < size - 1) {
//                sb.append(",");
//            }
//            i++;
//        }
//
//        return sb.toString();
//    }
//
//    public boolean parse(final String conf) {
//        if (StringUtils.isBlank(conf)) {
//            return false;
//        }
//        reset();
//        final String[] peerStrs = StringUtils.split(conf, ',');
//        for (String peerStr : peerStrs) {
//            final PeerId peer = new PeerId();
//            int index;
//            boolean isLearner = false;
//            if ((index = peerStr.indexOf(LEARNER_POSTFIX)) > 0) {
//                // It's a learner
//                peerStr = peerStr.substring(0, index);
//                isLearner = true;
//            }
//            if (peer.parse(peerStr)) {
//                if (isLearner) {
//                    addLearner(peer);
//                } else {
//                    addPeer(peer);
//                }
//            } else {
//                LOG.error("Fail to parse peer {} in {}, ignore it.", peerStr, conf);
//            }
//        }
//        return true;
//    }
//
//    /**
//     *  Get the difference between |*this| and |rhs|
//     *  |included| would be assigned to |*this| - |rhs|
//     *  |excluded| would be assigned to |rhs| - |*this|
//     */
//    public void diff(final MQConfiguration rhs, final MQConfiguration included, final MQConfiguration excluded) {
//        included.peers = new ArrayList<>(this.peers);
//        included.peers.removeAll(rhs.peers);
//        excluded.peers = new ArrayList<>(rhs.peers);
//        excluded.peers.removeAll(this.peers);
//    }
//}

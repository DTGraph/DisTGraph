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
package Region;

import DBExceptions.RegionStoreException;
import com.alipay.sofa.jraft.rhea.metadata.Peer;
import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.util.Copiable;
import config.DefaultOptions;
import tool.RangeTool;

import java.io.File;
import java.io.Serializable;
import java.util.*;

/**
 * Region is the most basic kv data unit.  Each region has a left-closed
 * right-open interval range.  The region is responsible for the crud
 * request in the range.  Each region is a raft group, which is distributed
 * on different store nodes. Each region copy is called a region peer.
 * As the data volume of the region reaches the threshold, it will trigger
 * split. In fact, it is only the metadata change. This action is very fast.
 * The two regions just after the split are still on the original store node.
 * The PD periodically checks the number of regions in each store node, and
 * the load situation. The raft snapshot is used to migrate the region between
 * the store nodes to ensure the load balance of the cluster.
 *
 * @author jiachun.fjc
 * @modified By: jinkai
 */
public class DTGRegion implements Copiable<DTGRegion>, Serializable {

    private static final long serialVersionUID        = -2610978803578899118L;

    // To distinguish the id automatically assigned by the PD,
    // the manually configured id ranges from [-1, 1000000L).
    public static final long  MIN_ID_WITH_MANUAL_CONF = -1L;
    public static final long  MAX_ID_WITH_MANUAL_CONF = 1000000L;

    private long              id;                                             // region id
    // Region key range [startKey, endKey)
    private List<long[]> NodeIdRangeList;
    private List<long[]> RelationIdRangeList;
    private List<long[]> TemporalPropertyTimeRangeList;
    //have node add or remove between two heartbeats;
    private boolean IsNodeChange;
    private boolean IsRelationChange;
    private boolean IsTemporalPropertyChange;

    private int nodeUpperBound, relationUpperBound;
    private int nodecount, relationcount;
    private boolean hasNextRegion;

    private RegionEpoch       regionEpoch;                                    // region term
    private List<Peer>        peers;                                          // all peers in the region

    public DTGRegion(long nodeIdStart, long relationIdStart, long tempProStart) {
        this(DefaultOptions.DEFAULTREGIONNODESIZE, DefaultOptions.DEFAULTREGIONRELATIONSIZE,nodeIdStart, relationIdStart, tempProStart);
    }

    public DTGRegion(int nodeUpperBound, int relationUpperBound){
        this.relationUpperBound = relationUpperBound;
        this.nodeUpperBound = nodeUpperBound;
        nodecount = 0;
        relationcount = 0;
        this.NodeIdRangeList = new ArrayList<>();
        this.RelationIdRangeList = new ArrayList<>();
        this.TemporalPropertyTimeRangeList = new ArrayList<>();
    }

    public DTGRegion(int nodeUpperBound, int relationUpperBound, long nodeIdStart, long relationIdStart, long tempProStart){
        this(nodeUpperBound, relationUpperBound);
        addToNodeRange(nodeIdStart);
        addToRelationRange(relationIdStart);
        addToTempRange(tempProStart);
    }

    public DTGRegion(long id, int nodeUpperBound, int relationUpperBound, long nodeIdStart, long relationIdStart, long tempProStart, List<Peer> peers){
        this(nodeUpperBound, relationUpperBound, nodeIdStart, relationIdStart, tempProStart);
        this.id = id;
        this.peers = peers;
    }

    public DTGRegion(long id, List<long[]> NodeIdRegionList, List<long[]> RelationIdRegionList,
                     List<long[]> TemporalPropertyTimeRegionList, RegionEpoch regionEpoch,
                     List<Peer> peers, int nodeUpperBound, int relationUpperBound, int nodecount, int relationcount) {
//        this(nodeUpperBound, relationUpperBound);
        this.relationUpperBound = relationUpperBound;
        this.nodeUpperBound = nodeUpperBound;
        this.id = id;
        this.NodeIdRangeList = NodeIdRegionList;
        this.RelationIdRangeList = RelationIdRegionList;
        this.TemporalPropertyTimeRangeList = TemporalPropertyTimeRegionList;
        this.regionEpoch = regionEpoch;
        this.peers = peers;
        this.nodecount = nodecount;
        this.relationcount = relationcount;
    }

//    public DTGRegion(long id, byte[] nodeStartId, byte[] nodeEndId, byte[] relationStartId, byte[] relationEndId,
//                     byte[] temporalPropertyStartId, byte[] temporalPropertyEndId, RegionEpoch regionEpoch,
//                     List<Peer> peers) {
//        this.id = id;
//        this.nodeStartId = nodeStartId;
//        this.nodeEndId = nodeEndId;
//        this.relationStartId = relationStartId;
//        this.relationEndId = relationEndId;
//        this.temporalPropertyStartId = temporalPropertyStartId;
//        this.temporalPropertyEndId = temporalPropertyEndId;
//        this.regionEpoch = regionEpoch;
//        this.peers = peers;
//    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

//    public byte[] getStartKey() {
//        return startKey;
//    }
//
//    public void setStartKey(byte[] startKey) {
//        this.startKey = startKey;
//    }
//
//    public byte[] getEndKey() {
//        return endKey;
//    }
//
//    public void setEndKey(byte[] endKey) {
//        this.endKey = endKey;
//    }
//
//    public byte[] getNodeStartId() {
//        return nodeStartId;
//    }
//
//    public void setNodeStartId(byte[] nodeStartId) {
//        this.nodeStartId = nodeStartId;
//    }
//
//    public byte[] getNodeEndId() {
//        return nodeEndId;
//    }
//
//    public void setNodeEndId(byte[] nodeEndId) {
//        this.nodeEndId = nodeEndId;
//    }
//
//    public byte[] getRelationStartId() {
//        return relationStartId;
//    }
//
//    public void setRelationStartId(byte[] relationStartId) {
//        this.relationStartId = relationStartId;
//    }
//
//    public byte[] getRelationEndId() {
//        return relationEndId;
//    }
//
//    public void setRelationEndId(byte[] relationEndId) {
//        this.relationEndId = relationEndId;
//    }
//
//    public byte[] getTemporalPropertyStartId() {
//        return temporalPropertyStartId;
//    }
//
//    public void setTemporalPropertyStartId(byte[] temporalPropertyStartId) {
//        this.temporalPropertyStartId = temporalPropertyStartId;
//    }
//
//    public byte[] getTemporalPropertyEndId() {
//        return temporalPropertyEndId;
//    }
//
//    public void setTemporalPropertyEndId(byte[] temporalPropertyEndId) {
//        this.temporalPropertyEndId = temporalPropertyEndId;
//    }


    public List<long[]> getNodeIdRangeList() {
        return NodeIdRangeList;
    }

    public void setNodeIdRangeList(List<long[]> nodeIdRegionList) {
        NodeIdRangeList = nodeIdRegionList;
        IsNodeChange = true;
    }

    public void addNodeIdRegion(long start, long end){
        long[] idRegion = new long[2];
        idRegion[0] = start;
        idRegion[1] = end;
        NodeIdRangeList.add(idRegion);
        IsNodeChange = true;
    }

    public List<long[]> getRelationIdRangeList() {
        return RelationIdRangeList;
    }

    public void setRelationIdRangeList(List<long[]> relationIdRegionList) {
        RelationIdRangeList = relationIdRegionList;
        IsRelationChange = true;
    }

    public void addRelationIdRegion(long start, long end){
        long[] idRegion = new long[2];
        idRegion[0] = start;
        idRegion[1] = end;
        RelationIdRangeList.add(idRegion);
        IsRelationChange = true;
    }

    public List<long[]> getTemporalPropertyTimeRangeList() {
        return TemporalPropertyTimeRangeList;
    }

    public void setTemporalPropertyTimeRangeList(List<long[]> temporalPropertyTimeRegionList) {
        TemporalPropertyTimeRangeList = temporalPropertyTimeRegionList;
        IsTemporalPropertyChange = true;
    }

    public void addTemporalPropertyTimeRegion(long start, long end){
        long[] idRegion = new long[2];
        idRegion[0] = start;
        idRegion[1] = end;
        TemporalPropertyTimeRangeList.add(idRegion);
        IsTemporalPropertyChange = true;
    }

    public RegionEpoch getRegionEpoch() {
        return regionEpoch;
    }

    public void setRegionEpoch(RegionEpoch regionEpoch) {
        this.regionEpoch = regionEpoch;
    }

    public List<Peer> getPeers() {
        return peers;
    }

    public void setPeers(List<Peer> peers) {
        this.peers = peers;
    }

    @Override
    public DTGRegion copy() {
        RegionEpoch regionEpoch = null;
        if (this.regionEpoch != null) {
            regionEpoch = this.regionEpoch.copy();
        }
        List<Peer> peers = null;
        if (this.peers != null) {
            peers = Lists.newArrayListWithCapacity(this.peers.size());
            for (Peer peer : this.peers) {
                peers.add(peer.copy());
            }
        }
        return new DTGRegion(this.id, this.NodeIdRangeList, this.RelationIdRangeList, this.TemporalPropertyTimeRangeList, regionEpoch,
                peers, this.nodeUpperBound, this.relationUpperBound, this.nodecount, this.relationcount);
    }

    public DTGRegion copyNull(long regionId, long nodeIdStart, long relationIdStart, long tempProStart) {
        RegionEpoch regionEpoch = null;
        if (this.regionEpoch != null) {
            regionEpoch = this.regionEpoch.copy();
        }
        List<Peer> peers = null;
        if (this.peers != null) {System.out.println("copy peers...");
            peers = Lists.newArrayListWithCapacity(this.peers.size());
            for (Peer peer : this.peers) {
                peers.add(peer.copy());
            }
        }
        return new DTGRegion(regionId, this.nodeUpperBound, this.relationUpperBound, nodeIdStart, relationIdStart, tempProStart, peers);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        DTGRegion region = (DTGRegion) o;
        return id == region.id && Objects.equals(regionEpoch, region.regionEpoch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, regionEpoch);
    }

    public boolean isNodeFull(){
        if(this.nodecount >= nodeUpperBound ){
            return true;
        }else {
            return false;
        }
    }

//    public boolean isTemporalPropertyFull(){
//        if(this.TemporalPropertyTimeRangeList.size() >= upperBound ){
//            return true;
//        }else {
//            return false;
//        }
//    }

    public boolean isRelationFull(){
        if(this.relationcount >= relationUpperBound ){
            return true;
        }else {
            return false;
        }
    }

    public void extendNodeBound(){
        this.nodeUpperBound = this.nodeUpperBound * 2;
    }

    public void shrinkNodeBound(){
        this.nodeUpperBound = this.nodeUpperBound / 2;
    }

    public void extendRelationBound(){
        this.relationUpperBound = this.relationUpperBound * 2;
    }

    public void shrinkRelationBound(){
        this.relationUpperBound = this.relationUpperBound / 2;
    }

    public int getNodecount() {
        return this.nodecount;
    }

    public int getRelationcount() {
        return this.relationcount;
    }

    public synchronized void addNode() throws RegionStoreException {
        if(isNodeFull())throw new RegionStoreException("region " + this.id + " save enough node! please choose other region");
        nodecount++;
    }

    public synchronized void addRelation() throws RegionStoreException {
        if(isRelationFull())throw new RegionStoreException("region " + this.id + " save enough relation! please choose other region");
        relationcount++;
    }

    public synchronized void removeNode() {
        nodecount--;
    }

    public synchronized void removeRelation() {
        relationcount--;
    }

    public synchronized void removeNode(int count) {
        nodecount = nodecount - count;
    }

    public synchronized void removeRelation(int count) {
        relationcount = relationcount - count;
    }

    public synchronized void addToNodeRange(long id){
        RangeTool.addNumberToRange(NodeIdRangeList, id);
        IsNodeChange = true;
    }

    public synchronized void removeFromNodeRange(long id){
        RangeTool.removeNumber(NodeIdRangeList, id);
        IsNodeChange = true;
    }

    public synchronized void addToRelationRange(long id){
        RangeTool.addNumberToRange(RelationIdRangeList, id);
        IsRelationChange = true;
    }

    public synchronized void removeFromRelationRange(long id){
        RangeTool.removeNumber(RelationIdRangeList, id);
        IsRelationChange = true;
    }

    public synchronized void addToTempRange(long id){
        RangeTool.addNumberToRange(TemporalPropertyTimeRangeList, id);
        //IsRelationChange = true;
    }

    public synchronized void removeFromTempRange(long id){
        RangeTool.removeNumber(TemporalPropertyTimeRangeList, id);
        //IsRelationChange = true;
    }

    public boolean isNodeChange() {
        return IsNodeChange;
    }

    public boolean isRelationChange() {
        return IsRelationChange;
    }

    public boolean isTemporalPropertyChange() {
        return IsTemporalPropertyChange;
    }


    public long[] getNextRegionObjectStartId(){
        long[] result = new long[2]; //first number is node start id, second number is relation start id
        result[0] = nodeUpperBound - nodecount + getMaxNodeId();//System.out.println("node : " + nodeUpperBound + ", " + nodecount + ", " + getMaxNodeId());
        result[1] = relationUpperBound - relationcount + getMaxRelationId();//System.out.println("relation : " + relationUpperBound + ", " + relationcount + ", " + getMaxRelationId());
        return result;
    }

    public long getMaxNodeId(){
        long[] maxNodeId = NodeIdRangeList.get(NodeIdRangeList.size() - 1);
        return  maxNodeId[1] - 1;
    }

    public long getMaxRelationId(){
        long[] maxRelationId = RelationIdRangeList.get(RelationIdRangeList.size() - 1);
        return  maxRelationId[1] - 1;
    }

    @Override
    public String toString() {
        return "Region{" + "id=" + id + ", regionEpoch=" + regionEpoch + ", peers=" + peers + '}';
    }
}

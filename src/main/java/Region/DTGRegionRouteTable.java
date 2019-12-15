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

import DBExceptions.IdNotExistException;
import DBExceptions.TypeDoesnotExistException;
import Element.EntityEntry;
import PlacementDriver.DTGPlacementDriverClient;
import com.alipay.sofa.jraft.rhea.client.RegionRouteTable;
import com.alipay.sofa.jraft.rhea.errors.RouteTableException;
import com.alipay.sofa.jraft.rhea.options.HeartbeatOptions;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Requires;
import com.alipay.sofa.jraft.rhea.util.Maps;
import com.alipay.sofa.jraft.rhea.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tool.ObjectAndByte;

import java.util.*;
import java.util.concurrent.locks.StampedLock;

import static config.MainType.*;

/**
 * @author :jinkai
 * @date :Created in 2019/10/14 20:17
 * @description:
 * @modified By:
 * @version: 1.0
 */

public class DTGRegionRouteTable {

    private static final Logger LOG = LoggerFactory.getLogger(RegionRouteTable.class);

    private static final Comparator<byte[]> keyBytesComparator  = BytesUtil.getDefaultByteArrayComparator();

    private static final long   TOPBOUND = -9999L;

    protected final StampedLock              stampedLock        = new StampedLock();
    private final NavigableMap<Long, Long>   rangeTable         = new TreeMap<>();
    //private final NavigableMap<byte[], Long> rangeTable         = new TreeMap<>(keyBytesComparator);
    private final Map<Long, DTGRegion>       regionTable        = Maps.newHashMap();
    private byte tableType;
    private long topRestrict;

    private DTGPlacementDriverClient pdClient;
    //private LinkedList<Long> idList;
//    private DTGMetadataRpcClient metadataRpcClient;
//    private byte type;
//    private int minIdBatchSize;

    public DTGRegionRouteTable(byte tableType, DTGPlacementDriverClient pdClient){
        this.tableType = tableType;
        topRestrict = 0;
        this.pdClient = pdClient;
        //this.idList = new LinkedList<Long>();
//        this.metadataRpcClient = metadataRpcClient;
//        this.type = type;
//        this.minIdBatchSize = minIdBatchSize;
//        if(isLocalClient){
//            getIds();
//        }
    }

    public Map[] findRegionsByEntityEntries(final List<EntityEntry> entityEntries) {
        Requires.requireNonNull(entityEntries, "entityEntries");
        Map[] returnMap = new Map[2];
        final Map<DTGRegion, List<EntityEntry>> regionMap = new HashMap<>();
        final Map<Integer, Long> transactionNumRegion = new HashMap<>();
        final StampedLock stampedLock = this.stampedLock;
        final long stamp = stampedLock.readLock();
        try {
            for (final EntityEntry entityEntry : entityEntries) {
//                if(entityEntry.getId() == -1){
//                    entityEntry.setId(idList.poll());
//                }
                final DTGRegion region;
                if(entityEntry.getId() == -2){
                    region = getRegionById(transactionNumRegion.get(entityEntry.getParaId()));
                }
                else {
                    region = findRegionByKeyWithoutLock(entityEntry.getId());
                }
                //checkRegion(entityEntry, region);
                final DTGRegion region1;
                if(region == null){
                    int a =0 ;
                    region1 = findRegionByKeyWithoutLock(entityEntry.getId());
                }
                else {
                    region1 = region;
                }
                //final DTGRegion region = findRegionByKeyWithoutLock(toByteArray(entityEntry.getKey()));
                transactionNumRegion.put(entityEntry.getTransactionNum(), region1.getId());
                regionMap.computeIfAbsent(region1, k -> Lists.newArrayList()).add(entityEntry);
            }
            returnMap[0] = regionMap;
            returnMap[1] = transactionNumRegion;
            return returnMap;
        } finally {
            stampedLock.unlockRead(stamp);
        }
    }

    public Map[] findRegionsByEntityEntries(final List<EntityEntry> entityEntries, final Map[] map) {
        Requires.requireNonNull(entityEntries, "entityEntries");
        final Map<DTGRegion, List<EntityEntry>> regionMap = map[0];
        final Map<Integer, Long> transactionNumRegion = map[1];
        final StampedLock stampedLock = this.stampedLock;
        final long stamp = stampedLock.readLock();
        try {
            for (final EntityEntry entityEntry : entityEntries) {
//                if(entityEntry.getId() == -1){
//                    entityEntry.setId(idList.poll());
//                }

                final DTGRegion region;
                if(entityEntry.getId() == -2){
                    region = getRegionById(transactionNumRegion.get(entityEntry.getParaId()));
                }
                else {
                    region = findRegionByKeyWithoutLock(entityEntry.getId());
                }
                //checkRegion(entityEntry, region);
                //final DTGRegion region = findRegionByKeyWithoutLock(toByteArray(entityEntry.getKey()));
                transactionNumRegion.put(entityEntry.getTransactionNum(), region.getId());
                regionMap.computeIfAbsent(region, k -> Lists.newArrayList()).add(entityEntry);
            }
//            if(idList.size() < minIdBatchSize){
//                CompletableFuture.runAsync(() -> {
//                   getIds();
//                });
//            }
            return map;
        } finally {
            stampedLock.unlockRead(stamp);
        }
    }

    public DTGRegion findRegionByEntityEntry(EntityEntry entityEntry){
        DTGRegion region = findRegionByKeyWithoutLock(entityEntry.getId());
        //checkRegion(entityEntry, region);
        return region;
    }

//    public void getIds() {
//        if(type == TEMPORALPROPERTYTYPE)return;
//        List<Long> list = this.metadataRpcClient.getIds(type);
//        if(!this.idList.addAll(list)){
//            try {
//                throw new IdDistributeException("can not get id from PD");
//            } catch (IdDistributeException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//    public void returnIds() {
//        if(type == TEMPORALPROPERTYTYPE)return;
//        if(! this.metadataRpcClient.returnIds(idList, type)){
//            try {
//                throw new IdDistributeException("can not return id to PD");
//            } catch (IdDistributeException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//    public void removeId(long id){
//        if(type == TEMPORALPROPERTYTYPE)return;
//        idList.add(id);
//    }

    public DTGRegion getRegionById(final long regionId) {
        final StampedLock stampedLock = this.stampedLock;
        long stamp = stampedLock.tryOptimisticRead();
        // validate() emit a load-fence, but no store-fence.  So you should only have
        // load instructions inside a block of tryOptimisticRead() / validate(),
        // because it is meant to the a read-only operation, and therefore, it is fine
        // to use the loadFence() function to avoid re-ordering.
        DTGRegion region = safeCopy(this.regionTable.get(regionId));
        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                region = safeCopy(this.regionTable.get(regionId));
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return region;
    }

    public void addOrUpdateRegion(final DTGRegion region, boolean needLock) {
        Requires.requireNonNull(region, "region");
        Requires.requireNonNull(region.getRegionEpoch(), "regionEpoch");
        final long regionId = region.getId();
        final StampedLock stampedLock = this.stampedLock;
        long stamp = 0;
        if(needLock){
            stamp = stampedLock.writeLock();
        }
        try {
            //rangeTable.remove(topRestrict);
            List<long[]> rangeList = getRangeList(region);//System.out.println("put region:");
            for(long[] range : rangeList){
                //final byte[] startKey = BytesUtil.nullToEmpty(ObjectAndByte.toByteArray(range[0]));
                //final byte[] startKey = BytesUtil.nullToEmpty(region.getStartKey());
                //this.rangeTable.put(startKey, regionId);
                this.rangeTable.put(range[0], regionId);//System.out.println("rang[0] = " + range[0] + ", region id = " + regionId + ", region epoch = " + region.getRegionEpoch());
            }
//            this.regionTable.put(regionId, region.copy());
            //System.out.println("region id = " + regionId + ", region epoch = " + region.getRegionEpoch());
            this.regionTable.put(regionId, region);
        } catch (TypeDoesnotExistException e) {
            e.printStackTrace();
        } finally {
            if(needLock){
                stampedLock.unlockWrite(stamp);
            }

        }
    }

//    public void addOrUpdateRegion(final DTGRegion region, byte type) {
//        Requires.requireNonNull(region, "region");
//        Requires.requireNonNull(region.getRegionEpoch(), "regionEpoch");
//        final long regionId = region.getId();
//        System.out.println("add region : " + regionId);
//        final StampedLock stampedLock = this.stampedLock;
//        final long stamp = stampedLock.writeLock();
//        try {
//            List<long[]> rangeList = getRangeList(region);
//            for(long[] range : rangeList){
//                final byte[] startKey = BytesUtil.nullToEmpty(ObjectAndByte.toByteArray(range[0]));
//                //final byte[] startKey = BytesUtil.nullToEmpty(region.getStartKey());
//                this.rangeTable.put(startKey, regionId);
//            }
//            this.regionTable.put(regionId, region.copy());
//        } catch (TypeDoesnotExistException e) {
//            e.printStackTrace();
//        } finally {
//            stampedLock.unlockWrite(stamp);
//        }
//    }

//    public void splitRegion(final long leftId, final DTGRegion right) {
//        Requires.requireNonNull(right, "right");
//        Requires.requireNonNull(right.getRegionEpoch(), "right.regionEpoch");
//        final StampedLock stampedLock = this.stampedLock;
//        final long stamp = stampedLock.writeLock();
//        try {
//            final DTGRegion left = this.regionTable.get(leftId);
//            Requires.requireNonNull(left, "left");
//            final byte[] leftStartKey = BytesUtil.nullToEmpty(left.getStartKey());
//            final byte[] leftEndKey = left.getEndKey();
//            final long rightId = right.getId();
//            final byte[] rightStartKey = right.getStartKey();
//            final byte[] rightEndKey = right.getEndKey();
//            Requires.requireNonNull(rightStartKey, "rightStartKey");
//            Requires.requireTrue(BytesUtil.compare(leftStartKey, rightStartKey) < 0,
//                    "leftStartKey must < rightStartKey");
//            if (leftEndKey == null || rightEndKey == null) {
//                Requires.requireTrue(leftEndKey == rightEndKey, "leftEndKey must == rightEndKey");
//            } else {
//                Requires.requireTrue(BytesUtil.compare(leftEndKey, rightEndKey) == 0, "leftEndKey must == rightEndKey");
//                Requires.requireTrue(BytesUtil.compare(rightStartKey, rightEndKey) < 0,
//                        "rightStartKey must < rightEndKey");
//            }
//            final RegionEpoch leftEpoch = left.getRegionEpoch();
//            leftEpoch.setVersion(leftEpoch.getVersion() + 1);
//            left.setEndKey(rightStartKey);
//            this.regionTable.put(rightId, right.copy());
//            this.rangeTable.put(rightStartKey, rightId);
//        } finally {
//            stampedLock.unlockWrite(stamp);
//        }
//    }

    public boolean removeRegion(final long regionId) {
        final StampedLock stampedLock = this.stampedLock;
        final long stamp = stampedLock.writeLock();
        try {
            final DTGRegion region = this.regionTable.remove(regionId);
            if (region != null) {
                List<long[]> rangeList = getRangeList(region);
                for(long[] range : rangeList){
                    final byte[] startKey = BytesUtil.nullToEmpty(ObjectAndByte.toByteArray(range[0]));
                    return this.rangeTable.remove(startKey) != null;
                }
//                final byte[] startKey = BytesUtil.nullToEmpty(region.getStartKey());
//                return this.rangeTable.remove(startKey) != null;
            }
        } catch (TypeDoesnotExistException e) {
            e.printStackTrace();
        } finally {
            stampedLock.unlockWrite(stamp);
        }
        return false;
    }

    /**
     * Returns the region to which the key belongs.
     */

    public DTGRegion findRegionByKey(final long key) {
        Requires.requireNonNull(key, "key");
        final StampedLock stampedLock = this.stampedLock;
        final long stamp = stampedLock.readLock();
        try {
            return findRegionByKeyWithoutLock(key);
        } finally {
            stampedLock.unlockRead(stamp);
        }
    }

    protected DTGRegion findRegionByKeyWithoutLock(final long key) {
        // return the greatest key less than or equal to the given key
        Map.Entry<Long, Long> entry = this.rangeTable.floorEntry(key);
        if (entry == null) {
            reportFail(ObjectAndByte.toByteArray(key));
            throw reject(ObjectAndByte.toByteArray(key), "fail to find region by key");
        }
        return this.regionTable.get(entry.getValue());
    }

    /**
     * Returns the list of regions to which the keys belongs.
     */
    public Map<DTGRegion, List<Long>> findRegionsByKeys(final long[] keys) {
        Requires.requireNonNull(keys, "keys");
        final Map<DTGRegion, List<Long>> regionMap = Maps.newHashMap();
        final StampedLock stampedLock = this.stampedLock;
        final long stamp = stampedLock.readLock();
        try {
            for (final long key : keys) {
                final DTGRegion region = findRegionByKeyWithoutLock(key);
                regionMap.computeIfAbsent(region, k -> Lists.newArrayList()).add(key);
            }
            return regionMap;
        } finally {
            stampedLock.unlockRead(stamp);
        }
    }

    /**
     * Returns the list of regions to which the keys belongs.
     */
//    public Map<DTGRegion, List<KVEntry>> findRegionsByKvEntries(final List<KVEntry> kvEntries) {
//        Requires.requireNonNull(kvEntries, "kvEntries");
//        final Map<DTGRegion, List<KVEntry>> regionMap = Maps.newHashMap();
//        final StampedLock stampedLock = this.stampedLock;
//        final long stamp = stampedLock.readLock();
//        try {
//            for (final KVEntry kvEntry : kvEntries) {
//                final DTGRegion region = findRegionByKeyWithoutLock(kvEntry.getKey());
//                regionMap.computeIfAbsent(region, k -> Lists.newArrayList()).add(kvEntry);
//            }
//            return regionMap;
//        } finally {
//            stampedLock.unlockRead(stamp);
//        }
//    }

    /**
     * Returns the list of regions covered by startKey and endKey.
     */

    public List<DTGRegion> findRegionsByKeyRange(final long startKey, final long endKey) {
        final StampedLock stampedLock = this.stampedLock;
        final long stamp = stampedLock.readLock();
        try {
            final NavigableMap<Long, Long> subRegionMap;
            if (endKey < 0) {
                subRegionMap = this.rangeTable.tailMap(startKey, false);
            } else {
                subRegionMap = this.rangeTable.subMap(startKey, false, endKey, true);
            }
            final List<DTGRegion> regionList = Lists.newArrayListWithCapacity(subRegionMap.size() + 1);
            final Map.Entry<Long, Long> headEntry = this.rangeTable.floorEntry(startKey);
            if (headEntry == null) {
                reportFail(ObjectAndByte.toByteArray(startKey));
                throw reject(ObjectAndByte.toByteArray(startKey), "fail to find region by startKey");
            }
            regionList.add(safeCopy(this.regionTable.get(headEntry.getValue())));
            for (final Long regionId : subRegionMap.values()) {
                regionList.add(safeCopy(this.regionTable.get(regionId)));
            }
            return regionList;
        } finally {
            stampedLock.unlockRead(stamp);
        }
    }

    /**
     * Returns the startKey of next region.
     */

    public long findStartKeyOfNextRegion(final long key) {
        Requires.requireNonNull(key, "key");
        final StampedLock stampedLock = this.stampedLock;
        long stamp = stampedLock.tryOptimisticRead();
        // get the least key strictly greater than the given key
        long nextStartKey = this.rangeTable.higherKey(key);
        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                // get the least key strictly greater than the given key
                nextStartKey = this.rangeTable.higherKey(key);
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return nextStartKey;
    }

    public void updataTop(long top){
        System.out.println("top bound :" + top);
        if(rangeTable.containsKey(topRestrict) && (long)rangeTable.get(topRestrict) == TOPBOUND){
            rangeTable.remove(topRestrict);System.out.println("remove top bound :" + topRestrict);
        }
        if(rangeTable.higherEntry(top) != null){
            top = rangeTable.lastEntry().getKey() + 1;
        }
        rangeTable.put(top, TOPBOUND);
        topRestrict = top;
    }

    // Should be in lock
    //
    // If this method is called, either because the registered region table is incomplete (by user)
    // or because of a bug.
    private void reportFail(final byte[] relatedKey) {
        if (LOG.isErrorEnabled()) {
            LOG.error("There is a high probability that the data in the region table is corrupted.");
            LOG.error("---------------------------------------------------------------------------");
            LOG.error("* RelatedKey:  {}.", BytesUtil.toHex(relatedKey));
            LOG.error("* RangeTable:  {}.", this.rangeTable);
            LOG.error("* RegionTable: {}.", this.regionTable);
            LOG.error("---------------------------------------------------------------------------");
        }
    }

    private static DTGRegion safeCopy(final DTGRegion region) {
        if (region == null) {
            return null;
        }
        return region.copy();
    }

    private static RouteTableException reject(final byte[] relatedKey, final String message) {
        return new RouteTableException("key: " + BytesUtil.toHex(relatedKey) + ", message: " + message);
    }

//    private byte[][] getStartEndKeyByType(DTGRegion region, byte type) {
//        byte[][] startEndKey = new byte[2][];
//        switch (type) {
//            case 0x01: {//represent node
//                startEndKey[0] = BytesUtil.nullToEmpty(region.getNodeStartId());
//                startEndKey[1] = BytesUtil.nullToEmpty(region.getNodeEndId());
//                break;
//            }
//            case 0x02: {//represent relation
//                startEndKey[0] = BytesUtil.nullToEmpty(region.getRelationStartId());
//                startEndKey[1] = BytesUtil.nullToEmpty(region.getRelationEndId());
//                break;
//            }
//            case 0x03: {//represent temporal property
//                startEndKey[0] = BytesUtil.nullToEmpty(region.getStartKey());
//                startEndKey[1] = BytesUtil.nullToEmpty(region.getEndKey());
//                break;
//            }
//            default: {
//                LOG.error("can not find region route tabale of type");
//                try {
//                    throw new Exception("can not find region route tabale of type");
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//        return startEndKey;
//    }

    private List<long[]> getRangeList(DTGRegion region) throws TypeDoesnotExistException {
        switch (this.tableType){
            case NODETYPE :{
                return region.getNodeIdRangeList();
            }
            case RELATIONTYPE:{
                return region.getRelationIdRangeList();
            }
            case TEMPORALPROPERTYTYPE: {
                return region.getTemporalPropertyTimeRangeList();
            }
            default:{
                throw new TypeDoesnotExistException(this.tableType, "regionTable");
            }
        }
    }

    private boolean checkRegion(EntityEntry entityEntry, DTGRegion region){
        try {
            if(region == null){
                if(entityEntry.getOperationType() != EntityEntry.ADD){
                    String type;
                    if(entityEntry.getType() == NODETYPE)type = "node";
                    else type = "relation";
                    throw new IdNotExistException(type, entityEntry.getId());
                }
                else {
                    System.out.println("send add region request!");
                    this.pdClient.getDTGMetadataRpcClient().createRegion(pdClient.getClusterId(), this.tableType, entityEntry.getId());
                    long waitTime = (new HeartbeatOptions()).getRegionHeartbeatIntervalSeconds() * 3000;
                    Thread.sleep(waitTime);
                    this.pdClient.refreshRouteTable(false);
                    System.out.println("finish refresh!");
                    return false;
                }
            }
        } catch (IdNotExistException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return true;
    }

    public long getTopRestrict() {
        return topRestrict;
    }
}

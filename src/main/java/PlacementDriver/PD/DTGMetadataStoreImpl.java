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
package PlacementDriver.PD;

import com.alipay.sofa.jraft.rhea.MetadataKeyHelper;
import Region.DTGRegionStats;
import com.alipay.sofa.jraft.rhea.client.RheaKVStore;
import com.alipay.sofa.jraft.rhea.metadata.*;
import com.alipay.sofa.jraft.rhea.serialization.Serializer;
import com.alipay.sofa.jraft.rhea.serialization.Serializers;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.storage.LongSequence;
import com.alipay.sofa.jraft.rhea.storage.Sequence;
import com.alipay.sofa.jraft.rhea.util.*;
import com.alipay.sofa.jraft.util.Bits;
import com.alipay.sofa.jraft.util.BytesUtil;
import com.alipay.sofa.jraft.util.Endpoint;
import config.DefaultOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.DTGCluster;
import storage.DTGStore;
import Region.DTGRegion;
import tool.ObjectAndByte;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;

/**
 *
 * @author jiachun.fjc
 */
public class DTGMetadataStoreImpl implements DTGMetadataStore {

    private static final Logger                       LOG                  = LoggerFactory
                                                                               .getLogger(DTGMetadataStoreImpl.class);

    private final ConcurrentMap<String, LongSequence> storeSequenceMap     = Maps.newConcurrentMap();
    private final ConcurrentMap<String, LongSequence> regionSequenceMap    = Maps.newConcurrentMap();
    private final ConcurrentMap<Long, Set<Long>>      clusterStoreIdsCache = Maps.newConcurrentMapLong();
    private final Serializer                          serializer           = Serializers.getDefault();
    private final RheaKVStore                         rheaKVStore;

    public DTGMetadataStoreImpl(RheaKVStore rheaKVStore) {
        this.rheaKVStore = rheaKVStore;
    }

    @Override
    public DTGCluster getClusterInfo(final long clusterId) {
        final Set<Long> storeIds = getClusterIndex(clusterId);
        if (storeIds == null) {
            return null;
        }
        final List<byte[]> storeKeys = Lists.newArrayList();
        for (final Long storeId : storeIds) {
            final String storeInfoKey = MetadataKeyHelper.getStoreInfoKey(clusterId, storeId);
            storeKeys.add(BytesUtil.writeUtf8(storeInfoKey));
        }
        final Map<ByteArray, byte[]> storeInfoBytes = this.rheaKVStore.bMultiGet(storeKeys);
        final List<DTGStore> stores = Lists.newArrayListWithCapacity(storeInfoBytes.size());
        for (final byte[] storeBytes : storeInfoBytes.values()) {
            final DTGStore store = this.serializer.readObject(storeBytes, DTGStore.class);
            stores.add(store);
        }
        long maxNodeId = getNewRegionNodeStartId(clusterId);
        long maxRelationId = getNewRegionRelationStartId(clusterId);
        DTGCluster cluster = new DTGCluster(clusterId, stores, maxNodeId, maxRelationId);
        return cluster;
    }

    @Override
    public Long getOrCreateStoreId(final long clusterId, final Endpoint endpoint) {
        final String storeIdKey = MetadataKeyHelper.getStoreIdKey(clusterId, endpoint);
        final byte[] bytesVal = this.rheaKVStore.bGet(storeIdKey);
        if (bytesVal == null) {
            final String storeSeqKey = MetadataKeyHelper.getStoreSeqKey(clusterId);
            LongSequence storeSequence = this.storeSequenceMap.get(storeSeqKey);
            if (storeSequence == null) {
                final LongSequence newStoreSequence = new LongSequence() {

                    @Override
                    public Sequence getNextSequence() {
                        return rheaKVStore.bGetSequence(storeSeqKey, 32);
                    }
                };
                storeSequence = this.storeSequenceMap.putIfAbsent(storeSeqKey, newStoreSequence);
                if (storeSequence == null) {
                    storeSequence = newStoreSequence;
                }
            }
            final long newStoreId = storeSequence.next();
            final byte[] newBytesVal = new byte[8];
            Bits.putLong(newBytesVal, 0, newStoreId);
            final byte[] oldBytesVal = this.rheaKVStore.bPutIfAbsent(storeIdKey, newBytesVal);
            if (oldBytesVal != null) {
                return Bits.getLong(oldBytesVal, 0);
            } else {
                return newStoreId;
            }
        }
        return Bits.getLong(bytesVal, 0);
    }

    @Override
    public DTGStore getStoreInfo(final long clusterId, final long storeId) {
        final String storeInfoKey = MetadataKeyHelper.getStoreInfoKey(clusterId, storeId);
        final byte[] bytes = this.rheaKVStore.bGet(storeInfoKey);
        if (bytes == null) {
            DTGStore empty = new DTGStore();
            empty.setId(storeId);
            return empty;
        }
        return this.serializer.readObject(bytes, DTGStore.class);
    }

    @Override
    public DTGStore getStoreInfo(final long clusterId, final Endpoint endpoint) {
        final long storeId = getOrCreateStoreId(clusterId, endpoint);
        return getStoreInfo(clusterId, storeId);
    }

    @Override
    public CompletableFuture<DTGStore> updateStoreInfo(final long clusterId, final DTGStore store) {
        System.out.println("updateStoreInfo");
        final long storeId = store.getId();
        final String storeInfoKey = MetadataKeyHelper.getStoreInfoKey(clusterId, storeId);
        final byte[] bytes = this.serializer.writeObject(store);
        final List<DTGRegion> regions = store.getRegions();
        CompletableFuture.runAsync(()->{
            long[] nextId = new long[2];
            for(DTGRegion region : regions){
                long[] nextRegionId = region.getNextRegionObjectStartId();
                if(nextRegionId[0] > nextId[0]) nextId[0] = nextRegionId[0];
                if(nextRegionId[1] > nextId[1]) nextId[1] = nextRegionId[1];
                //System.out.println("node next id : " + nextId[0] + ", relation next id : " + nextId[1]);
            }
            //System.out.println("save!");
            byte[] saveNextNodeId = this.rheaKVStore.bGet(clusterId + "NewRegionNodeStartId");
            byte[] saveNextRelationId = this.rheaKVStore.bGet(clusterId + "NewRegionRelationStartId");
            if(saveNextNodeId == null||nextId[0] > (long)ObjectAndByte.toObject(saveNextNodeId)){
                this.rheaKVStore.bPut(clusterId + "NewRegionNodeStartId", ObjectAndByte.toByteArray(nextId[0]));
            }
            if(saveNextRelationId == null||nextId[1] > (long)ObjectAndByte.toObject(saveNextRelationId)){
                this.rheaKVStore.bPut(clusterId + "NewRegionRelationStartId", ObjectAndByte.toByteArray(nextId[1]));
            }
        });

        final CompletableFuture<DTGStore> future = new CompletableFuture<>();
        this.rheaKVStore.getAndPut(storeInfoKey, bytes).whenComplete((prevBytes, getPutThrowable) -> {
            if (getPutThrowable == null) {
                if (prevBytes != null) {
                    future.complete(serializer.readObject(prevBytes, DTGStore.class));
                } else {
                    mergeClusterIndex(clusterId, storeId).whenComplete((ignored, mergeThrowable) -> {
                        if (mergeThrowable == null) {
                            future.complete(null);
                        } else {
                            future.completeExceptionally(mergeThrowable);
                        }
                    });
                }
            } else {
                future.completeExceptionally(getPutThrowable);
            }
        });
        return future;
    }

    @Override
    public Long createRegionId(final long clusterId) {
        final String regionSeqKey = MetadataKeyHelper.getRegionSeqKey(clusterId);
        LongSequence regionSequence = this.regionSequenceMap.get(regionSeqKey);
        if (regionSequence == null) {
            final LongSequence newRegionSequence = new LongSequence(Region.MAX_ID_WITH_MANUAL_CONF) {

                @Override
                public Sequence getNextSequence() {
                    return rheaKVStore.bGetSequence(regionSeqKey, 32);
                }
            };
            regionSequence = this.regionSequenceMap.putIfAbsent(regionSeqKey, newRegionSequence);
            if (regionSequence == null) {
                regionSequence = newRegionSequence;
            }
        }
        return regionSequence.next();
    }

    @Override
    public StoreStats getStoreStats(final long clusterId, final long storeId) {
        final String key = MetadataKeyHelper.getStoreStatsKey(clusterId, storeId);
        final byte[] bytes = this.rheaKVStore.bGet(key);
        if (bytes == null) {
            return null;
        }
        return this.serializer.readObject(bytes, StoreStats.class);
    }

    @Override
    public CompletableFuture<Boolean> updateStoreStats(final long clusterId, final StoreStats storeStats) {
        final String key = MetadataKeyHelper.getStoreStatsKey(clusterId, storeStats.getStoreId());
        final byte[] bytes = this.serializer.writeObject(storeStats);
        return this.rheaKVStore.put(key, bytes);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Pair<DTGRegion, DTGRegionStats> getRegionStats(final long clusterId, final DTGRegion region) {
        final String key = MetadataKeyHelper.getRegionStatsKey(clusterId, region.getId());
        final byte[] bytes = this.rheaKVStore.bGet(key);
        if (bytes == null) {
            return null;
        }
        return this.serializer.readObject(bytes, Pair.class);
    }

    @Override
    public CompletableFuture<Boolean> updateRegionStats(final long clusterId, final DTGRegion region,
                                                        final DTGRegionStats regionStats) {
        final String key = MetadataKeyHelper.getRegionStatsKey(clusterId, region.getId());
        final byte[] bytes = this.serializer.writeObject(Pair.of(region, regionStats));
        return this.rheaKVStore.put(key, bytes);
    }

    @Override
    public CompletableFuture<Boolean> batchUpdateRegionStats(final long clusterId,
                                                             final List<Pair<DTGRegion, DTGRegionStats>> regionStatsList) {
        final List<KVEntry> entries = Lists.newArrayListWithCapacity(regionStatsList.size());
        for (final Pair<DTGRegion, DTGRegionStats> p : regionStatsList) {
            final String key = MetadataKeyHelper.getRegionStatsKey(clusterId, p.getKey().getId());
            final byte[] bytes = this.serializer.writeObject(p);
            entries.add(new KVEntry(BytesUtil.writeUtf8(key), bytes));
        }
        return this.rheaKVStore.put(entries);
    }

    @Override
    public CompletableFuture<Boolean> batchUpdateRegionRange(final long clusterId,
                                                      final List<Pair<DTGRegion, List<long[]>>> regionRangeList){
        final List<KVEntry> entries = Lists.newArrayListWithCapacity(regionRangeList.size());
        for (final Pair<DTGRegion, List<long[]>> p : regionRangeList) {
            final String key = MetadataKeyHelper.getRegionStatsKey(clusterId, p.getKey().getId());
            final byte[] bytes = this.serializer.writeObject(p);
            entries.add(new KVEntry(BytesUtil.writeUtf8(key), bytes));
        }
        return this.rheaKVStore.put(entries);
    }

    @Override
    public Set<Long> unsafeGetStoreIds(final long clusterId) {
        Set<Long> storeIds = this.clusterStoreIdsCache.get(clusterId);
        if (storeIds != null) {
            return storeIds;
        }
        storeIds = getClusterIndex(clusterId);
        this.clusterStoreIdsCache.put(clusterId, storeIds);
        return storeIds;
    }

    @Override
    public Map<Long, Endpoint> unsafeGetStoreIdsByEndpoints(final long clusterId, final List<Endpoint> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            return Collections.emptyMap();
        }
        final List<byte[]> storeIdKeyList = Lists.newArrayListWithCapacity(endpoints.size());
        final Map<ByteArray, Endpoint> keyToEndpointMap = Maps.newHashMapWithExpectedSize(endpoints.size());
        for (final Endpoint endpoint : endpoints) {
            final byte[] keyBytes = BytesUtil.writeUtf8(MetadataKeyHelper.getStoreIdKey(clusterId, endpoint));
            storeIdKeyList.add(keyBytes);
            keyToEndpointMap.put(ByteArray.wrap(keyBytes), endpoint);
        }
        final Map<ByteArray, byte[]> storeIdBytes = this.rheaKVStore.bMultiGet(storeIdKeyList);
        final Map<Long, Endpoint> ids = Maps.newHashMapWithExpectedSize(storeIdBytes.size());
        for (final Map.Entry<ByteArray, byte[]> entry : storeIdBytes.entrySet()) {
            final Long storeId = Bits.getLong(entry.getValue(), 0);
            final Endpoint endpoint = keyToEndpointMap.get(entry.getKey());
            ids.put(storeId, endpoint);
        }
        return ids;
    }

    @Override
    public synchronized long getNewRegionNodeStartId(final long clusterId) {
        long newRegionNodeStartId;
        final byte[] id = this.rheaKVStore.bGet(clusterId + "NewRegionNodeStartId");
        if(id == null){
            newRegionNodeStartId = 0;
        }else {
            newRegionNodeStartId = (long)ObjectAndByte.toObject(id);
        }
        return newRegionNodeStartId;
    }

    @Override
    public long getNewRegionRelationStartId(final long clusterId) {
        long NewRegionRelationStartId;
        final byte[] id = this.rheaKVStore.bGet(clusterId + "NewRegionRelationStartId");
        if(id == null){
            NewRegionRelationStartId = 0;
        }else {
            NewRegionRelationStartId = (long)ObjectAndByte.toObject(id);
        }
        return NewRegionRelationStartId;
    }

    @Override
    public synchronized long updateRegionNodeStartId(final long clusterId) {
        long oldRegionNodeStartId = getNewRegionNodeStartId(clusterId);
        long newRegionNodeStartId = oldRegionNodeStartId + DefaultOptions.DEFAULTREGIONNODESIZE;//System.out.println("update node max id" + newRegionNodeStartId);
        this.rheaKVStore.bPut(clusterId + "NewRegionNodeStartId", ObjectAndByte.toByteArray(newRegionNodeStartId));
        return oldRegionNodeStartId;
    }

    @Override
    public synchronized long updateRegionRelationStartId(final long clusterId) {
        long oldRegionRelationStartId = getNewRegionRelationStartId(clusterId);
        long NewRegionRelationStartId = oldRegionRelationStartId  + DefaultOptions.DEFAULTREGIONRELATIONSIZE;//System.out.println("update relation max id" + NewRegionRelationStartId);
        this.rheaKVStore.bPut(clusterId + "NewRegionRelationStartId", ObjectAndByte.toByteArray(NewRegionRelationStartId));
        return oldRegionRelationStartId;
    }


    @Override
    public void invalidCache() {
        this.clusterStoreIdsCache.clear();
    }

    private Set<Long> getClusterIndex(final long clusterId) {
        final String key = MetadataKeyHelper.getClusterInfoKey(clusterId);
        final byte[] indexBytes = this.rheaKVStore.bGet(key);
        if (indexBytes == null) {
            return null;
        }
        final String strVal = BytesUtil.readUtf8(indexBytes);
        final String[] array = Strings.split(strVal, ',');
        if (array == null) {
            return null;
        }
        final Set<Long> storeIds = new HashSet<>(array.length);
        for (final String str : array) {
            storeIds.add(Long.parseLong(str.trim()));
        }
        return storeIds;
    }

    private CompletableFuture<Boolean> mergeClusterIndex(final long clusterId, final long storeId) {
        final String key = MetadataKeyHelper.getClusterInfoKey(clusterId);
        final CompletableFuture<Boolean> future = this.rheaKVStore.merge(key, String.valueOf(storeId));
        future.whenComplete((ignored, throwable) -> {
            if (throwable != null) {
                LOG.error("Fail to merge cluster index, {}, {}.", key, StackTraceUtil.stackTrace(throwable));
            }
            clusterStoreIdsCache.clear();
        });
        return future;
    }
}

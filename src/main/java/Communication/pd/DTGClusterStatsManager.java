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
package Communication.pd;

import Region.DTGRegionStats;
import com.alipay.remoting.util.ConcurrentHashSet;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.rhea.util.Maps;
import com.alipay.sofa.jraft.rhea.util.Pair;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import Region.DTGRegion;

/**
 *
 * @author jiachun.fjc
 */
public final class DTGClusterStatsManager {

    private static final ConcurrentMap<Long, DTGClusterStatsManager> clusterStatsManagerTable = Maps
                                                                                               .newConcurrentMapLong();

    private final long                                            clusterId;
    // Map<StoreId, Set<RegionId>>
    private final ConcurrentMap<Long, Set<Long>>                  leaderTable              = Maps
                                                                                               .newConcurrentMapLong();
    // Map<RegionId, Pair<Region, RegionStats>>
    private final ConcurrentMap<Long, Pair<DTGRegion, DTGRegionStats>>  regionStatsTable         = Maps
                                                                                               .newConcurrentMapLong();

    private DTGClusterStatsManager(long clusterId) {
        this.clusterId = clusterId;
    }

    public static DTGClusterStatsManager getInstance(final long clusterId) {
        DTGClusterStatsManager instance = clusterStatsManagerTable.get(clusterId);
        if (instance == null) {
            final DTGClusterStatsManager newInstance = new DTGClusterStatsManager(clusterId);
            instance = clusterStatsManagerTable.putIfAbsent(clusterId, newInstance);
            if (instance == null) {
                instance = newInstance;
            }
        }
        return instance;
    }

    public long getClusterId() {
        return clusterId;
    }

    public int regionSize() {
        return this.regionStatsTable.size();
    }

    public void addOrUpdateLeader(final long storeId, final long regionId) {
        Set<Long> regionTable = this.leaderTable.get(storeId);
        if (regionTable == null) {
            final Set<Long> newRegionTable = new ConcurrentHashSet<>();
            regionTable = this.leaderTable.putIfAbsent(storeId, newRegionTable);
            if (regionTable == null) {
                regionTable = newRegionTable;
            }
        }
        if (regionTable.add(regionId)) {
            for (final Map.Entry<Long, Set<Long>> entry : this.leaderTable.entrySet()) {
                if (storeId == entry.getKey()) {
                    continue;
                }
                entry.getValue().remove(regionId);
            }
        }
    }

    // Looking for a model worker
    public Pair<Set<Long /* storeId */>, Integer /* leaderCount */> findModelWorkerStores(final int above) {
        final Set<Map.Entry<Long, Set<Long>>> values = this.leaderTable.entrySet();
        if (values.isEmpty()) {
            return Pair.of(Collections.emptySet(), 0);
        }
        final Map.Entry<Long, Set<Long>> modelWorker = Collections.max(values, (o1, o2) -> {
            final int o1Val = o1.getValue() == null ? 0 : o1.getValue().size();
            final int o2Val = o2.getValue() == null ? 0 : o2.getValue().size();
            return Integer.compare(o1Val, o2Val);
        });
        final int maxLeaderCount = modelWorker.getValue().size();
        if (maxLeaderCount <= above) {
            return Pair.of(Collections.emptySet(), maxLeaderCount);
        }
        final Set<Long> modelWorkerStoreIds = new HashSet<>();
        for (final Map.Entry<Long, Set<Long>> entry : values) {
            if (entry.getValue().size() >= maxLeaderCount) {
                modelWorkerStoreIds.add(entry.getKey());
            }
        }
        return Pair.of(modelWorkerStoreIds, maxLeaderCount);
    }

    // Investigate who is lazy
    public List<Pair<Long /* storeId */, Integer /* leaderCount */>> findLazyWorkerStores(final Collection<Long> storeCandidates) {
        if (storeCandidates == null || storeCandidates.isEmpty()) {
            return Collections.emptyList();
        }
        final Set<Map.Entry<Long, Set<Long>>> values = this.leaderTable.entrySet();
        if (values.isEmpty()) {
            return Collections.emptyList();
        }
        final Map.Entry<Long, Set<Long>> lazyWorker = Collections.min(values, (o1, o2) -> {
            final int o1Val = o1.getValue() == null ? 0 : o1.getValue().size();
            final int o2Val = o2.getValue() == null ? 0 : o2.getValue().size();
            return Integer.compare(o1Val, o2Val);
        });
        final int minLeaderCount = lazyWorker.getValue().size();
        final List<Pair<Long, Integer>> lazyCandidates = Lists.newArrayList();
        for (final Long storeId : storeCandidates) {
            final Set<Long> regionTable = this.leaderTable.get(storeId);
            int leaderCount = regionTable == null ? 0 : regionTable.size();
            if (leaderCount <= minLeaderCount) {
                lazyCandidates.add(Pair.of(storeId, leaderCount));
            }
        }
        return lazyCandidates;
    }

    public void addOrUpdateRegionStats(final List<Pair<DTGRegion, DTGRegionStats>> regionStatsList) {
        for (final Pair<DTGRegion, DTGRegionStats> p : regionStatsList) {
            this.regionStatsTable.put(p.getKey().getId(), p);
        }
    }

    public Pair<DTGRegion, DTGRegionStats> findModelWorkerRegion() {
        if (this.regionStatsTable.isEmpty()) {
            return null;
        }
        return Collections.max(this.regionStatsTable.values(), (o1, o2) -> {
            final long o1Val = o1.getValue().getApproximateKeys();
            final long o2Val = o2.getValue().getApproximateKeys();
            return Long.compare(o1Val, o2Val);
        });
    }

    public static void invalidCache() {
        for (final DTGClusterStatsManager manager : clusterStatsManagerTable.values()) {
            manager.leaderTable.clear();
            manager.regionStatsTable.clear();
        }
        clusterStatsManagerTable.clear();
    }
}

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
package PlacementDriver;

import Communication.DTGMetadataRpcClient;
import Element.EntityEntry;
import PlacementDriver.PD.DTGMetadataStore;
import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.rhea.client.pd.PlacementDriverClient;
import com.alipay.sofa.jraft.rhea.client.pd.PlacementDriverRpcService;
import com.alipay.sofa.jraft.rhea.metadata.Peer;
import com.alipay.sofa.jraft.rhea.metadata.Region;
import com.alipay.sofa.jraft.rhea.metadata.Store;
import com.alipay.sofa.jraft.rhea.options.StoreEngineOptions;
import com.alipay.sofa.jraft.util.Endpoint;
import options.DTGPlacementDriverOptions;
import Region.DTGRegionRouteTable;
import Region.DTGRegion;
import options.DTGStoreEngineOptions;
import storage.DTGStore;

import java.util.List;
import java.util.Map;

/**
 * @author :jinkai
 * @date :Created in 2019/10/14 16:38
 * @description:user client use this to communicate with PD cluseter
 * @modified By:
 * @version: 1.0
 */

public interface DTGPlacementDriverClient extends Lifecycle<DTGPlacementDriverOptions> {

    /**
     * Returns the cluster id.
     */
    long getClusterId();

    /**
     * Query the region by region id.
     */
    DTGRegion getRegionByRegionId(final long regionId);

    /**
     * Returns the region to which the id belongs.
     */
//    DTGRegion findRegionById(final byte[] id, final boolean forceRefresh, byte type);
    DTGRegion findRegionById(final long id, final boolean forceRefresh, byte type);
    /**
     * Returns the regions to which the ids belongs.
     */
    Map<DTGRegion, List<Long>> findRegionsByIds(final long[] ids, final boolean forceRefresh, byte type);

    /**
     * Returns the regions to which the entities belongs.
     */
    Map[] findRegionsByEntityEntries(final List<EntityEntry> entityEntries, final boolean forceRefresh, byte type);

    Map[] findRegionsByEntityEntries(final List<EntityEntry> entityEntries, final boolean forceRefresh, byte type, Map[] map);

    /**
     * Returns the list of regions covered by startId and endId.
     */
    //List<DTGRegion> findRegionsByIdRange(final byte[] startId, final byte[] endId, final boolean forceRefresh, byte type);
    List<DTGRegion> findRegionsByIdRange(final long startId, final long endId, final boolean forceRefresh, byte type);

    /**
     * Returns the startId of next region.
     */
//    byte[] findStartIdOfNextRegion(final byte[] Id, final boolean forceRefresh, byte type);
    long findStartIdOfNextRegion(final long Id, final boolean forceRefresh, byte type);

    /**
     * Returns the regionRouteTable instance.
     */
    DTGRegionRouteTable getRegionRouteTable(byte type);

    /**
     * Returns the store metadata of the current instance's store.
     * Construct initial data based on the configuration file if
     * the data on {@link PlacementDriverClient} is empty.
     */
    DTGStore getStoreMetadata(final DTGStoreEngineOptions opts);

    long getStoreId(final DTGStoreEngineOptions opts);

    /**
     * Get the specified region leader communication address.
     */
    Endpoint getLeader(final long regionId, final boolean forceRefresh, final long timeoutMillis);

    /**
     * Get the specified region random peer communication address,
     * format: [ip:port]
     */
    Endpoint getLuckyPeer(final long regionId, final boolean forceRefresh, final long timeoutMillis,
                          final Endpoint unExpect);

    /**
     * Refresh the routing information of the specified region
     */
    void refreshRouteConfiguration(final long regionId);

    /**
     * Transfer leader to specified peer.
     */
    boolean transferLeader(final long regionId, final Peer peer, final boolean refreshConf);

    /**
     * Join the specified region group.
     */
    boolean addReplica(final long regionId, final Peer peer, final boolean refreshConf);

    /**
     * Depart from the specified region group.
     */
    boolean removeReplica(final long regionId, final Peer peer, final boolean refreshConf);

    /**
     * Returns raft cluster prefix id.
     */
    String getClusterName();

    /**
     * Get the placement driver server's leader communication address.
     */
    Endpoint getPdLeader(final boolean forceRefresh, final long timeoutMillis);

    /**
     * Returns the pd rpc service client.
     */
    PlacementDriverRpcService getPdRpcService();

    void refreshRouteTable(boolean needLock);

    boolean isRemotePd();

    void returnIds(byte type);

    void removeId(byte type, long id);

    void getIds(byte type);

    void initIds();

    long getId(byte type);

    long getVersion();

    DTGMetadataRpcClient getDTGMetadataRpcClient();

}

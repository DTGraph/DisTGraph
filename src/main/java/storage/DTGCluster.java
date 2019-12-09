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
package storage;

import com.alipay.sofa.jraft.rhea.metadata.Store;
import com.alipay.sofa.jraft.rhea.util.Lists;
import com.alipay.sofa.jraft.util.Copiable;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 *
 * @author jiachun.fjc
 */
public class DTGCluster implements Copiable<DTGCluster>, Serializable {

    private static final long serialVersionUID = 3291666486933960310L;

    private long              clusterId;
    private List<DTGStore>    stores;

    private long              nodeIdTopRestrict;
    private long              relationIdTopRestrict;

    public DTGCluster(long clusterId, List<DTGStore> stores) {
        this.clusterId = clusterId;
        this.stores = stores;
        this.nodeIdTopRestrict = 0;
        this.relationIdTopRestrict = 0;
    }

    public DTGCluster(long clusterId, List<DTGStore> stores, long nodeIdTopRestrict, long relationIdTopRestrict) {
        this.clusterId = clusterId;
        this.stores = stores;
        this.nodeIdTopRestrict = nodeIdTopRestrict;
        this.relationIdTopRestrict = relationIdTopRestrict;
    }

    public long getClusterId() {
        return clusterId;
    }

    public void setClusterId(long clusterId) {
        this.clusterId = clusterId;
    }

    public List<DTGStore> getStores() {
        return stores;
    }

    public void setStores(List<DTGStore> stores) {
        this.stores = stores;
    }

    public long getNodeIdTopRestrict() {
        return nodeIdTopRestrict;
    }

    public void setNodeIdTopRestrict(long nodeIdTopRestrict) {
        this.nodeIdTopRestrict = nodeIdTopRestrict;
    }

    public void setRelationIdTopRestrict(long relationIdTopRestrict) {
        this.relationIdTopRestrict = relationIdTopRestrict;
    }

    public long getRelationIdTopRestrict() {
        return relationIdTopRestrict;
    }

    @Override
    public DTGCluster copy() {
        List<DTGStore> stores = null;
        if (this.stores != null) {
            stores = Lists.newArrayListWithCapacity(this.stores.size());
            for (DTGStore store : this.stores) {
                stores.add(store.copy());
            }
        }
        return new DTGCluster(this.clusterId, stores, this.nodeIdTopRestrict, this.relationIdTopRestrict);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        DTGCluster cluster = (DTGCluster) o;
        return clusterId == cluster.clusterId && Objects.equals(stores, cluster.stores);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterId, stores);
    }

    @Override
    public String toString() {
        return "Cluster{" + "clusterId=" + clusterId + ", stores=" + stores + '}';
    }
}

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
package options;

import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rhea.options.RegionRouteTableOptions;
import com.alipay.sofa.jraft.rhea.options.RpcOptions;

import java.util.List;

/**
 * @author :jinkai
 * @date :Created in 2019/10/14 16:43
 * @description:
 * @modified By:
 * @version:1.0
 */

public class DTGPlacementDriverOptions {

    private CliOptions                    cliOptions;
    private RpcOptions                    pdRpcOptions;
    private List<RegionRouteTableOptions> regionRouteTableOptionsList;
    private String                        initialServerList;
    private String                        pdGroupId;
    private String                        initialPdServerList;
    private boolean                       isRemotePd;
    private int                           minIdBatchSize;//if the number of id client own smaller than this, it will request id from pd.
    private boolean                       isLocalClient;

    public CliOptions getCliOptions() {
        return cliOptions;
    }

    public void setCliOptions(CliOptions cliOptions) {
        this.cliOptions = cliOptions;
    }

    public RpcOptions getPdRpcOptions() {
        return pdRpcOptions;
    }

    public void setPdRpcOptions(RpcOptions pdRpcOptions) {
        this.pdRpcOptions = pdRpcOptions;
    }

    public List<RegionRouteTableOptions> getRegionRouteTableOptionsList() {
        return regionRouteTableOptionsList;
    }

    public void setRegionRouteTableOptionsList(List<RegionRouteTableOptions> regionRouteTableOptionsList) {
        this.regionRouteTableOptionsList = regionRouteTableOptionsList;
    }

    public String getInitialServerList() {
        return initialServerList;
    }

    public void setInitialServerList(String initialServerList) {
        this.initialServerList = initialServerList;
    }

    public String getPdGroupId() {
        return pdGroupId;
    }

    public void setPdGroupId(String pdGroupId) {
        this.pdGroupId = pdGroupId;
    }

    public String getInitialPdServerList() {
        return initialPdServerList;
    }

    public void setInitialPdServerList(String initialPdServerList) {
        this.initialPdServerList = initialPdServerList;
    }

    public void setRemotePd(boolean remotePd) {
        isRemotePd = remotePd;
    }

    public boolean isRemotePd() {
        return isRemotePd;
    }

    public int getMinIdBatchSize() {
        return minIdBatchSize;
    }

    public void setMinIdBatchSize(int minIdBatchSize) {
        this.minIdBatchSize = minIdBatchSize;
    }

    public boolean isLocalClient() {
        return isLocalClient;
    }

    public void setLocalClient(boolean localClient) {
        isLocalClient = localClient;
    }

    @Override
    public String toString() {
        return "PlacementDriverOptions{" +  "cliOptions=" + cliOptions + ", pdRpcOptions="
                + pdRpcOptions + ", pdGroupId='" + pdGroupId + '\'' + ", regionRouteTableOptionsList="
                + regionRouteTableOptionsList + ", initialServerList='" + initialServerList + '\''
                + ", initialPdServerList='" + initialPdServerList + '\'' + '}';
    }
}

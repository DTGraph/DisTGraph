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



import com.alipay.sofa.jraft.rhea.pd.PlacementDriverServer;
import com.alipay.sofa.jraft.rhea.pd.errors.PlacementDriverServerStartupException;
import com.alipay.sofa.jraft.rhea.pd.options.PlacementDriverServerOptions;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import config.DefaultOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * @author jiachun.fjc
 */
public class PlacementDriverStartup {

    private static final Logger LOG = LoggerFactory.getLogger(PlacementDriverStartup.class);

    public static void main(String[] args) throws Exception {
//        if (args.length != 1) {
//            LOG.error("Usage: com.alipay.sofa.jraft.rhea.PlacementDriverStartup <ConfigFilePath>");
//            System.exit(1);
//        }
        //final String configPath = args[0];
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        final PlacementDriverServerOptions opts = DefaultOptions.defaultPlacementDriverServerOptions("127.0.0.1", 10086, "D:\\garbage");
        final PlacementDriverServer pdServer = new PlacementDriverServer();
        if (!pdServer.init(opts)) {
            throw new PlacementDriverServerStartupException("Fail to start [PlacementDriverServer].");
        }
        LOG.info("Starting PlacementDriverServer with config: {}.", opts);
    }
}

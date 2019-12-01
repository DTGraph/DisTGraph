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
package Communication.pd.pipeline.handler;


import Communication.pd.pipeline.event.DTGPingEvent;
import Communication.pd.pipeline.event.PongEvent;
import Communication.util.pipeline.DTGHandler;
import Communication.util.pipeline.DTGHandlerContext;
import Communication.util.pipeline.DTGInboundHandlerAdapter;
import Communication.util.pipeline.event.DTGInboundMessageEvent;
import Region.DTGRegion;
import com.alipay.sofa.jraft.rhea.util.Lists;


/**
 * @author jiachun.fjc
 */
@DTGHandler.Sharable
public class DTGPlacementDriverTailHandler extends DTGInboundHandlerAdapter<DTGPingEvent<?>> {

    @Override
    public void handleInbound(final DTGHandlerContext ctx, final DTGInboundMessageEvent<?> event) throws Exception {
        if (isAcceptable(event)) {
            // to outbound
            DTGPingEvent<?> ping = (DTGPingEvent<?>) event;
            ctx.fireOutbound(new PongEvent(ping.getInvokeId(), Lists.newArrayList(ping.getInstructions())));
        }
    }

    @Override
    public void readMessage(final DTGHandlerContext ctx, final DTGPingEvent<?> event) throws Exception {
        // no-op
    }
}

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
package Communication.util.pipeline;

import Communication.util.pipeline.event.DTGInboundMessageEvent;
import Communication.util.pipeline.event.DTGMessageEvent;

/**
 * Most of the code references the pipeline design of
 * <a href="https://github.com/netty/netty">Netty</a>.
 *
 * @author jiachun.fjc
 */
public abstract class DTGInboundHandlerAdapter<I> extends DTGHandlerAdapter implements DTGInboundHandler {

    private final DTGTypeParameterMatcher matcher;

    protected DTGInboundHandlerAdapter() {
        this.matcher = DTGTypeParameterMatcher.find(this, DTGInboundHandlerAdapter.class, "I");
    }

    @Override
    public boolean isAcceptable(final DTGMessageEvent<?> event) {
        return matcher.match(event);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void handleInbound(final DTGHandlerContext ctx, final DTGInboundMessageEvent<?> event) throws Exception {
        if (isAcceptable(event)) {
            readMessage(ctx, (I) event);
        }
        ctx.fireInbound(event);
    }

    /**
     * If you expect that the event to continue to flow in the pipeline,
     * you should to call {@code ctx.fireInbound(event)} or {@code ctx.fireOutbound(event)}
     * at the end of the method.
     */
    public abstract void readMessage(final DTGHandlerContext ctx, final I event) throws Exception;
}

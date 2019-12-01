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

import Communication.util.pipeline.event.DTGMessageEvent;
import Communication.util.pipeline.event.DTGOutboundMessageEvent;

/**
 * Most of the code references the pipeline design of
 * <a href="https://github.com/netty/netty">Netty</a>.
 *
 * @author jiachun.fjc
 */
public abstract class DTGOutboundHandlerAdapter<I> extends DTGHandlerAdapter implements DTGOutboundHandler {

    private final DTGTypeParameterMatcher matcher;

    protected DTGOutboundHandlerAdapter() {
        this.matcher = DTGTypeParameterMatcher.find(this, DTGOutboundHandlerAdapter.class, "I");
    }

    @Override
    public boolean isAcceptable(final DTGMessageEvent<?> event) {
        return matcher.match(event);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void handleOutbound(final DTGHandlerContext ctx, final DTGOutboundMessageEvent<?> event) throws Exception {
        if (isAcceptable(event)) {
            writeMessage(ctx, (I) event);
        }
        ctx.fireOutbound(event);
    }

    /**
     * If you expect that the event to continue to flow in the pipeline,
     * you should to call {@code ctx.fireOutbound(event)} at the end of the method.
     */
    public abstract void writeMessage(final DTGHandlerContext ctx, final I event) throws Exception;
}

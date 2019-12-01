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
import Communication.util.pipeline.event.DTGOutboundMessageEvent;

/**
 * Most of the code references the pipeline design of
 * <a href="https://github.com/netty/netty">Netty</a>.
 *
 * @author jiachun.fjc
 */
abstract class DTGAbstractHandlerContext implements DTGHandlerContext {

    volatile DTGAbstractHandlerContext next;
    volatile DTGAbstractHandlerContext prev;

    private final boolean           inbound;
    private final boolean           outbound;
    private final DTGDefaultPipeline pipeline;
    private final String            name;
    private boolean                 removed;

    final DTGHandlerInvoker invoker;

    public DTGAbstractHandlerContext(DTGDefaultPipeline pipeline, DTGHandlerInvoker invoker, String name, boolean inbound,
                                     boolean outbound) {

        if (name == null) {
            throw new NullPointerException("name");
        }

        this.pipeline = pipeline;
        if (invoker == null) {
            this.invoker = new DTGDefaultHandlerInvoker();
        } else {
            this.invoker = invoker;
        }
        this.name = name;

        this.inbound = inbound;
        this.outbound = outbound;
    }

    @Override
    public DTGPipeline pipeline() {
        return pipeline;
    }

    @Override
    public DTGHandlerInvoker invoker() {
        return invoker;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean isRemoved() {
        return removed;
    }

    void setRemoved() {
        removed = true;
    }

    @Override
    public DTGHandlerContext fireInbound(final DTGInboundMessageEvent<?> event) {
        final DTGAbstractHandlerContext next = findContextInbound();
        next.invoker().invokeInbound(next, event);
        return this;
    }

    @Override
    public DTGHandlerContext fireOutbound(final DTGOutboundMessageEvent<?> event) {
        final DTGAbstractHandlerContext next = findContextOutbound();
        next.invoker().invokeOutbound(next, event);
        return this;
    }

    @Override
    public DTGHandlerContext fireExceptionCaught(final DTGMessageEvent<?> event, final Throwable cause) {
        final DTGAbstractHandlerContext next = this.next;
        next.invoker().invokeExceptionCaught(next, event, cause);
        return this;
    }

    private DTGAbstractHandlerContext findContextInbound() {
        DTGAbstractHandlerContext ctx = this;
        do {
            ctx = ctx.next;
        } while (!ctx.inbound);
        return ctx;
    }

    private DTGAbstractHandlerContext findContextOutbound() {
        DTGAbstractHandlerContext ctx = this;
        do {
            ctx = ctx.prev;
        } while (!ctx.outbound);
        return ctx;
    }
}

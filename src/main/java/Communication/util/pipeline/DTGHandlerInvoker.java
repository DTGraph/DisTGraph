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

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

/**
 * Most of the code references the pipeline design of
 * <a href="https://github.com/netty/netty">Netty</a>.
 *
 * @author jiachun.fjc
 */
public interface DTGHandlerInvoker {

    /**
     * Returns the {@link Executor} which is used to execute an arbitrary task.
     */
    ExecutorService executor();

    /**
     * Invokes {@link DTGInboundHandler#handleInbound(DTGHandlerContext, DTGInboundMessageEvent)}. This method is not for a user
     * but for the internal {@link DTGHandlerContext} implementation. To trigger an event, use the methods in
     * {@link DTGHandlerContext} instead.
     */
    void invokeInbound(final DTGHandlerContext ctx, final DTGInboundMessageEvent<?> event);

    /**
     * Invokes {@link DTGOutboundHandler#handleOutbound(DTGHandlerContext, DTGOutboundMessageEvent)}. This method is not for a user
     * but for the internal {@link DTGHandlerContext} implementation. To trigger an event, use the methods in
     * {@link DTGHandlerContext} instead.
     */
    void invokeOutbound(final DTGHandlerContext ctx, final DTGOutboundMessageEvent<?> event);

    /**
     * Received an {@link Throwable} in one of its inbound operations.
     *
     * This will result in having the {@link DTGInboundHandler#exceptionCaught(DTGHandlerContext, DTGMessageEvent, Throwable)}
     * method called of the next {@link DTGInboundHandler} contained in the {@link DTGPipeline}.
     */
    void invokeExceptionCaught(final DTGHandlerContext ctx, final DTGMessageEvent<?> event, final Throwable cause);

    /**
     * Attempts to stop all actively executing tasks.
     */
    void shutdown();
}

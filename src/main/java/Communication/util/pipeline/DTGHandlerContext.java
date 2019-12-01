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
 * Enables a {@link DTGHandler} to interact with its {@link DTGPipeline}
 * and other handlers. Among other things a handler can notify the next {@link DTGHandler} in the
 * {@link DTGPipeline} as well as modify the {@link DTGPipeline} it belongs to dynamically.
 *
 * Most of the code references the pipeline design of
 * <a href="https://github.com/netty/netty">Netty</a>.
 *
 * @author jiachun.fjc
 */
public interface DTGHandlerContext {

    /**
     * Return the assigned {@link DTGPipeline}.
     */
    DTGPipeline pipeline();

    /**
     * Returns the {@link DTGHandlerInvoker} which is used to trigger an event for the associated
     * {@link DTGHandler}. Note that the methods in {@link DTGHandlerInvoker} are not intended to be called
     * by a user. Use this method only to obtain the reference to the {@link DTGHandlerInvoker}
     * (and not calling its methods) unless you know what you are doing.
     */
    DTGHandlerInvoker invoker();

    /**
     * The unique name of the {@link DTGHandlerContext}.The name was used when then {@link DTGHandler}
     * was added to the {@link DTGPipeline}. This name can also be used to access the registered
     * {@link DTGHandler} from the {@link DTGPipeline}.
     */
    String name();

    /**
     * The {@link DTGHandler} that is bound this {@link DTGHandlerContext}.
     */
    DTGHandler handler();

    /**
     * Return {@code true} if the {@link DTGHandler} which belongs to this context was removed
     * from the {@link DTGPipeline}.
     */
    boolean isRemoved();

    /**
     * Received a message.
     */
    DTGHandlerContext fireInbound(final DTGInboundMessageEvent<?> event);

    /**
     * Request to write a message via this {@link DTGHandlerContext} through the {@link DTGPipeline}.
     */
    DTGHandlerContext fireOutbound(final DTGOutboundMessageEvent<?> event);

    /**
     * Received an {@link Throwable} in one of its inbound operations.
     */
    DTGHandlerContext fireExceptionCaught(final DTGMessageEvent<?> event, final Throwable cause);
}

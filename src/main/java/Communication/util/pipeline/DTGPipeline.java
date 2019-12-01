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
import Communication.util.pipeline.future.DTGPipelineFuture;

/**
 * A list of {@link DTGHandler}s which handles or intercepts
 * inbound events and outbound operations.
 *
 * Most of the code references the pipeline design of
 * <a href="https://github.com/netty/netty">Netty</a>.
 *
 * @author jiachun.fjc
 */
public interface DTGPipeline {

    /**
     * Inserts a {@link DTGHandler} at the first position of this pipeline.
     *
     * @param name      the name of the handler to insert first.
     * @param handler   the handler to insert first.
     *
     * @return itself
     */
    DTGPipeline addFirst(final String name, final DTGHandler handler);

    /**
     * Inserts a {@link DTGHandler} at the first position of this pipeline.
     *
     * @param invoker   the {@link DTGHandlerInvoker} which will be used to execute the {@link DTGHandler}'s methods.
     * @param name      the name of the handler to insert first.
     * @param handler   the handler to insert first.
     *
     * @return itself
     */
    DTGPipeline addFirst(final DTGHandlerInvoker invoker, final String name, final DTGHandler handler);

    /**
     * Appends a {@link DTGHandler} at the last position of this pipeline.
     *
     * @param name      the name of the handler to append.
     * @param handler   the handler to append.
     *
     * @return itself
     */
    DTGPipeline addLast(final String name, final DTGHandler handler);

    /**
     * Appends a {@link DTGHandler} at the last position of this pipeline.
     *
     * @param invoker   the {@link DTGHandlerInvoker} which will be used to execute the {@link DTGHandler}'s methods.
     * @param name      the name of the handler to append.
     * @param handler   the handler to append.
     *
     * @return itself
     */
    DTGPipeline addLast(final DTGHandlerInvoker invoker, final String name, final DTGHandler handler);

    /**
     * Inserts a {@link DTGHandler} before an existing handler of this pipeline.
     *
     * @param baseName  the name of the existing handler.
     * @param name      the name of the handler to insert before.
     * @param handler   the handler to insert before.
     *
     * @return itself
     */
    DTGPipeline addBefore(final String baseName, final String name, final DTGHandler handler);

    /**
     * Inserts a {@link DTGHandler} before an existing handler of this pipeline.
     *
     * @param invoker   the {@link DTGHandlerInvoker} which will be used to execute the {@link DTGHandler}'s methods.
     * @param baseName  the name of the existing handler.
     * @param name      the name of the handler to insert before.
     * @param handler   the handler to insert before.
     *
     * @return itself
     */
    DTGPipeline addBefore(final DTGHandlerInvoker invoker, final String baseName, final String name, final DTGHandler handler);

    /**
     * Inserts a {@link DTGHandler} after an existing handler of this pipeline.
     *
     * @param baseName  the name of the existing handler.
     * @param name      the name of the handler to insert after.
     * @param handler   the handler to insert after.
     *
     * @return itself
     */
    DTGPipeline addAfter(final String baseName, final String name, final DTGHandler handler);

    /**
     * Inserts a {@link DTGHandler} after an existing handler of this pipeline.
     *
     * @param invoker   the {@link DTGHandlerInvoker} which will be used to execute the {@link DTGHandler}'s methods.
     * @param baseName  the name of the existing handler.
     * @param name      the name of the handler to insert after.
     * @param handler   the handler to insert after.
     *
     * @return itself
     */
    DTGPipeline addAfter(final DTGHandlerInvoker invoker, final String baseName, final String name, final DTGHandler handler);

    /**
     * Inserts {@link DTGHandler}s at the first position of this pipeline.
     *
     * @param handlers  the handlers to insert first.
     *
     * @return itself
     */
    DTGPipeline addFirst(final DTGHandler... handlers);

    /**
     * Inserts {@link DTGHandler}s at the first position of this pipeline.
     *
     * @param invoker   the {@link DTGHandlerInvoker} which will be used to execute the {@link DTGHandler}s's methods.
     * @param handlers  the handlers to insert first.
     */
    DTGPipeline addFirst(final DTGHandlerInvoker invoker, final DTGHandler... handlers);

    /**
     * Inserts {@link DTGHandler}s at the last position of this pipeline.
     *
     * @param handlers  the handlers to insert last.
     *
     * @return itself
     */
    DTGPipeline addLast(final DTGHandler... handlers);

    /**
     * Inserts {@link DTGHandler}s at the last position of this pipeline.
     *
     * @param invoker   the {@link DTGHandlerInvoker} which will be used to execute the {@link DTGHandler}s's methods.
     * @param handlers  the handlers to insert last.
     *
     * @return itself
     */
    DTGPipeline addLast(final DTGHandlerInvoker invoker, final DTGHandler... handlers);

    /**
     * Removes the specified {@link DTGHandler} from this pipeline.
     *
     * @param  handler  the {@link DTGHandler} to remove
     *
     * @return the removed handler
     */
    DTGPipeline remove(final DTGHandler handler);

    /**
     * Removes the {@link DTGHandler} with the specified name from this pipeline.
     *
     * @param name  the name under which the {@link DTGHandler} was stored.
     *
     * @return the removed handler
     */
    DTGHandler remove(final String name);

    /**
     * Removes the {@link DTGHandler} of the specified type from this pipeline.
     *
     * @param handlerType   the type of the handler.
     * @param <T>           the type of the handler.
     *
     * @return the removed handler
     */
    <T extends DTGHandler> T remove(final Class<T> handlerType);

    /**
     * Removes the first {@link DTGHandler} in this pipeline.
     *
     * @return the removed handler
     */
    DTGHandler removeFirst();

    /**
     * Removes the last {@link DTGHandler} in this pipeline.
     *
     * @return the removed handler
     */
    DTGHandler removeLast();

    /**
     * Replaces the specified {@link DTGHandler} with a new handler in this pipeline.
     *
     * @param  oldHandler    the {@link DTGHandler} to be replaced
     * @param  newName       the name under which the replacement should be added.
     *                       {@code null} to use the same name with the handler being replaced.
     * @param  newHandler    the {@link DTGHandler} which is used as replacement
     *
     * @return itself
     */
    DTGPipeline replace(final DTGHandler oldHandler, final String newName, final DTGHandler newHandler);

    /**
     * Replaces the {@link DTGHandler} of the specified name with a new handler in this pipeline.
     *
     * @param oldName       the name of the {@link DTGHandler} to be replaced.
     * @param newName       the name under which the replacement should be added.
     *                      {@code null} to use the same name with the handler being replaced.
     * @param newHandler    the {@link DTGHandler} which is used as replacement.
     *
     * @return the removed handler
     */
    DTGHandler replace(final String oldName, final String newName, final DTGHandler newHandler);

    /**
     * Replaces the {@link DTGHandler} of the specified type with a new handler in this pipeline.
     *
     * @param  oldHandlerType   the type of the handler to be removed
     * @param  newName          the name under which the replacement should be added.
     *                          {@code null} to use the same name with the handler being replaced.
     * @param  newHandler       the {@link DTGHandler} which is used as replacement
     *
     * @return the removed handler
     */
    <T extends DTGHandler> T replace(final Class<T> oldHandlerType, final String newName, final DTGHandler newHandler);

    /**
     * Returns the {@link DTGHandler} with the specified name in this
     * pipeline.
     *
     * @return the handler with the specified name.
     *         {@code null} if there's no such handler in this pipeline.
     */
    DTGHandler get(final String name);

    /**
     * Returns the {@link DTGHandler} of the specified type in this
     * pipeline.
     *
     * @return the handler of the specified handler type.
     *         {@code null} if there's no such handler in this pipeline.
     */
    <T extends DTGHandler> T get(final Class<T> handlerType);

    /**
     * Returns the context object of the specified {@link DTGHandler} in
     * this pipeline.
     *
     * @return the context object of the specified handler.
     *         {@code null} if there's no such handler in this pipeline.
     */
    DTGHandlerContext context(final DTGHandler handler);

    /**
     * Returns the context object of the {@link DTGHandler} with the
     * specified name in this pipeline.
     *
     * @return the context object of the handler with the specified name.
     *         {@code null} if there's no such handler in this pipeline.
     */
    DTGHandlerContext context(final String name);

    /**
     * Returns the context object of the {@link DTGHandler} of the
     * specified type in this pipeline.
     *
     * @return the context object of the handler of the specified type.
     *         {@code null} if there's no such handler in this pipeline.
     */
    DTGHandlerContext context(final Class<? extends DTGHandler> handlerType);

    /**
     * Sends the specified {@link DTGMessageEvent} to the first {@link DTGInboundHandler} in this pipeline.
     */
    DTGPipeline fireInbound(final DTGInboundMessageEvent<?> event);

    /**
     * Sends the specified {@link DTGMessageEvent} to the first {@link DTGOutboundHandler} in this pipeline.
     */
    DTGPipeline fireOutbound(final DTGOutboundMessageEvent<?> event);

    /**
     * Received an {@link Throwable} in one of its inbound operations.
     *
     * This will result in having the  {@link DTGInboundHandler#exceptionCaught(DTGHandlerContext, DTGMessageEvent, Throwable)}
     * method  called of the next  {@link DTGInboundHandler} contained in the  {@link DTGPipeline}.
     */
    DTGPipeline fireExceptionCaught(final DTGMessageEvent<?> event, final Throwable cause);

    /**
     * Invoke in the pipeline.
     *
     * @param event an inbound message event
     * @param <R>   expected return type
     * @param <M>   message type
     */
    <R, M> DTGPipelineFuture<R> invoke(final DTGInboundMessageEvent<M> event);

    /**
     *
     * @param event an inbound message event
     * @param timeoutMillis timeout
     * @param <R>   expected return type
     * @param <M>   message type
     */
    <R, M> DTGPipelineFuture<R> invoke(final DTGInboundMessageEvent<M> event, final long timeoutMillis);
}

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

import java.lang.annotation.*;

/**
 * Most of the code references the pipeline design of
 * <a href="https://github.com/netty/netty">Netty</a>.
 *
 * @author jiachun.fjc
 */
public interface DTGHandler {

    boolean isAcceptable(final DTGMessageEvent<?> event);

    /**
     * Gets called after the {@link DTGHandler} was added to the actual context and it's ready to handle events.
     */
    void handlerAdded(final DTGHandlerContext ctx) throws Exception;

    /**
     * Gets called after the {@link DTGHandler} was removed from the actual context and it doesn't handle events anymore.
     */
    void handlerRemoved(final DTGHandlerContext ctx) throws Exception;

    /**
     * Gets called if a {@link Throwable} was thrown.
     */
    void exceptionCaught(final DTGHandlerContext ctx, final DTGMessageEvent<?> event, final Throwable cause) throws Exception;

    /**
     * Indicates that the same instance of the annotated {@link DTGHandler}
     * can be added to one or more {@link DTGPipeline}s multiple times
     * without a race condition.
     *
     * If this annotation is not specified, you have to create a new handler
     * instance every time you add it to a pipeline because it has unshared
     * state such as member variables.
     */
    @Inherited
    @Documented
    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    @interface Sharable {
        // no value
    }
}

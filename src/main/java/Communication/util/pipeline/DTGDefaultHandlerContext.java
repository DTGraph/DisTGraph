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

/**
 *
 * @author jiachun.fjc
 */
class DTGDefaultHandlerContext extends DTGAbstractHandlerContext {

    private final DTGHandler handler;

    public DTGDefaultHandlerContext(DTGDefaultPipeline pipeline, DTGHandlerInvoker invoker, String name, DTGHandler handler) {
        super(pipeline, invoker, name, isInbound(handler), isOutbound(handler));

        this.handler = handler;
    }

    @Override
    public DTGHandler handler() {
        return handler;
    }

    private static boolean isInbound(final DTGHandler handler) {
        return handler instanceof DTGInboundHandler;
    }

    private static boolean isOutbound(final DTGHandler handler) {
        return handler instanceof DTGOutboundHandler;
    }
}

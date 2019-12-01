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
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.util.ExecutorServiceHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

/**
 *
 * @author jiachun.fjc
 */
public class DTGDefaultHandlerInvoker implements DTGHandlerInvoker {

    private static final Logger   LOG = LoggerFactory.getLogger(DTGDefaultHandlerInvoker.class);

    private final ExecutorService executor;

    public DTGDefaultHandlerInvoker() {
        this(null);
    }

    public DTGDefaultHandlerInvoker(ExecutorService executor) {
        this.executor = executor;
    }

    @Override
    public ExecutorService executor() {
        return executor;
    }

    @Override
    public void invokeInbound(final DTGHandlerContext ctx, final DTGInboundMessageEvent<?> event) {
        if (this.executor == null) {
            DTGHandlerInvokerUtil.invokeInboundNow(ctx, event);
        } else {
            this.executor.execute(() -> DTGHandlerInvokerUtil.invokeInboundNow(ctx, event));
        }
    }

    @Override
    public void invokeOutbound(final DTGHandlerContext ctx, final DTGOutboundMessageEvent<?> event) {
        if (this.executor == null) {
            DTGHandlerInvokerUtil.invokeOutboundNow(ctx, event);
        } else {
            this.executor.execute(() -> DTGHandlerInvokerUtil.invokeOutboundNow(ctx, event));
        }
    }

    @Override
    public void invokeExceptionCaught(final DTGHandlerContext ctx, final DTGMessageEvent<?> event, final Throwable cause) {
        if (cause == null) {
            throw new NullPointerException("cause");
        }

        if (this.executor == null) {
            DTGHandlerInvokerUtil.invokeExceptionCaughtNow(ctx, event, cause);
        } else {
            try {
                this.executor.execute(() -> DTGHandlerInvokerUtil.invokeExceptionCaughtNow(ctx, event, cause));
            } catch (final Throwable t) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("Failed to submit an exceptionCaught() event: {}.", StackTraceUtil.stackTrace(t));
                    LOG.warn("The exceptionCaught() event that was failed to submit was: {}.",
                        StackTraceUtil.stackTrace(cause));
                }
            }
        }
    }

    @Override
    public void shutdown() {
        ExecutorServiceHelper.shutdownAndAwaitTermination(this.executor);
    }
}

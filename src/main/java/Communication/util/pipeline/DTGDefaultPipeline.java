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
import Communication.util.pipeline.future.DTGDefaultPipelineFuture;
import Communication.util.pipeline.future.DTGPipelineFuture;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.rhea.util.pipeline.PipelineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.WeakHashMap;

/**
 * Most of the code references the pipeline design of
 * <a href="https://github.com/netty/netty">Netty</a>.
 *
 * @author jiachun.fjc
 */
public final class DTGDefaultPipeline implements DTGPipeline {

    private static final Logger                             LOG        = LoggerFactory.getLogger(DTGDefaultPipeline.class);

    private static final ThreadLocal<Map<Class<?>, String>> nameCaches = ThreadLocal.withInitial(WeakHashMap::new);

    final DTGAbstractHandlerContext head;
    final DTGAbstractHandlerContext tail;

    public DTGDefaultPipeline() {
        tail = new TailContext(this);
        head = new HeadContext(this);

        head.next = tail;
        tail.prev = head;
    }

    @Override
    public DTGPipeline addFirst(String name, DTGHandler handler) {
        return addFirst(null, name, handler);
    }

    @Override
    public DTGPipeline addFirst(DTGHandlerInvoker invoker, String name, DTGHandler handler) {
        name = filterName(name, handler);
        addFirst0(new DTGDefaultHandlerContext(this, invoker, name, handler));
        return this;
    }

    private void addFirst0(DTGAbstractHandlerContext newCtx) {
        checkMultiplicity(newCtx);

        DTGAbstractHandlerContext nextCtx = head.next;
        newCtx.prev = head;
        newCtx.next = nextCtx;
        head.next = newCtx;
        nextCtx.prev = newCtx;

        callHandlerAdded(newCtx);
    }

    @Override
    public DTGPipeline addLast(String name, DTGHandler handler) {
        return addLast(null, name, handler);
    }

    @Override
    public DTGPipeline addLast(DTGHandlerInvoker invoker, String name, DTGHandler handler) {
        name = filterName(name, handler);
        addLast0(new DTGDefaultHandlerContext(this, invoker, name, handler));
        return this;
    }

    private void addLast0(DTGAbstractHandlerContext newCtx) {
        checkMultiplicity(newCtx);

        DTGAbstractHandlerContext prev = tail.prev;
        newCtx.prev = prev;
        newCtx.next = tail;
        prev.next = newCtx;
        tail.prev = newCtx;

        callHandlerAdded(newCtx);
    }

    @Override
    public DTGPipeline addBefore(String baseName, String name, DTGHandler handler) {
        return addBefore(null, baseName, name, handler);
    }

    @Override
    public DTGPipeline addBefore(DTGHandlerInvoker invoker, String baseName, String name, DTGHandler handler) {
        DTGAbstractHandlerContext ctx = getContextOrDie(baseName);
        name = filterName(name, handler);
        addBefore0(ctx, new DTGDefaultHandlerContext(this, invoker, name, handler));
        return this;
    }

    private void addBefore0(DTGAbstractHandlerContext ctx, DTGAbstractHandlerContext newCtx) {
        checkMultiplicity(newCtx);

        newCtx.prev = ctx.prev;
        newCtx.next = ctx;
        ctx.prev.next = newCtx;
        ctx.prev = newCtx;

        callHandlerAdded(newCtx);
    }

    @Override
    public DTGPipeline addAfter(String baseName, String name, DTGHandler handler) {
        return addAfter(null, baseName, name, handler);
    }

    @Override
    public DTGPipeline addAfter(DTGHandlerInvoker invoker, String baseName, String name, DTGHandler handler) {
        DTGAbstractHandlerContext ctx = getContextOrDie(baseName);
        name = filterName(name, handler);
        addAfter0(ctx, new DTGDefaultHandlerContext(this, invoker, name, handler));
        return this;
    }

    private void addAfter0(DTGAbstractHandlerContext ctx, DTGAbstractHandlerContext newCtx) {
        checkMultiplicity(newCtx);

        newCtx.prev = ctx;
        newCtx.next = ctx.next;
        ctx.next.prev = newCtx;
        ctx.next = newCtx;

        callHandlerAdded(newCtx);
    }

    @Override
    public DTGPipeline addFirst(DTGHandler... handlers) {
        return addFirst(null, handlers);
    }

    @Override
    public DTGPipeline addFirst(DTGHandlerInvoker invoker, DTGHandler... handlers) {
        if (handlers == null) {
            throw new NullPointerException("handlers");
        }
        if (handlers.length == 0 || handlers[0] == null) {
            return this;
        }

        int size;
        for (size = 1; size < handlers.length; size ++) {
            if (handlers[size] == null) {
                break;
            }
        }

        for (int i = size - 1; i >= 0; i --) {
            DTGHandler h = handlers[i];
            addFirst(invoker, null, h);
        }

        return this;
    }

    @Override
    public DTGPipeline addLast(DTGHandler... handlers) {
        return addLast(null, handlers);
    }

    @Override
    public DTGPipeline addLast(DTGHandlerInvoker invoker, DTGHandler... handlers) {
        if (handlers == null) {
            throw new NullPointerException("handlers");
        }

        for (DTGHandler h: handlers) {
            if (h == null) {
                break;
            }
            addLast(invoker, null, h);
        }

        return this;
    }

    @Override
    public DTGPipeline remove(DTGHandler handler) {
        remove(getContextOrDie(handler));
        return this;
    }

    @Override
    public DTGHandler remove(String name) {
        return remove(getContextOrDie(name)).handler();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends DTGHandler> T remove(Class<T> handlerType) {
        return (T) remove(getContextOrDie(handlerType)).handler();
    }

    private DTGAbstractHandlerContext remove(final DTGAbstractHandlerContext ctx) {
        assert ctx != head && ctx != tail;

        synchronized (this) {
            remove0(ctx);
            return ctx;
        }
    }

    private void remove0(DTGAbstractHandlerContext ctx) {
        DTGAbstractHandlerContext prev = ctx.prev;
        DTGAbstractHandlerContext next = ctx.next;
        prev.next = next;
        next.prev = prev;
        callHandlerRemoved(ctx);
    }

    @Override
    public DTGHandler removeFirst() {
        if (head.next == tail) {
            throw new NoSuchElementException();
        }
        return remove(head.next).handler();
    }

    @Override
    public DTGHandler removeLast() {
        if (head.next == tail) {
            throw new NoSuchElementException();
        }
        return remove(tail.prev).handler();
    }

    @Override
    public DTGPipeline replace(DTGHandler oldHandler, String newName, DTGHandler newHandler) {
        replace(getContextOrDie(oldHandler), newName, newHandler);
        return this;
    }

    @Override
    public DTGHandler replace(String oldName, String newName, DTGHandler newHandler) {
        return replace(getContextOrDie(oldName), newName, newHandler);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends DTGHandler> T replace(Class<T> oldHandlerType, String newName, DTGHandler newHandler) {
        return (T) replace(getContextOrDie(oldHandlerType), newName, newHandler);
    }

    private DTGHandler replace(
            final DTGAbstractHandlerContext ctx, String newName, DTGHandler newHandler) {

        assert ctx != head && ctx != tail;

        synchronized (this) {
            if (newName == null) {
                newName = ctx.name();
            } else if (!ctx.name().equals(newName)) {
                newName = filterName(newName, newHandler);
            }

            final DTGAbstractHandlerContext newCtx =
                    new DTGDefaultHandlerContext(this, ctx.invoker, newName, newHandler);

            replace0(ctx, newCtx);
            return ctx.handler();
        }
    }

    private void replace0(DTGAbstractHandlerContext oldCtx, DTGAbstractHandlerContext newCtx) {
        checkMultiplicity(newCtx);

        DTGAbstractHandlerContext prev = oldCtx.prev;
        DTGAbstractHandlerContext next = oldCtx.next;
        newCtx.prev = prev;
        newCtx.next = next;

        // Finish the replacement of oldCtx with newCtx in the linked list.
        // Note that this doesn't mean events will be sent to the new handler immediately
        // because we are currently at the event handler thread and no more than one handler methods can be invoked
        // at the same time (we ensured that in replace().)
        prev.next = newCtx;
        next.prev = newCtx;

        // update the reference to the replacement so forward of buffered content will work correctly
        oldCtx.prev = newCtx;
        oldCtx.next = newCtx;

        // Invoke newHandler.handlerAdded() first (i.e. before oldHandler.handlerRemoved() is invoked)
        // because callHandlerRemoved() will trigger inboundBufferUpdated() or flush() on newHandler and those
        // event handlers must be called after handlerAdded().
        callHandlerAdded(newCtx);
        callHandlerRemoved(oldCtx);
    }

    @Override
    public DTGHandler get(String name) {
        DTGHandlerContext ctx = context(name);
        if (ctx == null) {
            return null;
        } else {
            return ctx.handler();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends DTGHandler> T get(Class<T> handlerType) {
        DTGHandlerContext ctx = context(handlerType);
        if (ctx == null) {
            return null;
        } else {
            return (T) ctx.handler();
        }
    }

    @Override
    public DTGHandlerContext context(DTGHandler handler) {
        if (handler == null) {
            throw new NullPointerException("handler");
        }

        DTGAbstractHandlerContext ctx = head.next;
        for (;;) {

            if (ctx == null) {
                return null;
            }

            if (ctx.handler() == handler) {
                return ctx;
            }

            ctx = ctx.next;
        }
    }

    @Override
    public DTGHandlerContext context(String name) {
        if (name == null) {
            throw new NullPointerException("name");
        }

        return context0(name);
    }

    @Override
    public DTGHandlerContext context(Class<? extends DTGHandler> handlerType) {
        if (handlerType == null) {
            throw new NullPointerException("handlerType");
        }

        DTGAbstractHandlerContext ctx = head.next;
        for (;;) {
            if (ctx == null) {
                return null;
            }
            if (handlerType.isAssignableFrom(ctx.handler().getClass())) {
                return ctx;
            }
            ctx = ctx.next;
        }
    }

    @Override
    public DTGPipeline fireInbound(DTGInboundMessageEvent<?> event) {
        head.fireInbound(event);
        return this;
    }

    @Override
    public DTGPipeline fireOutbound(DTGOutboundMessageEvent<?> event) {
        tail.fireOutbound(event);
        return this;
    }

    @Override
    public DTGPipeline fireExceptionCaught(DTGMessageEvent<?> event, Throwable cause) {
        head.fireExceptionCaught(event, cause);
        return this;
    }

    @Override
    public <R, M> DTGPipelineFuture<R> invoke(DTGInboundMessageEvent<M> event) {
        return invoke(event, -1);
    }

    @Override
    public <R, M> DTGPipelineFuture<R> invoke(DTGInboundMessageEvent<M> event, long timeoutMillis) {
        DTGPipelineFuture<R> future = DTGDefaultPipelineFuture.with(event.getInvokeId(), timeoutMillis);
        head.fireInbound(event);
        return future;
    }

    private void callHandlerAdded(final DTGAbstractHandlerContext ctx) {
        try {
            ctx.handler().handlerAdded(ctx);
        } catch (Throwable t) {
            boolean removed = false;
            try {
                remove(ctx);
                removed = true;
            } catch (Throwable t2) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("Failed to remove a handler: {}, {}.", ctx.name(), StackTraceUtil.stackTrace(t2));
                }
            }

        fireExceptionCaught(null, new PipelineException(
                ctx.handler().getClass().getName() +
                        ".handlerAdded() has thrown an exception; " + (removed ? "removed." :  "also failed to remove."), t));
        }
    }

    private void callHandlerRemoved(final DTGAbstractHandlerContext ctx) {
        // Notify the complete removal.
        try {
            ctx.handler().handlerRemoved(ctx);
            ctx.setRemoved();
        } catch (Throwable t) {
            fireExceptionCaught(null, new PipelineException(
                    ctx.handler().getClass().getName() + ".handlerRemoved() has thrown an exception.", t));
        }
    }

    private static void checkMultiplicity(DTGHandlerContext ctx) {
        DTGHandler handler = ctx.handler();
        if (handler instanceof DTGHandlerAdapter) {
            DTGHandlerAdapter h = (DTGHandlerAdapter) handler;
            if (!h.isSharable() && h.added) {
                throw new PipelineException(
                        h.getClass().getName() +
                                " is not a @Sharable handler, so can't be added or removed multiple times.");
            }
            h.added = true;
        }
    }

    private String filterName(String name, DTGHandler handler) {
        if (name == null) {
            return generateName(handler);
        }

        if (context0(name) == null) {
            return name;
        }

        throw new IllegalArgumentException("Duplicate handler name: " + name);
    }

    private DTGAbstractHandlerContext context0(String name) {
        DTGAbstractHandlerContext context = head.next;
        while (context != tail) {
            if (context.name().equals(name)) {
                return context;
            }
            context = context.next;
        }
        return null;
    }

    private DTGAbstractHandlerContext getContextOrDie(String name) {
        DTGAbstractHandlerContext ctx = (DTGAbstractHandlerContext) context(name);
        if (ctx == null) {
            throw new NoSuchElementException(name);
        } else {
            return ctx;
        }
    }

    private DTGAbstractHandlerContext getContextOrDie(DTGHandler handler) {
        DTGAbstractHandlerContext ctx = (DTGAbstractHandlerContext) context(handler);
        if (ctx == null) {
            throw new NoSuchElementException(handler.getClass().getName());
        } else {
            return ctx;
        }
    }

    private DTGAbstractHandlerContext getContextOrDie(Class<? extends DTGHandler> handlerType) {
        DTGAbstractHandlerContext ctx = (DTGAbstractHandlerContext) context(handlerType);
        if (ctx == null) {
            throw new NoSuchElementException(handlerType.getName());
        } else {
            return ctx;
        }
    }

    private String generateName(DTGHandler handler) {
        Map<Class<?>, String> cache = nameCaches.get();
        Class<?> handlerType = handler.getClass();
        String name = cache.get(handlerType);
        if (name == null) {
            name = generateName0(handlerType);
            cache.put(handlerType, name);
        }

        synchronized (this) {
            // It's not very likely for a user to put more than one handler of the same type, but make sure to avoid
            // any name conflicts.  Note that we don't cache the names generated here.
            if (context0(name) != null) {
                String baseName = name.substring(0, name.length() - 1); // Strip the trailing '0'.
                for (int i = 1;; i ++) {
                    String newName = baseName + i;
                    if (context0(newName) == null) {
                        name = newName;
                        break;
                    }
                }
            }
        }

        return name;
    }

    private static String generateName0(Class<?> handlerType) {
        if (handlerType == null) {
            throw new NullPointerException("handlerType");
        }

        String className = handlerType.getName();
        final int lastDotIdx = className.lastIndexOf('.');
        if (lastDotIdx > -1) {
            className = className.substring(lastDotIdx + 1);
        }
        return className + "#0";
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder()
                .append(getClass().getSimpleName())
                .append('{');
        DTGAbstractHandlerContext ctx = head.next;
        for (;;) {
            if (ctx == tail) {
                break;
            }

            buf.append('(')
                    .append(ctx.name())
                    .append(" = ")
                    .append(ctx.handler().getClass().getName())
                    .append(')');

            ctx = ctx.next;
            if (ctx == tail) {
                break;
            }

            buf.append(", ");
        }
        buf.append('}');
        return buf.toString();
    }

    // A special catch-all handler that handles both messages.
    static final class TailContext extends DTGAbstractHandlerContext implements DTGInboundHandler {

        private static final String TAIL_NAME = generateName0(TailContext.class);

        public TailContext(DTGDefaultPipeline pipeline) {
            super(pipeline, null, TAIL_NAME, true, false);
        }

        @Override
        public boolean isAcceptable(DTGMessageEvent<?> event) {
            return true;
        }

        @Override
        public void handleInbound(DTGHandlerContext ctx, DTGInboundMessageEvent<?> event) throws Exception {
            System.out.println("handle in bound!");
            if (event != null) {
                DTGDefaultPipelineFuture.received(event.getInvokeId(), event.getMessage());
            }
        }

        @Override
        public void exceptionCaught(DTGHandlerContext ctx, DTGMessageEvent<?> event, Throwable cause) throws Exception {
            LOG.warn("An exceptionCaught() event was fired, {}.", StackTraceUtil.stackTrace(cause));

            if (event != null) {
                DTGDefaultPipelineFuture.received(event.getInvokeId(), cause);
            }
        }

        @Override
        public void handlerAdded(DTGHandlerContext ctx) throws Exception {}

        @Override
        public void handlerRemoved(DTGHandlerContext ctx) throws Exception {}

        @Override
        public DTGHandler handler() {
            return this;
        }
    }

    static final class HeadContext extends DTGAbstractHandlerContext implements DTGOutboundHandler {

        private static final String HEAD_NAME = generateName0(HeadContext.class);

        public HeadContext(DTGDefaultPipeline pipeline) {
            super(pipeline, null, HEAD_NAME, false, true);
        }

        @Override
        public boolean isAcceptable(DTGMessageEvent<?> event) {
            return true;
        }

        @Override
        public void handlerAdded(DTGHandlerContext ctx) throws Exception {}

        @Override
        public void handlerRemoved(DTGHandlerContext ctx) throws Exception {}

        @Override
        public void exceptionCaught(DTGHandlerContext ctx, DTGMessageEvent<?> event, Throwable cause) throws Exception {
            ctx.fireExceptionCaught(event, cause);
        }

        @Override
        public DTGHandler handler() {
            return this;
        }

        @Override
        public void handleOutbound(DTGHandlerContext ctx, DTGOutboundMessageEvent<?> event) throws Exception {
            if (event != null) {
                DTGDefaultPipelineFuture.received(event.getInvokeId(), event.getMessage());
            }
        }
    }
}

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
package raft;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.client.failover.RetryRunner;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.errors.ErrorsHelper;
import config.DTGConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A helper closure for failover, which is an immutable object.
 * A new object will be created when a retry operation occurs
 * and {@code retriesLeft} will decrease by 1, until
 * {@code retriesLeft} == 0.
 *
 * @author jiachun.fjc
 */
public final class FailoverClosureImpl<T> extends BaseStoreClosure implements FailoverClosure<T> {

    private static final Logger        LOG = LoggerFactory.getLogger(FailoverClosureImpl.class);

    private final CompletableFuture<T> future;
    private final boolean              retryOnInvalidEpoch;
    private final int                  retriesLeft;
    private final RetryRunner          retryRunner;
    private long                       returnTxId;
    private int                        RetrySleep = 0;

    private static AtomicInteger errorCount = new AtomicInteger(0);
    private static AtomicInteger errorCount2 = new AtomicInteger(0);

    public FailoverClosureImpl(CompletableFuture<T> future, int retriesLeft, RetryRunner retryRunner) {
        this(future, true, retriesLeft, retryRunner);
    }

    public FailoverClosureImpl(CompletableFuture<T> future, int retriesLeft, RetryRunner retryRunner ,int RetrySleep) {
        this(future, true, retriesLeft, retryRunner);
        this.RetrySleep = RetrySleep;
    }

    public FailoverClosureImpl(CompletableFuture<T> future, boolean retryOnInvalidEpoch, int retriesLeft, RetryRunner retryRunner ,int RetrySleep) {
        this(future, retryOnInvalidEpoch, retriesLeft, retryRunner);
        this.RetrySleep = RetrySleep;
    }

    public FailoverClosureImpl(CompletableFuture<T> future, boolean retryOnInvalidEpoch, int retriesLeft,
                               RetryRunner retryRunner) {
        this.future = future;
        this.retryOnInvalidEpoch = retryOnInvalidEpoch;
        this.retriesLeft = retriesLeft;
        this.retryRunner = retryRunner;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run(final Status status) {
        if (status.isOk()) {
            success((T) getData());
            return;
        }

        final Errors error = getError();
        if(error == Errors.REQUEST_REPEATE){
            LOG.error("Request Rpeate");
            failure(error);
            return;
        }
        if (this.retriesLeft > 0
            && (ErrorsHelper.isInvalidPeer(error) || (this.retryOnInvalidEpoch && ErrorsHelper.isInvalidEpoch(error)) || ErrorsHelper.isTransactionError(error))) {
            LOG.warn("[Failover] status: {}, error: {}, [{}] retries left.", status, error, this.retriesLeft);
            if(this.RetrySleep != 0){
                try {
                    int t = (new Random()).nextInt(DTGConstants.FAILOVERRETRIES);
                    Thread.sleep(t * this.RetrySleep);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            this.retryRunner.run(error);
        } else {
            if (this.retriesLeft <= 0) {
                if(this.RetrySleep != 0){
                    LOG.error("[InvalidEpoch-Failover] status: {}, error: {}, {} retries left. errorCount {}", status, error,
                            this.retriesLeft, errorCount.getAndIncrement());
                }
                else {
                    LOG.error("[InvalidEpoch-Failover] status: {}, error: {}, {} retries left. errorCount2 {}", status, error,
                            this.retriesLeft, errorCount2.getAndIncrement());
                }
            }
            failure(error);
        }
    }

    @Override
    public CompletableFuture<T> future() {
        return future;
    }

    @Override
    public void success(final T result) {
        this.future.complete(result);
    }

    @Override
    public void failure(final Throwable cause) {
        this.future.completeExceptionally(cause);
    }

    @Override
    public void failure(final Errors error) {
        if (error == null) {
            failure(new NullPointerException(
                "The error message is missing, this should not happen, now only the stack information can be referenced."));
        } else {
            failure(error.exception());
        }
    }

    @Override
    public long getReturnTxId() {
        return returnTxId;
    }

    @Override
    public void setReturnTxId(final long returnTxId) {
        this.returnTxId = returnTxId;
    }
}

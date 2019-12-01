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

import Element.EntityEntry;
import Element.OperationName;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import com.alipay.sofa.jraft.rhea.storage.KVEntry;
import com.alipay.sofa.jraft.rhea.storage.KVOperation;
import com.alipay.sofa.jraft.rhea.storage.KVStoreClosure;
import com.alipay.sofa.jraft.rhea.util.ByteArray;
import com.codahale.metrics.Timer;

import javax.naming.ldap.PagedResultsControl;
import java.util.List;
import java.util.Map;

import static com.alipay.sofa.jraft.rhea.metrics.KVMetricNames.*;

/**
 *
 * @author jiachun.fjc
 */
public class MetricsClosureAdapter implements EntityStoreClosure {

    private final EntityStoreClosure       done;
    private final String                   regionId;
    private final byte                     Op;
    private final Timer.Context            ctx;
    private final Timer.Context            opCtx;
    private final int                      entrySize;

    public MetricsClosureAdapter(EntityStoreClosure done, String regionId, byte Op,int entrySize, Timer.Context ctx) {
        this.done = done;
        this.regionId = regionId;
        this.Op = Op;
        this.ctx = ctx;
        this.entrySize = entrySize;
        this.opCtx = opTimeCtx(Op);
    }

    @Override
    public Errors getError() {
        if (this.done != null) {
            return this.done.getError();
        }
        return null;
    }

    @Override
    public void setError(Errors error) {
        if (this.done != null) {
            this.done.setError(error);
        }
    }

    @Override
    public Object getData() {
        if (this.done != null) {
            return this.done.getData();
        }
        return null;
    }

    @Override
    public void setData(Object data) {
        if (this.done != null) {
            this.done.setData(data);
        }
    }

    @Override
    public void run(Status status) {
        try {
            if (this.done != null) {
                this.done.run(status);
            }
        } finally {
            if (status.isOk()) {
                doStatistics();
            }
            this.ctx.stop();
            this.opCtx.stop();
        }
    }

    private Timer.Context opTimeCtx(final byte op) {
        return KVMetrics.timer(RPC_REQUEST_HANDLE_TIMER, this.regionId, KVOperation.opName(op)).time();
    }

    @SuppressWarnings("unchecked")
    private void doStatistics() {
        final String id = this.regionId; // stack copy
        switch (this.Op) {
            case OperationName.TRANSACTIONOP:
                KVMetrics.counter(TRANSACTIONOP, id).inc();
                KVMetrics.counter(ENTRYSIZE, id).inc(entrySize);
                break;
            case OperationName.MERGE: {
                KVMetrics.counter(MERGE, id).inc();
                //KVMetrics.counter(REGION_BYTES_WRITTEN, id).inc(this.bytesWritten);
                break;
            }
            case OperationName.READONLY:{
                KVMetrics.counter(READONLYOP, id).inc();
                break;
            }
            case OperationName.SPLIT:{
                KVMetrics.counter(SPLIT, id).inc();
                break;
            }
        }
    }
}

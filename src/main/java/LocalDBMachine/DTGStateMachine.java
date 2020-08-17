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
package LocalDBMachine;

import Communication.instructions.AddRegionInfo;
import Element.DTGOperation;
import Element.OperationName;
import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.entity.LeaderChangeContext;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rhea.StateListener;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.errors.StoreCodecException;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import com.alipay.sofa.jraft.rhea.serialization.Serializer;
import com.alipay.sofa.jraft.rhea.serialization.Serializers;
import com.alipay.sofa.jraft.rhea.util.StackTraceUtil;
import com.alipay.sofa.jraft.rhea.util.ThrowUtil;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.EntityEntryClosureAdapter;
import raft.EntityStoreClosure;
import storage.DTGStoreEngine;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import Region.DTGRegion;
import tool.ObjectAndByte;


import static com.alipay.sofa.jraft.rhea.metrics.KVMetricNames.STATE_MACHINE_APPLY_QPS;
import static com.alipay.sofa.jraft.rhea.metrics.KVMetricNames.STATE_MACHINE_BATCH_WRITE;
import static config.MainType.NODETYPE;
import static config.MainType.RELATIONTYPE;

/**
 * Rhea KV store state machine
 *
 * @author jiachun.fjc
 */
public class DTGStateMachine extends StateMachineAdapter implements Serializable {

    private static final Logger       LOG        = LoggerFactory.getLogger(DTGStateMachine.class);

    private final List<StateListener> listeners  = new CopyOnWriteArrayList<>();
    private final AtomicLong          leaderTerm = new AtomicLong(-1L);
    private final Serializer          serializer = Serializers.getDefault();
    private final DTGRegion           region;
    private final DTGStoreEngine      storeEngine;
    private final LocalDB             localDB;
    private final Meter               applyMeter;


    public DTGStateMachine(DTGRegion region, DTGStoreEngine storeEngine) {
        this.region = region;
        this.storeEngine = storeEngine;
        this.localDB = storeEngine.getlocalDB();
        final String regionStr = String.valueOf(this.region.getId());
        this.applyMeter = KVMetrics.meter(STATE_MACHINE_APPLY_QPS, regionStr);
}

    @Override
    public void onApply(final Iterator it) {
        int index = 0;
        int applied = 0;
        try{
            while (it.hasNext()){
                DTGOperation op;
                EntityEntryClosureAdapter done = (EntityEntryClosureAdapter)it.done();
                if (done != null) {
                    op = done.getOperation();
                } else {
                    final ByteBuffer buf = it.getData();
                    try {
                        if (buf.hasArray()) {
                            op = this.serializer.readObject(buf.array(), DTGOperation.class);
                        } else {
                            op = this.serializer.readObject(buf, DTGOperation.class);
                        }
                    } catch (final Throwable t) {
                        throw new StoreCodecException("Decode operation error", t);
                    }
                }
                switch (op.getType()){
                    case OperationName.COMMITTRANS:
                    case OperationName.ROLLBACK:
                    case OperationName.TRANSACTIONOP:{
                        this.localDB.runOp(op, done, isLeader(),region);
                        break;
                    }
                    case OperationName.ADDREMOVELIST:{
                        this.localDB.addRemoveLock(op, done);
                        break;
                    }
                    case OperationName.ADDREGION:{
                        System.out.println("add region!");
                        doAddRegion(op, done);
                        break;
                    }
                    case OperationName.MERGE:{
                        break;
                    }
                    case OperationName.READONLY:{
                        break;
                    }
                    case OperationName.SPLIT:{
                        break;
                    }
                }
                ++index;
                it.next();
            }
        }catch (final Throwable t){
            LOG.error("StateMachine meet critical error: {}.", StackTraceUtil.stackTrace(t));
            it.setErrorAndRollback(index - applied, new Status(RaftError.ESTATEMACHINE,
                    "StateMachine meet critical error: %s.", t.getMessage()));
        } finally {
            // metrics: qps
            this.applyMeter.mark(applied);
        }
    }

    private void doAddRegion(final DTGOperation operation, final EntityEntryClosureAdapter closure){
        AddRegionInfo addRegionInfo = (AddRegionInfo) ObjectAndByte.toObject(operation.getOpData());
        try {//System.out.println("add region");
            this.storeEngine.doAddRegion(addRegionInfo);
            if(closure != null){
                closure.setData(Boolean.TRUE);
                closure.run(Status.OK());
            }
        }catch (final Throwable t) {
            LOG.error("Fail to add, fullregionId={}, newRegionId={}.", addRegionInfo.getFullRegionId(), addRegionInfo.getNewRegionId());
            setCriticalError(closure, t);
        }

    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
        //this.storeSnapshotFile.save(writer, done, this.region.copy(), this.storeEngine.getSnapshotExecutor());
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        if (isLeader()) {
            LOG.warn("Leader is not supposed to load snapshot.");
            return false;
        }
        return true;
        //return this.storeSnapshotFile.load(reader, this.region.copy());
    }

    @Override
    public void onLeaderStart(final long term) {
        super.onLeaderStart(term);
        this.leaderTerm.set(term);
        // Because of the raft state machine must be a sequential commit, in order to prevent the user
        // doing something (needs to go through the raft state machine) in the listeners, we need
        // asynchronously triggers the listeners.
        this.storeEngine.getRaftStateTrigger().execute(() -> {
            for (final StateListener listener : this.listeners) { // iterator the snapshot
                listener.onLeaderStart(term);
            }
        });
    }

    @Override
    public void onLeaderStop(final Status status) {
        super.onLeaderStop(status);
        final long oldTerm = this.leaderTerm.get();
        this.leaderTerm.set(-1L);
        // Because of the raft state machine must be a sequential commit, in order to prevent the user
        // doing something (needs to go through the raft state machine) in the listeners, we asynchronously
        // triggers the listeners.
        this.storeEngine.getRaftStateTrigger().execute(() -> {
            for (final StateListener listener : this.listeners) { // iterator the snapshot
                listener.onLeaderStop(oldTerm);
            }
        });
    }

    @Override
    public void onStartFollowing(final LeaderChangeContext ctx) {
        super.onStartFollowing(ctx);
        // Because of the raft state machine must be a sequential commit, in order to prevent the user
        // doing something (needs to go through the raft state machine) in the listeners, we need
        // asynchronously triggers the listeners.
        this.storeEngine.getRaftStateTrigger().execute(() -> {
            for (final StateListener listener : this.listeners) { // iterator the snapshot
                listener.onStartFollowing(ctx.getLeaderId(), ctx.getTerm());
            }
        });
    }

    @Override
    public void onStopFollowing(final LeaderChangeContext ctx) {
        super.onStopFollowing(ctx);
        // Because of the raft state machine must be a sequential commit, in order to prevent the user
        // doing something (needs to go through the raft state machine) in the listeners, we need
        // asynchronously triggers the listeners.
        this.storeEngine.getRaftStateTrigger().execute(() -> {
            for (final StateListener listener : this.listeners) { // iterator the snapshot
                listener.onStopFollowing(ctx.getLeaderId(), ctx.getTerm());
            }
        });
    }

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    public void addStateListener(final StateListener listener) {
        this.listeners.add(listener);
    }

    public long getRegionId() {
        return this.region.getId();
    }

    /**
     * Sets critical error and halt the state machine.
     *
     * If current node is a leader, first reply to client
     * failure response.
     *
     * @param closure callback
     * @param ex      critical error
     */
    private static void setCriticalError(final EntityStoreClosure closure, final Throwable ex) {
        // Will call closure#run in FSMCaller
        if (closure != null) {
            closure.setError(Errors.forException(ex));
        }
        ThrowUtil.throwException(ex);
    }
}

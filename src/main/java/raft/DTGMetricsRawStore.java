package raft;

import Element.DTGOperation;
import Element.OperationName;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import scala.collection.Iterator;

import com.codahale.metrics.Timer;

import static com.alipay.sofa.jraft.rhea.metrics.KVMetricNames.RPC_REQUEST_HANDLE_TIMER;

/**
 * @author :jinkai
 * @date :Created in 2019/10/22 13:24
 * @description:
 * @modified By:
 * @version:
 */

public class DTGMetricsRawStore implements DTGRawStore {

    private final String regionId;
    private final DTGRawStore rawStore;
    private final Timer timer;

    public DTGMetricsRawStore(long regionId, DTGRawStore rawStore){
        this.regionId = String.valueOf(regionId);
        this.rawStore = rawStore;
        this.timer = KVMetrics.timer(RPC_REQUEST_HANDLE_TIMER, this.regionId);
    }

    public DTGRawStore getRawStore() {
        return rawStore;
    }

    @Override
    public Iterator localIterator() {
        return null;
    }

    @Override
    public void ApplyEntityEntries(DTGOperation op, EntityStoreClosure closure) {
        final EntityStoreClosure c = metricsAdapter(closure, OperationName.TRANSACTIONOP, op.getSize());
        this.rawStore.ApplyEntityEntries(op, c);
    }

    @Override
    public void readOnlyEntityEntries(DTGOperation op, EntityStoreClosure closure) {
        final EntityStoreClosure c = metricsAdapter(closure, OperationName.READONLY, 0);
        this.rawStore.readOnlyEntityEntries(op, c);
    }

    @Override
    public void merge() {
    }

    @Override
    public void split() {

    }

    private MetricsClosureAdapter metricsAdapter(final EntityStoreClosure closure, final byte op, int entrySize) {
        return new MetricsClosureAdapter(closure, this.regionId, op,entrySize, timeCtx());
    }

    private Timer.Context timeCtx() {
        return this.timer.time();
    }
}

package raft;

import Element.DTGOperation;
import Element.OperationName;
import MQ.ByteTask;
import MQ.DTGMQ;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.rhea.metrics.KVMetrics;
import options.DTGMetricsRawStoreOptions;
import options.MQOptions;
import scala.collection.Iterator;

import com.codahale.metrics.Timer;
import tool.ObjectAndByte;

import java.io.File;

import static com.alipay.sofa.jraft.rhea.metrics.KVMetricNames.RPC_REQUEST_HANDLE_TIMER;

/**
 * @author :jinkai
 * @date :Created in 2019/10/22 13:24
 * @description:
 * @modified By:
 * @version:
 */

public class DTGMetricsRawStore implements DTGRawStore, Lifecycle<DTGMetricsRawStoreOptions> {

    private final String regionId;
    private final DTGRawStore rawStore;
    private final Timer timer;
    private final DTGMQ mq;

    public DTGMetricsRawStore(long regionId, DTGRawStore rawStore){
        this.regionId = String.valueOf(regionId);
        this.rawStore = rawStore;
        this.timer = KVMetrics.timer(RPC_REQUEST_HANDLE_TIMER, this.regionId);
        this.mq = new DTGMQ();
    }

    public DTGRawStore getRawStore() {
        return rawStore;
    }

    @Override
    public Iterator localIterator() {
        return null;
    }

    @Override
    public void saveLog(LogStoreClosure closure) {
        Object data = closure.getLog();
        //MQClosure mqClosure = new MQClosure(closure, data);
        ByteTask task = new ByteTask();
        task.setDone(closure);
        task.setData(ObjectAndByte.toByteArray(data));
        this.mq.apply(task);
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

    @Override
    public boolean init(DTGMetricsRawStoreOptions opts) {
        File file =new File(opts.getUrl());
        if  (!file.exists()  && !file .isDirectory())
        {
            file .mkdir();
        }
        MQOptions mqopts = new MQOptions();
        mqopts.setLogUri(opts.getUrl() + "\\Log");
        mqopts.setRockDBPath(opts.getUrl() + "\\RockDB");
        mqopts.setSaveStore(opts.getSaveStore());
        if(!mq.init(mqopts)){
            return false;
        }
        return true;
    }

    @Override
    public void shutdown() {

    }
}

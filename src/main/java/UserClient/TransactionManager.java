package UserClient;

import DBExceptions.TxError;
import PlacementDriver.DTGPlacementDriverClient;
import UserClient.Transaction.DTGTransaction;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.util.*;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import config.DTGConstants;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class TransactionManager implements AutoCloseable {

    private Disruptor<VersionRequestClosure> versionDisruptor;
    private RingBuffer<VersionRequestClosure> applyQueue;
    private DTGPlacementDriverClient pdClient;

    public TransactionManager(DTGPlacementDriverClient pdClient){
        this.pdClient = pdClient;
        this.versionDisruptor = DisruptorBuilder.<VersionRequestClosure> newInstance() //
                .setRingBufferSize(DTGConstants.disruptorBufferSize) //
                .setEventFactory(new VersionClosureFactory()) //
                .setThreadFactory(new NamedThreadFactory("Version-Process-Disruptor-", true)) //
                .setProducerType(ProducerType.MULTI) //
                .setWaitStrategy(new BlockingWaitStrategy()) //
                .build();
        this.versionDisruptor.handleEventsWith(new VersionHandler());
        this.versionDisruptor.setDefaultExceptionHandler(new LogExceptionHandler<Object>(getClass().getSimpleName()));
        this.applyQueue = this.versionDisruptor.start();
    }

    @Override
    public void close() throws Exception {

    }

    private static class VersionClosureFactory implements EventFactory<VersionRequestClosure> {

        @Override
        public VersionRequestClosure newInstance() {
            return new VersionRequestClosure();
        }
    }

    class VersionHandler implements EventHandler<VersionRequestClosure> {
        private final List<VersionRequestClosure> tasks = new ArrayList<>(DTGConstants.applyBatch);

        @Override
        public void onEvent(final VersionRequestClosure versionRequestClosure, final long sequence, final boolean endOfBatch) throws Exception {
            if (versionRequestClosure.shutdownLatch != null) {
                if (!this.tasks.isEmpty()) {
                    executeVersionTasks(this.tasks);
                }
                versionRequestClosure.shutdownLatch.countDown();
                return;
            }
            tasks.add(versionRequestClosure);
            if (this.tasks.size() >= DTGConstants.applyBatch || endOfBatch) {
                executeVersionTasks(this.tasks);
                this.tasks.clear();
            }
        }
    }



    private void executeVersionTasks(List<VersionRequestClosure> tasks){
        int txNumber = tasks.size();
        Long[] res = this.pdClient.getVersions(txNumber);
        if(res[1] - res[0] != txNumber){
            for(int i = 0; i < txNumber; i++){
                VersionRequestClosure closure = tasks.get(i);
                closure.setError(Errors.TRANSACTION_SECOND_ERROR);
                closure.run(new Status(TxError.FAILED.getNumber(), "can not get right version!"));
            }
        }
        for(int i = 0; i < txNumber; i++){
            VersionRequestClosure closure = tasks.get(i);
            closure.setVersion(res[0] + i);
            closure.run(Status.OK());
        }
    }

    public void applyRequestVersion(DTGTransaction transaction, CompletableFuture future){
        try {
            final EventTranslator<VersionRequestClosure> translator = (event, sequence) -> {
                event.reset();
                event.setDone(future);
                event.setTransaction(transaction);
            };
            int retryTimes = 0;
            while (true) {
                if (this.applyQueue.tryPublishEvent(translator)) {
                    break;
                } else {
                    retryTimes++;
                    if (retryTimes > DTGConstants.FAILOVERRETRIES) {
                        future.complete(-1);
                        return;
                    }
                    ThreadHelper.onSpinWait();
                }
            }
        } catch (final Exception e) {
            future.complete(-1);
        }
    }
}

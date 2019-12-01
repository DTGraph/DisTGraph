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
package UserClient.Transaction;

import Element.EntityEntry;
import UserClient.DTGSaveStore;
import options.TransactionOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Map;


public class DTGTransaction implements AutoCloseable {

    Logger log = LoggerFactory.getLogger(DTGTransaction.class);

    private static boolean isClose = false;
    private final String txId;
    private int entityNum;
    private int failoverRetries;
    private DTGSaveStore store;
    private boolean readOnly;
    private Map<Integer, Object> result;

    private LinkedList<EntityEntry> entityEntryList;

    public DTGTransaction(DTGSaveStore store, TransactionOptions opts, String txId){
        System.out.println("INIT : " + System.currentTimeMillis());
        log.info("start a distribute transaction ...");
        entityEntryList = new LinkedList<>();
        entityNum = 0;
        this.store = store;
        this.failoverRetries = opts.getFailoverRetries();
        readOnly = true;
        this.txId = txId;
    }

    public boolean isClosed(){
        return this.isClose;
    }

    public void start(final Throwable lastCause){
        System.out.println("START : " + System.currentTimeMillis());
        this.isClose = false;
        result =  store.applyRequest(this.entityEntryList, this.txId, this.failoverRetries, null, true);
        System.out.println(result.size());
    }

    public void startNow(final Throwable lastCause){
        this.isClose = false;
        result =  store.applyRequest(this.entityEntryList, this.txId, this.failoverRetries, null, false);
    }

    public void close(){
        this.isClose = true;
        System.out.println("Transaction close");
    }

    public void addEntityEntries(EntityEntry entityEntry){
        //entityEntry.setTransactionNum(entityNum++);
        entityEntryList.add(entityEntry);
    }

    public boolean commit(){
        System.out.println("COMMIT : " + System.currentTimeMillis());
        log.info("waitting to commit transaction");
        //CompletableFuture<Boolean> future = store.applyCommitRequest(this.txId, true, this.failoverRetries);
        //return FutureHelper.get(future);
        return store.applyCommitRequest(this.txId, true, this.failoverRetries);
    }

    public boolean rollback(){
        log.info("waitting to rollback transaction");
        //CompletableFuture<Boolean> future = store.applyCommitRequest(this.txId, false, this.failoverRetries);
        //return FutureHelper.get(future);
        return store.applyCommitRequest(this.txId, false, this.failoverRetries);
    }

    public synchronized int getEntityNum() {
        return entityNum++;
    }

    public void NotReadOnly() {
        this.readOnly = false;
    }

    public String getTxId() {
        return txId;
    }

    public Object getResult(int i){
        return result.get(i);
    }
}

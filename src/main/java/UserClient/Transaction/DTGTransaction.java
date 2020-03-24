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
import config.DTGConstants;
import options.TransactionOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Map;

import static config.MainType.NODETYPE;
import static config.MainType.RELATIONTYPE;


public class DTGTransaction implements AutoCloseable {

    Logger log = LoggerFactory.getLogger(DTGTransaction.class);

    private static boolean isClose = false;
    private final String txId;
    private int entityNum;
    private int failoverRetries;
    private DTGSaveStore store;
    private boolean readOnly;
    private Map<Integer, Object> result;
    private long maxNodeId = -1;
    private long maxRelationId = -1;
    private long version = -1;

    private LinkedList<EntityEntry> entityEntryList;

    public DTGTransaction(DTGSaveStore store, TransactionOptions opts, String txId){
        //System.out.println("INIT : " + System.currentTimeMillis());
        log.info("start a distribute transaction ...");
        entityEntryList = new LinkedList<>();
        entityNum = 0;
        this.store = store;
        this.failoverRetries = DTGConstants.FAILOVERRETRIES;
        readOnly = true;
        this.txId = txId;
    }

    public boolean isClosed(){
        return this.isClose;
    }

    public Map<Integer, Object> start(){
        //System.out.println("start : " + entityEntryList.size());
        if(!readOnly){
            this.version = this.store.getPlacementDriverClient().getVersion();
        }
        long start = System.currentTimeMillis();
        //System.out.println("START : " + System.currentTimeMillis());
        this.isClose = false;
        //store.applyTransaction(this.entityEntryList, this.txId, this.failoverRetries, version);
        Map<Integer, Object> result =  store.applyRequest(this.entityEntryList, this.txId, this.readOnly, true,
                this.failoverRetries, null, true, version);
        //System.out.println("END : " + System.currentTimeMillis());
        long end = System.currentTimeMillis();
        //System.out.println("cost : " + (end-start));
        return result;
    }

    public void close(){
        this.isClose = true;
        //System.out.println("Transaction close");
    }

    public void addEntityEntries(EntityEntry entityEntry){
        //entityEntry.setTransactionNum(entityNum++);
        entityEntryList.add(entityEntry);
        if(entityEntry.getOperationType() == EntityEntry.ADD){
            if(entityEntry.getType() == NODETYPE && entityEntry.getId() > maxNodeId){
                maxNodeId = entityEntry.getId();
            }else if(entityEntry.getType() == RELATIONTYPE && entityEntry.getId() > maxRelationId){
                maxRelationId = entityEntry.getId();
            }
        }
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

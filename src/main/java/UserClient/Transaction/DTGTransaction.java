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

import DBExceptions.TransactionException;
import Element.EntityEntry;
import UserClient.DTGSaveStore;
import UserClient.TransactionManager;
import com.alipay.sofa.jraft.rhea.client.FutureHelper;
import config.DTGConstants;
import config.MainType;
import options.TransactionOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tool.ObjectAndByte;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

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
    private TransactionManager transactionManager;
    private boolean isolateRead = false;

    private LinkedList<EntityEntry> entityEntryList;

    public DTGTransaction(DTGSaveStore store, TransactionManager transactionManager, TransactionOptions opts, String txId){
        log.info("start a distribute transaction ...");
        entityEntryList = new LinkedList<>();
        entityNum = 0;
        this.store = store;
        this.transactionManager = transactionManager;
        this.failoverRetries = DTGConstants.FAILOVERRETRIES;
        readOnly = true;
        this.txId = txId;
    }

    public boolean isClosed(){
        return this.isClose;
    }

//    public Map<Integer, Object> start(){
//        if(!readOnly){
//            this.version = this.store.getPlacementDriverClient().getVersion();
//        }
//        long start = System.currentTimeMillis();
//        this.isClose = false;
//        Map<Integer, Object> result =  store.applyRequest(this.entityEntryList, this.txId, this.readOnly, true,
//                this.failoverRetries, null, true, version);
//        long end = System.currentTimeMillis();
//        return result;
//    }

    public Map<Integer, Object> start(){
        if(!readOnly){
            CompletableFuture<Long> future = new CompletableFuture();
            transactionManager.applyRequestVersion(this, future);
            this.version = FutureHelper.get(future);System.out.println("GET VERSION :" + version);
            if(version == -1){
                try {
                    throw new TransactionException("get an error version!");
                } catch (TransactionException e) {
                    e.printStackTrace();
                }
                return null;
            }
        }
        this.isClose = false;
        Map<Integer, Object> result =  store.applyRequest(this.entityEntryList, this.txId, this.readOnly, true,
                this.failoverRetries, true, version, null);
        Map<Integer, Long> firstGetList = new HashMap<>();
        if(this.isolateRead){
            LinkedList<EntityEntry> readList = new LinkedList<>();
            for(EntityEntry entry : this.entityEntryList){
                if(entry.getOperationType() == MainType.GET){
                    readList.add(entry);
                }
            }
            Map<Integer, Object> result2 =  store.applyRequest(readList, this.txId, true, true,
                    this.failoverRetries, true, version, ObjectAndByte.toByteArray(firstGetList.get(-1)));
            for(int t : result2.keySet()){
                result.put(t, result2.get(t));
            }
        }
        return result;
    }

    public void close(){
        this.isClose = true;
    }

    public void addEntityEntries(EntityEntry entityEntry){
        entityEntryList.add(entityEntry);
        if(entityEntry.getOperationType() == EntityEntry.ADD){
            if(entityEntry.getType() == NODETYPE && entityEntry.getId() > maxNodeId){
                maxNodeId = entityEntry.getId();
            }else if(entityEntry.getType() == RELATIONTYPE && entityEntry.getId() > maxRelationId){
                maxRelationId = entityEntry.getId();
            }
        }
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

    public void setVersion(long version) {
        this.version = version;
    }

    public boolean isIsolateRead() {
        return isolateRead;
    }

    public void setIsolateRead() {
        isolateRead = true;
    }
}

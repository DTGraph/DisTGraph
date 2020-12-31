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
import java.util.concurrent.TimeUnit;

import static com.alipay.sofa.jraft.rhea.client.FutureHelper.DEFAULT_TIMEOUT_MILLIS;
import static config.DTGConstants.STORECOUNTSTATIC;
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
        return start(-1);
    }

    public Map<Integer, Object> start(int pool_size){
        System.out.println(System.currentTimeMillis() + "  " + txId + " : start transaction");
        long version2 = 0;
        try{
            if(true){
                CompletableFuture<Long> future = new CompletableFuture();
                transactionManager.applyRequestVersion(future);
                //this.version = FutureHelper.get(future);
                this.version = future.get(DEFAULT_TIMEOUT_MILLIS, TimeUnit.SECONDS);
                if(version == -1){
                    throw new TransactionException("get an error version!");
                }

            }
            CompletableFuture<Long> future2 = new CompletableFuture();
            transactionManager.applyRequestVersion(future2);
            //version2 = FutureHelper.get(future2);
            version2 = future2.get(DEFAULT_TIMEOUT_MILLIS, TimeUnit.SECONDS);
        }catch (Exception e){
            System.out.println(e);
            return null;
        }
        //if(!readOnly){


        this.isClose = false;
        System.out.println(System.currentTimeMillis() + "  " + txId + " : get version = " + version);
        Map<Integer, Object> result;
        if(pool_size == -1){
            result =  store.applyRequest(this.entityEntryList, this.txId, this.readOnly, true,
                    this.failoverRetries, false, version, ObjectAndByte.toByteArray(version2 + ""));
        }else {
            result =  store.applyRequest(this.entityEntryList, this.txId, this.readOnly, true,
                    this.failoverRetries, false, version, null);
        }

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

        if(pool_size == -1){
            if(!(result.containsKey(-2) && result.containsKey(-3) && result.containsKey(-4)) && STORECOUNTSTATIC == 3){
                try {
                    throw new TransactionException("CAN NOT CONNECT SOME STORE!");
                } catch (TransactionException e) {
                    e.printStackTrace();
                }
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

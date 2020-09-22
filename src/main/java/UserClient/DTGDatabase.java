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
package UserClient;

/**
 * @author      :jinkai
 * @date        :Created in 2019-10-13
 * @description :this is user API
 * @modified By :
 * @version:     1.0
 */

import Element.EntityEntry;
import Element.NodeAgent;
import Element.RelationshipAgent;
import PlacementDriver.DTGPlacementDriverClient;
import PlacementDriver.IdManage.IdGenerator;
import UserClient.Transaction.DTGTransaction;
import UserClient.Transaction.TransactionManage;
import com.alipay.sofa.jraft.rhea.rpc.ExtSerializerSupports;
import options.DTGStoreOptions;
import options.TransactionOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tool.GetSystem;

import java.io.File;

import static config.DefaultOptions.*;
import static config.MainType.*;

public class DTGDatabase {

    private static final Logger LOG = LoggerFactory.getLogger(DTGDatabase.class);

    static {
        ExtSerializerSupports.init();
    }

    private DTGPlacementDriverClient pdClient;
    private static TransactionManage transactionManage;
    private DTGSaveStore store;
    private IdGenerator TxIdGenerator;
    private String macAddress = "";
    private TransactionManager versionManager;

    public DTGDatabase(){
        this.transactionManage = new TransactionManage();
    }

    public DTGDatabase(DTGSaveStore store){
        this.store = store;
        this.transactionManage = new TransactionManage();
    }

    public synchronized boolean init(String ip, int port, String path){
        LOG.info("build DTGDatabaseClient ...");
        if(store == null){
            this.store= new DTGSaveStore();
        }
        DTGStoreOptions opts = defaultClientDTGStoreOptions(ip, port, path);
        opts.getPlacementDriverOptions().setLocalClient(true);
        opts.getPlacementDriverOptions().setRemotePd(false);
        store.init(opts);
        this.pdClient = store.getPlacementDriverClient();
        this.TxIdGenerator = createTxIdGenerator();
        this.pdClient.initIds();
        this.macAddress = GetSystem.getMacAddress();
        this.versionManager = new TransactionManager(this.store.getPlacementDriverClient());
        //System.out.println("macAddress : " + macAddress);
        return true;
    }

    public void shutdown(){
        this.TxIdGenerator.close();
        this.pdClient.shutdown();
        this.store.shutdown();
    }

    public DTGTransaction CreateTransaction(){
        //System.out.println(System.currentTimeMillis());
        String txId = this.macAddress + "" + TxIdGenerator.nextId();
        DTGTransaction tx = new DTGTransaction(store, versionManager, new TransactionOptions().newDefaultOpt(), txId);
        if(transactionManage.getTransaction() !=null ){
            DTGTransaction transaction = transactionManage.getTransaction();
            if(transaction.isClosed()){
                transactionManage.removeBound();
            }
        }
        transactionManage.BindTransaction(tx);
        return tx;
    }

    public NodeAgent addNode(){
        DTGTransaction transaction = transactionManage.getTransaction();
        NodeAgent node = new NodeAgent(transactionManage);
        EntityEntry entry = new EntityEntry();
        entry.setId(pdClient.getId(NODETYPE));System.out.println("node id = " + entry.getId());
        entry.setTransactionNum(transaction.getEntityNum());
        entry.setType(NODETYPE);
        entry.setOperationType(EntityEntry.ADD);
        entry.setIsTemporalProperty(false);
        node.setTransactionObjectId(entry.getTransactionNum());
        transaction.addEntityEntries(entry);
        transaction.NotReadOnly();
        return node;
    }

    public RelationshipAgent addRelationship(NodeAgent startNode, NodeAgent endNode){
        DTGTransaction transaction = transactionManage.getTransaction();
        RelationshipAgent relationship = new RelationshipAgent(transactionManage);
        EntityEntry entry = new EntityEntry();
        entry.setId(pdClient.getId(RELATIONTYPE));
        entry.setTransactionNum(transaction.getEntityNum());
        entry.setType(RELATIONTYPE);
        entry.setOperationType(EntityEntry.ADD);
        entry.setStart(startNode.getTransactionObjectId());
        entry.setOther(endNode.getTransactionObjectId());
        entry.setIsTemporalProperty(false);
        relationship.setTransactionObjectId(entry.getTransactionNum());
        transaction.addEntityEntries(entry);
        transaction.NotReadOnly();
        return relationship;
    }

    public NodeAgent getNodeById(long id){
        DTGTransaction transaction = transactionManage.getTransaction();
        NodeAgent node = new NodeAgent(transactionManage);
        EntityEntry entry = new EntityEntry();
        entry.setId(id);
        entry.setTransactionNum(transaction.getEntityNum());
        entry.setType(NODETYPE);
        entry.setOperationType(EntityEntry.GET);
        entry.setIsTemporalProperty(false);
        node.setTransactionObjectId(entry.getTransactionNum());
        transaction.addEntityEntries(entry);
        return node;
    }

    public RelationshipAgent getRelationshipById(long id){
        DTGTransaction transaction = transactionManage.getTransaction();
        RelationshipAgent relationship = new RelationshipAgent(transactionManage);
        EntityEntry entry = new EntityEntry();
        entry.setId(id);
        entry.setTransactionNum(transaction.getEntityNum());
        entry.setType(RELATIONTYPE);
        entry.setOperationType(EntityEntry.GET);
        entry.setIsTemporalProperty(false);
        relationship.setTransactionObjectId(entry.getTransactionNum());
        transaction.addEntityEntries(entry);
        return relationship;
    }

    private IdGenerator createTxIdGenerator(){
        //File file = new File(System.getProperty("java.io.tmpdir"), "txId");
        File file = new File("D:\\garbage", "txId");
        if(!file.exists()){
            IdGenerator.createGenerator(file, 0, false);
        }
        IdGenerator IdG = new IdGenerator(new File("D:\\garbage", "txId"),
                50000, Long.MAX_VALUE, 0);
        return IdG;
    }

}

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
//package UserClient.Transaction;
//
//import DBExceptions.TypeDoesnotExistException;
//import DistributeDatabaseBuild.Config.IdType;
//import DistributeDatabaseBuild.DBMaster;
//import Master.Communication.ClusterNodeConnection;
//import Master.Communication.ConnectionPool;
//import Master.Elements.MasterAction;
//import Master.IdManage.IdDistribute;
//import com.google.protobuf.ByteString;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.HashMap;
//import java.util.LinkedHashMap;
//import java.util.LinkedList;
//import java.util.Map;
//
//public class TransactionThread implements Runnable, AutoCloseable {
//
//    Logger log = LoggerFactory.getLogger(TransactionThread.class);
//
//    private final ByteString[] result = new ByteString[1];
//    private static boolean isClose = false;
//    private LinkedList<MasterAction> actionLinkedList = new LinkedList<MasterAction>();
//    private static Map<Integer, ClusterNodeConnection> aliveConnections;
//    private static Map<Integer, ConnectionPool> poolsMap;
//    private int name;
//    private static int count = 0;
//
//    public TransactionThread(){
//        aliveConnections = new HashMap<>();
//        poolsMap = new HashMap<>();
//        this.name=count;count++;
//    }
//
//    @Override
//    public void run() {
//        for(int i = 0; i < actionLinkedList.size(); i++){
//            MasterAction action = actionLinkedList.get(i);
//
//            try {
//                if(action.isHasConns()){
//                    int[] con = action.getConns();
//                    action.runAction(getConnection(con));
//                    //log.info(action.getObjectId() + "         " + con);
//                }
//                else {
//                    action.runAction(getConnection(action));
//                }
//            } catch (TypeDoesnotExistException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//    public void addAction(MasterAction action){
//        actionLinkedList.add(action);
//    }
//
//    public boolean isClosed(){
//        return this.isClose;
//    }
//
//    public void start(){
//        this.isClose = false;
//        aliveConnections = new LinkedHashMap<Integer, ClusterNodeConnection>();
//        poolsMap = new LinkedHashMap<Integer, ConnectionPool>();
//    }
//
//    public void close(){
//        for(int i : aliveConnections.keySet() ){
//            ClusterNodeConnection connection = aliveConnections.get(i);
//            while (!connection.couldClose()){}
//            connection.onCompleted();
//            poolsMap.get(i).returnConnection(connection);
//        }
//        this.isClose = true;
//    }
//
//    private LinkedList<ClusterNodeConnection> getConnection(MasterAction action) throws TypeDoesnotExistException {
//        int[] location;
//        switch (action.getIdType()){
//            case 1:{
//                location = IdDistribute.getLocation(action.getObjectId(), IdType.NodeId);
//                break;
//            }
//            case 2:{
//                location = IdDistribute.getLocation(action.getObjectId(), IdType.RelationId);
//                break;
//            }
//            case 3:{
//                location = IdDistribute.getLocation(action.getObjectId(), IdType.TemporalPropertyId);
//                break;
//            }
//            default:throw new TypeDoesnotExistException(action.getIdType());
//        }
//        return getConnection(location);
//    }
//
//    public LinkedList<ClusterNodeConnection> getConnection(int[] address){
//        LinkedList connections = new LinkedList();
//        for(int i = 0; i < address.length; i++){
//            if(!aliveConnections.containsKey(address[i])){
//                ConnectionPool pool = DBMaster.getConnectionPools(address[i]);
//                ClusterNodeConnection conn = pool.borrowConnection();
//                conn.PrepareAction();
//                connections.add(conn);
//                aliveConnections.put(address[i], conn);
//                poolsMap.put(address[i], pool);
//            }
//            else connections.add(aliveConnections.get(address[i]));
//        }
//        return connections;
//    }
//
//}

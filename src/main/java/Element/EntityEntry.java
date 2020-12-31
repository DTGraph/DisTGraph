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
package Element;

import DBExceptions.TypeDoesnotExistException;
import config.DTGConstants;

import java.io.Serializable;

import static config.MainType.*;

public class EntityEntry implements Serializable, Comparable<EntityEntry> {

    private static final long serialVersionUID = 1304171229405186819L;

    public static final byte  ADD          = 0x04;
    public static final byte  SET          = 0x05;
    public static final byte  REMOVE       = 0x06;
    public static final byte  GET          = 0x07;
    public static final byte  GETLIMIT     = 0x08;

    private int               transactionNum;

    private byte              operationType;//add, remove, set or get
    private String            key = DTGConstants.NULLSTRING; //property name
    private Object            value; //property value
    private byte              type; //node or relation
    private int               start = -1;  //temporal property time, if other is not -1, it is end time.
                                       //if isTemporalProperty = false, it represent start node id
    private int               other = -1; //end time, if isTemporalProperty = false, it represent end node id
    private long              id; //node or relation id, if id = -2, it represnet real id is paraId
    private int               paraId;
    private boolean           isTemporalProperty = false;
    private long              txVersion = -1;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getOther() {
        return other;
    }

    public void setOther(int other) {
        this.other = other;
    }

    public byte getType() {
        return this.type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public int getTransactionNum() {
        return transactionNum;
    }

    public void setTransactionNum(int transactionNum) {
        this.transactionNum = transactionNum;
    }

    public byte getOperationType() {
        return operationType;
    }

    public void setOperationType(byte operationType) {
        this.operationType = operationType;
    }

    public boolean isTemporalProperty() {
        return isTemporalProperty;
    }

    public void setIsTemporalProperty(boolean temporalProperty) {
        isTemporalProperty = temporalProperty;
    }

    public long getTxVersion() {
        return txVersion;
    }

    public void setTxVersion(long txVersion) {
        this.txVersion = txVersion;
    }

    public String typeString(){
        try {
            switch (this.type){
                case NODETYPE: return "Node entity";
                case RELATIONTYPE: return "Relation entity";
                case TEMPORALPROPERTYTYPE: return "Temporal property entity";
                default:{
                    throw new TypeDoesnotExistException(this.type, "entity type");
                }
            }
        }catch (Throwable e){
            System.out.println(e);
        }
        return null;
    }

    public int getParaId() {
        return paraId;
    }

    public void setParaId(int paraId) {
        this.paraId = paraId;
    }

    @Override
    public String toString() {
        return "EntityEntry{" + "key=" + key + ", value=" + value + ", id=" + id + ", entity type="
                + typeString() + ", start time=" + start + ", isTemporalProperty=" + isTemporalProperty +  ", other=" + other +  ", paraId=" + paraId +
                ", operationType=" + operationType +  ", transactionNum=" + transactionNum +'}';
    }

    @Override
    public int compareTo(EntityEntry o) {
        return this.transactionNum - o.getTransactionNum();
    }
}

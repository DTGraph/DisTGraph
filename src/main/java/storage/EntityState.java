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
package storage;

import Element.DTGOpreration;
import Element.EntityEntry;
import com.alipay.sofa.jraft.rhea.storage.KVOperation;
import com.alipay.sofa.jraft.rhea.storage.KVStoreClosure;
import raft.EntityStoreClosure;

import java.util.List;

/**
 * @author jiachun.fjc
 */
public class EntityState {

    private final DTGOpreration op;
    private final EntityStoreClosure done;

    public static EntityState of(final DTGOpreration op, final EntityStoreClosure done) {
        return new EntityState(op, done);
    }

    public EntityState(DTGOpreration op, EntityStoreClosure done) {
        this.op = op;
        this.done = done;
    }

    public boolean isSameOp(final DTGOpreration o) {
        return this.op.getType() == o.getType();
    }

    public DTGOpreration getOp() {
        return op;
    }

    public List<EntityEntry> getOpEntities() {
        return this.op.getEntityEntries();
    }

    public EntityStoreClosure getDone() {
        return done;
    }

    @Override
    public String toString() {
        return "KVState{" + "op=" + op + ", done=" + done + '}';
    }
}

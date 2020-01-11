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
package MQ;

import com.alipay.sofa.jraft.entity.Checksum;
import com.alipay.sofa.jraft.util.Bits;
import com.alipay.sofa.jraft.util.Copiable;
import com.alipay.sofa.jraft.util.CrcUtil;
import scala.Array;
import tool.ObjectAndByte;

import java.io.Serializable;

/**
 * Log identifier.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-12 3:12:29 PM
 */
public class MQLogId implements Comparable<MQLogId>, Copiable<MQLogId>, Serializable, Checksum {

    private static final long serialVersionUID = -6680425579347357313L;

    private long              index;
    private long              transactionId;

    @Override
    public MQLogId copy() {
        return new MQLogId(this.index, this.transactionId);
    }

    @Override
    public long checksum() {
        byte[] id = ObjectAndByte.toByteArray(transactionId);
        byte[] bs = new byte[8 + id.length];
        Bits.putLong(bs, 0, this.index);
        Array.copy(id, 0, bs, 8, id.length);
        return CrcUtil.crc64(bs);
    }

    public MQLogId(final long index) {
        this(index,0);
    }

    public MQLogId(final long index, final long transactionId) {
        super();
        setIndex(index);
        setTransactionId(transactionId);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (this.index ^ (this.index >>> 32));
        result = prime * result + (int)(this.transactionId ^ (this.transactionId >>> 32));
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final MQLogId other = (MQLogId) obj;
        if (this.index != other.index) {
            return false;
        }
        // noinspection RedundantIfStatement
        if (this.transactionId != other.getTransactionId()) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(final MQLogId o) {
        // Compare term at first
        final int c = Long.compare(getTransactionId(), o.getTransactionId());
        if (c == 0) {
            return Long.compare(getIndex(), o.getIndex());
        } else {
            return c;
        }
    }

    public void setTransactionId(long transactionId) {
        this.transactionId = transactionId;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public long getIndex() {
        return this.index;
    }

    public void setIndex(final long index) {
        this.index = index;
    }

    @Override
    public String toString() {
        return "LogId [index=" + this.index + ", transactionId=" + this.transactionId + "]";
    }

}

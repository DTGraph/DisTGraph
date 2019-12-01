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
package Communication.pd.pipeline.event;

import Communication.util.pipeline.event.DTGInboundMessageEvent;
import PlacementDriver.PD.DTGMetadataStore;
import com.alipay.sofa.jraft.rhea.metadata.Instruction;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @author jiachun.fjc
 */
public abstract class DTGPingEvent<T> extends DTGInboundMessageEvent<T> {

    private final Collection<Instruction> instructions = new LinkedBlockingDeque<>();
    private final DTGMetadataStore           metadataStore;

    public DTGPingEvent(T message, DTGMetadataStore metadataStore) {
        super(message);
        this.metadataStore = metadataStore;
    }

    public DTGMetadataStore getMetadataStore() {
        return metadataStore;
    }

    public Collection<Instruction> getInstructions() {
        return instructions;
    }

    public void addInstruction(Instruction instruction) {
        this.instructions.add(instruction);
    }

    public boolean isReady() {
        return !this.instructions.isEmpty();
    }
}

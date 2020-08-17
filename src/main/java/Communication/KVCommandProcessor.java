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
package Communication;

import Communication.RequestAndResponse.*;
import Region.DTGRegionService;
import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.alipay.remoting.rpc.protocol.AsyncUserProcessor;
import com.alipay.sofa.jraft.rhea.RequestProcessClosure;
import com.alipay.sofa.jraft.rhea.cmd.store.*;
import com.alipay.sofa.jraft.rhea.errors.Errors;
import com.alipay.sofa.jraft.rhea.errors.RheaRuntimeException;
import com.alipay.sofa.jraft.util.Requires;
import config.DTGConstants;
import storage.DTGStoreEngine;

import java.util.concurrent.Executor;

/**
 * Rhea KV store RPC request processing service.
 *
 * @author jiachun.fjc
 */
public class KVCommandProcessor<T extends BaseRequest> extends AsyncUserProcessor<T> {

    private final Class<T>    reqClazz;
    private final DTGStoreEngine storeEngine;

    public KVCommandProcessor(Class<T> reqClazz, DTGStoreEngine storeEngine) {
        this.reqClazz = Requires.requireNonNull(reqClazz, "reqClazz");
        this.storeEngine = Requires.requireNonNull(storeEngine, "storeEngine");
    }

    @Override
    public void handleRequest(final BizContext bizCtx, final AsyncContext asyncCtx, final T request) {
        Requires.requireNonNull(request, "request");
        final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure = new RequestProcessClosure<>(request,
            bizCtx, asyncCtx);
        final DTGRegionService regionService = this.storeEngine.getRegionKVService(request.getRegionId());
        if (regionService == null) {
            final NoRegionFoundResponse noRegion = new NoRegionFoundResponse();
            noRegion.setRegionId(request.getRegionId());
            noRegion.setError(Errors.NO_REGION_FOUND);
            noRegion.setValue(false);
            closure.sendResponse(noRegion);
            return;
        }
        switch (request.magic()) {
            case BaseRequest.MERGE:
                regionService.handleMergeRequest((MergeRequest) request, closure);
                break;
            case DTGConstants.FIRST_PHASE_REQUEST:
                regionService.handleFirstPhase((FirstPhaseRequest)request, closure);
                break;
            case DTGConstants.SECOND_PHASE_REQUEST:
                regionService.handleSecondPhase((SecondPhaseRequest)request, closure);
                break;
            case BaseRequest.RANGE_SPLIT:
                regionService.handleRangeSplitRequest((RangeSplitRequest) request, closure);
                break;
            case DTGConstants.FIRST_PHASE_SUCCESS_REQUEST:
                regionService.handleFirstPhaseSuccessRequest((FirstPhaseSuccessRequest) request, closure);
                break;
            default:
                throw new RheaRuntimeException("Unsupported request type: " + request.getClass().getName());
        }
    }

    @Override
    public String interest() {
        return this.reqClazz.getName();
    }

    @Override
    public Executor getExecutor() {
        return this.storeEngine.getKvRpcExecutor();
    }
}

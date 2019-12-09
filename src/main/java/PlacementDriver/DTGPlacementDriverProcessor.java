package PlacementDriver;

import Communication.RequestAndResponse.CreateRegionRequest;
import Communication.RequestAndResponse.DTGRegionHeartbeatRequest;
import Communication.RequestAndResponse.SetDTGStoreInfoRequest;
import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.alipay.remoting.rpc.protocol.AsyncUserProcessor;
import com.alipay.sofa.jraft.rhea.RequestProcessClosure;
import com.alipay.sofa.jraft.rhea.cmd.pd.*;
import com.alipay.sofa.jraft.rhea.errors.RheaRuntimeException;
import com.alipay.sofa.jraft.util.Requires;

import java.util.concurrent.Executor;

/**
 * @author :jinkai
 * @date :Created in 2019/10/20 17:51
 * @description:
 * @modified By:
 * @version:
 */

public class DTGPlacementDriverProcessor<T extends BaseRequest> extends AsyncUserProcessor<T> {

    private final Class<T> reqClazz;
    private final DTGPlacementDriverService placementDriverService;
    private final Executor executor;

    public DTGPlacementDriverProcessor(Class<T> reqClazz, DTGPlacementDriverService placementDriverService, Executor executor) {
        this.reqClazz = Requires.requireNonNull(reqClazz, "reqClazz");
        this.placementDriverService = Requires.requireNonNull(placementDriverService, "placementDriverService");
        this.executor = executor;
    }

    @Override
    public void handleRequest(BizContext bizCtx, AsyncContext asyncCtx, T request) {
        Requires.requireNonNull(request, "request");
        final RequestProcessClosure<BaseRequest, BaseResponse> closure = new RequestProcessClosure<>(request, bizCtx,
                asyncCtx);
        switch (request.magic()) {
            case BaseRequest.STORE_HEARTBEAT:
                this.placementDriverService.handleStoreHeartbeatRequest((StoreHeartbeatRequest) request, closure);
                break;
            case BaseRequest.DTGREGION_HEARTBEAT:
                this.placementDriverService.handleRegionHeartbeatRequest((DTGRegionHeartbeatRequest) request, closure);
                break;
            case BaseRequest.GET_CLUSTER_INFO:
                this.placementDriverService.handleGetClusterInfoRequest((GetClusterInfoRequest) request, closure);
                break;
            case BaseRequest.GET_STORE_INFO:
                this.placementDriverService.handleGetStoreInfoRequest((GetStoreInfoRequest) request, closure);
                break;
            case BaseRequest.SET_STORE_INFO:
                this.placementDriverService.handleSetStoreInfoRequest((SetDTGStoreInfoRequest) request, closure);
                break;
            case BaseRequest.GET_STORE_ID:
                this.placementDriverService.handleGetStoreIdRequest((GetStoreIdRequest) request, closure);
                break;
            case BaseRequest.CREATE_REGION_ID:
                this.placementDriverService.handleCreateRegionIdRequest((CreateRegionIdRequest) request, closure);
                break;
            case BaseRequest.GETIDS:
                this.placementDriverService.handleGetIdsRequest((GetIdsRequest) request, closure);
                break;
            case BaseRequest.RETURNIDS:
                this.placementDriverService.handleReturnIdsRequest((ReturnIdsRequest) request, closure);
                break;
            case BaseRequest.CREATE_REGION:
                this.placementDriverService.handleCreateRegionRequest((CreateRegionRequest) request, closure);
                break;
            default:
                throw new RheaRuntimeException("Unsupported request type: " + request.getClass().getName());
        }
    }

    @Override
    public String interest() {
        return reqClazz.getName();
    }

    @Override
    public Executor getExecutor() {
        return this.executor;
    }
}
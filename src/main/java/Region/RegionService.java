package Region;

import Communication.RequestAndResponse.*;
import Element.DTGOperation;
import com.alipay.sofa.jraft.rhea.RequestProcessClosure;
import com.alipay.sofa.jraft.rhea.cmd.store.*;
import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;
import raft.FailoverClosure;

/**
 * @author :jinkai
 * @date :Created in 2019/10/17 20:25
 * @description:
 * @modified By:
 * @version:
 */

public interface RegionService {

    long getRegionId();

    RegionEpoch getRegionEpoch();

    void handleFirstPhase(final FirstPhaseRequest request,
                          final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);

    void handleSecondPhase(final SecondPhaseRequest request,
                          final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);

    void handleSecondRead(final SecondReadRequest request,
                          final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);

    void handleMergeRequest(final MergeRequest request,
                            final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);

    void handleRangeSplitRequest(final RangeSplitRequest request,
                                 final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);

    //void internalFirstPhase(final DTGOperation op, final FailoverClosure closure);
}

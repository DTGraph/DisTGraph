package Region;

import Communication.RequestAndResponse.CommitRequest;
import Communication.RequestAndResponse.TransactionRequest;
import com.alipay.sofa.jraft.rhea.RequestProcessClosure;
import com.alipay.sofa.jraft.rhea.cmd.store.*;
import com.alipay.sofa.jraft.rhea.metadata.RegionEpoch;

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

    void handleTransactionRequest(final TransactionRequest request,
                                  final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);

    void handleMergeRequest(final MergeRequest request,
                            final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);

    void handleRangeSplitRequest(final RangeSplitRequest request,
                                 final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);

    void handleCommitRequest(final CommitRequest request,
                                 final RequestProcessClosure<BaseRequest, BaseResponse<?>> closure);
}
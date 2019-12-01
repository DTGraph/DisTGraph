package tool;

import com.alipay.sofa.jraft.rhea.client.FutureGroup;
import com.alipay.sofa.jraft.rhea.client.FutureHelper;
import com.alipay.sofa.jraft.rhea.util.Maps;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static tool.ObjectAndByte.toObject;

/**
 * @author :jinkai
 * @date :Created in 2019/10/23 10:31
 * @description:
 * @modified By:
 * @version:
 */

public final class DTGFutureHelper {

    public static <K, V> CompletableFuture<Map<K, V>> joinBytes(final FutureGroup<byte[]> futureGroup){
        return joinBytes(futureGroup, 0, new CompletableFuture<>() );
    }

    public static <K, V> CompletableFuture<Map<K, V>> joinBytes(final FutureGroup<byte[]> futureGroup, final int size,
                                                              final CompletableFuture<Map<K, V>> future) {
        CompletableFuture.allOf(futureGroup.toArray()).whenComplete((ignored, throwable) -> {
            if (throwable == null) {
                final Map<K, V> allResult = size > 0 ? Maps.newHashMapWithExpectedSize(size) : Maps.newHashMap();
                for (final CompletableFuture<byte[]> partOf : futureGroup.futures()) {
                    byte[] res = partOf.join();
                    allResult.putAll((Map<K, V>)toObject(res));
                }
                future.complete(allResult);
            } else {
                future.completeExceptionally(throwable);
            }
        });
        return future;
    }


}

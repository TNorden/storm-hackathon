package aatn;

import com.twitter.algebird.HLL;
import com.twitter.algebird.HyperLogLogMonoid;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: tnorden@visiblemeasures.com
 * Date: 7/20/13
 * Time: 3:49 PM
 */
public class RiakStatePutHLL extends BaseStateUpdater<HLLRiakState> {
  @Override
  public void updateState(HLLRiakState riakState, List<TridentTuple> tridentTuples, TridentCollector tridentCollector) {

    HyperLogLogMonoid hyperLogLogMonoid = new HyperLogLogMonoid(12);
    Map<String, HLL> batches = new HashMap<String, HLL>();

    for(TridentTuple t : tridentTuples) {
      String userid = t.getStringByField("userId");
      String word = t.getStringByField("word");
      if (!batches.containsKey(userid)){
        batches.put(userid, hyperLogLogMonoid.zero());
      }
      HLL hll = batches.get(userid);
      batches.put(userid, hll.$plus(hyperLogLogMonoid.create(word.getBytes())));
    }
    for (Map.Entry<String, HLL> entry : batches.entrySet()) {
      riakState.putHLL(entry.getKey(), entry.getValue());
    }
  }
}

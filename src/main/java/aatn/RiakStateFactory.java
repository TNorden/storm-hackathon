package aatn;

import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: tnorden@visiblemeasures.com
 * Date: 7/20/13
 * Time: 3:35 PM
 */
public class RiakStateFactory implements StateFactory {

  private String bucket;
  private Class t;
  public RiakStateFactory(String bucket, Class t) {
    this.bucket = bucket;
    this.t = t;
  }



  @Override
  public State makeState(Map map, IMetricsContext iMetricsContext, int i, int i2) {
    return new HLLRiakState(map, i, i2, bucket);
  }
}

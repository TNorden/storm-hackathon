package aatn;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.DefaultRetrier;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterConfig;
import com.twitter.algebird.HLL;
import com.twitter.algebird.HyperLogLogMonoid;
import org.hackreduce.storm.example.common.Common;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.State;

import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: tnorden@visiblemeasures.com
 * Date: 7/20/13
 * Time: 3:34 PM
 */
public class HLLRiakState implements State {
  private static final Logger LOG = LoggerFactory.getLogger(HLLRiakState.class);

  private String bucket_name;
  private Bucket bucket;

  public HLLRiakState(Map conf, int partitionIndex, int numPartitions, String bucket_name) {
    this.bucket_name = bucket_name;

    try {
      PBClusterConfig clusterConfig = new PBClusterConfig(5);

      PBClientConfig clientConfig = new PBClientConfig.Builder()
          .withPort(Common.getRiakPort())
          .build();

      List<String> hosts = Common.getRiakHosts();
      clusterConfig.addHosts(clientConfig, hosts.toArray(new String[hosts.size()]));

      IRiakClient riakClient = RiakFactory.newClient(clusterConfig);

      bucket = riakClient.createBucket(this.bucket_name)
          .withRetrier(DefaultRetrier.attempts(3))
          .execute();

    } catch (Exception e) {
      throw new RuntimeException("Exception while talking to Riak!", e);
    }
  }

  @Override
  public void beginCommit(Long aLong) {
    LOG.info("begin commit {}", aLong);
  }

  @Override
  public void commit(Long aLong) {
    LOG.info("commit {}", aLong);
  }
  public void putHLL(String key, HLL hll) {
    try {
      HLL current = bucket.fetch(key, HLL.class)
          .withConverter(new HLLConverter(bucket.getName()))
          .withRetrier(DefaultRetrier.attempts(3))
          .execute();
      if (current == null) {
        current = new HyperLogLogMonoid(12).zero();
      }
      HLL updated = current.$plus(hll);
      bucket.store(key, updated)
        .withConverter(new HLLConverter(bucket.getName()))
        .withRetrier(DefaultRetrier.attempts(3))
        .execute();
      LOG.info("update key: {}, estimated size: {}", key, updated.estimatedSize());
    } catch (RiakRetryFailedException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }
}

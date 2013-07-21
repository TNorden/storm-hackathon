package tnorden;

import aatn.HLLConverter;
import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.DefaultRetrier;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.basho.riak.client.raw.pbc.PBClusterConfig;
import com.twitter.algebird.HLL;
import com.twitter.algebird.HyperLogLogMonoid;
import org.hackreduce.storm.example.common.Common;
import scala.collection.JavaConversions;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: tnorden@visiblemeasures.com
 * Date: 7/20/13
 * Time: 11:43 AM
 */
public class LocalRiakTest {
  public static void main(String[] args) {
    try {
      PBClusterConfig clusterConfig = new PBClusterConfig(5);
      PBClientConfig clientConfig = new PBClientConfig.Builder()
          .withPort(Common.getRiakPort())
          .build();
      List<String> hosts = Common.getRiakHosts();
      clusterConfig.addHosts(clientConfig, hosts.toArray(new String[hosts.size()]));

      IRiakClient riakClient = RiakFactory.newClient(clusterConfig);

      Bucket bucket = riakClient.createBucket("aatnfirstbucket")
          .withRetrier(DefaultRetrier.attempts(3))
          .execute();

      HLL fetch1 = bucket.fetch("2Zx3gFaIFfu5pa5TJQ1jVHQJR1D", HLL.class)
          .withConverter(new HLLConverter(bucket.getName()))
          .execute();

      System.out.println("size of fetch1: "+fetch1.estimatedSize());

      HLL fetch2 = bucket.fetch("SHB56Ft2rJEOPZKlSur1rIyb1vF", HLL.class)
          .withConverter(new HLLConverter(bucket.getName()))
          .execute();

      System.out.println("size of fetch2: "+fetch2.estimatedSize());

      HyperLogLogMonoid hllm = new HyperLogLogMonoid(12);
      List<HLL> list = new ArrayList<HLL>();
      list.add(fetch1);
      list.add(fetch2);
      double intersection = hllm.estimateIntersectionSize(JavaConversions.asScalaBuffer(list));
      System.out.println("intersection of fetch1 and fetch2: "+intersection);



    } catch (RiakException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }
}

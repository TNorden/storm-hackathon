package aatn;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.twitter.algebird.HLL;
import org.hackreduce.storm.example.common.Common;
import storm.kafka.StringScheme;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;

import static org.hackreduce.storm.HackReduceStormSubmitter.teamPrefix;

/**
 * Created with IntelliJ IDEA.
 * User: tnorden@visiblemeasures.com
 * Date: 7/20/13
 * Time: 3:33 PM
 */
public class AATNTopology {


  public StormTopology build(String bucket) {
    Config config = new Config();

    // The number of processes to spin up for this job
    config.setNumWorkers(10);

    TridentTopology topology = new TridentTopology();

    TridentKafkaConfig spoutConfig = new TridentKafkaConfig(
        Common.getKafkaHosts(),
        "twitter_spritzer"
    );

    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

    // This tells the spout to start at the very beginning of the data stream
    // If you just want to resume where you left off, remove this line
    spoutConfig.forceStartOffsetTime(-2);

    TridentTopology builder = new TridentTopology();

    builder
        .newStream(teamPrefix("lines"), new TransactionalTridentKafkaSpout(spoutConfig))
        .parallelismHint(6)
        .each(new Fields("str"), new ExtractUserWords(), new Fields("userId", "word"))
        .partitionBy(new Fields("userId"))
        .partitionPersist(new RiakStateFactory(bucket, HLL.class), new Fields("userId", "word"), new RiakStatePutHLL());

    return topology.build();
  }

  public static void main(String[] args) {
    AATNTopology aatnTopology = new AATNTopology();
    Config config = new Config();
    config.setNumWorkers(10);
    config.setNumAckers(10);

    LocalCluster localCluster = new LocalCluster();
    localCluster.submitTopology("AATN-HLLRIAK", config, aatnTopology.build("aatnfirstbucket"));

    Utils.sleep(60000);
    localCluster.killTopology("AATN-HLLRIAK");

    localCluster.shutdown();

//    try {
//      StormSubmitter.submitTopology("AATN-HLLRIAK", config, aatnTopology.build("aatnfirstbucket"));
//    } catch (AlreadyAliveException e) {
//      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//    } catch (InvalidTopologyException e) {
//      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//    }
  }
}

package aatn;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.utils.Utils;
import org.testng.annotations.Test;

/**
 * Created with IntelliJ IDEA.
 * User: tnorden@visiblemeasures.com
 * Date: 7/20/13
 * Time: 5:59 PM
 */
public class AATNTopologyTest {

  @Test
  public void localTest() {
    Config config = new Config();
    AATNTopology aatnTopology = new AATNTopology();
    LocalCluster localCluster = new LocalCluster();

    localCluster.submitTopology("aatn-local", config, aatnTopology.build("aatn-test"));
    Utils.sleep(30000);
    localCluster.killTopology("aatn-local");
    localCluster.shutdown();
  }
}

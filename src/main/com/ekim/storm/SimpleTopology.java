package com.ekim.storm;

/**
 * Created by simpl on 5/6/2017.
 */

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleTopology {
    private static final Logger logger = LoggerFactory.getLogger(SimpleTopology.class);

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        logger.info("Setting spouts and bolts...");
        builder.setSpout("word", new TestWordSpout());
        builder.setBolt("exclaim1", new ExclamationBolt()).shuffleGrouping("word");
        builder.setBolt("exclaim2", new ExclamationBolt()).shuffleGrouping("exclaim1");

        logger.info("Setting configurations...");
        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(3);
        conf.setDebug(false);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
//            Local Storm
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(10000);

            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}
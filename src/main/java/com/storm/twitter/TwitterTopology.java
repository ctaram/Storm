package com.storm.twitter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class TwitterTopology {

    public static void main(String[] args) throws Exception {

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("tweet-spout", new TwitterSpout(), 1);
        topologyBuilder.setBolt("counter", new CountBolt(), 5).shuffleGrouping("tweet-spout");

        Config config = new Config();
        config.setDebug(true);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("TwitterTopology", config, topologyBuilder.createTopology());

        Thread.sleep(30000);

        localCluster.killTopology("TwitterTopology");
        localCluster.shutdown();
    }
}

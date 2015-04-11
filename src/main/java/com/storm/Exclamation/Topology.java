package com.storm.Exclamation;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class Topology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("random-sentence", new RandomSentenceSpout(), 10);
        topologyBuilder.setBolt("ex1", new Bolt(), 3).shuffleGrouping("random-sentence");
        //topologyBuilder.setBolt("ex2", new Bolt(), 2).shuffleGrouping("ex1");

        Config config = new Config();
        config.setDebug(true);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("exclamation", config, topologyBuilder.createTopology());

        Thread.sleep(30000);

        localCluster.killTopology("exclamation");
        localCluster.shutdown();
    }
}

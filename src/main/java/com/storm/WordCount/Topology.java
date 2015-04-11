package com.storm.WordCount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class Topology {
    public static void main(String[] args) throws Exception {

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("name-spout", new Spout(), 5);
        topologyBuilder.setBolt("word-count", new CountBolt(), 10).fieldsGrouping("name-spout", new Fields("name"));
        topologyBuilder.setBolt("reporter", new ReportBolt(), 1).globalGrouping("word-count");

        Config config = new Config();
        config.setNumWorkers(2);
        config.setDebug(true);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("WordCount", config, topologyBuilder.createTopology());

        Thread.sleep(30000);

        localCluster.killTopology("WordCount");
        localCluster.shutdown();
    }
}

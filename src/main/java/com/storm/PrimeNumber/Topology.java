package com.storm.PrimeNumber;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Created by Krishna on 10/04/15.
 */
public class Topology {
    public  static  void main(String args[]) throws InterruptedException {

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("number-spout", new Spout(), 1);
        topologyBuilder.setBolt("prime-number", new PrimeNumberBolt(), 10).shuffleGrouping("number-spout");
        topologyBuilder.setBolt("printer", new PrintBolt(), 1).globalGrouping("prime-number");
        topologyBuilder.setBolt("redisStore",new JedisBolt(),1).globalGrouping("prime-number");
        Config config = new Config();
        config.setNumWorkers(2);
        config.setDebug(false);

        LocalCluster localCluster = new LocalCluster();

        localCluster.submitTopology("prime-number", config, topologyBuilder.createTopology());
    }
}

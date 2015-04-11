package com.storm.WordCount;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

import java.util.Map;

public class ReportBolt extends BaseRichBolt {

    // Jedis jedis;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        //jedis = new Jedis("localhost", 6379);
    }

    @Override
    public void execute(Tuple tuple) {
        String name = tuple.getStringByField("name");
        Integer count = tuple.getIntegerByField("count");
        //jedis.publish("WordCountTopology", name + "|" + Long.toString(count));
        System.out.println(name + " => " + count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}

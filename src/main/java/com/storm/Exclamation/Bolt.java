package com.storm.Exclamation;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import redis.clients.jedis.Jedis;

import java.util.Map;

public class Bolt extends BaseRichBolt {

    OutputCollector outputCollector;
    //RedisConnection<String,String> redis;
    Jedis jedis;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        //RedisClient client = new RedisClient("localhost", 6379);
        jedis = new Jedis("localhost", 6379);
        //redis = client.connect();
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(word).append("!!!");
        outputCollector.emit(tuple, new Values(stringBuilder.toString()));
        //redis.publish("WordCountTopology", stringBuilder.toString() + "|" + Long.toString(30));
        jedis.publish("WordCountTopology", stringBuilder.toString() + "|" + Long.toString(30));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}

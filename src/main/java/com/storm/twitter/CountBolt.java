package com.storm.twitter;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.mortbay.log.Log;

import java.util.Map;

public class CountBolt extends BaseRichBolt {


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple tuple) {
        String text = tuple.getStringByField("tweet");
        Log.info(text);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}

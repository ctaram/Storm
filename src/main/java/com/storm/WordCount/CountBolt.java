package com.storm.WordCount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class CountBolt extends BaseRichBolt {

    OutputCollector outputCollector;
    Map<String, Integer> namesMap;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        this.outputCollector = collector;
        namesMap = new HashMap<String, Integer>();
    }

    @Override
    public void execute(Tuple tuple) {

        String name = tuple.getStringByField("name");

        if (namesMap.get(name) != null) {
            Integer count = namesMap.get(name);
            namesMap.put(name, ++count);
        } else {
            namesMap.put(name, 1);
        }

        outputCollector.emit(new Values(name, namesMap.get(name)));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("name", "count"));
    }
}

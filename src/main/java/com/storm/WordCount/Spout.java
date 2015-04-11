package com.storm.WordCount;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class Spout extends BaseRichSpout {

    SpoutOutputCollector spoutOutputCollector;
    String[] names;
    Random random;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        names = new String[] {"Avinash", "Naveen", "Vishnu", "Arun", "Sundar"};
        random = new Random();
        this.spoutOutputCollector = collector;
    }

    @Override
    public void nextTuple() {
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        String name = names[random.nextInt(names.length)];
        spoutOutputCollector.emit(new Values(name));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("name"));
    }

}

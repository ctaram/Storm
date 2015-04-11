package com.storm.PrimeNumber;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by Krishna on 10/04/15.
 */
public class Spout extends BaseRichSpout {
    int number;
    SpoutOutputCollector spoutOutputCollector;
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        number =0;
        this.spoutOutputCollector = collector;
    }
    @Override
    public void nextTuple() {
        number = number +1;
        spoutOutputCollector.emit(new Values(number));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("number"));
    }


}

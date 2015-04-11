package com.storm.PrimeNumber;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by Krishna on 10/04/15.
 */
public class PrintBolt extends BaseRichBolt{
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        //jedis = new Jedis("localhost", 6379);
    }

    @Override
    public void execute(Tuple tuple) {
        int number = tuple.getIntegerByField("number");
        if(number != 0)
            System.out.println("Prime ="+number);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
    }

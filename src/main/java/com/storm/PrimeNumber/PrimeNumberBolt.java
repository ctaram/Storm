package com.storm.PrimeNumber;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Krishna on 10/04/15.
 */
public class PrimeNumberBolt extends BaseRichBolt{
    OutputCollector outputCollector;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        this.outputCollector = collector;
    }

    @Override
    public void execute(Tuple tuple) {

        int number = tuple.getIntegerByField("number");


        if(isPrime(number))
            outputCollector.emit(new Values(number));
        else
            outputCollector.emit(new Values(0));

    }
    private boolean isPrime(int number)
    {
        if(number % 2 ==0)
            return false;
        for (int l = 2;l<=(number/2);l++)
        {
            if(number % l == 0)
                return false;
        }
        return true;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("number"));
    }

}

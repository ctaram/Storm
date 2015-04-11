package com.storm.twitter;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.storm.utils.SleepUtil;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterSpout extends BaseRichSpout {

    String CUSTOMER_KEY = "ozgxftrZEzpGrhoXWKvLpWbF0";
    String CUSTOMER_SECRET = "hgj2GGzE16f8to01GIAM0g1gbohQnFZKBNxn7cetWRFOVbradG";
    String ACCESS_TOKEN = "16630163-jERNaDVQAhpCOHPDvFm7F4VCW9EhumV3Dg1XLm8vv";
    String ACCESS_SECRET = "xAdeRJABYJICT60RFgHWKWvsIle3MvehHyozcVqsvu6nM";

    TwitterStream twitterStream;
    LinkedBlockingQueue<String> queue;

    SpoutOutputCollector outputCollector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        this.outputCollector = collector;

        queue = new LinkedBlockingQueue<String>(1000);

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder()
                .setOAuthConsumerKey(CUSTOMER_KEY)
                .setOAuthConsumerSecret(CUSTOMER_SECRET)
                .setOAuthAccessToken(ACCESS_TOKEN)
                .setOAuthAccessTokenSecret(ACCESS_SECRET);

        TwitterStreamFactory streamFactory = new TwitterStreamFactory(configurationBuilder.build());

        twitterStream = streamFactory.getInstance();
        twitterStream.addListener(new StatusListener() {

            @Override
            public void onStatus(Status status) {
                HashtagEntity[] hashtagEntities = status.getHashtagEntities();
                for(HashtagEntity hashtagEntity: hashtagEntities){
                    queue.offer(hashtagEntity.getText());
                }
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }

            @Override
            public void onTrackLimitationNotice(int i) {

            }

            @Override
            public void onScrubGeo(long l, long l1) {

            }

            @Override
            public void onStallWarning(StallWarning stallWarning) {

            }

            @Override
            public void onException(Exception e) {
                e.printStackTrace();
            }
        });

        twitterStream.sample();

    }

    @Override
    public void nextTuple() {
        String text = queue.poll();

        if(text == null) {
            SleepUtil.sleep(1000);
        } else {
            outputCollector.emit(new Values(text));
        }

    }

    @Override
    public void close()
    {
        // shutdown the stream - when we are going to exit
        twitterStream.shutdown();
    }

    @Override
    public Map<String, Object> getComponentConfiguration()
    {
        // create the component config
        Config ret = new Config();

        // set the parallelism for this spout to be 1
        ret.setMaxTaskParallelism(1);

        return ret;
    }
}

package com.storm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.*;
import backtype.storm.tuple.Tuple;
import com.model.Image;
import com.util.FeedConfig;
import com.util.JedisClient;
import com.util.JedisClientSingle;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Administrator on 2016/8/1.
 */
public class TBolt implements IBasicBolt {
    private static JedisClient jedisClient;
    private static ExecutorService executorService;
    @Override
    public void prepare(Map map, TopologyContext topologyContext) {
        jedisClient=new JedisClientSingle();
        executorService = Executors.newFixedThreadPool(FeedConfig.MAX_REDIS);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        Image image = (Image) tuple.getValues().get(0);
        List<String> uuid = (List<String>) tuple.getValues().get(1);
        sendMessage(image,uuid);
        throw new FailedException();
    }

    private void sendMessage(final Image image,final List<String> uuids) {
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                for (String uuid : uuids){
                    jedisClient.zadd(uuid+FeedConfig.feed,Double.valueOf((0-image.getCreatedDate().getTime())), String.valueOf(image.getId()));
                }
            }
        };
        executorService.submit(runnable);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}

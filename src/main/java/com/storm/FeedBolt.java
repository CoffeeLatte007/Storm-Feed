package com.storm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.model.Image;
import com.util.FeedConfig;
import com.util.JedisClient;
import com.util.JedisClientSingle;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2016/8/1.
 */
public class FeedBolt implements IBasicBolt {

    private static JedisClient jedisClient;
    @Override
    public void prepare(Map map, TopologyContext topologyContext) {
        jedisClient=new JedisClientSingle();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        Image image = (Image) tuple.getValues().get(0);

        List<String> pushfollowers = findPushFollower(image);
        basicOutputCollector.emit(new Values(image,pushfollowers));
    }

    /**
     * 找到我们应该推送的
     * version-1:直接找所有的followers
     * version-2:找活跃用户和follower的交集
     * @param image
     * @return
     */
    private List<String> findPushFollower(Image image) {
        Map<String,String> map = jedisClient.hgetAll(String.valueOf(image.getUserId())+ FeedConfig.followers);
        List<String> uuids = new ArrayList<>();
        for (Map.Entry<String,String> entry:map.entrySet()){
            uuids.add(entry.getKey());
        }
        return uuids;
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("image","pushUuid"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}

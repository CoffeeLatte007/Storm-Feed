package com.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.client.spout.IAckValueSpout;
import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.model.Image;
import com.rmq.ConsumerFactory;
import com.util.KryoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Administrator on 2016/8/1.
 */
public class FeedSpout implements IRichSpout, MessageListenerConcurrently,IAckValueSpout, IFailValueSpout {
    private SpoutOutputCollector spoutCollector;
    private transient DefaultMQPushConsumer consumer;
    private LinkedBlockingQueue<Image> emitQueue;
    private static final Logger log = LoggerFactory.getLogger(FeedSpout.class);
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("test"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        // TODO Auto-generated method stub
        spoutCollector = spoutOutputCollector;
        try {
            consumer = ConsumerFactory.mkPushConsumerInstance(this);
        } catch (MQClientException e) {
            // TODO Auto-generated catch block
            //logger.error(RaceConfig.LogTracker + "ZY spout failed to create pushconsumer", e);
        }
        emitQueue = new LinkedBlockingQueue<Image>();
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        try {
            Image image = emitQueue.take();
            sendMeessage(image);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    public void sendMeessage(Image image){
        Values values = new Values(image);
        //不需要传输message
        spoutCollector.emit(values);
    }
    @Deprecated
    @Override
    public void ack(Object o) {

    }
    @Deprecated
    @Override
    public void fail(Object o) {

    }

    @Override
    public void ack(Object o, List<Object> list) {

    }

    @Override
    public void fail(Object o, List<Object> list) {
        //失败控制重复发送，但是这里业务需要并不需要控制重复发送
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
        String topic;
        byte[] body;
        for (MessageExt msg : msgs) {
            body = msg.getBody();
            Image image = KryoUtils.readKryoObject(Image.class,body);
//            log.info("得到数据");
//            Image image = KryoUtils.readKryoObject(Image.class,body);
            //可以设置一些不必要的参数为空
            image.setUrl(null);
            try {
                emitQueue.put(image);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
}

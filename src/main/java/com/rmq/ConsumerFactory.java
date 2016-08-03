package com.rmq;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.util.FeedConfig;

public class ConsumerFactory {
	public static synchronized DefaultMQPushConsumer mkPushConsumerInstance(MessageListenerConcurrently listener) throws MQClientException {
		DefaultMQPushConsumer pushConsumer = new DefaultMQPushConsumer("lz");
		
		// 系统自动设置,提交的时候去掉
		 //pushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

		// 正式提交的时候去掉这行
		//pushConsumer.setNamesrvAddr("10.109.247.167:9876");
		// consumer.start();
		pushConsumer.setNamesrvAddr("119.29.75.199:9876");
		pushConsumer.subscribe(FeedConfig.FEED_TOPIC, "*");
		pushConsumer.setConsumeMessageBatchMaxSize(4);
		pushConsumer.registerMessageListener(listener);
		pushConsumer.start();
		return pushConsumer;
	}
}

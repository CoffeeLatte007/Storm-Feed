package com.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeedTopology {
	private static final Logger log = LoggerFactory.getLogger(FeedTopology.class);
	/**
	 *
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Config conf = new Config();
		//关闭ack机制，业务不需要
//		conf.setNumAckers(1);conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 20000);
		conf.setNumAckers(0);
		int spout_Parallelism_hint = 1;
		int process_Parallelism_hint = 1;
		int cal_Parallelism_hint = 1;

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("feedspout", new FeedSpout(), spout_Parallelism_hint).setNumTasks(1);
		builder.setBolt("calfollowers", new FeedBolt(), process_Parallelism_hint).shuffleGrouping("feedspout").setNumTasks(1);
		builder.setBolt("writeRedis", new TBolt(), cal_Parallelism_hint).shuffleGrouping("calfollowers").setNumTasks(1);
		LocalCluster cluster = new LocalCluster();
		conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
		try {
			cluster.submitTopology("SequenceTest", conf, builder.createTopology());
//			StormSubmitter.submitTopology("lizhaoTopology2", conf, builder.createTopology());
			//等待1分钟， 1分钟后会停止拓扑和集群， 视调试情况可增大该数值
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}

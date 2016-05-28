package com.vineet.domain.agg.kafka.storm;

import java.util.Map;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.vineet.domain.agg.util.PropertiesLoader;

public class Invoker {
	public static void main(String[] args) throws Exception {

		Map<String, String> properties = PropertiesLoader.getProperties();

		if (properties.isEmpty())
			throw new Exception("Load Properties got Failed!");

		ZkHosts zkHosts = new ZkHosts(properties.get("zkQuorum"));

		String topic_name = properties.get("topic");
		String consumer_group_id = "2";
		String zookeeper_root = "";
		SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, topic_name, zookeeper_root, consumer_group_id);

		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		kafkaConfig.forceFromStart = true;

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), properties.get("spoutParallelismHint") == null ? 1
				: Integer.parseInt(properties.get("spoutParallelismHint")));
		builder.setBolt("Parser", new ParserBolt(), properties.get("boltParserParallelismHint") == null ? 1
				: Integer.parseInt(properties.get("boltParserParallelismHint"))).globalGrouping("KafkaSpout");
		builder.setBolt("Persist", new RealTimeAggregationBolt(),
				properties.get("boltPersistParallelismHint") == null ? 1
						: Integer.parseInt(properties.get("boltPersistParallelismHint")))
				.fieldsGrouping("Parser", new Fields("key"));

		Config config = new Config();
		config.put(Config.TOPOLOGY_WORKERS, 2);
		config.setNumAckers(2);
		config.setNumWorkers(2);
		config.setMaxSpoutPending(20);
		config.setMaxTaskParallelism(20);

		config.putAll(properties);

		StormSubmitter.submitTopology(properties.get("topologyName"), config, builder.createTopology());
	}
}
package com.vineet.domain.agg.kafka.storm;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.vineet.domain.agg.parser.core.Parser;
import com.vineet.domain.agg.parser.core.ParserFactory;


public class ParserBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;
	private String word = null;
	private Parser parserType = null;
	private Object value = null;
	private Map<String, String> stormConf = null;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		super.prepare(stormConf, context);
		this.stormConf = stormConf;
		
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		
		synchronized(this) {
			
		word = input.getString(0);
		
		if (StringUtils.isBlank(word)) {
			return;
		}

		parserType = ParserFactory.getComponentType(word, stormConf);
		value = parserType.parse(word);
		
		if(value!=null) {
			collector.emit((Values)value);
		}
		
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare( new Fields( "key", "value" ) );
	}
}
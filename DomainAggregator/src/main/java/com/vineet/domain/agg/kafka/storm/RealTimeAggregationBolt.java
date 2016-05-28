package com.vineet.domain.agg.kafka.storm;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.vineet.domain.agg.persist.core.Persist;
import com.vineet.domain.agg.persist.core.PersistRealTimeFactory;
import com.vineet.domain.agg.pojo.EventPOJO;
import com.vineet.domain.agg.util.CommonFunction;
import com.vineet.domain.agg.util.Constant;

public class RealTimeAggregationBolt extends BaseBasicBolt {
	
	private static final long serialVersionUID = 1L;
	private AtomicLong count = new AtomicLong(0);
	private EventPOJO responseHandler = null;
	private CommonFunction comm = null;
	private int flushSize = 50000;
	private List<Persist> persistenceType = null;
	
	
	private static Map<String, Boolean> domainSafeMapping = null;
	private Map<Long, Map<String, Long>> dailySafeUnsafeCounter = null;
	private Map<Long, Map<String, Map<String, String>>> timeDomainAgg = null;
	
	private Map<String,Map<String,String>> domainAgg = null;
	private Map<String, String> domainAttribute = null;

	private int id;
	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		super.cleanup();
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		super.prepare(stormConf, context);
		
		id = context.getThisTaskId();
		
		domainSafeMapping = new HashMap<String, Boolean>();
		dailySafeUnsafeCounter = new HashMap<Long, Map<String,Long>>();
		timeDomainAgg = new HashMap<Long, Map<String,Map<String,String>>>();
		
		domainAgg = new HashMap<String, Map<String,String>>();
		domainAttribute = new HashMap<String, String>();
		
		
		comm = new CommonFunction();
		
		
		flushSize = Integer.parseInt(stormConf.get("recordFlushSize").toString());
		
		try {
			persistenceType= PersistRealTimeFactory.getPersistenceType(stormConf);
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			System.err.println(e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.err.println(e);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		
		responseHandler = (EventPOJO)input.getValueByField("value");

		synchronized (this) {
			
			try {
				
				Long windowEpoch = 0L;

				Calendar cal = Calendar.getInstance();
				cal.setTimeInMillis(responseHandler.getTime().getTime());
				
				if(!domainSafeMapping.containsKey(responseHandler.getDomain())) {
					
					comm.updateDomainMap(domainSafeMapping, responseHandler.getDomain());
					
				}
				
				/////////////////////////////////////////////
				//Aggregation for Daily Safe Unsafe Counters
				////////////////////////////////////////////
				
				Long todayStartDayInEpoch = comm.getTodaysDateStartEpoch(responseHandler.getTime());
				
				if(dailySafeUnsafeCounter.containsKey(todayStartDayInEpoch)) {
					
					boolean isDomainSafe = domainSafeMapping.get(responseHandler.getDomain());
					Map<String, Long> map = dailySafeUnsafeCounter.get(todayStartDayInEpoch);
					
					if(isDomainSafe) {
						
						if(map.containsKey(Constant.SAFE))
							map.put(Constant.SAFE, map.get(Constant.SAFE)+1);
						else
							map.put(Constant.SAFE, 1L);
						
					} else  {
						
						if(map.containsKey(Constant.UNSAFE))
							map.put(Constant.UNSAFE, map.get(Constant.UNSAFE)+1);
						else
							map.put(Constant.UNSAFE, 1L);
					}
					
					dailySafeUnsafeCounter.put(todayStartDayInEpoch, map);
					
				} else {
					
					boolean isDomainSafe = domainSafeMapping.get(responseHandler.getDomain());
					Map<String, Long> map = new HashMap<String, Long>();
					
					if(isDomainSafe) {
						
							map.put(Constant.SAFE, 1L);
						
					} else  {
						
							map.put(Constant.UNSAFE, 1L);
					}
					
					map.put(Constant.COMPONENT_ID, (long)id);
					
					dailySafeUnsafeCounter.put(todayStartDayInEpoch, map);
					
				}
				
				/////////////////////////////////////////////
				//Domain Level Count Aggregation
				/////////////////////////////////////////////
				
				Long timeEpoch = responseHandler.getTime().getTime();
				
				if(!timeDomainAgg.containsKey(timeEpoch)) {
					
					domainAgg = new HashMap<String, Map<String,String>>();
					timeDomainAgg.put(timeEpoch, domainAgg);
					
				} else {
					
					domainAgg = timeDomainAgg.get(timeEpoch);
					
				}
				
				if(!domainAgg.containsKey(responseHandler.getDomain())) {
					
					domainAttribute = new HashMap<String, String>();
					domainAttribute.put(Constant.CATEGORY, domainSafeMapping.get(responseHandler.getDomain())?Constant.CATEGORY_SAFE:Constant.CATEGORY_UNSAFE);
					domainAttribute.put(Constant.SAFE_FLAG, domainSafeMapping.get(responseHandler.getDomain())?Constant.SAFE_FLAG_TRUE:Constant.SAFE_FLAG_FALSE);
					domainAttribute.put(Constant.EVENT_COUNTS, "1");
					
					domainAgg.put(responseHandler.getDomain(), domainAttribute);
					
				} else {
					
					domainAttribute = domainAgg.get(responseHandler.getDomain());
					domainAttribute.put(Constant.EVENT_COUNTS, String.valueOf(Long.parseLong(domainAttribute.get(Constant.EVENT_COUNTS))+1));
					
				} 
				
				
				/////////////////////////////////
				//Persistence
				////////////////////////////////
				
				if((count.incrementAndGet() % flushSize ==0)) {

					
					if((count.get() % (100*flushSize) == 0)) {
						
						Long maxRange = comm.getTimeStampForNDaysBefore(cal, 2);
						timeDomainAgg = comm.removeKeysWithLesserRange(maxRange, timeDomainAgg);
						dailySafeUnsafeCounter = comm.removeKeysWithLesserRange(maxRange, dailySafeUnsafeCounter);
					}
						
					System.out.println("Dumping out Persist data " + timeDomainAgg.keySet() );

					for(Persist persist : persistenceType) {
						persist.persistData(dailySafeUnsafeCounter, timeDomainAgg);
					}
				}
						
			} catch (Exception e) {
				// TODO Auto-generated catch block
				System.err.println(e);
			}
		
	}
		
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}
}
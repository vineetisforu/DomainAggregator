package com.vineet.domain.agg.persist.client;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import com.vineet.domain.agg.persist.core.Persist;
import com.vineet.domain.agg.util.Constant;

public class InfluxRealTimePersister implements Persist{

	private InfluxDB influx = null;
	private String influxDB = null;
	private Map<String, String> globalConfig = null;
	
	public InfluxRealTimePersister(Map<String, String> globalConfig) {
		configure();
		this.globalConfig = globalConfig;
	}
	
	public void configure() {
		
		System.out.println("Making Connection to Influx DB");
		influx = InfluxDBFactory.connect("http://"+globalConfig.get("influx.hostname")+":8086", globalConfig.get("influx.username"), globalConfig.get("influx.password"));
		System.out.println("Connection Established Influx DB " + influx);
		influxDB = globalConfig.get("influx.dbname");
		System.out.println("Creating Database " + influxDB);
		influx.createDatabase(influxDB);
		influx.enableBatch(2000, 100, TimeUnit.MILLISECONDS);
		System.out.println("Created Database " + influxDB);

	}

	public void persistData(Map dailyCounts, Map rollingAggregate) {
		
		Set<Long> time = new HashSet<Long>();
		Set<Long> dailyCountTime = dailyCounts.keySet();
		Set<Long> rollingAggTime = rollingAggregate.keySet();
		

		if(dailyCountTime.size()==0 && rollingAggTime.size()==0)
			return ;
		
		if(rollingAggTime.size()>0)
		time.addAll(rollingAggTime);

		System.out.println("Started Building Influx Writer");
		
		
		BatchPoints batchPoints = BatchPoints.database(influxDB)
				.tag("async", "true").retentionPolicy("default")
				.consistency(ConsistencyLevel.ALL).build();

		influx.write(batchPoints);
 
		
		for(Long daily : dailyCountTime) {
			
			Map<String, Long> dailyCount =  (Map<String, Long>)dailyCounts.get(daily);
			
			Point safePoint = Point.measurement(Constant.SAFE).time(daily+dailyCount.get(Constant.COMPONENT_ID), TimeUnit.MILLISECONDS).
					field("value",dailyCount.get(Constant.SAFE)).build();
			
			influx.write(influxDB, "default", safePoint);
			
			Point unSafePoint = Point.measurement(Constant.UNSAFE).time(daily+dailyCount.get(Constant.COMPONENT_ID), TimeUnit.MILLISECONDS).
					field("value",dailyCount.get(Constant.UNSAFE)).build();
			
			influx.write(influxDB, "default", unSafePoint);
			
		}
		
		for (Long unifiedKey : time) {

			
			if(rollingAggregate.containsKey(unifiedKey)) {
				
				Map<String,Map<String,String>> domainAgg = (Map<String,Map<String,String>>)rollingAggregate.get(unifiedKey);
				
				for(String domain : domainAgg.keySet()) {
					
					Map<String, String> domainAttribute = domainAgg.get(domain);
					
					Point point = Point.measurement(domain)
							.time(unifiedKey, TimeUnit.MILLISECONDS).fields(Collections.<String, Object>unmodifiableMap(domainAttribute)).build();
					
					influx.write(influxDB, "default", point);
					
					}
				}
			}
			
		}
		
	}

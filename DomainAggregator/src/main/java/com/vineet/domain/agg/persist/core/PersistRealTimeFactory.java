package com.vineet.domain.agg.persist.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.vineet.domain.agg.persist.client.InfluxRealTimePersister;

public class PersistRealTimeFactory {

	public static synchronized List<Persist> getPersistenceType(Map<String, String> stormConf) throws Exception {
		
			List<Persist> persistence = new ArrayList<Persist>();
			
			String persistRTType = stormConf.get("persistRTType");
			
			if(persistRTType==null)
				throw new Exception("persistType not found - Please specify one or more of persistence type(influx, hbase, elastic, etc)");
		
			if(persistRTType.equalsIgnoreCase("influx") || 
					persistRTType.startsWith("influx") ||
					persistRTType.contains("influx"))
			{
				persistence.add((Persist)new InfluxRealTimePersister(stormConf));
			}
	
					if(persistence!=null && persistence.size()>0){
						return persistence;
					}
			System.out.println("Returning Null");	
			return null;
	}
}

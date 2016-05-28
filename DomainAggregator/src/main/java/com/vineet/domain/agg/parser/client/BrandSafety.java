package com.vineet.domain.agg.parser.client;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import backtype.storm.tuple.Values;

import com.vineet.domain.agg.parser.core.Parser;
import com.vineet.domain.agg.pojo.EventPOJO;

public class BrandSafety implements Parser {

	private JSONObject json = null;
	private JSONParser parser = null;
	private DateFormat format = null;
	private EventPOJO responseHandler = null;
	private Map<String, String> stormConf = null;
	
	public BrandSafety(Map<String, String> stormConf) {
		this.stormConf = stormConf;
		configure();
	}
	
	public void configure() {
		// TODO Auto-generated method stub
		format = new SimpleDateFormat(stormConf.get("dateTimeFormat").toString());
		parser = new JSONParser();
	}

	public Values parse(Object o) {
		
		if(o !=null ) {
			
			try {
				json = (JSONObject) parser.parse(o.toString());
				
				responseHandler = new EventPOJO();
				responseHandler.setTime(new Date(Long.parseLong(json.get("timestamp").toString())*1000));
				responseHandler.setDate(format.format(new Date(Long.parseLong(json.get("timestamp").toString())*1000)));
				responseHandler.setDomain(json.get("domain").toString());
				responseHandler.setType(json.get("type").toString());
				responseHandler.setSession_id(json.get("session_id").toString());
				
				return new Values(String.valueOf(responseHandler.getDate()+responseHandler.getDomain()) , responseHandler);
				
			} catch (ParseException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return null;
	}

}

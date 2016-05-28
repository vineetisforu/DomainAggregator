package com.vineet.domain.agg.parser.core;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.vineet.domain.agg.parser.client.BrandSafety;

public class ParserFactory {

	private static Map<String, Parser> parserType = new HashMap<String, Parser>();
	private static JSONParser parser = new JSONParser();

	public static Parser getComponentType(Object jsonString, Map<String, String> stormConf) {

		try {
			
			synchronized (ParserFactory.class) {
				
				
				if(!parserType.containsKey("brand-safety"))
					parserType.put("brand-safety", new BrandSafety(stormConf));
				
				return parserType.get("brand-safety");
				
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

}

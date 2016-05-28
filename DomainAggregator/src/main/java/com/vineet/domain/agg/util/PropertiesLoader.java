package com.vineet.domain.agg.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropertiesLoader {

	public static Map<String, String> prop ;
	
	public static Map<String, String> getProperties() {
		
		Properties properties = new Properties();
		prop = new HashMap<String, String>();
		
		try {
			
			InputStream is = new FileInputStream("job.properties");
			properties.load(is);
			
			for(Object key : properties.keySet()) {
				prop.put(key.toString(), properties.get(key).toString());
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return prop;
	}
}

package com.vineet.domain.agg.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import com.squareup.okhttp.internal.http.RealResponseBody;

public class CommonFunction {

	private JSONObject json = null;
	private JSONParser parser = null;
	
	public CommonFunction() {
		// TODO Auto-generated constructor stub
		parser = new JSONParser();
	}
	
	public static Long getTimeStampForNDaysBefore(Calendar cal, int days) {
		
		cal.add(Calendar.DATE, -days);
		
		cal.set(Calendar.MILLISECOND, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.HOUR_OF_DAY, 24);
		
		return cal.getTime().getTime();
		
	}
	
	
	public static Map removeKeysWithLesserRange(Long maxRange, Map map) {
		
		Iterator<Map.Entry> iter = map.entrySet().iterator();
		
		while (iter.hasNext()) {
			
		    Map.Entry entry = iter.next();
		    
		    if(Long.parseLong(entry.getKey().toString())<maxRange){
		        iter.remove();
		    }
		}
		
		return map;
	}
	
	public static String getDateFromString(String stringContainingDate) {
		
		Matcher m = Pattern.compile("(19|20)\\d\\d[- /.](0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01])").matcher(stringContainingDate);
		  if (m.find()) {
		    return m.group();
		  }
		  
		return null;
	}
	
	public Response getDomainType(String domain, String url) throws IOException {
		
		OkHttpClient client = new OkHttpClient();

		Request request = new Request.Builder()
		  .url(url + domain)
		  .get()
		  .addHeader("cache-control", "no-cache")
		  .addHeader("postman-token", "c22f55a0-cf64-cc41-66d1-a9280e9af600")
		  .build();

		Response response = client.newCall(request).execute();
		return response;
		
	}
	
	public void updateDomainMap(Map<String, Boolean> domainSafeMapping, String domain) {
		
		
		Response response;
		try {
			response = getDomainType(domain, Constant.DOMAIN_URL);
			RealResponseBody rrb = (RealResponseBody)response.body();
			StringWriter writer = new StringWriter();
			IOUtils.copy(rrb.byteStream(), writer, "UTF-8");
			json = (JSONObject) parser.parse(writer.toString());
			domainSafeMapping.put(domain, json.get("category").toString().toLowerCase().equals("other")?true:false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Long getTodaysDateStartEpoch(Date date) {
		
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(date.getTime());
		
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.HOUR, 0);
		
		return cal.getTimeInMillis();
		
	}
	
}

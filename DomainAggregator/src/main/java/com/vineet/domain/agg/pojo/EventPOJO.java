package com.vineet.domain.agg.pojo;

import java.io.Serializable;
import java.util.Date;

public class EventPOJO implements Serializable {

	private static final long serialVersionUID = 1L;
	private Date time = null ;
	private String date = null;
	private String domain = null;
	private String type = null;
	private String session_id = null;
	
	public Date getTime() {
		return time;
	}
	public void setTime(Date time) {
		this.time = time;
	}
	
	public String getDomain() {
		return domain;
	}
	public void setDomain(String domain) {
		this.domain = domain;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getSession_id() {
		return session_id;
	}
	public void setSession_id(String session_id) {
		this.session_id = session_id;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	
	
}

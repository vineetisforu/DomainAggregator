package com.vineet.domain.agg.parser.core;


public interface Parser {

	public void configure();
	public Object parse(Object o);
	
}

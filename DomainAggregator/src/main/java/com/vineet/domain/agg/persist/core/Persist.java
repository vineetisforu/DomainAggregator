package com.vineet.domain.agg.persist.core;

import java.util.Map;

public interface Persist {

	public void configure();
	public void persistData(Map o, Map o2);
}

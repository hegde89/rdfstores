package edu.unika.aifb.graphindex.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;

public class StatisticsCollector {
	private List<Timings> m_timings;
	
	private final static Logger log = Logger.getLogger(StatisticsCollector.class);
	
	public StatisticsCollector() {
		m_timings = new Vector<Timings>();
	}
	
	public void addTimings(Timings t) {
		m_timings.add(t);
	}
	
	public void logStats() {
		long[] timings = new long[20];
		for (Timings t : m_timings) {
			for (int i = 0; i < t.getTimings().length; i++)
				timings[i] += t.getTimings()[i];
		}
		log.debug("time spent");
		log.debug(" rcp: " + (timings[Timings.RCP] / 1000.0));
		log.debug(" join matching: " + (timings[Timings.MATCH] / 1000.0));
		log.debug(" mapgen: " + (timings[Timings.MAPGEN] / 1000.0));
		log.debug(" ground terms: " + (timings[Timings.GT] / 1000.0));
		log.debug(" retrieving data: " + (timings[Timings.DATA] / 1000.0));
		log.debug(" subject filter: " + (timings[Timings.SUBJECT_FILTER] / 1000.0));
		log.debug(" result joining: " + (timings[Timings.JOIN] / 1000.0));
		log.debug(" building result set: " + (timings[Timings.RS] / 1000.0));
	}
	
	public void reset() {
		for (Timings t : m_timings) 
			t.reset();
	}
}

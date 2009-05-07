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
		int[] counts = new int[20];
		for (Timings t : m_timings) {
			for (int i = 0; i < t.getTimings().length; i++) {
				timings[i] += t.getTimings()[i];
				counts[i] += t.getCounts()[i];
			}
			
		}
		log.debug("time spent");
//		log.debug(" rcp: " + (timings[Timings.RCP]));
//		log.debug(" setup: " + timings[Timings.SETUP] + " " + counts[Timings.SETUP]);
		log.debug(" join matching: " + (timings[Timings.MATCH]) + " " + counts[Timings.MATCH]);
//		log.debug(" ml: " + (timings[Timings.ML]) + " " + counts[Timings.ML]);
//		log.debug(" mapgen: " + (timings[Timings.MAPGEN]));
		log.debug(" ground terms: " + (timings[Timings.GT]) + " " + counts[Timings.GT]);
		log.debug(" extsetup: " + timings[Timings.EXTSETUP] + " " + counts[Timings.EXTSETUP]);
		log.debug(" retrieving data: " + (timings[Timings.DATA]) + " " + counts[Timings.DATA]);
//		log.debug(" retrieving extension data: " + (timings[Timings.DATA_E]) + " " + counts[Timings.DATA_E]);
//		log.debug(" subject filter: " + (timings[Timings.SUBJECT_FILTER]));
//		log.debug(" sorting: " + (timings[Timings.TABLESORT]) + " " + counts[Timings.TABLESORT]);
		log.debug(" result joining: " + (timings[Timings.JOIN]) + " " + counts[Timings.JOIN]);
		log.debug(" uc: " + (timings[Timings.UC]) + " " + counts[Timings.UC]);
//		log.debug(" building result set: " + (timings[Timings.RS]) + " " + counts[Timings.RS]);
	}
	
	public long[] getConsolidated() {
		long[] timings = new long[20];
		int[] counts = new int[20];
		for (Timings t : m_timings) {
			for (int i = 0; i < t.getTimings().length; i++) {
				timings[i] += t.getTimings()[i];
				counts[i] += t.getCounts()[i];
			}
			
		}
		
		return timings;
	}
	
	public void reset() {
		for (Timings t : m_timings) 
			t.reset();
	}
}

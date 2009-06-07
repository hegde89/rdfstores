package edu.unika.aifb.graphindex.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.util.Counters;

public class StatisticsCollector {
	private List<Timings> m_timings;
	private List<Counters> m_counters;
	
	private final static Logger log = Logger.getLogger(StatisticsCollector.class);
	
	public StatisticsCollector() {
		m_timings = new Vector<Timings>();
		m_counters = new Vector<Counters>();
	}
	
	public void addTimings(Timings t) {
		m_timings.add(t);
	}
	
	public void addCounters(Counters c) {
		m_counters.add(c);
	}
	
	public void logStats() {
		long[] timings = new long[Timings.stats.size()];

		for (Timings t : m_timings) {
			for (int i = 0; i < t.getTimings().length; i++) {
				timings[i] += t.getTimings()[i];
			}
		}
		
		long[] counts = new long [Counters.stats.size()];
		for (Counters c : m_counters) {
			for (int i = 0; i < c.getCounts().length; i++) {
				counts[i] += c.getCounts()[i];
			}
		}
		
		log.debug("time spent");
		for (Stat s : Timings.stats) {
			log.debug(" " + s.name + "\t" + timings[s.idx]);
		}
		
		log.debug("counters");
		for (Stat s : Counters.stats) {
			log.debug(" " + s.name + "\t" + counts[s.idx]);
		}
		
	}
	
	public long[] getConsolidated() {
		long[] timings = new long[20];

		for (Timings t : m_timings) {
			for (int i = 0; i < t.getTimings().length; i++) {
				timings[i] += t.getTimings()[i];
			}
			
		}
		
		long[] counts = new long [20];
		for (Counters c : m_counters) {
			for (int i = 0; i < c.getCounts().length; i++) {
				counts[i] += c.getCounts()[i];
			}
		}
		
		return timings;
	}
	
	public void consolidate(Timings timings, Counters counters) {
		for (Timings t : m_timings) {
			for (Stat s : Timings.stats)
				timings.set(s, timings.get(s) + t.get(s));
		}
		
		for (Counters c : m_counters) {
			for (Stat s : Counters.stats)
				counters.set(s, counters.get(s) + c.get(s));
		}
	}
	
	public void reset() {
		for (Timings t : m_timings) 
			t.reset();
		for (Counters c : m_counters) 
			c.reset();
	}
	
	public void clear() {
		m_timings.clear();
		m_counters.clear();
	}
}

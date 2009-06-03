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
		
		log.debug("time spent");
		log.debug(" load data list: " + (timings[Timings.LOAD_DATA_LIST]));
		log.debug(" load data set:  " + (timings[Timings.LOAD_DATA_SET]));
		log.debug(" load ht:        " + (timings[Timings.LOAD_HT]));
		log.debug(" load it:        " + (timings[Timings.LOAD_IT]));
		log.debug(" load its:       " + (timings[Timings.LOAD_ITS]));
		log.debug(" join merge:     " + (timings[Timings.JOIN_MERGE]));
		log.debug(" tbl sort:       " + (timings[Timings.TBL_SORT]));
		log.debug(" im purge:       " + (timings[Timings.IM_PURGE]));
		log.debug(" dm filter:      " + (timings[Timings.DM_FILTER]));
		log.debug(" dm classes:     " + (timings[Timings.DM_CLASSES]));
		log.debug(" kw entity:      " + (timings[Timings.KW_ENTITY_SEARCH]));
		log.debug(" kw asm:         " + (timings[Timings.KW_ASM]));
		log.debug(" IM total:       " + (timings[Timings.STEP_IM]));
		log.debug(" DM total:       " + (timings[Timings.STEP_DM]));
		log.debug(" query total:    " + (timings[Timings.TOTAL_QUERY_EVAL]));
		
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
	
	public void reset() {
		for (Timings t : m_timings) 
			t.reset();
	}
}

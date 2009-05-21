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
//		public static final int LOAD_DATA_LIST = 0;
//		public static final int LOAD_DATA_SET = 1;
//		public static final int LOAD_HT = 2;
//		public static final int LOAD_IT = 3;
//		public static final int LOAD_EXT_OBJECT = 10;
//		public static final int LOAD_EXT_SUBJECT = 11;
//		public static final int JOIN_MERGE = 4;
//		public static final int JOIN_HASH = 5;
//		public static final int TBL_SORT = 6;
//		public static final int TBL_MERGE = 7;
//		public static final int IM_PURGE = 8;
//		public static final int DM_FILTER = 9;
//		public static final int DM_CLASSES = 14;
//		
//		public static final int STEP_IM = 12;
//		public static final int STEP_DM = 13;
		
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
		log.debug(" IM total:       " + (timings[Timings.STEP_IM]));
		log.debug(" DM total:       " + (timings[Timings.STEP_DM]));
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

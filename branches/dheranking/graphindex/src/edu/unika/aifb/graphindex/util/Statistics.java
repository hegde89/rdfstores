package edu.unika.aifb.graphindex.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

public class Statistics {
	private static abstract class AbstractStat {
		protected String m_name;
		protected int m_idx;
		public AbstractStat(String name, int idx) {
			m_name = name;
			m_idx = idx;
		}
	}
	
	public static class Timing extends AbstractStat {
		private Timing(String name, int idx) {
			super(name, idx);
		}
		
		public static final Timing HY_SEARCH = addTiming("hy_search");
		public static final Timing HY_EXPLORE = addTiming("hy_explore");
		public static final Timing EX_NEIGHBORS = addTiming("ex_neighbors");
		public static final Timing EX_NEXTCURSOR = addTiming("ex_nextcursor");
		public static final Timing EX_TOPK_COMBINATIONS = addTiming("ex_topk_combinations");
		public static final Timing EX_TOPK_SUBGRAPH = addTiming("ex_topk_subgraph");
		public static final Timing EX_TOPK_SUBGRAPH_CREATION = addTiming("ex_topk_subgraph_creation");
		public static final Timing EX_TOPK_SUBGRAPH_ISO = addTiming("ex_topk_subgraph_iso");
		public static final Timing EX_SUBGRAPH_CURSORS = addTiming("ex_subgraph_cursors");
		public static final Timing EX_SUBGRAPH_CURSORS_GRAPH = addTiming("ex_subgraph_cursors_graph");
		public static final Timing EX_SUBGRAPH_ALLOWED_CHECK = addTiming("ex_subgraph_allowed_check");
		public static final Timing EX_KWCURSOR_NEXT = addTiming("ex_kwcursor_next");
		public static final Timing EX_NODECURSOR_NEXT = addTiming("ex_nodecursor_next");
	}
	
	public static class Counter extends AbstractStat {
		private Counter(String name, int idx) {
			super(name, idx);
		}
		
		public static final Counter EX_NODECURSORS = addCounter("ex_node_cursors");
		public static final Counter EX_TOPK_COMBINATIONS = addCounter("ex_subgraph_topk_combs");
		public static final Counter EX_SUBGRAPH_CREATED = addCounter("ex_subgraph_created");
		public static final Counter EX_SUBGRAPH_INVALID1 = addCounter("ex_subgraph_invalid1");
		public static final Counter EX_SUBGRAPH_INVALID2 = addCounter("ex_subgraph_invalid2");
		public static final Counter EX_SUBGRAPH_INVALID3 = addCounter("ex_subgraph_invalid3");
		public static final Counter EX_SUBGRAPH_INVALID4 = addCounter("ex_subgraph_invalid4");
		public static final Counter ISO_ALL = addCounter("iso_all");
		public static final Counter ISO_CHECK = addCounter("iso_check");
		public static final Counter ISO_END = addCounter("iso_end");
	}
	
	private static final List<AbstractStat> m_stats = new ArrayList<AbstractStat>();
	private static int m_statIdx = 0;
	
	private static Timing addTiming(String name) {
		Timing t = new Timing(name, m_statIdx++);
		m_stats.add(t);
		return t;
	}

	private static Counter addCounter(String name) {
		Counter c = new Counter(name, m_statIdx++);
		m_stats.add(c);
		return c;
	}
	
	private static double[] m_consolidated = new double[m_stats.size()];
	private static Map<Object,double[]> m_statValues = new HashMap<Object,double[]>();
	private static Map<Object,double[]> m_running = new HashMap<Object,double[]>();
	
	private static final Logger log = Logger.getLogger(Statistics.class);
	
	static {
		Counter c = new Counter("", -1);
		Timing t = new Timing("", -1);
	}

	public static void start(Object object, Timing timing) {
		synchronized (object) {
			double[] running = m_running.get(object);
			if (running == null) {
				running = new double[m_stats.size()];
				m_running.put(object, running);
				m_statValues.put(object, new double[m_stats.size()]);
			}
			running[timing.m_idx] = System.currentTimeMillis();
		}
	}
	
	public static void end(Object object, Timing timing) {
		synchronized (object) {
			double[] running = m_running.get(object);
			double[] values = m_statValues.get(object);

			values[timing.m_idx] += System.currentTimeMillis() - running[timing.m_idx];
		}
	}
	
	public static void finish(Object object) {
		synchronized (object) {
			double[] values = m_statValues.get(object);
			for (int i = 0; i < values.length; i++)
				m_consolidated[i] += values[i];
			m_statValues.remove(object);
			m_running.remove(object);
		}
	}
	
	public static void inc(Object object, Counter counter) {
		synchronized (object) {
			double[] values = m_statValues.get(object);
			if (values == null) {
				values = new double[m_stats.size()];
				m_statValues.put(object, values);
			}
			values[counter.m_idx]++;
		}
	}

	public static void inc(Object object, Counter counter, double amount) {
		synchronized (object) {
			double[] values = m_statValues.get(object);
			values[counter.m_idx] += amount;
		}
	}

	public static void reset() {
		m_consolidated = new double[m_stats.size()];
		m_statValues.clear();
		m_running.clear();
	}
	
	public static void print() {
		double[] consolidated = new double[m_stats.size()];
		
		for (double[] values : m_statValues.values()) {
			for (int i = 0; i < values.length; i++)
				consolidated[i] += values[i];
		}
		
		for (int i = 0; i < m_consolidated.length; i++)
			consolidated[i] += m_consolidated[i];
		
		for (AbstractStat stat : m_stats) {
			log.info(stat.m_name + ":\t" + consolidated[stat.m_idx]);
		}
	}
}

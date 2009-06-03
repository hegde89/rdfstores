package edu.unika.aifb.graphindex.util;

import java.util.ArrayList;
import java.util.List;

public class Counters {
	long[] counts = new long [20];

	public final static List<Stat> stats = new ArrayList<Stat>();
	
	private static Stat addStat(String name) {
		Stat stat = new Stat(stats.size(), name);
		stats.add(stat);
		return stat;
	}
	
	public static final Stat QUERY_EDGES = addStat("query_edges");
	public static final Stat QUERY_NODES = addStat("query_nodes");
	public static final Stat QUERY_DEFERRED_EDGES = addStat("query_deferred_edges");
	
	public static final Stat ES_RESULT_SIZE = addStat("es_result_size");
	public static final Stat ES_PROCESSED_EDGES = addStat("es_processed_edges");
	
	public static final Stat ASM_INDEX_MATCHES = addStat("asm_index_matches");
	public static final Stat ASM_RESULT_SIZE = addStat("asm_result_size");
	
	public static final Stat IM_INDEX_MATCHES = addStat("im_index_matches");
	public static final Stat IM_PROCESSED_EDGES = addStat("im_processed_edges");
	
	public static final Stat DM_REM_EDGES = addStat("dm_rem_edges");
	public static final Stat DM_REM_NODES = addStat("dm_rem_nodes");
	public static final Stat DM_PROCESSED_EDGES = addStat("dm_processed_edges");
	
	public static final Stat RESULTS = addStat("result_size");

	
	public Counters() {
		
	}
	
	public void inc(Stat c) {
		counts[c.idx]++;
	}
	
	public void inc(Stat c, int amount) {
		counts[c.idx] += amount;
	}
	
	public void set(Stat c, int val) {
		counts[c.idx] = val;
	}

	public long[] getCounts() {
		return counts;
	}
}

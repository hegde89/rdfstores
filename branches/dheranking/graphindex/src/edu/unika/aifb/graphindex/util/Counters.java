package edu.unika.aifb.graphindex.util;

/**
 * Copyright (C) 2009 GŸnter Ladwig (gla at aifb.uni-karlsruhe.de)
 * 
 * This file is part of the graphindex project.
 *
 * graphindex is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2
 * as published by the Free Software Foundation.
 * 
 * graphindex is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with graphindex.  If not, see <http://www.gnu.org/licenses/>.
 */

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

public class Counters {

	double[] counts = new double [20];

	public final static List<Stat> stats = new ArrayList<Stat>();
	
	private static Stat addStat(String name) {
		Stat stat = new Stat(stats.size(), name);
		stats.add(stat);
		return stat;
	}
	
	public static final Stat QUERY_EDGES = addStat("query_edges");
	public static final Stat QUERY_NODES = addStat("query_nodes");
	public static final Stat QUERY_DEFERRED_EDGES = addStat("query_deferred_edges");
	public static final Stat KWQUERY_KEYWORDS = addStat("query_keywords");
	public static final Stat KWQUERY_NODE_KEYWORDS = addStat("query_node_keywords");
	public static final Stat KWQUERY_EDGE_KEYWORDS = addStat("query_edge_keywords");
	
	public static final Stat ES_CUTOFF = addStat("es_cutoff");
	public static final Stat ES_RESULT_SIZE = addStat("es_result_size");
	public static final Stat ES_PROCESSED_EDGES = addStat("es_processed_edges");
	
	public static final Stat ASM_INDEX_MATCHES = addStat("asm_index_matches");
	public static final Stat ASM_RESULT_SIZE = addStat("asm_result_size");
	
	public static final Stat QT_QUERIES = addStat("qt_queries");
	public static final Stat QT_QUERY_EDGES = addStat("qt_query_edges");
	
	public static final Stat IM_INDEX_MATCHES = addStat("im_index_matches");
	public static final Stat IM_PROCESSED_EDGES = addStat("im_processed_edges");
	public static final Stat IM_RESULT_SIZE = addStat("im_result_size");
	
	public static final Stat INC_PRCS_ES = addStat("inc_prcs_es");
	public static final Stat INC_PRCS_ASM = addStat("inc_prcs_asm");
	public static final Stat INC_PRCS_SBR = addStat("inc_prcs_sbr");
	
	public static final Stat DM_REM_EDGES = addStat("dm_rem_edges");
	public static final Stat DM_REM_NODES = addStat("dm_rem_nodes");
	public static final Stat DM_PROCESSED_EDGES = addStat("dm_processed_edges");
	
	public static final Stat RESULTS = addStat("result_size");

	private static final Logger log = Logger.getLogger(Counters.class);
	
	public Counters() {
		counts = new double [stats.size()];
	}
	
	public void inc(Stat c) {
		counts[c.idx]++;
	}
	
	public void inc(Stat c, double amount) {
		counts[c.idx] += amount;
	}
	
	public void set(Stat c, double val) {
		counts[c.idx] = val;
	}

	public double[] getCounts() {
		return counts;
	}

	public double get(Stat s) {
		return counts[s.idx];
	}

	public void reset() {
		counts = new double [counts.length];
	}
	
	public void logStats() {
		logStats(stats);
	}
	

	public void logStats(List<Stat> _stats) {
		log.debug("counters");
		for (Stat s : _stats) {
			log.debug(" " + s.name + "\t" + counts[s.idx]);
		}
	}
}

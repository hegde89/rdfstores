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

public class Timings {
	public final static List<Stat> stats = new ArrayList<Stat>();
	
	private static Stat addStat(String name) {
		Stat stat = new Stat(stats.size(), name);
		stats.add(stat);
		return stat;
	}

	private long[] starts = new long[20];
	private long[] timings = new long[20];
	
	public static final Stat ASM_REACHABLE = addStat("asm_reachable");
	
	public static final Stat LOAD_DATA_LIST = addStat("load_data_list");
	public static final Stat LOAD_DATA_SET = addStat("load_data_set");
	public static final Stat LOAD_DATA_ITEM = addStat("load_data_item");
	public static final Stat LOAD_HT = addStat("load_ht");
	public static final Stat LOAD_IT = addStat("load_it");
	public static final Stat LOAD_ITS = addStat("load_its");
	public static final Stat LOAD_EXT_OBJECT = addStat("load_ext_object");
	public static final Stat LOAD_EXT_SUBJECT = addStat("load_ext_subject");
	public static final Stat JOIN_MERGE = addStat("join_merge");
	public static final Stat JOIN_HASH = addStat("join_hash");
	public static final Stat TBL_SORT = addStat("tbl_sort");
	public static final Stat TBL_MERGE = addStat("tbl_merge");
	public static final Stat IM_PURGE = addStat("im_purge");
	public static final Stat DM_FILTER = addStat("dm_filter");
	public static final Stat DM_CLASSES = addStat("dm_classes");
	
	public static final Stat STEP_ES = addStat("step_es");
	public static final Stat STEP_ASM = addStat("step_asm");
	public static final Stat STEP_ASM2IM = addStat("step_asm2im");
	public static final Stat STEP_KWSEARCH = addStat("step_kwsearch");
	public static final Stat STEP_EXPLORE = addStat("step_explore");
	public static final Stat STEP_IQA = addStat("step_iqa_query");
	public static final Stat STEP_QA = addStat("step_qa_query");
	public static final Stat STEP_IM = addStat("step_im");
	public static final Stat STEP_IM2DM = addStat("step_im2dm");
	public static final Stat STEP_DM = addStat("step_dm");
	
	public static final Stat VP_LOAD = addStat("vp_load");
	
	public static final Stat TOTAL_QUERY_EVAL = addStat("total_query");
	
	private static final Logger log = Logger.getLogger(Timings.class);
	
	public Timings() {
		starts = new long [stats.size()];
		timings = new long [stats.size()];
	}
	
	public void start(Stat timer) {
		starts[timer.idx] = System.currentTimeMillis();
	}
	
	public void end(Stat timer) {
		timings[timer.idx] += System.currentTimeMillis() - starts[timer.idx];
	}
	
	public void set(Stat timer, long val) {
		timings[timer.idx] = val;
	}
	
	public long get(Stat timer) {
		return timings[timer.idx];
	}
	
	public long[] getTimings() {
		return timings;
	}

	public void reset() {
		for (int i = 0; i < starts.length; i++) {
			starts[i] = timings[i] = 0;
		}
	}
	
	public void logStats() {
		logStats(stats);
	}
	
	public void logStats(List<Stat> _stats) {
		log.debug("time spent");
		for (Stat s : _stats) {
			log.debug(" " + s.name + "\t" + timings[s.idx]);
		}
	}
}

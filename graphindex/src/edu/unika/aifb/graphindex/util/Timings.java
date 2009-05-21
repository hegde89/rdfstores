package edu.unika.aifb.graphindex.util;

public class Timings {
	private long[] starts = new long[20];
	private long[] timings = new long[20];
	private int[] counts = new int[20];
	public static final int LOAD_DATA_LIST = 0;
	public static final int LOAD_DATA_SET = 1;
	public static final int LOAD_HT = 2;
	public static final int LOAD_IT = 3;
	public static final int LOAD_ITS = 16;
	public static final int LOAD_EXT_OBJECT = 10;
	public static final int LOAD_EXT_SUBJECT = 11;
	public static final int JOIN_MERGE = 4;
	public static final int JOIN_HASH = 5;
	public static final int TBL_SORT = 6;
	public static final int TBL_MERGE = 7;
	public static final int IM_PURGE = 8;
	public static final int DM_FILTER = 9;
	public static final int DM_CLASSES = 14;
	
	public static final int STEP_IM = 12;
	public static final int STEP_DM = 13;
	
	public static final int VP_LOAD = 15;
	
	public Timings() {
		
	}
	
	public void start(int timer) {
		starts[timer] = System.currentTimeMillis();
		counts[timer]++;
	}
	
	public void end(int timer) {
		timings[timer] += System.currentTimeMillis() - starts[timer];
	}

	public long[] getTimings() {
		return timings;
	}

	public void reset() {
		for (int i = 0; i < starts.length; i++) {
			starts[i] = timings[i] = counts[i] = 0;
		}
	}

	public int[] getCounts() {
		return counts;
	}
}

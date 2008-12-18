package edu.unika.aifb.graphindex.util;

public class Timings {
	private long[] starts = new long[20];
	private long[] timings = new long[20];
	private int[] counts = new int[20];
	public static final int DATA = 0;
	public static final int JOIN = 1;
	public static final int ML = 2;
	public static final int SETUP = 3;
	public static final int MATCH = 4;
	public static final int UC = 5;
	public static final int GT = 6;
	public static final int EXTSETUP = 7;
	public static final int TABLEMERGE = 11;
	public static final int TABLESORT = 12;
	public static final int DATA_E = 13;
	
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

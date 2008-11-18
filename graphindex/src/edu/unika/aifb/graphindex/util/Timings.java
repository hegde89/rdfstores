package edu.unika.aifb.graphindex.util;

public class Timings {
	private long[] starts = new long[20];
	private long[] timings = new long[20];
	public static final  int DATA = 0, JOIN = 1, RS = 3, MATCH = 4;
	public static final int GT = 6;
	public static final int RCP = 7;
	public static final int MAPGEN = 8;
	public static final int SUBJECT_FILTER = 9;
	
	public Timings() {
		
	}
	
	public void start(int timer) {
		starts[timer] = System.currentTimeMillis();
	}
	
	public void end(int timer) {
		timings[timer] += System.currentTimeMillis() - starts[timer];
	}

	public long[] getTimings() {
		return timings;
	}

	public void reset() {
		for (int i = 0; i < starts.length; i++)
			starts[i] = timings[i] = 0;
	}
}

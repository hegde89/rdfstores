package edu.unika.aifb.graphindex;

public class Timings {
	private long[] starts = new long[10];
	private long[] timings = new long[10];
	public static final  int DATA = 0, JOIN = 1, MAPPING = 2, RS = 3, MATCH = 4;
	
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
}

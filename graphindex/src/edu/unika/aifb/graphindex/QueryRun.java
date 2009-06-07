package edu.unika.aifb.graphindex;

import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.Timings;

public class QueryRun {
	private String m_name;
	private String m_system;
	private Timings m_timings;
	private Counters m_counters;
	
	public QueryRun(String name, String system, Timings t, Counters c) {
		m_name = name;
		m_system = system;
		m_timings = t;
		m_counters = c;
	}

	public String getSystem() {
		return m_system;
	}
	
	public String getName() {
		return m_name;
	}

	public Timings getTimings() {
		return m_timings;
	}
	
	public Counters getCounters() {
		return m_counters;
	}
}

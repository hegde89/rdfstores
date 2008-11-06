package edu.unika.aifb.graphindex.graph.isomorphism;

import java.util.HashMap;
import java.util.Map;

public class VertexMapping {
	private Map<String,String> m_l2r;
	private Map<String,String> m_r2l;
	
	public VertexMapping(Map<String,String> l2r) {
		m_l2r = l2r;
		m_r2l = new HashMap<String,String>();
		
		for (String l : m_l2r.keySet())
			m_r2l.put(m_l2r.get(l), l);
	}
	
	public String getVertexCorrespondence(String vertex, boolean l2r) {
		if (l2r)
			return m_l2r.get(vertex);
		else
			return m_r2l.get(vertex);
	}
	
	public Map<String,String> getMap() {
		return m_l2r;
	}
	
	public String toString() {
		return m_l2r.toString();
	}
}

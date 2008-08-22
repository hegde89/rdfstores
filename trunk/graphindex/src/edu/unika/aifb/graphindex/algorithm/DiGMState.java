package edu.unika.aifb.graphindex.algorithm;

import java.util.Map;

public class DiGMState<V,E> {
	private DiGraphMatcher<V,E> m_gm;
	
	public DiGMState(DiGraphMatcher<V,E> gm, V n1, V n2) {
		m_gm = gm;
	}
}

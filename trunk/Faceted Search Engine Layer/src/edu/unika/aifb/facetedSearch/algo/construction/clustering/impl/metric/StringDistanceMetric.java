package edu.unika.aifb.facetedSearch.algo.construction.clustering.impl.metric;

import java.math.BigDecimal;

import org.apache.log4j.Logger;

import edu.unika.aifb.facetedSearch.algo.construction.clustering.IDistanceMetric;

public class StringDistanceMetric implements IDistanceMetric<String> {

	@SuppressWarnings("unused")
	private static Logger s_log = Logger.getLogger(StringDistanceMetric.class);

	private static StringDistanceMetric s_instance;

	public static StringDistanceMetric getInstance() {
		return s_instance == null ? s_instance = new StringDistanceMetric()
				: s_instance;
	}

	private StringDistanceMetric() {
	}

	public BigDecimal getDistance(String lit1, String lit2) {
		return LexicalEditDistance.getDistance(lit1, lit2);
	}
}

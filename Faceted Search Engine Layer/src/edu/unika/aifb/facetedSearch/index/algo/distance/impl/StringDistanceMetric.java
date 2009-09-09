package edu.unika.aifb.facetedSearch.index.algo.distance.impl;

import org.apache.log4j.Logger;

import edu.unika.aifb.facetedSearch.api.model.ILiteral;
import edu.unika.aifb.facetedSearch.index.algo.distance.IDistanceMetric;

public class StringDistanceMetric implements IDistanceMetric {

	@SuppressWarnings("unused")
	private static Logger s_log = Logger.getLogger(StringDistanceMetric.class);

	private static StringDistanceMetric s_instance;

	public static StringDistanceMetric getInstance() {
		return s_instance == null ? s_instance = new StringDistanceMetric()
				: s_instance;
	}

	private StringDistanceMetric() {
	}

	public double getDistance(ILiteral lit1, ILiteral lit2) {
		return LexicalEditDistance
				.getDistance(lit1.getValue(), lit2.getValue());
	}
}

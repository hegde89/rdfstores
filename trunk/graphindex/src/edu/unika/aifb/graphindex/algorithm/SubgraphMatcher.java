package edu.unika.aifb.graphindex.algorithm;

import edu.unika.aifb.graphindex.graph.Graph;

public interface SubgraphMatcher {
	public Matching match(Graph host, Graph pattern);
}

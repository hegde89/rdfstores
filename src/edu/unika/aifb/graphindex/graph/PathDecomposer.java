package edu.unika.aifb.graphindex.graph;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

public class PathDecomposer {
	
	public List<NamedGraph<String,LabeledEdge<String>>> decompose(NamedGraph<String,LabeledEdge<String>> graph) {
		List<NamedGraph<String,LabeledEdge<String>>> paths = new ArrayList<NamedGraph<String,LabeledEdge<String>>>();
		
		Set<String> visited = new HashSet<String>();
		Stack<String> toVisit = new Stack<String>();
		
		List<String> startCandidates = new ArrayList<String>();
		for (String v : graph.vertexSet())
			if (graph.inDegreeOf(v) == 0)
				startCandidates.add(v);
		
		toVisit.push(startCandidates.get(0));
		
		while (toVisit.size() > 0) {
			String v = toVisit.pop();
		}
		
		return paths;
	}
}

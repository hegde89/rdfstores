package edu.unika.aifb.graphindex.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jgrapht.DirectedGraph;
import org.jgrapht.experimental.isomorphism.IsomorphismRelation;
import org.jgrapht.graph.ClassBasedEdgeFactory;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.junit.Test;

import edu.unika.aifb.graphindex.algorithm.DiGraphMatcher;
import edu.unika.aifb.graphindex.algorithm.FeasibilityChecker;
import edu.unika.aifb.graphindex.graph.LabeledEdge;

public class DiGraphMatcherTest {
	
	private void addEdge(DirectedGraph<String,LabeledEdge<String>> g, String src, String edge, String dst) {
		g.addVertex(src);
		g.addVertex(dst);
		g.addEdge(src, dst, new LabeledEdge<String>(src, dst, edge));
	}
	
	@Test
	public void testMatcher() {
		DirectedGraph<String,LabeledEdge<String>> g1 = new DefaultDirectedGraph<String,LabeledEdge<String>>(new ClassBasedEdgeFactory<String,LabeledEdge<String>>((Class<? extends LabeledEdge<String>>)LabeledEdge.class));
		DirectedGraph<String,LabeledEdge<String>> g2 = new DefaultDirectedGraph<String,LabeledEdge<String>>(new ClassBasedEdgeFactory<String,LabeledEdge<String>>((Class<? extends LabeledEdge<String>>)LabeledEdge.class));
		DirectedGraph<String,LabeledEdge<String>> g3 = new DefaultDirectedGraph<String,LabeledEdge<String>>(new ClassBasedEdgeFactory<String,LabeledEdge<String>>((Class<? extends LabeledEdge<String>>)LabeledEdge.class));

		addEdge(g1, "A", "a", "B");
//		addEdge(g1, "B", "b", "B");
		addEdge(g1, "B", "a", "A");
		addEdge(g1, "B", "a", "C");
//		addEdge(g1, "C", "a", "D");
		
		addEdge(g2, "Y", "a", "Z");

		addEdge(g3, "X", "a", "D");
//		addEdge(g3, "D", "b", "D");
		addEdge(g3, "D", "c", "X");
//		addEdge(g3, "X", "a", "C");
		
		DiGraphMatcher<String,LabeledEdge<String>> gm = new DiGraphMatcher<String,LabeledEdge<String>>(g2, g1, true,
				new FeasibilityChecker<String,LabeledEdge<String>,DirectedGraph<String,LabeledEdge<String>>>() {
					public boolean isSemanticallyFeasible(DirectedGraph<String,LabeledEdge<String>> g1,	DirectedGraph<String,LabeledEdge<String>> g2,	String n1, String n2, Map<String,String> core1, Map<String,String> core2) {
						return true;
//						System.out.println(n1 + " " + n2);
//						List<String> g1labels = new ArrayList<String>();
//						for (LabeledEdge<String> e : g1.incomingEdgesOf(n1)) {
//							if (core1.containsKey(g1.getEdgeSource(e)))
//								g1labels.add(e.getLabel());
//						}
//
//						List<String> g2labels = new ArrayList<String>();
//						for (LabeledEdge<String> e : g2.incomingEdgesOf(n2)) {
//							if (core2.containsKey(g2.getEdgeSource(e)))
//								g2labels.add(e.getLabel());
//						}
//						System.out.println(g1labels + " " + g2labels);
//						
//						return g1labels.equals(g2labels);
					}

					public boolean isEdgeCompatible(LabeledEdge<String> e1, LabeledEdge<String> e2) {
						return e1.getLabel().equals(e2.getLabel());
					}

					public boolean isVertexCompatible(String n1, String n2) {
						return true;
					}
				});
		System.out.println(gm.isSubgraphIsomorphic());
		System.out.println(gm.numberOfMappings());
		for (IsomorphismRelation<String,LabeledEdge<String>> iso : gm) {
			System.out.println(iso);
		}
	}
}

package edu.unika.aifb.graphindex.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jgrapht.DirectedGraph;
import org.jgrapht.experimental.isomorphism.IsomorphismRelation;
import org.jgrapht.graph.ClassBasedEdgeFactory;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.junit.Test;

import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.isomorphism.DiGraphMatcher;
import edu.unika.aifb.graphindex.graph.isomorphism.EdgeLabelFeasibilityChecker;
import edu.unika.aifb.graphindex.graph.isomorphism.FeasibilityChecker;

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

		addEdge(g1, "A", "f", "J");
		addEdge(g1, "A", "f", "B");
		addEdge(g1, "B", "f", "A");
		addEdge(g1, "A", "a", "C");
		addEdge(g1, "B", "a", "C");
		addEdge(g1, "C", "p", "D");
		addEdge(g1, "G", "a", "C");
		addEdge(g1, "G", "f", "H");
		
		addEdge(g2, "X", "a", "Z");
		addEdge(g2, "X", "f", "Y");
		addEdge(g2, "Y", "a", "Z");

		addEdge(g3,"b49","subClassOf","b48");
		addEdge(g3,"b56","is_a","b49");
		addEdge(g3,"b50","is_a","b44");
		addEdge(g3,"b40","a","b37");
		addEdge(g3,"b57","a","b37");
		addEdge(g3,"b41","a","b56");
		addEdge(g3,"b26","k","b46");
		addEdge(g3,"b40","f","b40");
		addEdge(g3,"b40","f","b57");
		addEdge(g3,"b57","name","b33");
		addEdge(g3,"b1","a","b22");
		addEdge(g3,"b41","name","b54");
		addEdge(g3,"b57","reflexive","b57");
		addEdge(g3,"b37","p","b50");
		addEdge(g3,"b41","is_a","b52");
		addEdge(g3,"b52","subClassOf","b48");
		addEdge(g3,"b37","is_a","b49");
		addEdge(g3,"b1","blah","b48");
		addEdge(g3,"b1","m","b26");
		addEdge(g3,"b22","is_a","b49");
		addEdge(g3,"b1","is_a","b48");
		addEdge(g3,"b1","name","b18");
		addEdge(g3,"b40","name","b33");
		addEdge(g3,"b57","is_a","b52");
		addEdge(g3,"b1","is_a","b52");
		addEdge(g3,"b3","is_a","b44");
		addEdge(g3,"b40","is_a","b52");
		addEdge(g3,"b22","p","b50");
		addEdge(g3,"b1","o","b46");
		addEdge(g3,"b56","p","b50");
		addEdge(g3,"b44","subClassOf","b48");
		addEdge(g3,"b37","p","b3");
		addEdge(g3,"b40","reflexive","b40");
		addEdge(g3,"b57","f","b40");
		addEdge(g3,"b40","f","b41");
		
		DiGraphMatcher<String,LabeledEdge<String>> gm = new DiGraphMatcher<String,LabeledEdge<String>>(g2, g1, true, new EdgeLabelFeasibilityChecker());
		System.out.println(gm.isSubgraphIsomorphic());
		System.out.println(gm.numberOfMappings());
		for (IsomorphismRelation<String,LabeledEdge<String>> iso : gm) {
			System.out.println(iso);
		}
	}
}

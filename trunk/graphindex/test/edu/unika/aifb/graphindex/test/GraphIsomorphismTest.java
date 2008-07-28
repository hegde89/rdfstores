package edu.unika.aifb.graphindex.test;

import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;

import edu.unika.aifb.graphindex.Util;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphIsomorphism;
import edu.unika.aifb.graphindex.graph.GraphManager;
import edu.unika.aifb.graphindex.graph.NullGraphStorage;

public class GraphIsomorphismTest {

	private Graph g, h;
	
	@Before
	public void setUp() throws Exception {
		GraphManager.getInstance().setStorageEngine(new NullGraphStorage());

		g = new Graph("testgraph");
		g.addEdge("A", "x", "B");
		g.addEdge("A", "x", "C");
		g.addEdge("B", "b", "B2");
		g.addEdge("B", "b", "B3");
		g.addEdge("C", "a", "C2");
		g.addEdge("C", "a", "C3");
		g.addEdge("C3", "d", "A");
		g.setRoot("A");

		Util.printDOT("g.dot", g);

		h = new Graph("testgraph2");
		h.addEdge("F", "x", "X");
		h.addEdge("F", "x", "Y");
		h.addEdge("X", "b", "X2");
		h.addEdge("X", "b", "X3");
		h.addEdge("Y", "a", "Y2");
		h.addEdge("Y", "a", "Y3");
		h.addEdge("Y3", "d", "F");
		h.setRoot("F");
	}
	
	@Test
	public void testIsIsomorphm() {
		GraphIsomorphism gi = new GraphIsomorphism(false);
		System.out.println(gi.isIsomorph(g.getVertex("B"), h.getVertex("X")));
		System.out.println(gi.cacheHits + "/" + gi.cacheMisses + " " + gi.getCache());
		System.out.println(gi.isIsomorph(g.getRoot(), h.getRoot()));
		System.out.println(gi.cacheHits + "/" + gi.cacheMisses + " " + gi.getCache());
	}
}

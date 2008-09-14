package edu.unika.aifb.graphindex.test;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.ClassBasedEdgeFactory;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.unika.aifb.graphindex.algorithm.NaiveOneIndex;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.storage.StorageException;

public class NaiveOneIndexTest {
	
	DirectedGraph<String,LabeledEdge<String>> graph;
	@Before
	public void setUp() throws Exception {
		graph = new DefaultDirectedGraph<String,LabeledEdge<String>>(new ClassBasedEdgeFactory<String,LabeledEdge<String>>((Class<? extends LabeledEdge<String>>)LabeledEdge.class));
		
		addEdge("A", "B", "a");
		addEdge("B", "C", "a");
		addEdge("A", "D", "c");
		addEdge("A", "G", "c");
//		addEdge("D", "C", "a");
		addEdge("A", "E", "c");
		addEdge("E", "F", "a");
//		addEdge("C", "E", "d");
//		addEdge("A", "C", "b");
//		addEdge("G", "C", "g");
//		addEdge("A", "D", "a");
//		addEdge("D", "E", "a");
//		addEdge("B", "F", "a");
//		addEdge("F", "A", "a");
//		addEdge("A", "X", "y");
//		addEdge("A", "Y", "y");
//		addEdge("A", "Z", "y");
//		addEdge("Y", "T", "x");
//		addEdge("Z", "S", "z");
	}
	
	private void addEdge(String src, String dst, String edge) {
		graph.addVertex(src);
		graph.addVertex(dst);
		graph.addEdge(src, dst, new LabeledEdge<String>(src, dst, edge));
	}
	
	@Test
	public void testNaiveOneIndex() throws StorageException {
		NaiveOneIndex idx = new NaiveOneIndex(graph);
		Set<String> vertices = new HashSet<String>();
		vertices.addAll(Arrays.asList(new String[] {"A", "B", "C", "E", "F", "G"}));//, "C", "D", "E", "F", "G", "X", "Y", "Z", "S", "T"}));
		idx.createOneIndex(vertices);
	}

	@After
	public void tearDown() throws Exception {
	}

}

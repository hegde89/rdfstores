package edu.unika.aifb.graphindex.test;


import org.junit.Before;
import org.junit.Test;

import edu.unika.aifb.graphindex.Util;
import edu.unika.aifb.graphindex.graph.DFSCoding;
import edu.unika.aifb.graphindex.graph.DFSGraphVisitor;
import edu.unika.aifb.graphindex.graph.DFSListener;
import edu.unika.aifb.graphindex.graph.Edge;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphManager;
import edu.unika.aifb.graphindex.graph.NullGraphStorage;
import edu.unika.aifb.graphindex.graph.Vertex;

public class DFSTest {

	private Graph g, h;
	
	@Before
	public void setUp() throws Exception {
		GraphManager.getInstance().setStorageEngine(new NullGraphStorage());
		g = new Graph("testgraph");
		
		g.addEdge("A", "x", "B");
		g.addEdge("A", "x", "C");
//		g.addEdge("A", "x", "D");
		g.addEdge("B", "b", "B2");
		g.addEdge("B", "b", "B3");
		g.addEdge("C", "a", "C2");
		g.addEdge("C", "a", "C3");
		g.addEdge("C3", "d", "A");
		g.setRoot("A");
		Util.printDOT("g.dot", g);

		h = new Graph("testgraph2");
		
		h.addEdge("Z", "x", "Y");
		h.addEdge("Y", "y", "F");
		h.addEdge("Y", "a", "X");
		h.addEdge("X", "i", "Z");
		h.addEdge("X", "j", "F");
		h.setRoot("F");
	}

	@Test
	public void testDFS() {
		DFSGraphVisitor dfs = new DFSGraphVisitor(new DFSListener() {
			public void encounterBackwardEdge(int tree, Vertex src, String edge, Vertex dst, int srcDfsNr, int dstDfsNr) {
				System.out.println("backward: (" + tree + ") " + src + " " + edge + " " + dst + " (" + srcDfsNr + "," + dstDfsNr + ")");
			}

			public void encounterForwardEdge(int tree, Vertex src, String edge, Vertex dst, int srcDfsNr, int dstDfsNr) {
				System.out.println("forward: (" + tree + ") " + src + " " + edge + " " + dst + " (" + srcDfsNr + "," + dstDfsNr + ")");
			}

			public void encounterVertex(int tree, Vertex v, Edge e, int dfsNr) {
//				System.out.println("encounter: (" + tree + ") " + v + " " + dfsNr);
			}

			public void encounterVertexAgain(int tree, Vertex v, Edge e, int dfsNr) {
//				System.out.println("encounter again: (" + tree + ") " + v + " " + dfsNr);
			}

			public void treeComplete(int tree) {
				// TODO Auto-generated method stub
				
			}
		}, false, true, true);
		
		g.getRoot().acceptVisitor(dfs);
	}
	
	@Test
	public void testDFSCoding() {
		DFSCoding dfsc = new DFSCoding();
		System.out.println(dfsc.getCanonicalLabel(g.getRoot(), false));
//		System.out.println(dfsc.getCanonicalLabel(h.getRoot(), true));
	}
}

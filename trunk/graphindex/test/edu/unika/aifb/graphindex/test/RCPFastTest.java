package edu.unika.aifb.graphindex.test;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.ClassBasedEdgeFactory;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DirectedMultigraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.unika.aifb.graphindex.Util;
import edu.unika.aifb.graphindex.algorithm.NaiveOneIndex;
import edu.unika.aifb.graphindex.algorithm.RCP;
import edu.unika.aifb.graphindex.algorithm.RCP.Block;
import edu.unika.aifb.graphindex.algorithm.RCP.Partition;
import edu.unika.aifb.graphindex.algorithm.rcp.RCPFast;
import edu.unika.aifb.graphindex.data.LVertex;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.NamedGraph;
import edu.unika.aifb.graphindex.graph.SVertex;
import edu.unika.aifb.graphindex.storage.GraphManager;
import edu.unika.aifb.graphindex.storage.GraphManagerImpl;
import edu.unika.aifb.graphindex.storage.NullGraphStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;


public class RCPFastTest {

	@Before
	public void setUp() throws Exception {
		GraphManager gm = new GraphManagerImpl();
		gm.setGraphStorage(new NullGraphStorage());
		StorageManager.getInstance().setGraphManager(gm);


		LVertex A = new LVertex(0);
		LVertex B = new LVertex(1);
		LVertex C = new LVertex(2);
		LVertex D = new LVertex(3);
		LVertex F = new LVertex(4);
		LVertex G = new LVertex(5);
		LVertex H = new LVertex(6);
		LVertex T = new LVertex(7);
		LVertex X = new LVertex(8);
		LVertex Y = new LVertex(9);
		
		A.addToImage(0, B);
		A.addToImage(0, D);
		A.addToImage(1, G);
		A.addToImage(1, F);
		A.addToImage(0, T);
		A.addToImage(2, X);
		
		G.addToImage(0, H);
		
		F.addToImage(0, C);
		
		C.addToImage(3, A);
		
		X.addToImage(2, A);
		
		Y.addToImage(4, T);
		
		vertices.add(A);
		vertices.add(B);
		vertices.add(C);
		vertices.add(D);
		vertices.add(F);
		vertices.add(G);
		vertices.add(H);
		vertices.add(T);
		vertices.add(X);
		vertices.add(Y);
		
//		addEdge("A", "B", "a");
//		addEdge("A", "D", "a");
//		addEdge("A", "G", "e");
//		addEdge("G", "H", "a");
//		addEdge("A", "F", "e");
//		addEdge("F", "C", "a");
//		addEdge("C", "A", "f");
//		addEdge("A", "X", "x");
//		addEdge("X", "A", "x");
//		addEdge("Y", "T", "r");
//		addEdge("A", "T", "a");
	}
	
	List<LVertex> vertices = new ArrayList<LVertex>();
	
	@Test
	public void testNaiveOneIndex() throws StorageException {
		LVertex y1 = new LVertex(0);
		LVertex x5 = new LVertex(1);
		LVertex x9 = new LVertex(2);
		y1.addToImage(0, x5);
		x5.addToImage(1, x9);
		List<LVertex> v = new ArrayList<LVertex>();
		v.add(y1);
		v.add(x5);
		v.add(x9);
		
		RCPFast rcp = new RCPFast(null, null);
		rcp.createIndex(vertices);
	}

	@After
	public void tearDown() throws Exception {
	}

}

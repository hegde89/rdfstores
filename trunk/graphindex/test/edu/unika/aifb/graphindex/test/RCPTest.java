package edu.unika.aifb.graphindex.test;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
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
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.NamedGraph;
import edu.unika.aifb.graphindex.graph.SVertex;
import edu.unika.aifb.graphindex.storage.GraphManager;
import edu.unika.aifb.graphindex.storage.GraphManagerImpl;
import edu.unika.aifb.graphindex.storage.NullGraphStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;


public class RCPTest {
	DirectedGraph<SVertex,LabeledEdge<SVertex>> graph;
	Set<Set<SVertex>> blocks;

	private void loadRCPTestFile(File file) {
		try {
			BufferedReader in = new BufferedReader(new FileReader(file));
			
			boolean graph = true;
			blocks = new HashSet<Set<SVertex>>();
			
			String input;
			while ((input = in.readLine()) != null) {
				if (input.equals("")) {
					graph = false;
					continue;
				}
				String[] t = input.split("\\t");
				
				if (graph) {
					addEdge(t[0], t[2], t[1]);
				}
				else {
					Set<SVertex> block = new HashSet<SVertex>();
					for (String v : t)
						block.add(new SVertex(v));
					blocks.add(block);
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Before
	public void setUp() throws Exception {
		GraphManager gm = new GraphManagerImpl();
		gm.setGraphStorage(new NullGraphStorage());
		StorageManager.getInstance().setGraphManager(gm);
		
		graph = new DirectedMultigraph<SVertex,LabeledEdge<SVertex>>(new ClassBasedEdgeFactory<SVertex,LabeledEdge<SVertex>>((Class<? extends LabeledEdge<SVertex>>)LabeledEdge.class));

		loadRCPTestFile(new File("/Users/gl/Studium/diplomarbeit/workspace/graphindex/test.rcp"));
		
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
	
	Map<String,SVertex> vertices = new HashMap<String,SVertex>();
	
	private void addEdge(String src, String dst, String edge) {
		SVertex source = vertices.get(src);
		if (source == null) {
			source = new SVertex(src);
			vertices.put(src, source);
			graph.addVertex(source);
		}
		
		SVertex dest = vertices.get(dst);
		if (dest == null) {
			dest = new SVertex(dst);
			vertices.put(dst, dest);
			graph.addVertex(dest);
		}
		
		graph.addEdge(source, dest, new LabeledEdge<SVertex>(source, dest, edge));
	}
	
	@Test
	public void testNaiveOneIndex() throws StorageException {
		RCP idx = new RCP(graph);
		Partition p = idx.createPartition(new HashSet<SVertex>(graph.vertexSet()));
		Set<Set<SVertex>> pblocks = new HashSet<Set<SVertex>>();
		for (Block b : p.getBlocks()) {
			pblocks.add(b.getVertices());
		}
		
		System.out.println(blocks);
		System.out.println(pblocks);
		assertEquals(pblocks, blocks);
	}

	@After
	public void tearDown() throws Exception {
	}

}

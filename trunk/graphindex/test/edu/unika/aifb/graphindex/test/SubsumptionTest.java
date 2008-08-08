package edu.unika.aifb.graphindex.test;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import edu.unika.aifb.graphindex.Util;
import edu.unika.aifb.graphindex.algorithm.DGMerger;
import edu.unika.aifb.graphindex.graph.DFSCoding;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphManager;
import edu.unika.aifb.graphindex.graph.NullGraphStorage;

public class SubsumptionTest {
	@Before
	public void setUp() {
		GraphManager.getInstance().setStorageEngine(new NullGraphStorage());
	}
	
	@Test
	public void testSubsumption() throws IOException {
		Graph g1 = Util.loadGT("graph126.gt");
		g1.setRoot("dgr9624");
		Graph g2 = Util.loadGT("graph18.gt");
		g2.setRoot("dgr1189");
		
		DGMerger dgm = new DGMerger();
		DFSCoding dfsc = new DFSCoding();
		
		System.out.println(dgm.subsumes(g1, g2));
	}
}

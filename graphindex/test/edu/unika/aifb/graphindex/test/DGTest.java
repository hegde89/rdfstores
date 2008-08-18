package edu.unika.aifb.graphindex.test;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import edu.unika.aifb.graphindex.Util;
import edu.unika.aifb.graphindex.algorithm.DGBuilder;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphManager;
import edu.unika.aifb.graphindex.graph.NullGraphStorage;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;
import edu.unika.aifb.graphindex.storage.lucene.LuceneExtensionManager;
import edu.unika.aifb.graphindex.storage.lucene.LuceneExtensionStorage;

public class DGTest {
	ExtensionManager manager;
	
	@Before
	public void setUp() throws StorageException {
		GraphManager.getInstance().setStorageEngine(new NullGraphStorage());
		ExtensionStorage es = new LuceneExtensionStorage("/Users/gl/Studium/diplomarbeit/workspace/graphindex/index/test");
		
		manager = new LuceneExtensionManager();
		manager.setExtensionStorage(es);
		manager.initialize(true, false);
		
		StorageManager.getInstance().setExtensionManager(manager);
	}
	
	@Test
	public void testSubsumption() throws IOException, StorageException {
		Graph g1 = Util.loadGT("graph4783.gt");
		g1.setRoot("http://dbpedia.org/resource/1880_Wimbledon_Championship");
		
		System.out.println(g1);
		
		manager.startBulkUpdate();
		DGBuilder dgb = new DGBuilder();
		Graph dg = dgb.buildDataGuide(g1);
		manager.finishBulkUpdate();
		
		System.out.println(dg);
	}
}

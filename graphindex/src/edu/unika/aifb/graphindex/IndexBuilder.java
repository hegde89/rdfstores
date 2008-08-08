package edu.unika.aifb.graphindex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.semanticweb.kaon2.datalog.disjunction.SubsumptionHelper;

import edu.unika.aifb.graphindex.algorithm.DGBuilder;
import edu.unika.aifb.graphindex.algorithm.DGMerger;
import edu.unika.aifb.graphindex.algorithm.Partitioner;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphFactory;
import edu.unika.aifb.graphindex.graph.GraphManager;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;

public class IndexBuilder {
	
	private ExtensionManager m_em = StorageManager.getInstance().getExtensionManager();
	private Graph m_sourceGraph;
	private Index m_index;
	private DGMerger m_dgm;
	private LinkedList<Graph> m_guides;
	
	private static final Logger log = Logger.getLogger(IndexBuilder.class);

	public IndexBuilder(Graph graph) {
		m_sourceGraph = graph;
		m_guides = new LinkedList<Graph>();
		m_dgm = new DGMerger();
	}
	
	public Index getIndex() {
		return m_index;
	}
	
	private int searchList(List<Graph> c, Graph g) {
		int idx = Collections.binarySearch(c, g, new Comparator<Graph>() {
			public int compare(Graph g1, Graph g2) {
				return ((Integer)g1.numberOfVertices()).compareTo(g2.numberOfVertices()) * -1;
			}
		});
		
		idx = idx >= 0 ? idx : -1 - idx;

		// If a collection has multiple entries with the same value
		// Collections.binarySearch make no guarantee which one will be returned.
		// This makes sure idx points to the left-most entry.
		for (int i = idx - 1; i >= 0; i--) {
			if (m_guides.get(i).numberOfVertices() > g.numberOfVertices()) {
				idx = i + 1;
				break;
			}
		}
		
		return idx;
	}
	
	public void mergeWeak(Graph dg) throws StorageException {
		int idx = searchList(m_guides, dg);

		boolean merged = false;
		
		if (idx < m_guides.size()) {
			Graph lg = m_guides.get(idx);
			while (lg.numberOfVertices() == dg.numberOfVertices()) {
				if (m_dgm.subsumesWeak(lg, dg)) {
					m_dgm.merge(lg, dg);
					merged = true;
					break;
				}
				idx++;
			}
		}
		
		if (!merged) {
			m_guides.add(idx, dg);
		}
	}
	
	public void merge(Graph dg) throws StorageException {
		int idx = searchList(m_guides, dg);
		
		boolean merged = false;
		for (int i = idx; i < m_guides.size(); i++) {
			Graph lg = m_guides.get(i);
			if (m_dgm.subsumes(dg, lg)) {
				m_dgm.merge(dg, lg);
				lg.remove();
				m_guides.remove(i);
				i--;
				merged = true;
			}
		}
		
		if (!merged) {
			for (int i = idx - 1; i >= 0; i--) {
				Graph lg = m_guides.get(i);
				if (m_dgm.subsumes(lg, dg)) {
					m_dgm.merge(lg, dg);
					dg.remove();
					merged = true;
					break;
				}
			}
			if (!merged)
				m_guides.add(idx, dg);
		}
		else
			m_guides.add(idx, dg);
		
	}
	
	public void buildIndex() throws StorageException {
		m_em.setMode(ExtensionManager.MODE_WRITECACHE);
		
		m_em.startBulkUpdate();
		
		Partitioner p = new Partitioner(m_sourceGraph);
		DGBuilder dgb = new DGBuilder();
		
		long start = System.currentTimeMillis();
		long lastStatus = start;
		log.debug("free: " + Util.freeMemory() / 1000);
		
		Graph partition;
		int partitions = 0;
		int maxVertices = 0, maxEdges = 0;
		while ((partition = p.nextPartition()) != null) {
//			log.debug(partition);
			
			if (maxVertices < partition.numberOfVertices()) {
				maxVertices = partition.numberOfVertices();
				log.debug("new max v: " + maxVertices);
			}
			
			if (maxEdges < partition.numberOfEdges()) {
				maxEdges = partition.numberOfEdges();
				log.debug("new max e: " + maxEdges);
			}
			
			Graph dg = dgb.buildDataGuide(partition);
			partitions++;
			
//			if (dg.getName().equals("graph4782") || dg.getName().equals("graph22760"))
//				Util.printDOT(dg.getName() + ".dot", dg);

//			log.debug(m_guides);
			if (m_guides.size() == 0) {
				m_guides.add(dg);
				continue;
			}
			
			mergeWeak(dg);
//			merge(dg);
			
			if (System.currentTimeMillis() - lastStatus > 1000 * 60 * 2) {
				lastStatus = System.currentTimeMillis();
				log.debug("guides: " + m_guides.size() + ", partitions: " + partitions + ", partitions/second: " + partitions / ((System.currentTimeMillis() - start) / 1000.0));
				log.debug("free: " + Util.freeMemory() / 1000);
			}
			
			if (Util.freeMemory() < 150000) {
				log.debug("flushing caches (free: " + Util.freeMemory() / 1000+")");
				m_em.flushAllCaches();
				log.debug("free: " + Util.freeMemory() / 1000);
			}
		}
		
		for (Graph g : m_guides)
			g.unload();
		
		log.debug("number of guides: " + m_guides.size());
		log.debug("partitions/second: " + partitions / ((System.currentTimeMillis() - start) / 1000.0));
		log.debug("free: " + Util.freeMemory() / 1000);
		
		m_em.flushAllCaches();
		
		m_em.finishBulkUpdate();
	}
}

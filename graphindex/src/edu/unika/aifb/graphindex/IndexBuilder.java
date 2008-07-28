package edu.unika.aifb.graphindex;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.algorithm.DGBuilder;
import edu.unika.aifb.graphindex.algorithm.DGMerger;
import edu.unika.aifb.graphindex.algorithm.Partitioner;
import edu.unika.aifb.graphindex.extensions.ExtensionManager;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphFactory;
import edu.unika.aifb.graphindex.graph.GraphManager;

public class IndexBuilder {
	
	private ExtensionManager m_em = ExtensionManager.getInstance();
	private Graph m_sourceGraph;
	private Index m_index;
	
	private static final Logger log = Logger.getLogger(IndexBuilder.class);

	public IndexBuilder(Graph graph) {
		m_sourceGraph = graph;
	}
	
	public Index getIndex() {
		return m_index;
	}
	
	public void buildIndex(int stage) {
		List<Graph> guides = new ArrayList<Graph>();
		if (stage == 0 || stage == 1) {
			m_em.removeAllExtensions();
			
			log.info("source graph: " + m_sourceGraph.numberOfVertices() + " vertices, " + m_sourceGraph.numberOfEdges() + " edges");
			Partitioner p = new Partitioner(m_sourceGraph);
	
			DGBuilder dgb = new DGBuilder();
			
			long start = System.currentTimeMillis();
			
			int i = 0;
			Graph partition;
			while ((partition = p.nextPartition()) != null) {
				Graph guide = dgb.buildDataGuide(partition);
				guide.setId(partition.getId());
				
//				Util.printDOT("dg_" + guide.getName() + ".dot", guide);
				
				guide.unload();
				guides.add(guide);
				i++;
				
				if (Util.freeMemory() < 150000) {
					long free = Util.freeMemory();
					log.debug(m_em.stats());
					m_em.unloadAllExtensions();
					log.debug("free: " + Util.freeMemory() / 1000 + ", before: " + free / 1000 + " (" + (Util.freeMemory() - free) / 1000 + " freed)");
					log.debug("guides: " + i);
					log.debug("guides/second: " + i / ((System.currentTimeMillis() - start) / 1000.0));
				}
			}
			m_em.unloadAllExtensions();
			log.debug("guides/second: " + guides.size() / ((System.currentTimeMillis() - start) / 1000.0));
			log.debug("free: " + Util.freeMemory() / 1000);
			p = null;
			log.debug("cleaning up");
			System.gc();
			log.debug("free: " + Util.freeMemory() / 1000);
		}
		
		if (stage == 2) {
			for (String name : GraphManager.getInstance().getStoredGraphs()) {
				Graph g = GraphFactory.graph(name);
				guides.add(g);
			}
		}
		
		if (stage == 0 ||stage == 2){
			DGMerger dgm = new DGMerger(guides);
			dgm.merge();
			
			m_index = new Index(dgm.getMerged());
		
			log.info(m_em.stats());
		}
	}
}

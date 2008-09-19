package edu.unika.aifb.graphindex;

import java.util.Comparator;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jgrapht.DirectedGraph;
import org.jgrapht.experimental.isomorphism.IsomorphismRelation;

import edu.unika.aifb.graphindex.algorithm.RCP;
import edu.unika.aifb.graphindex.algorithm.WeaklyConnectedComponents;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.NamedGraph;
import edu.unika.aifb.graphindex.graph.SVertex;
import edu.unika.aifb.graphindex.graph.isomorphism.DiGraphMatcher;
import edu.unika.aifb.graphindex.graph.isomorphism.EdgeLabelFeasibilityChecker;
import edu.unika.aifb.graphindex.indexing.IndexGraphMerger;
import edu.unika.aifb.graphindex.indexing.MergedIndexList;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;

public class OneIndexBuilder {

	/**
	 * An implementation of IndexMerger for 1-index graphs.
	 * 
	 * @author gl
	 *
	 */
	private class OneIndexMerger implements IndexGraphMerger<NamedGraph<String,LabeledEdge<String>>> {

		public boolean merge(NamedGraph<String,LabeledEdge<String>> small, NamedGraph<String,LabeledEdge<String>> large) throws StorageException {
//			return false;
			DiGraphMatcher<String,LabeledEdge<String>> matcher = new DiGraphMatcher<String,LabeledEdge<String>>(small, large, true, new EdgeLabelFeasibilityChecker());
			
			if (!matcher.isIsomorphic())
				return false;

//			log.debug("merge " + small + " in " + large);
			
//			if (matcher.numberOfMappings() > 1)
//				log.debug(matcher.numberOfMappings());
			
			for (IsomorphismRelation<String,LabeledEdge<String>> iso : matcher) {
				for (String v : large.vertexSet()) {
					m_em.extension(v).mergeExtension(m_em.extension(iso.getVertexCorrespondence(v, true)));
				}
				break;
			}
			
			return true;
		}
	}
	
	private DirectedGraph<SVertex,LabeledEdge<SVertex>> m_graph;
	private ExtensionManager m_em;
	private static final Logger log = Logger.getLogger(OneIndexBuilder.class);

	public OneIndexBuilder(DirectedGraph<SVertex,LabeledEdge<SVertex>> graph) {
		m_graph = graph;
		m_em = StorageManager.getInstance().getExtensionManager();
	}
	
	public void buildIndex() throws StorageException {
		long start = System.currentTimeMillis();
		
		RCP idx = new RCP(m_graph);
		log.debug(m_graph.vertexSet().size());
		WeaklyConnectedComponents<SVertex,LabeledEdge<SVertex>> components = new WeaklyConnectedComponents<SVertex,LabeledEdge<SVertex>>(m_graph);
		
		m_em.setMode(ExtensionManager.MODE_WRITECACHE);
		m_em.startBulkUpdate();
		
		MergedIndexList<NamedGraph<String,LabeledEdge<String>>> list = new MergedIndexList<NamedGraph<String,LabeledEdge<String>>>(
				new OneIndexMerger(), new Comparator<NamedGraph<String,LabeledEdge<String>>>() {
					public int compare(NamedGraph<String,LabeledEdge<String>> g1, NamedGraph<String,LabeledEdge<String>> g2) {
						return ((Integer)g1.vertexSet().size()).compareTo(g2.vertexSet().size()) * -1;
					}
				});
		
		int cnr = 0;
		Set<SVertex> component;
		int size = components.size();
		while ((component = components.nextComponent()) != null) {
//			log.debug("component: " + component.size());
			
			log.info("component " + cnr + "/" + size + ": " + component.size() + " vertices");
			
			NamedGraph<String,LabeledEdge<String>> g = idx.createOneIndex(component);
			log.debug("graph: " + g.vertexSet().size() + " " + g.edgeSet().size());
			list.add(g);
			cnr++;
			
			if (Util.freeMemory() < 100000) {
				log.debug("flushing caches (free: " + Util.freeMemory() / 1000+")");
				m_em.flushAllCaches();
				log.debug("free: " + Util.freeMemory() / 1000);
			}
		}
		
		m_em.flushAllCaches();
		m_em.finishBulkUpdate();
		
		int vmin = Integer.MAX_VALUE, vmax = 0, vavg = 0, emin = Integer.MAX_VALUE, emax = 0, eavg = 0;
		
		for (NamedGraph<String,LabeledEdge<String>> g : list.getList()) {
			g.store();
			if (g.vertexSet().size() > vmax)
				vmax = g.vertexSet().size();
			if (g.vertexSet().size() < vmin)
				vmin = g.vertexSet().size();
			vavg += g.vertexSet().size();

			if (g.edgeSet().size() > emax)
				emax = g.edgeSet().size();
			if (g.edgeSet().size() < emin)
				emin = g.edgeSet().size();
			eavg += g.edgeSet().size();
			
			Util.printDOT(g);
		}
		
		int vtotal = vavg, etotal = eavg;
		
		vavg /= list.getList().size();
		eavg /= list.getList().size();
		
		double duration = (System.currentTimeMillis() - start) / 1000;
		
		log.info("weakly connected components: " + cnr);
//		log.info("component size (min/max/avg): " + idx.ps_min + "/" + idx.ps_max + "/" + (idx.ps_avg / (double)cnr));
		log.info("structure index graphs: " + list.getList().size());
		log.info("  vertices (min/max/avg/total): " + vmin + "/" + vmax + "/" + vavg + "/" + vtotal);
		log.info("  edges (min/max/avg/vtotal): " + emin + "/" + emax + "/" + eavg + "/" + etotal);
		log.info("duration: " + duration + " seconds");
	}
}

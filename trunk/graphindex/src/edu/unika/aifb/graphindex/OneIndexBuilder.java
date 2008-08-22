package edu.unika.aifb.graphindex;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jgrapht.DirectedGraph;
import org.jgrapht.experimental.isomorphism.IsomorphismRelation;

import edu.unika.aifb.graphindex.algorithm.DiGraphMatcher;
import edu.unika.aifb.graphindex.algorithm.NaiveOneIndex;
import edu.unika.aifb.graphindex.algorithm.WeaklyConnectedComponents;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.NamedGraph;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;
import edu.unika.aifb.graphindex.test.FeasibilityChecker;

public class OneIndexBuilder {

	private class OneIndexMerger implements IndexMerger<NamedGraph<String,LabeledEdge<String>>> {

		public boolean merge(NamedGraph<String,LabeledEdge<String>> small, NamedGraph<String,LabeledEdge<String>> large) throws StorageException {
//			return false;
			DiGraphMatcher<String,LabeledEdge<String>> matcher = new DiGraphMatcher<String,LabeledEdge<String>>(small, large, true, 
					new FeasibilityChecker<String,LabeledEdge<String>,DirectedGraph<String,LabeledEdge<String>>>() {
						public boolean isSemanticallyFeasible(DirectedGraph<String,LabeledEdge<String>> g1, DirectedGraph<String,LabeledEdge<String>> g2, String n1, String n2, Map<String,String> core1, Map<String,String> core2) {
							List<String> g1labels = new ArrayList<String>();
							for (LabeledEdge<String> e : g1.incomingEdgesOf(n1)) {
								if (core1.containsKey(g1.getEdgeSource(e)))
									g1labels.add(e.getLabel());
							}

							List<String> g2labels = new ArrayList<String>();
							for (LabeledEdge<String> e : g2.incomingEdgesOf(n2)) {
								if (core2.containsKey(g2.getEdgeSource(e)))
									g2labels.add(e.getLabel());
							}
							
							return g1labels.equals(g2labels);
						}
			});
			
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
	
	private DirectedGraph<String,LabeledEdge<String>> m_graph;
	private ExtensionManager m_em;
	private static final Logger log = Logger.getLogger(OneIndexBuilder.class);

	public OneIndexBuilder(DirectedGraph<String,LabeledEdge<String>> graph) {
		m_graph = graph;
		m_em = StorageManager.getInstance().getExtensionManager();
	}
	
	public void buildIndex() throws StorageException {
		long start = System.currentTimeMillis();
		
		NaiveOneIndex idx = new NaiveOneIndex(m_graph);
		
		WeaklyConnectedComponents<String,LabeledEdge<String>> components = new WeaklyConnectedComponents<String,LabeledEdge<String>>(m_graph);
		
		m_em.setMode(ExtensionManager.MODE_WRITECACHE);
		m_em.startBulkUpdate();
		
		MergedIndexList<NamedGraph<String,LabeledEdge<String>>> list = new MergedIndexList<NamedGraph<String,LabeledEdge<String>>>(
				new OneIndexMerger(), new Comparator<NamedGraph<String,LabeledEdge<String>>>() {
					public int compare(NamedGraph<String,LabeledEdge<String>> g1, NamedGraph<String,LabeledEdge<String>> g2) {
						return ((Integer)g1.vertexSet().size()).compareTo(g2.vertexSet().size()) * -1;
					}
				});
		
		int cnr = 0;
		Set<String> component;
		while ((component = components.nextComponent()) != null) {
			NamedGraph<String,LabeledEdge<String>> g = idx.createOneIndex(component);
			list.add(g);
			cnr++;
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
		}
		
		int vtotal = vavg, etotal = eavg;
		
		vavg /= list.getList().size();
		eavg /= list.getList().size();
		
		double duration = (System.currentTimeMillis() - start) / 1000;
		
		log.info("weakly connected components: " + cnr);
		log.info("component size (min/max/avg): " + idx.ps_min + "/" + idx.ps_max + "/" + (idx.ps_avg / (double)cnr));
		log.info("structure index graphs: " + list.getList().size());
		log.info("  vertices (min/max/avg/total): " + vmin + "/" + vmax + "/" + vavg + "/" + vtotal);
		log.info("  edges (min/max/avg/vtotal): " + emin + "/" + emax + "/" + eavg + "/" + etotal);
		log.info("duration: " + duration + " seconds");
	}
}

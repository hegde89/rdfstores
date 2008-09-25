package edu.unika.aifb.graphindex.indexing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jgrapht.DirectedGraph;
import org.jgrapht.experimental.isomorphism.IsomorphismRelation;

import edu.unika.aifb.graphindex.Util;
import edu.unika.aifb.graphindex.algorithm.NaiveOneIndex;
import edu.unika.aifb.graphindex.algorithm.RCP;
import edu.unika.aifb.graphindex.algorithm.WeaklyConnectedComponents;
import edu.unika.aifb.graphindex.algorithm.rcp.RCPFast;
import edu.unika.aifb.graphindex.algorithm.rcp.RCPFast2;
import edu.unika.aifb.graphindex.data.HashValueProvider;
import edu.unika.aifb.graphindex.data.IVertex;
import edu.unika.aifb.graphindex.data.LVertex;
import edu.unika.aifb.graphindex.data.VertexListProvider;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.NamedGraph;
import edu.unika.aifb.graphindex.graph.SVertex;
import edu.unika.aifb.graphindex.graph.isomorphism.DiGraphMatcher;
import edu.unika.aifb.graphindex.graph.isomorphism.EdgeLabelFeasibilityChecker;
import edu.unika.aifb.graphindex.storage.Extension;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;
import edu.unika.aifb.graphindex.storage.Triple;

public class FastIndexBuilder {

	/**
	 * An implementation of IndexMerger for 1-index graphs.
	 * 
	 * @author gl
	 *
	 */
	private class OneIndexMerger implements IndexGraphMerger<NamedGraph<String,LabeledEdge<String>>> {

		public boolean merge(NamedGraph<String,LabeledEdge<String>> small, NamedGraph<String,LabeledEdge<String>> large) throws StorageException {
			DiGraphMatcher<String,LabeledEdge<String>> matcher = new DiGraphMatcher<String,LabeledEdge<String>>(small, large, true, new EdgeLabelFeasibilityChecker());
			
			if (!matcher.isIsomorphic())
				return false;

			log.debug(small + " isomorphic to " + large);
			
			for (IsomorphismRelation<String,LabeledEdge<String>> iso : matcher) {
				for (String v : large.vertexSet()) {
					// TODO verify true or false (DiGraphMatcher constructor parameter order may have changed)
//					m_em.extension(v).mergeExtension(m_em.extension(iso.getVertexCorrespondence(v, false)));
					m_mergeMap.put(iso.getVertexCorrespondence(v, false), v);
				}
				break;
			}
			
			return true;
		}
	}
	
	private ExtensionManager m_em;
	private VertexListProvider m_vlp;
	private HashValueProvider m_hashProvider;
	private List<File> m_componentFiles;
	private final Map<String,String> m_mergeMap = new HashMap<String,String>();
	private static final Logger log = Logger.getLogger(FastIndexBuilder.class);

	public FastIndexBuilder(VertexListProvider vb, HashValueProvider hashProvider) {
		m_vlp = vb;
		m_em = StorageManager.getInstance().getExtensionManager();
		m_hashProvider = hashProvider;
		m_componentFiles = new ArrayList<File>();
	}
	
	private void createExtensions() throws StorageException, IOException {
		m_em.setMode(ExtensionManager.MODE_NOCACHE);
		m_em.startBulkUpdate();
		log.info("creating extensions... (" + Util.memory() + ")");
		int triples = 0;
		for (File componentFile : m_componentFiles) {
			log.info(" " + componentFile + ".partition");
			BufferedReader in = new BufferedReader(new FileReader(componentFile.getAbsolutePath() + ".partition"));
			String input;
			while ((input = in.readLine()) != null) {
				input = input.trim();
				String[] t = input.split("\t");
				if (m_mergeMap.containsKey(t[0])) {
					t[0] = m_mergeMap.get(t[0]);
				}
				Extension ext = m_em.extension(t[0]);
				ext.addTriple(new Triple(
						m_hashProvider.getValue(Long.parseLong(t[1])),
						m_hashProvider.getValue(Long.parseLong(t[2])),
						m_hashProvider.getValue(Long.parseLong(t[3]))
				));
				
				if (Util.belowMemoryLimit(20)) {
					m_em.flushAllCaches();
					log.info("caches flushed, " + Util.memory());
				}
				
				triples++;
				if (triples % 1000000 == 0)
					log.debug(" triples: " + triples);
			}
		}
		
//		m_em.flushAllCaches();
		m_em.finishBulkUpdate();
	}
	
	public void buildIndex() throws StorageException, NumberFormatException, IOException {
		long start = System.currentTimeMillis();
		
		RCPFast2 rcp = new RCPFast2(m_hashProvider);

		MergedIndexList<NamedGraph<String,LabeledEdge<String>>> list = new MergedIndexList<NamedGraph<String,LabeledEdge<String>>>(
				new OneIndexMerger(), new Comparator<NamedGraph<String,LabeledEdge<String>>>() {
					public int compare(NamedGraph<String,LabeledEdge<String>> g1, NamedGraph<String,LabeledEdge<String>> g2) {
						return ((Integer)g1.vertexSet().size()).compareTo(g2.vertexSet().size()) * -1;
					}
				});
		
		int cnr = 0;
		List<IVertex> component;
		while ((component = m_vlp.nextComponent()) != null) {
			log.info("component size: " + component.size() + " vertices");
			log.info("unique edges: " + m_hashProvider.getEdges().size());
	
			m_componentFiles.add(m_vlp.getComponentFile());
			NamedGraph<String,LabeledEdge<String>> g = rcp.createIndex(component, m_vlp.getComponentFile().getAbsolutePath() + ".partition");
			log.info("index graph vertices: " + g.vertexSet().size() + ", edges: " + g.edgeSet().size());
			list.add(g);
			cnr++;
			Util.printDOT(g);
			if (Util.freeMemory() < 100000) {
				log.debug("flushing caches (free: " + Util.freeMemory() / 1000+")");
				m_em.flushAllCaches();
				log.debug("free: " + Util.freeMemory() / 1000);
			}
			log.info("------------------------------------------------------------");
		}
		
		log.debug(m_mergeMap);
		
		rcp = null;
		System.gc();
		
		createExtensions();
		
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
//			Util.printDOT(g);
		}
		
		int vtotal = vavg, etotal = eavg;
		
		vavg /= list.getList().size();
		eavg /= list.getList().size();
		
		double duration = (System.currentTimeMillis() - start) / 1000;
		
		log.info("============================================================");
		log.info("INDEXING FINISHED");
		log.info("============================================================");
		log.info("components: " + cnr);
//		log.info("component size (min/max/avg): " + idx.ps_min + "/" + idx.ps_max + "/" + (idx.ps_avg / (double)cnr));
		log.info("structure index graphs: " + list.getList().size());
		log.info("  vertices (min/max/avg/total): " + vmin + "/" + vmax + "/" + vavg + "/" + vtotal);
		log.info("  edges (min/max/avg/vtotal): " + emin + "/" + emax + "/" + eavg + "/" + etotal);
		log.info("duration: " + duration + " seconds");
		log.info(Util.memory());
	}
}

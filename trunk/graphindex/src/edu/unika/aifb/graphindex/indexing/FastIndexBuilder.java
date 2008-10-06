package edu.unika.aifb.graphindex.indexing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jgrapht.experimental.isomorphism.IsomorphismRelation;

import edu.unika.aifb.graphindex.Util;
import edu.unika.aifb.graphindex.algorithm.rcp.RCPFast2;
import edu.unika.aifb.graphindex.data.HashValueProvider;
import edu.unika.aifb.graphindex.data.IVertex;
import edu.unika.aifb.graphindex.data.VertexListProvider;
import edu.unika.aifb.graphindex.graph.IndexEdge;
import edu.unika.aifb.graphindex.graph.IndexGraph;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.NamedGraph;
import edu.unika.aifb.graphindex.graph.isomorphism.FeasibilityChecker;
import edu.unika.aifb.graphindex.graph.isomorphism.GraphMatcher;
import edu.unika.aifb.graphindex.graph.isomorphism.VertexMapping;
import edu.unika.aifb.graphindex.storage.Extension;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;

public class FastIndexBuilder {

	/**
	 * An implementation of IndexMerger for 1-index graphs.
	 * 
	 * @author gl
	 *
	 */
	private class OneIndexMerger implements IndexGraphMerger<IndexGraph> {

		public boolean merge(IndexGraph small, IndexGraph large) throws StorageException {
			GraphMatcher matcher = new GraphMatcher(small, large, false, new FeasibilityChecker() {
				public boolean checkVertexCompatible(int n1, int n2) {
					return true;
				}

				public boolean isEdgeCompatible(IndexEdge e1, IndexEdge e2) {
					return e1.getLabel().equals(e2.getLabel());
				}

				public boolean isVertexCompatible(int n1, int n2) {
					return true;
				}
			
			});
			
			if (!matcher.isIsomorphic())
				return false;

			log.debug(small + " isomorphic to " + large);
			
			for (VertexMapping vm : matcher) {
				for (String v : large.nodeLabels()) {
					// TODO verify true or false (DiGraphMatcher constructor parameter order may have changed)
//					m_em.extension(v).mergeExtension(m_em.extension(iso.getVertexCorrespondence(v, false)));
					m_mergeMap.put(vm.getVertexCorrespondence(v, false), v);
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
			
			String currentExt = null;
			Long currentProperty = null, currentObject = null;
			Set<String> currentSubjects = new HashSet<String>();
			
			BufferedReader in = new BufferedReader(new FileReader(componentFile.getAbsolutePath() + ".partition"));
			String input;
			while ((input = in.readLine()) != null) {
				input = input.trim();
				String[] t = input.split("\t");
				if (m_mergeMap.containsKey(t[0])) {
					t[0] = m_mergeMap.get(t[0]);
				}
				
				long s = Long.parseLong(t[1]);
				long p = Long.parseLong(t[2]);
				long o = Long.parseLong(t[3]);
				
				if (!t[0].equals(currentExt) || currentProperty == null || currentProperty.longValue() != p || currentObject == null || currentObject.longValue() != o) {
					if (currentSubjects.size() > 0) {
						Extension ext = m_em.extension(currentExt);
						ext.addTriples(currentSubjects, m_hashProvider.getValue(currentProperty.longValue()), m_hashProvider.getValue(currentObject.longValue()));
					}
					currentSubjects = new HashSet<String>();
				}
				
				currentExt = t[0];
				currentSubjects.add(m_hashProvider.getValue(s));
				currentProperty = p;
				currentObject = o;
				
				if (Util.belowMemoryLimit(20)) {
					m_em.flushAllCaches();
					log.info("caches flushed, " + Util.memory());
				}
				
				triples++;
				if (triples % 1000000 == 0)
					log.debug(" triples: " + triples);
			}
			Extension ext = m_em.extension(currentExt);
			ext.addTriples(currentSubjects, m_hashProvider.getValue(currentProperty.longValue()), m_hashProvider.getValue(currentObject.longValue()));
		}
		
//		m_em.flushAllCaches();
		m_em.finishBulkUpdate();
	}
	
	public void buildIndex() throws StorageException, NumberFormatException, IOException, InterruptedException {
		long start = System.currentTimeMillis();
		
		RCPFast2 rcp = new RCPFast2(m_hashProvider);

		MergedIndexList<IndexGraph> list = new MergedIndexList<IndexGraph>(
				new OneIndexMerger(), new Comparator<IndexGraph>() {
					public int compare(IndexGraph g1, IndexGraph g2) {
						return ((Integer)g1.nodeCount()).compareTo(g2.nodeCount()) * -1;
					}
				});
		
		int cnr = 0;
		List<IVertex> component;
		while ((component = m_vlp.nextComponent()) != null) {
			log.info("component size: " + component.size() + " vertices");
			log.info("unique edges: " + m_hashProvider.getEdges().size());
	
			m_componentFiles.add(m_vlp.getComponentFile());
			IndexGraph g = rcp.createIndexGraph(component, m_vlp.getComponentFile().getAbsolutePath() + ".partition");
			log.info("index graph vertices: " + g.nodeCount() + ", edges: " + g.edgeSet().size());
			list.add(g);
			cnr++;

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
		
		for (IndexGraph g : list.getList()) {
			g.store();
			if (g.nodeCount() > vmax)
				vmax = g.nodeCount();
			if (g.nodeCount() < vmin)
				vmin = g.nodeCount();
			vavg += g.nodeCount();

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

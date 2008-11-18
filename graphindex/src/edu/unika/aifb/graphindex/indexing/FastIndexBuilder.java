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

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.algorithm.rcp.RCPFast2;
import edu.unika.aifb.graphindex.data.HashValueProvider;
import edu.unika.aifb.graphindex.data.IVertex;
import edu.unika.aifb.graphindex.data.Subject;
import edu.unika.aifb.graphindex.graph.IndexGraph;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.NamedGraph;
import edu.unika.aifb.graphindex.preprocessing.IVertexListProvider;
import edu.unika.aifb.graphindex.preprocessing.VertexListProvider;
import edu.unika.aifb.graphindex.storage.Extension;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;
import edu.unika.aifb.graphindex.util.Util;

public class FastIndexBuilder {

	private ExtensionManager m_em;
	private VertexListProvider m_vlp;
	private HashValueProvider m_hashProvider;
	private List<File> m_componentFiles;
	private StructureIndex m_index;
	static final Logger log = Logger.getLogger(FastIndexBuilder.class);

	public FastIndexBuilder(StructureIndex index, VertexListProvider vb, HashValueProvider hashProvider) {
		m_vlp = vb;
		m_index = index;
		m_em = index.getExtensionManager();
		m_hashProvider = hashProvider;
		m_componentFiles = new ArrayList<File>();
	}
	
	private void createExtensions(Map<String,String> mergeMap) throws StorageException, IOException {
		m_em.setMode(ExtensionManager.MODE_NOCACHE);
		m_em.startBulkUpdate();
		log.info("creating extensions... (" + Util.memory() + ")");
		int triples = 0;
		for (File componentFile : m_componentFiles) {
			log.info(" " + componentFile + ".partition");
			
			String currentExt = null;
			Long currentProperty = null, currentObject = null;
			Set<Subject> currentSubjects = new HashSet<Subject>();
			
			BufferedReader in = new BufferedReader(new FileReader(componentFile.getAbsolutePath() + ".partition"));
			String input;
			while ((input = in.readLine()) != null) {
				input = input.trim();
				String[] t = input.split("\t");
				if (mergeMap.containsKey(t[0])) {
					t[0] = mergeMap.get(t[0]);
				}
				
				long s = Long.parseLong(t[1]);
				long p = Long.parseLong(t[2]);
				long o = Long.parseLong(t[3]);
				String subjectExtension = t[4];
				
				if (!t[0].equals(currentExt) || currentProperty == null || currentProperty.longValue() != p || currentObject == null || currentObject.longValue() != o) {
					if (currentSubjects.size() > 0) {
						Extension ext = m_em.extension(currentExt);
						ext.addTriples(currentSubjects, m_hashProvider.getValue(currentProperty.longValue()), m_hashProvider.getValue(currentObject.longValue()));
					}
					currentSubjects = new HashSet<Subject>();
				}
				
				currentExt = t[0];
				currentSubjects.add(new Subject(m_hashProvider.getValue(s), subjectExtension));
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
		
		RCPFast2 rcp = new RCPFast2(m_index, m_hashProvider);
		OneIndexMerger merger = new OneIndexMerger();
		MergedIndexList<IndexGraph> list = new MergedIndexList<IndexGraph>(merger, new Comparator<IndexGraph>() {
					public int compare(IndexGraph g1, IndexGraph g2) {
						return ((Integer)g1.nodeCount()).compareTo(g2.nodeCount()) * -1;
					}
				});
		
		int cnr = 0;
		List<IVertex> component;
		while (m_vlp.nextComponent()) {
//			log.info("component size: " + component.size() + " vertices");
			log.info("unique edges: " + m_hashProvider.getEdges().size());
	
			m_componentFiles.add(m_vlp.getComponentFile());
			IndexGraph g = rcp.createIndexGraph(m_vlp, m_vlp.getComponentFile().getAbsolutePath() + ".partition");
			if (g == null)
				continue;
			log.info("index graph vertices: " + g.nodeCount() + ", edges: " + g.edgeCount());
			list.add(g);
			cnr++;

			if (Util.freeMemory() < 100000) {
				log.debug("flushing caches (free: " + Util.freeMemory() / 1000+")");
				m_em.flushAllCaches();
				log.debug("free: " + Util.freeMemory() / 1000);
			}
			log.info("------------------------------------------------------------");
		}
		
		rcp = null;
		System.gc();
		
		createExtensions(merger.getMergeMap());
		
		int vmin = Integer.MAX_VALUE, vmax = 0, vavg = 0, emin = Integer.MAX_VALUE, emax = 0, eavg = 0;
		
		for (IndexGraph g : list.getList()) {
			g.store(m_index.getGraphManager());
			if (g.nodeCount() > vmax)
				vmax = g.nodeCount();
			if (g.nodeCount() < vmin)
				vmin = g.nodeCount();
			vavg += g.nodeCount();

			if (g.edgeCount() > emax)
				emax = g.edgeCount();
			if (g.edgeCount() < emin)
				emin = g.edgeCount();
			eavg += g.edgeCount();
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

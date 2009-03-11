package edu.unika.aifb.graphindex.indexing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.GraphStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;
import edu.unika.aifb.graphindex.storage.ExtensionStorage.Index;
import edu.unika.aifb.graphindex.util.Util;

public class FastIndexBuilder {

	private ExtensionManager m_em;
	private VertexListProvider m_vlp;
	private HashValueProvider m_hashProvider;
	private List<File> m_componentFiles;
	private StructureIndex m_index;
	private ExtensionStorage m_es;
	static final Logger log = Logger.getLogger(FastIndexBuilder.class);

	public FastIndexBuilder(StructureIndex index, VertexListProvider vb, HashValueProvider hashProvider) {
		m_vlp = vb;
		m_index = index;
		m_em = index.getExtensionManager();
		m_es = m_em.getExtensionStorage();
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
			Long currentProperty = null;
			Map<String,List<String>> s2o = new HashMap<String,List<String>>();
			Map<String,List<String>> o2s = new HashMap<String,List<String>>();
			
			if (!new File(componentFile.getAbsolutePath() + ".partition").exists()) {
				log.debug(" ...not found");
				continue;
			}
			
			Set<String> oe = new HashSet<String>();
			Map<String,Set<String>> se = new HashMap<String,Set<String>>();
			
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
//				String subjectExtension = t[4];
				
				if (!t[0].equals(currentExt) || currentProperty == null || currentProperty.longValue() != p) {
					if (s2o.size() > 0) {
						for (String subject : s2o.keySet())
							m_es.addTriples(Index.EPS, currentExt, m_hashProvider.getValue(currentProperty), subject, s2o.get(subject));

						for (String object : o2s.keySet())
							m_es.addTriples(Index.EPO, currentExt, m_hashProvider.getValue(currentProperty), object, o2s.get(object));
						
					}
					s2o = new HashMap<String,List<String>>();
					o2s = new HashMap<String,List<String>>();
				}
				
				currentExt = t[0];
				currentProperty = p;
				
				String subject = m_hashProvider.getValue(s);
				String object = m_hashProvider.getValue(o);
				
				List<String> objects = s2o.get(subject);
				if (objects == null) {
					objects = new ArrayList<String>();
					s2o.put(subject, objects);
				}
				objects.add(object);
				
				List<String> subjects = o2s.get(object);
				if (subjects == null) {
					subjects = new ArrayList<String>();
					o2s.put(object, subjects);
				}
				subjects.add(subject);
				
				oe.add(object + "__" + currentExt);
				if (!se.containsKey(subject))
					se.put(subject, new HashSet<String>());
				se.get(subject).add(currentExt);

				if (Util.belowMemoryLimit(20)) {
					m_em.flushAllCaches();
					log.info("caches flushed, " + Util.memory());
				}
				
				triples++;
				if (triples % 1000000 == 0)
					log.debug(" triples: " + triples);
			}
			
			for (String subject : s2o.keySet())
				m_es.addTriples(Index.EPS, currentExt, m_hashProvider.getValue(currentProperty), subject, s2o.get(subject));

			for (String object : o2s.keySet())
				m_es.addTriples(Index.EPO, currentExt, m_hashProvider.getValue(currentProperty), object, o2s.get(object));
			
			m_es.createSEOE(se, oe);
		}
		
//		m_em.flushAllCaches();
		m_em.finishBulkUpdate();
	}
	
	public void createGraph(String graphFile) throws StorageException, IOException {
		GraphStorage gs = m_index.getGraphManager().getGraphStorage();
		BufferedReader in = new BufferedReader(new FileReader(graphFile));
		String input;
		int edges = 0;
		while ((input = in.readLine()) != null) {
			input = input.trim();
			String[] t = input.split("\t");
			gs.addEdge("graph1", t[0], t[1], t[2]);
			edges++;
		}
		in.close();
		gs.optimize();
		log.info("index graph edges: " + edges);
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
			IndexGraph g = rcp.createIndexGraph(m_vlp, m_vlp.getComponentFile().getAbsolutePath() + ".partition", m_vlp.getComponentFile().getAbsolutePath() + ".graph");
//			if (g == null)
//				continue;
//			log.info("index graph vertices: " + g.nodeCount() + ", edges: " + g.edgeCount());
//			list.add(g);
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
		createGraph(m_vlp.getComponentFile().getAbsolutePath() + ".graph");
		
		writeEdgeSet(m_index.getForwardEdges(), m_index.getDirectory() + "/forward_edgeset");
		writeEdgeSet(m_index.getBackwardEdges(), m_index.getDirectory() + "/backward_edgeset");
		
//		int vmin = Integer.MAX_VALUE, vmax = 0, vavg = 0, emin = Integer.MAX_VALUE, emax = 0, eavg = 0;
//		
//		for (IndexGraph g : list.getList()) {
//			g.store(m_index.getGraphManager());
//			if (g.nodeCount() > vmax)
//				vmax = g.nodeCount();
//			if (g.nodeCount() < vmin)
//				vmin = g.nodeCount();
//			vavg += g.nodeCount();
//
//			if (g.edgeCount() > emax)
//				emax = g.edgeCount();
//			if (g.edgeCount() < emin)
//				emin = g.edgeCount();
//			eavg += g.edgeCount();
//		}
//		
//		int vtotal = vavg, etotal = eavg;
//		
//		vavg /= list.getList().size();
//		eavg /= list.getList().size();
		
		double duration = (System.currentTimeMillis() - start) / 1000;
		
		log.info("============================================================");
		log.info("INDEXING FINISHED");
		log.info("============================================================");
		log.info("components: " + cnr);
//		log.info("component size (min/max/avg): " + idx.ps_min + "/" + idx.ps_max + "/" + (idx.ps_avg / (double)cnr));
		log.info("structure index graphs: " + list.getList().size());
//		log.info("  vertices (min/max/avg/total): " + vmin + "/" + vmax + "/" + vavg + "/" + vtotal);
//		log.info("  edges (min/max/avg/vtotal): " + emin + "/" + emax + "/" + eavg + "/" + etotal);
		log.info("duration: " + duration + " seconds");
		log.info(Util.memory());
	}

	private void writeEdgeSet(Set<String> edges, String file) throws IOException {
		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(file)));
		for (String s : edges)
			out.println(s);
		out.close();
	}
}

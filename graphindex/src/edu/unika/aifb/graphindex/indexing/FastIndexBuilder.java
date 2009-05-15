package edu.unika.aifb.graphindex.indexing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
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
import org.openrdf.model.vocabulary.RDFS;

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.algorithm.rcp.RCPFast;
import edu.unika.aifb.graphindex.data.HashValueProvider;
import edu.unika.aifb.graphindex.data.IVertex;
import edu.unika.aifb.graphindex.graph.IndexGraph;
import edu.unika.aifb.graphindex.preprocessing.VertexListProvider;
import edu.unika.aifb.graphindex.storage.BlockStorage;
import edu.unika.aifb.graphindex.storage.DataStorage;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.GraphStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.ExtensionStorage.DataField;
import edu.unika.aifb.graphindex.storage.ExtensionStorage.Index;
import edu.unika.aifb.graphindex.storage.ExtensionStorage.IndexDescription;
import edu.unika.aifb.graphindex.util.TypeUtil;
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
	
	private void sortPartitionFile(String partitionFile, int orientation) {
		try {
			if (orientation == 0) {
				// sort partition file by extension uri, property uri, object
				Process process = Runtime.getRuntime().exec("sort -k 1,1 -k 3,3n -k 2,2n -o " + partitionFile + " " + partitionFile);
				process.waitFor();
			}
			else {
				Process process = Runtime.getRuntime().exec("sort -k 5,5 -k 3,3n -k 4,4n -o " + partitionFile + " " + partitionFile);
				process.waitFor();
			}
			log.debug("sorted " + orientation);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private void importTriples(String partitionFile, int extIdx, IndexDescription index1, int indexFieldIdx1, int dataFieldIdx1, IndexDescription index2, int indexFieldIdx2, int dataFieldIdx2, boolean skipIncomplete) throws StorageException, NumberFormatException, IOException {
		String currentExt = null;
		Long currentProperty = null;
		Map<String,List<String>> idx2data1 = new HashMap<String,List<String>>();
		Map<String,List<String>> idx2data2 = new HashMap<String,List<String>>();
		
		boolean doIndex2 = index2 != null;
		
		int triples = 0;
		
		BufferedReader in = new BufferedReader(new FileReader(partitionFile));
		String input;
		while ((input = in.readLine()) != null) {
			input = input.trim();
			String[] t = input.split("\t");
//			if (mergeMap.containsKey(t[0])) {
//				t[0] = mergeMap.get(t[0]);
//			}
			
			if (skipIncomplete && t.length < 5)
				continue;

			String indexField1 = m_hashProvider.getValue(Long.parseLong(t[indexFieldIdx1]));
			String dataField1 = m_hashProvider.getValue(Long.parseLong(t[dataFieldIdx1]));

			String indexField2 = "", dataField2 = "";
			if (doIndex2) {
				indexField2 = m_hashProvider.getValue(Long.parseLong(t[indexFieldIdx2]));
				dataField2 = m_hashProvider.getValue(Long.parseLong(t[dataFieldIdx2]));
			}
			
			long p = Long.parseLong(t[2]);

			String ext = t[extIdx];

			if (!ext.equals(currentExt) || currentProperty == null || currentProperty.longValue() != p) {
				if (idx2data1.size() > 0) {
					for (String indexField : idx2data1.keySet())
						m_es.addTriples(index1, currentExt, m_hashProvider.getValue(currentProperty), indexField, idx2data1.get(indexField));

					if (doIndex2)
						for (String indexField : idx2data2.keySet())
							m_es.addTriples(index2, currentExt, m_hashProvider.getValue(currentProperty), indexField, idx2data2.get(indexField));
				}
				idx2data1 = new HashMap<String,List<String>>();
				idx2data2 = new HashMap<String,List<String>>();
			}
			
			currentExt = ext;
			currentProperty = p;
			
			List<String> data1 = idx2data1.get(indexField1);
			if (data1 == null) {
				data1 = new ArrayList<String>();
				idx2data1.put(indexField1, data1);
			}
			data1.add(dataField1);
			
			if (doIndex2) {
				List<String> data2 = idx2data2.get(indexField2);
				if (data2 == null) {
					data2 = new ArrayList<String>();
					idx2data2.put(indexField2, data2);
				}
				data2.add(dataField2);
			}
			
			if (Util.belowMemoryLimit(20)) {
				m_em.flushAllCaches();
				log.info("caches flushed, " + Util.memory());
			}
			
			triples++;
			if (triples % 1000000 == 0)
				log.debug(" triples: " + triples);
		}
		
		for (String indexField : idx2data1.keySet())
			m_es.addTriples(index1, currentExt, m_hashProvider.getValue(currentProperty), indexField, idx2data1.get(indexField));

		if (doIndex2)
			for (String indexField : idx2data2.keySet())
				m_es.addTriples(index2, currentExt, m_hashProvider.getValue(currentProperty), indexField, idx2data2.get(indexField));
	}
	
	private void secondaryIndex(String partitionFile, IndexDescription index, int... fieldIdxs) throws IOException, StorageException {
		Map<String,Set<String>> index2data = new HashMap<String,Set<String>>();

		BufferedReader in = new BufferedReader(new FileReader(partitionFile));
		String input;
		while ((input = in.readLine()) != null) {
			input = input.trim();
			String[] t = input.split("\t");
			
			String[] fieldValues = new String [fieldIdxs.length];
			for (int i = 0; i < fieldValues.length; i++) {
				fieldValues[i] = t[fieldIdxs[i]].startsWith("b") ? t[fieldIdxs[i]] : m_hashProvider.getValue(Long.parseLong(t[fieldIdxs[i]]));
			}
			
			String indexKey = m_es.concat(fieldValues, fieldValues.length - 1);
			Set<String> data = index2data.get(indexKey);
			if (data == null) {
				data = new HashSet<String>();
				index2data.put(indexKey, data);
			}
			data.add(fieldValues[fieldValues.length - 1]);
		}
		
		for (String indexKey : index2data.keySet()) {
			m_es.addData(index, indexKey, index2data.get(indexKey));
		}
	}
	
	private void createExtensions(Map<String,String> mergeMap) throws StorageException, IOException {
		m_em.setMode(ExtensionManager.MODE_NOCACHE);
		m_em.startBulkUpdate();
		
		m_index.clearIndexes();
		
		log.info("creating extensions... (" + Util.memory() + ")");
		for (File componentFile : m_componentFiles) {
			log.info(" " + componentFile + ".partition");
			
			String partitionFile = componentFile.getAbsolutePath() + ".partition";
			if (!new File(partitionFile).exists()) {
				log.debug(" ...not found");
				continue;
			}

			// "traditional"
//			sortPartitionFile(partitionFile, StructureIndex.ORIENTATION_OBJ);
//			importTriples(partitionFile, 4, Index.EPO, 3, 1, Index.EPS, 1, 3);
//			m_index.addIndex(new IndexDescription(Index.EPO.getIndexField(), Index.EPO.getValField(), DataField.EXT_OBJECT, DataField.PROPERTY, DataField.OBJECT, DataField.SUBJECT));
//			m_index.addIndex(new IndexDescription(Index.EPS.getIndexField(), Index.EPS.getValField(), DataField.EXT_OBJECT, DataField.PROPERTY, DataField.SUBJECT, DataField.OBJECT));
			
			// new
			sortPartitionFile(partitionFile, 0);
			importTriples(partitionFile, 0, IndexDescription.PSESO, 1, 3, IndexDescription.POESS, 3, 1, false);
			m_index.addIndex(IndexDescription.PSESO);
			m_index.addIndex(IndexDescription.POESS);

			secondaryIndex(partitionFile, IndexDescription.PSES, 2, 1, 0);
			secondaryIndex(partitionFile, IndexDescription.POES, 2, 3, 0);
			m_index.addIndex(IndexDescription.PSES);
			m_index.addIndex(IndexDescription.POES);
			
//			sortPartitionFile(partitionFile, 1);
//			importTriples(partitionFile, 4, IndexDescription.EOPO, 3, 1, null, -1, -1, true);
//			m_index.addIndex(IndexDescription.EOPO);
		}
		
//		m_em.flushAllCaches();
		m_em.finishBulkUpdate();
	}
	
	public void createBlocks(String blockFile) throws StorageException, IOException {
		BlockStorage bs = m_index.getBlockManager().getBlockStorage();
		BufferedReader in = new BufferedReader(new FileReader(blockFile));
		String input;
		int blocks = 0;
		while ((input = in.readLine()) != null) {
			input = input.trim();
			String[] t = input.split("\t");
			if(t.length == 2){
				bs.addBlock(t[0], t[1]);
				blocks++;
			}
		}
		in.close();
		bs.optimize();
		log.info("index blocks: " + blocks);
	}
	
	public void createData() throws StorageException, IOException {
		DataStorage ds = m_index.getDataManager().getDataStorage();
		int triple = 0;
		for (File componentFile : m_componentFiles) {
			log.info(" " + componentFile + ".partition");
			
			if (!new File(componentFile.getAbsolutePath() + ".partition").exists()) {
				log.debug(" ...not found");
				continue;
			}
			
			BufferedReader in = new BufferedReader(new FileReader(componentFile.getAbsolutePath() + ".partition"));
			String input;
			while ((input = in.readLine()) != null) {
				input = input.trim();
				String[] t = input.split("\t");
				
				long s = Long.parseLong(t[1]);
				long p = Long.parseLong(t[2]);
				long o = Long.parseLong(t[3]);
				
				String subject = m_hashProvider.getValue(s);
				String predicate = m_hashProvider.getValue(p);
				String object = m_hashProvider.getValue(o);
				String type = null; 
				
				if (TypeUtil.getSubjectType(predicate, object).equals(TypeUtil.ENTITY)
						&& TypeUtil.getObjectType(predicate, object).equals(TypeUtil.ENTITY)
						&& TypeUtil.getPredicateType(predicate, object).equals(TypeUtil.RELATION)) {
					type = TypeUtil.RELATION;
				}
				else if (TypeUtil.getSubjectType(predicate, object).equals(TypeUtil.ENTITY) && TypeUtil.getObjectType(predicate, object).equals(TypeUtil.LITERAL)
						&& TypeUtil.getPredicateType(predicate, object).equals(TypeUtil.ATTRIBUTE)) {
					type = TypeUtil.ATTRIBUTE;
				}
				else if (TypeUtil.getSubjectType(predicate, object).equals(TypeUtil.ENTITY)
						&& TypeUtil.getObjectType(predicate, object).equals(TypeUtil.CONCEPT)) {
					type = TypeUtil.TYPE;
				}
				else {
					type = "";
				}
				
				if(type.equals(TypeUtil.ATTRIBUTE) && predicate.equals(RDFS.LABEL.toString())) {
					type = TypeUtil.LABEL;
				}
				ds.addTriple(subject, predicate, object, type);
				triple++;
			}
			in.close();
		}	
		ds.optimize();
		log.info("data triples: " + triple);
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
		
		log.info("ignore data values: " + m_index.ignoreDataValues());
		
		RCPFast rcp = new RCPFast(m_index, m_hashProvider);
		rcp.setIgnoreDataValues(m_index.ignoreDataValues());
		OneIndexMerger merger = new OneIndexMerger();
		MergedIndexList<IndexGraph> list = new MergedIndexList<IndexGraph>(merger, new Comparator<IndexGraph>() {
					public int compare(IndexGraph g1, IndexGraph g2) {
						return ((Integer)g1.nodeCount()).compareTo(g2.nodeCount()) * -1;
					}
				});
		
		int cnr = 0;

		while (m_vlp.nextComponent()) {
//			log.info("component size: " + component.size() + " vertices");
			log.info("unique edges: " + m_hashProvider.getEdges().size());
	
			m_componentFiles.add(m_vlp.getComponentFile());
			rcp.createIndexGraph(m_index.getPathLength(),m_vlp, m_vlp.getComponentFile().getAbsolutePath() + ".partition", m_vlp.getComponentFile().getAbsolutePath() + ".graph", m_vlp.getComponentFile().getAbsolutePath() + ".block");
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
		createBlocks(m_vlp.getComponentFile().getAbsolutePath() + ".block");
		createData();
		
		writeEdgeSet(m_index.getForwardEdges(), m_index.getDirectory() + "/forward_edgeset");
		writeEdgeSet(m_index.getBackwardEdges(), m_index.getDirectory() + "/backward_edgeset");
		
		double duration = (System.currentTimeMillis() - start) / 1000;
		
		log.info("============================================================");
		log.info("INDEXING FINISHED");
		log.info("============================================================");
		log.info("components: " + cnr);
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

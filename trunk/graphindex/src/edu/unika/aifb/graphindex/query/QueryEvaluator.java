package edu.unika.aifb.graphindex.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.jgrapht.DirectedGraph;
import org.jgrapht.experimental.isomorphism.IsomorphismRelation;

import edu.unika.aifb.graphindex.Result;
import edu.unika.aifb.graphindex.ResultSet;
import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.StructureIndexReader;
import edu.unika.aifb.graphindex.algorithm.rcp.generic.GraphRCP;
import edu.unika.aifb.graphindex.algorithm.rcp.generic.LabelProvider;
import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.data.Triple;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.IndexEdge;
import edu.unika.aifb.graphindex.graph.IndexGraph;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.NamedGraph;
import edu.unika.aifb.graphindex.graph.QueryGraph;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.graph.isomorphism.SubgraphMatcher;
import edu.unika.aifb.graphindex.graph.isomorphism.FeasibilityChecker;
import edu.unika.aifb.graphindex.graph.isomorphism.MappingListener;
import edu.unika.aifb.graphindex.graph.isomorphism.VertexMapping;
import edu.unika.aifb.graphindex.query.model.Constant;
import edu.unika.aifb.graphindex.query.model.Individual;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.query.model.Term;
import edu.unika.aifb.graphindex.query.model.Variable;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;
import edu.unika.aifb.graphindex.storage.lucene.LuceneExtensionStorage;
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.graphindex.util.Util;

public class QueryEvaluator implements IQueryEvaluator {
	private StructureIndexReader m_indexReader;
	private StructureIndex m_index;
	private ExtensionManager m_em;
	private ExtensionStorage m_es;
	private Timings m_timings;
	
	static final Logger log = Logger.getLogger(QueryEvaluator.class);
	
	
	public QueryEvaluator(StructureIndexReader indexReader) {
		m_indexReader = indexReader;
		m_index = indexReader.getIndex();
		m_em = m_index.getExtensionManager();
		m_es = m_em.getExtensionStorage();
	}
	
	public void evaluate(Query query) throws StorageException, InterruptedException, ExecutionException {
		log.info("evaluating...");
		
		m_index.getCollector().reset();
		m_timings = new Timings();
		long start = System.currentTimeMillis();
		
		Graph<QueryNode> origGraph = query.toGraph();

		VCompatibilityCache vcc = new VCompatibilityCache(origGraph, m_index);
		
		m_em.setMode(ExtensionManager.MODE_READONLY);
		
		final ExecutorService executor = Executors.newFixedThreadPool(1);
		final ExecutorCompletionService<List<String[]>> completionService = new ExecutorCompletionService<List<String[]>>(executor);
		GTable.timings = m_timings;
		Tables.timings = m_timings;
		Set<Map<String,String>> results = new HashSet<Map<String,String>>();
		List<String[]> result = new ArrayList<String[]>();
		for (Graph<String> indexGraph : m_indexReader.getIndexGraphs()) {
			if (indexGraph.nodeCount() < 10)
				continue;
			vcc.setCurrentIndexGraph(indexGraph);
			
//			Util.printDOT(query.getName() + ".dot", origGraph);
//			Util.printDOT(query.getName() + "_m.dot", qg);

			QueryMappingListener listener = new QueryMappingListener(origGraph, indexGraph, m_indexReader, vcc, completionService);
			JoinMatcher jm = new JoinMatcher(origGraph, indexGraph, listener, m_index, m_timings);

			m_timings.start(Timings.MATCH);
			jm.match();
			m_timings.end(Timings.MATCH);

			m_timings.start(Timings.MAPGEN);
			List<Map<String,String>> mappings = listener.generateMappings();
			m_timings.end(Timings.MAPGEN);
			
			if (mappings.size() == 0)
				continue;
			
			listener = null;

			MappingListValidator mlv = new MappingListValidator(m_indexReader, origGraph, mappings, m_index.getCollector());
			List<String[]> res = mlv.validateMappings(origGraph, mappings, query.getEvalOrder());
			if (res.size() > 0)
				result.addAll(res);
		}
		executor.shutdown();
//		log.debug("result maps: " + results.size());
		
//		if (result.size() < 50) {
//			for (String[] res : result) {
//				String s = "";
//				for (String r : res)
//					s += ">" + r + "< ";
//				log.info("\t" + s);
//			}
//		}
		log.debug("size: " + result.size());
		
		long end = System.currentTimeMillis();
		log.info("duration: " + (end - start) / 1000.0);

		m_index.getCollector().addTimings(m_timings);
		m_index.getCollector().logStats();
		
//		log.debug("vcc size: " + vcc.size());
		vcc.clear();
		
		m_es.clearCaches();
		((LuceneExtensionStorage)m_es).logStats(log);
	}
}

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
import org.semanticweb.kaon2.saturation.indexes.UnaryTermIndex.Results;

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
	private HashMap<String,JoinMatcher> m_matchers;
	private MappingListValidator m_mlv;
	static final Logger log = Logger.getLogger(QueryEvaluator.class);
	
	public QueryEvaluator(StructureIndexReader indexReader) throws StorageException {
		m_indexReader = indexReader;
		m_index = indexReader.getIndex();
		m_em = m_index.getExtensionManager();
		m_em.setMode(ExtensionManager.MODE_READONLY);
		m_es = m_em.getExtensionStorage();
		m_matchers = new HashMap<String,JoinMatcher>();
		m_mlv = new MappingListValidator(m_indexReader, m_index.getCollector());
//		for (Graph<String> ig : m_indexReader.getIndexGraphs()) {
//			m_matchers.put(ig, new JoinMatcher(ig, m_listener, m_index));
//		}
		for (String ig : m_indexReader.getGraphNames()) {
			m_matchers.put(ig, new JoinMatcher(m_index, ig));
		}
	}
	
	public MappingListValidator getMLV() {
		return m_mlv;
	}
	
	public int evaluate(Query query) throws StorageException, InterruptedException, ExecutionException {
		log.info("evaluating...");

		m_index.getCollector().reset();
		m_timings = new Timings();
		
		long start = System.currentTimeMillis();
		
		Graph<QueryNode> origGraph = query.getGraph();
		
		List<String[]> result = new ArrayList<String[]>();
		for (String indexGraph : m_indexReader.getGraphNames()) {
			m_timings.start(Timings.SETUP);
			
			JoinMatcher matcher = m_matchers.get(indexGraph);
			
			matcher.setTimings(m_timings);
			matcher.setQueryGraph(query, origGraph);

			GTable.timings = null;
			Tables.timings = null;
			
			m_timings.end(Timings.SETUP);
			
			m_timings.start(Timings.MATCH);
			GTable<String> matches = matcher.match();
			m_timings.end(Timings.MATCH);

			if (matches == null || matches.rowCount() == 0)
				continue;
			
			GTable.timings = m_timings;
			Tables.timings = m_timings;
			
			long vt = System.currentTimeMillis();
			List<String[]> res = m_mlv.validateMappings(query, origGraph, matches, query.getEvalOrder(), query.getSelectVariables());

			if (res.size() > 0)
				result.addAll(res);
		}
		log.debug("size: " + result.size());
		
		long end = System.currentTimeMillis();
		log.info("duration: " + (end - start) / 1000.0);
		
		m_index.getCollector().addTimings(m_timings);

		((LuceneExtensionStorage)m_es).logStats(log);
		
		return result.size();
	}
	
	public void clearCaches() throws StorageException {
		m_mlv.clearCaches();
		m_es.clearCaches();
		((LuceneExtensionStorage)m_es).reopenAndWarmUp();
	}

	public long[] getTimings() {
		m_index.getCollector().logStats();
		return m_index.getCollector().getConsolidated();
	}
}

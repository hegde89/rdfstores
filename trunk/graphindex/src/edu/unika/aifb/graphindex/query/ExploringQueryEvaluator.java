package edu.unika.aifb.graphindex.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.RDF;

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.StructureIndexReader;
import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphEdge;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.query.exploring.EdgeElement;
import edu.unika.aifb.graphindex.query.exploring.ExploringIndexMatcher;
import edu.unika.aifb.graphindex.query.exploring.GraphElement;
import edu.unika.aifb.graphindex.query.exploring.NodeElement;
import edu.unika.aifb.graphindex.query.matcher_v2.SmallIndexGraphMatcher;
import edu.unika.aifb.graphindex.query.matcher_v2.SmallIndexMatchesValidator;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.graphindex.util.Util;
import edu.unika.aifb.keywordsearch.KeywordElement;
import edu.unika.aifb.keywordsearch.TransformedGraph;
import edu.unika.aifb.keywordsearch.TransformedGraphNode;
import edu.unika.aifb.keywordsearch.search.ApproximateStructureMatcher;
import edu.unika.aifb.keywordsearch.search.EntitySearcher;

public class ExploringQueryEvaluator implements IQueryEvaluator {

	private StructureIndexReader m_indexReader;
	private StructureIndex m_index;
	private ExploringIndexMatcher m_matcher;
	private IndexMatchesValidator m_validator;
	private EntitySearcher m_searcher;
	private int m_cutoff;
	
	private static final Logger log = Logger.getLogger(IncrementalQueryEvaluator.class);
	
	public ExploringQueryEvaluator(StructureIndexReader reader, EntitySearcher es) throws StorageException {
		m_indexReader = reader;
		m_index = reader.getIndex();
		
		for (String ig : m_indexReader.getGraphNames()) {
			m_matcher = new ExploringIndexMatcher(m_index, ig);
			m_matcher.initialize();
			break;
		}

		m_validator = new SmallIndexMatchesValidator(m_index, m_index.getCollector());
		
		m_searcher = es;
	}
	
	public void setCutoff(int cutoff) {
		m_cutoff = cutoff;
	}
	
	public List<String[]> evaluate(Query q) throws StorageException {
		Timings timings = new Timings();
		Counters counters = new Counters();
		
		m_matcher.setTimings(timings);
		m_matcher.setCounters(counters);
		m_validator.setTimings(timings);
		m_validator.setCounters(counters);
		m_index.getCollector().addTimings(timings);
		m_index.getCollector().addCounters(counters);
		GTable.timings = timings;
		Tables.timings = timings;

		log.info("evaluating...");
		timings.start(Timings.TOTAL_QUERY_EVAL);
		
//		Graph<QueryNode> queryGraph = q.getGraph();
//		TransformedGraph transformedGraph = new TransformedGraph(queryGraph);
		
//		counters.set(Counters.QUERY_EDGES, queryGraph.edgeCount());
//		counters.set(Counters.QUERY_NODES, queryGraph.nodeCount());
		
		// step 1: entity search
//		timings.start(Timings.STEP_ES);
//		if (m_cutoff < 0)
//			transformedGraph = m_searcher.searchEntities(transformedGraph);
//		else
//			transformedGraph = m_searcher.searchEntities(transformedGraph, m_cutoff);
//		timings.end(Timings.STEP_ES);
		
		// step 2: approximate structure matching
//		ApproximateStructureMatcher asm = new ApproximateStructureMatcher(transformedGraph, 2);
//		asm.setTimings(timings);
//		asm.setCounters(counters);
//		
//		timings.start(Timings.STEP_ASM);
//		GTable<KeywordElement> asmResult = asm.matching();
//		timings.end(Timings.STEP_ASM);
//		
//		counters.set(Counters.ASM_RESULT_SIZE, asmResult.rowCount());
//		log.debug("asm result table: " + asmResult);
//
//		timings.start(Timings.STEP_ASM2IM);
//		
//		List<GraphEdge<QueryNode>> deferredTypeEdges = new ArrayList<GraphEdge<QueryNode>>();
//		List<GraphEdge<QueryNode>> processedTypeEdges = new ArrayList<GraphEdge<QueryNode>>();
//		
//		for (GraphEdge<QueryNode> edge : queryGraph.edges()) {
//			if (edge.getLabel().equals(RDF.TYPE.toString())) {
//				String srcNode = queryGraph.getSourceNode(edge).getName();
//				TransformedGraphNode node = transformedGraph.getNode(srcNode);
//				log.debug(node.getAttributeQueries());
//				if (node.getAttributeQueries().size() > 0 && !node.getAttributeQueries().keySet().contains(RDF.TYPE.toString()))
//					processedTypeEdges.add(edge);
//				else
//					deferredTypeEdges.add(edge);
//			}
//		}
//		
//		log.debug("processed type edges: " + processedTypeEdges);
//		log.debug("deferred type edges: " + deferredTypeEdges);
//		
//		counters.set(Counters.QUERY_DEFERRED_EDGES, deferredTypeEdges.size());
		
		// result of ASM step, mapping query node labels to extensions
		// list of entities with associated extensions
		
//		Map<String,Set<String>> extNode2entity  = new HashMap<String,Set<String>>();
//		Map<String,List<String>> node2columns = new HashMap<String,List<String>>();
//		Map<String,Set<String>> node2exts = new HashMap<String,Set<String>>();
//		
//		GTable<String>[] imTables = new GTable [asmResult.columnCount()];
//		
//		for (KeywordElement[] row : asmResult) {
//			for (int i = 0; i < row.length; i++) {
//				KeywordElement ele = row[i];
//				
//				if (ele == null)
//					continue;
//				
//				String nodeLabel = asmResult.getColumnName(i);
//
//				String extNode = ele.getExtensionId() + nodeLabel;
//				
//				if (!extNode2entity.containsKey(extNode)) 
//					extNode2entity.put(extNode, new HashSet<String>());
//				extNode2entity.get(extNode).add(ele.getUri());
//				
//				if (!node2exts.containsKey(nodeLabel))
//				node2exts.put(nodeLabel, new HashSet<String>());
//			
//				if (!node2exts.get(nodeLabel).add(ele.getExtensionId()))
//					continue;
//			
//				if (!node2columns.containsKey(nodeLabel)) {
//					List<String> columns = new ArrayList<String>();
//					
//					columns.add(nodeLabel);
//					TransformedGraphNode node = transformedGraph.getNode(nodeLabel);
//					for (Collection<String> coll : node.getAttributeQueries().values()) {
//						columns.addAll(coll);
//						counters.inc(Counters.ES_PROCESSED_EDGES);
//					}
//					
//					for (String concept : node.getTypeQueries()) {
//						columns.add(concept);
//						counters.inc(Counters.ES_PROCESSED_EDGES);
//					}
//					
//					node2columns.put(nodeLabel, columns);
//				}
//				
//				List<String> columns = node2columns.get(nodeLabel);
//				
//				if (imTables[i] == null) {
//					imTables[i] = new GTable<String>(columns);
//				}
//				
//				String[] newRow = new String [columns.size()];
//				newRow[0] = ele.getExtensionId();
//				for (int j = 1; j < columns.size(); j++)
//					newRow[j] = "bxx" + nodeLabel + j;
//				imTables[i].addRow(newRow);
//			}
//		}
//		
//		List<GTable<String>> matchTables = new ArrayList<GTable<String>>();
//		int asmIndexMatches = 0;
//		for (GTable<String> table : imTables)
//			if (table != null) {
//				matchTables.add(table);
//				asmIndexMatches += table.rowCount();
//			}
//		
//		counters.set(Counters.ASM_INDEX_MATCHES, asmIndexMatches / matchTables.size());
//		log.debug("match tables: " + matchTables);
		
		
		Map<String,List<GraphElement>> keywords = new HashMap<String,List<GraphElement>>();
		
		List<GraphElement> list = new ArrayList<GraphElement>();
		list.add(new NodeElement("b114217"));
		keywords.put("student20", list);
		
		list = new ArrayList<GraphElement>();
		list.add(new NodeElement("b118409"));
		keywords.put("course3", list);
		
		list = new ArrayList<GraphElement>();
		list.add(new NodeElement("b115768"));
		keywords.put("student51", list);

//		list = new ArrayList<GraphElement>();
//		list.add(new EdgeElement(null, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor", null));
//		keywords.put("advisor", list);
		
		m_matcher.setKeywords(keywords);
		m_matcher.match();
		
		List<GTable<String>> indexMatches = new ArrayList<GTable<String>>();
		List<Query> queries = new ArrayList<Query>();
		
		m_matcher.indexMatches(indexMatches, queries);
		
		for (int i = 0; i < indexMatches.size(); i++) {
			log.debug(queries.get(i));
			log.debug(indexMatches.get(i).toDataString());
		}
		
		
		// step 3: structure-based refinement
//		QueryExecution qe = new QueryExecution(q, m_index);
//		qe.setMatchTables(matchTables);
//		
//		Set<String> constants = new HashSet<String>();
//		Set<String> entityNodes = new HashSet<String>();
//		Map<String,Set<String>> entity2constants = new HashMap<String,Set<String>>();
//		for (GraphEdge<QueryNode> edge : queryGraph.edges()) {
//			// assume edges with constants to be already processed
//			if (Util.isConstant(queryGraph.getTargetNode(edge).getSingleMember()) && !deferredTypeEdges.contains(edge)) {
//				qe.imVisited(edge);
//				
//				constants.add(queryGraph.getTargetNode(edge).getSingleMember());
//				entityNodes.add(queryGraph.getSourceNode(edge).getSingleMember());
//				
//				if (!entity2constants.containsKey(queryGraph.getSourceNode(edge).getName()))
//					entity2constants.put(queryGraph.getSourceNode(edge).getName(), new HashSet<String>());
//				entity2constants.get(queryGraph.getSourceNode(edge).getName()).add(queryGraph.getTargetNode(edge).getName());
//			}
//		}
//		
//		log.debug("im visited: " + qe.getIMVisited().size() + ", " + qe.getIMVisited());
//
//		timings.end(Timings.STEP_ASM2IM);
//
//		m_matcher.setQueryExecution(qe);
//		
//		timings.start(Timings.STEP_IM);
//		m_matcher.match();
//		timings.end(Timings.STEP_IM);
//
//		log.debug(qe.getIndexMatches());
//		
//		if (qe.getIndexMatches() != null) {
//			timings.start(Timings.STEP_IM2DM);
//
//			// step 4: result computation
//			// add entites from remaining extensions to an intermediate result set
//			
//			List<EvaluationClass> classes = new ArrayList<EvaluationClass>();
//			classes.add(new EvaluationClass(qe.getIndexMatches()));
//			
//			for (GraphEdge<QueryNode> edge : queryGraph.edges()) {
//				// assume edges with constants to be already processed
//				if (Util.isConstant(queryGraph.getTargetNode(edge).getSingleMember()) && !deferredTypeEdges.contains(edge)) {
//					qe.visited(edge);
//					
//					// update classes
//					List<EvaluationClass> newClasses = new ArrayList<EvaluationClass>();
//					for (EvaluationClass ec : classes) {
//						newClasses.addAll(ec.addMatch(queryGraph.getTargetNode(edge).getName(), true, null, null));
//					}
//					classes.addAll(newClasses);
//					newClasses.clear();
//					for (EvaluationClass ec : classes) {
//						newClasses.addAll(ec.addMatch(queryGraph.getSourceNode(edge).getName(), false, null, null));
//					}
//					classes.addAll(newClasses);
//				} 
//			}
//			
//			log.debug("dm visited: " + qe.getVisited().size() + ", " + qe.getVisited());
//			
//			int rows = 0;
//			for (EvaluationClass ec : classes) {
//				for (String entityNode : entityNodes) {
//					if (!ec.getMappings().hasColumn(entityNode))
//						continue;
//					if (qe.getQuery().getRemovedNodes().contains(entityNode))
//						continue;
//						
//					List<String> columns = new ArrayList<String>(entity2constants.get(entityNode));
//					columns.add(entityNode);
//					GTable<String> table = new GTable<String>(columns);
//					int col = table.getColumn(entityNode);
//	
//					for (String entity : extNode2entity.get(ec.getMatch(entityNode) + entityNode)) {
//						String[] row = new String [entity2constants.get(entityNode).size() + 1];
//						row[col] = entity;
//						for (String constant : entity2constants.get(entityNode))
//							row[table.getColumn(constant)] = constant;
//						table.addRow(row);
//					}
//					ec.getResults().add(table);
//					rows += table.rowCount();
//				}
//			}
//			qe.setEvaluationClasses(classes);
//			
//			timings.end(Timings.STEP_IM2DM);
//			
//			m_validator.setQueryExecution(qe);
//			((SmallIndexMatchesValidator)m_validator).setIncrementalState(m_searcher, deferredTypeEdges);
//	
//			timings.start(Timings.STEP_DM);
//			m_validator.validateIndexMatches();
//			timings.end(Timings.STEP_DM);
//		}
//		else
//			qe.addResult(new GTable<String>(qe.getQuery().getSelectVariables()), false);
//		
//		qe.finished();
//		log.debug(qe.getResult());
//		
//		timings.end(Timings.TOTAL_QUERY_EVAL);
//		
//		counters.set(Counters.RESULTS, qe.getResult().rowCount());
		
		return new ArrayList<String[]>();
	}

	public long[] getTimings() {
		return null;
	}

	public void clearCaches() throws StorageException {
	}
}

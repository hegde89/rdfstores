package edu.unika.aifb.graphindex.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.StructureIndexReader;
import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphEdge;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.query.matcher_v2.SmallIndexGraphMatcher;
import edu.unika.aifb.graphindex.query.matcher_v2.SmallIndexMatchesValidator;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Util;
import edu.unika.aifb.keywordsearch.TransformedGraph;
import edu.unika.aifb.keywordsearch.search.EntitySearcher;

public class IncrementalQueryEvaluator implements IQueryEvaluator {

	private StructureIndexReader m_indexReader;
	private StructureIndex m_index;
	private IndexGraphMatcher m_matcher;
	private IndexMatchesValidator m_validator;
	private EntitySearcher m_searcher;
	
	public IncrementalQueryEvaluator(StructureIndexReader reader, String keywordIndexDirectory) throws StorageException {
		m_indexReader = reader;
		m_index = reader.getIndex();
		
		for (String ig : m_indexReader.getGraphNames()) {
			m_matcher = new SmallIndexGraphMatcher(m_index, ig);
			break;
		}

		m_validator = new SmallIndexMatchesValidator(m_index, m_index.getCollector());
		
		m_searcher = new EntitySearcher(keywordIndexDirectory);
	}
	
	public List<String[]> evaluate(Query q) throws StorageException {
		Graph<QueryNode> queryGraph = q.getGraph();
		TransformedGraph transformedGraph = new TransformedGraph(queryGraph);
		
		// step 1: entity search
		transformedGraph = m_searcher.searchEntities(transformedGraph);
		
		// step 2: approximate structure matching
		
		// result of ASM step, mapping query node labels to extensions
		// list of entities with associated extensions
		
		
		// step 3: structure-based refinement
		QueryExecution qe = new QueryExecution(q, m_index);
		
		Set<String> constants = new HashSet<String>();
		for (GraphEdge<QueryNode> edge : queryGraph.edges()) {
			// assume edges with constants to be already processed
//			if (Util.isConstant(queryGraph.getTargetNode(edge).getSingleMember())) {
//				qe.imVisited(edge);
//				constants.add(queryGraph.getTargetNode(edge).getSingleMember());
//			}
		}

		// TODO modify SIGMatcher to work with partial matches
		
		m_matcher.setQueryExecution(qe);
		m_matcher.match();
		
		// TODO print intermediate results
		
		// step 4: result computation
		// add entites from remaining extensions to an intermediate result set
		
		m_validator.setQueryExecution(qe);
		m_validator.validateIndexMatches();
		
		qe.finished();
		
		return qe.getResult().getRows();
	}

	public long[] getTimings() {
		return null;
	}

	public void clearCaches() throws StorageException {
	}
}

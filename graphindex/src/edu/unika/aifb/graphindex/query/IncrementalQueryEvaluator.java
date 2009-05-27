package edu.unika.aifb.graphindex.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

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
	
	private static final Logger log = Logger.getLogger(IncrementalQueryEvaluator.class);
	
	public IncrementalQueryEvaluator(StructureIndexReader reader, String keywordIndexDirectory) throws StorageException {
		m_indexReader = reader;
		m_index = reader.getIndex();
		
		for (String ig : m_indexReader.getGraphNames()) {
			m_matcher = new SmallIndexGraphMatcher(m_index, ig);
			m_matcher.initialize();
			break;
		}

		m_validator = new SmallIndexMatchesValidator(m_index, m_index.getCollector());
		
		m_searcher = new EntitySearcher(keywordIndexDirectory);
	}
	
	public List<String[]> evaluate(Query q) throws StorageException {
		Graph<QueryNode> queryGraph = q.getGraph();
//		TransformedGraph transformedGraph = new TransformedGraph(queryGraph);
		
		// step 1: entity search
//		transformedGraph = m_searcher.searchEntities(transformedGraph);
		
		// step 2: approximate structure matching
		
		// result of ASM step, mapping query node labels to extensions
		// list of entities with associated extensions
		
		// test values for PathQuery.q75
		Map<String,String> entity2ext = new HashMap<String,String>();
		entity2ext.put("http://www.Department0.University0.edu/Course0", "b542");
		entity2ext.put("http://www.Department10.University0.edu/Course51", "b178");
		entity2ext.put("http://www.Department10.University0.edu/Course53", "b178");
		entity2ext.put("http://www.Department0.University0.edu/Course50", "b50");
		entity2ext.put("http://www.Department10.University0.edu/Course50", "b185");
		
		Map<String,Set<String>> ext2entity  = new HashMap<String,Set<String>>();
		for (String entity : entity2ext.keySet()) {
			String ext = entity2ext.get(entity);
			if (!ext2entity.containsKey(ext))
				ext2entity.put(ext, new HashSet<String>());
			ext2entity.get(ext).add(entity);
		}
		
		Map<String,Set<String>> node2entity = new HashMap<String,Set<String>>();
		node2entity.put("?x2", new HashSet<String>(Arrays.asList("http://www.Department0.University0.edu/Course0", "http://www.Department0.University10.edu/Course51", "http://www.Department10.University0.edu/Course53")));
		node2entity.put("?x3", new HashSet<String>(Arrays.asList("http://www.Department0.University0.edu/Course50", "http://www.Department0.University10.edu/Course50")));
		
		GTable<String> imMatch1 = new GTable<String>("?x2", "Course50");
		imMatch1.addRow(new String[] { "b542", "bx1" });
		imMatch1.addRow(new String[] { "b178", "bx1" });
		imMatch1.addRow(new String[] { "b178", "bx1" });
		
		GTable<String> imMatch2 = new GTable<String>("?x3", "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course");
		imMatch2.addRow(new String[] { "b50", "bx2" });
		imMatch2.addRow(new String[] { "b185", "bx2" });
		
		// TODO tables for connected edges have to be merged into one table
		
		// step 3: structure-based refinement
		QueryExecution qe = new QueryExecution(q, m_index);
		
		qe.setMatchTables(Arrays.asList(imMatch1, imMatch2));
		
		Set<String> constants = new HashSet<String>();
		Set<String> entityNodes = new HashSet<String>();
		Map<String,Set<String>> entity2constants = new HashMap<String,Set<String>>();
		for (GraphEdge<QueryNode> edge : queryGraph.edges()) {
			// assume edges with constants to be already processed
			if (Util.isConstant(queryGraph.getTargetNode(edge).getSingleMember())) {
				qe.imVisited(edge);
				qe.visited(edge);
				constants.add(queryGraph.getTargetNode(edge).getSingleMember());
				entityNodes.add(queryGraph.getSourceNode(edge).getSingleMember());
				
				if (!entity2constants.containsKey(queryGraph.getSourceNode(edge).getName()))
					entity2constants.put(queryGraph.getSourceNode(edge).getName(), new HashSet<String>());
				entity2constants.get(queryGraph.getSourceNode(edge).getName()).add(queryGraph.getTargetNode(edge).getName());
			}
		}

		m_matcher.setQueryExecution(qe);
		m_matcher.match();
		
		// TODO print intermediate results
		
		// step 4: result computation
		// add entites from remaining extensions to an intermediate result set
		
		List<EvaluationClass> classes = new ArrayList<EvaluationClass>();
		classes.add(new EvaluationClass(qe.getIndexMatches()));
		log.debug(qe.getIndexMatches().toDataString());
		
		for (GraphEdge<QueryNode> edge : queryGraph.edges()) {
			// assume edges with constants to be already processed
			if (Util.isConstant(queryGraph.getTargetNode(edge).getSingleMember())) {
				qe.imVisited(edge);
				List<EvaluationClass> newClasses = new ArrayList<EvaluationClass>();
				for (EvaluationClass ec : classes) {
					newClasses.addAll(ec.addMatch(queryGraph.getTargetNode(edge).getName(),true, null, null));
				}
				classes.addAll(newClasses);
				newClasses.clear();
				for (EvaluationClass ec : classes) {
					newClasses.addAll(ec.addMatch(queryGraph.getSourceNode(edge).getName(),false, null, null));
				}
				classes.addAll(newClasses);
			} 
		}
		
		for (EvaluationClass ec : classes) {
			log.debug(ec.getMatches());
			log.debug(ec.getMappings());
			
			for (String entityNode : entityNodes) {
				if (!ec.getMappings().hasColumn(entityNode))
					continue;
				
				List<String> columns = new ArrayList<String>(entity2constants.get(entityNode));
				columns.add(entityNode);
				GTable<String> table = new GTable<String>(columns);
				int col = table.getColumn(entityNode);
				for (String entity : ext2entity.get(ec.getMatch(entityNode))) {
					String[] row = new String [entity2constants.get(entityNode).size() + 1];
					row[col] = entity;
					for (String constant : entity2constants.get(entityNode))
						row[table.getColumn(constant)] = constant;
					table.addRow(row);
				}
				ec.getResults().add(table);
			}
		}
		
		qe.setEvaluationClasses(classes);
		
		m_validator.setQueryExecution(qe);
		m_validator.validateIndexMatches();
		
		qe.finished();
		log.debug(qe.getResult().toDataString());
		return qe.getResult().getRows();
	}

	public long[] getTimings() {
		return null;
	}

	public void clearCaches() throws StorageException {
	}
}

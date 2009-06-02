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
import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.GraphEdge;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.query.matcher_v2.SmallIndexGraphMatcher;
import edu.unika.aifb.graphindex.query.matcher_v2.SmallIndexMatchesValidator;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.graphindex.util.TypeUtil;
import edu.unika.aifb.graphindex.util.Util;
import edu.unika.aifb.keywordsearch.KeywordElement;
import edu.unika.aifb.keywordsearch.TransformedGraph;
import edu.unika.aifb.keywordsearch.TransformedGraphNode;
import edu.unika.aifb.keywordsearch.search.ApproximateStructureMatcher;
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
		Timings timings = new Timings();
		
		m_matcher.setTimings(timings);
		m_index.getCollector().addTimings(timings);
		
		log.info("evaluating...");
		
		Graph<QueryNode> queryGraph = q.getGraph();
		TransformedGraph transformedGraph = new TransformedGraph(queryGraph);
		
		// step 1: entity search
		timings.start(Timings.KW_ENTITY_SEARCH);
		transformedGraph = m_searcher.searchEntities(transformedGraph);
		timings.end(Timings.KW_ENTITY_SEARCH);
		
		// step 2: approximate structure matching
		timings.start(Timings.KW_ASM);
		ApproximateStructureMatcher asm = new ApproximateStructureMatcher(transformedGraph, 2);
		GTable<KeywordElement> asmResult = asm.matching();
		timings.end(Timings.KW_ASM);
		
		log.debug("asm result table: " + asmResult);
		
		m_index.getCollector().logStats();

		// result of ASM step, mapping query node labels to extensions
		// list of entities with associated extensions
		
		Map<String,Set<String>> extNode2entity  = new HashMap<String,Set<String>>();
		Map<String,List<String>> node2columns = new HashMap<String,List<String>>();
		Map<String,Set<String>> node2exts = new HashMap<String,Set<String>>();
		
		GTable<String>[] imTables = new GTable [asmResult.columnCount()];
		
		for (KeywordElement[] row : asmResult) {
			for (int i = 0; i < row.length; i++) {
				KeywordElement ele = row[i];
				
				if (ele == null)
					continue;
				
				String nodeLabel = asmResult.getColumnName(i);

				String extNode = ele.getExtensionId() + nodeLabel;
				
				if (!extNode2entity.containsKey(extNode)) 
					extNode2entity.put(extNode, new HashSet<String>());
				extNode2entity.get(extNode).add(ele.getUri());
				
				if (!node2exts.containsKey(nodeLabel))
				node2exts.put(nodeLabel, new HashSet<String>());
			
				if (!node2exts.get(nodeLabel).add(ele.getExtensionId()))
					continue;
			
				if (!node2columns.containsKey(nodeLabel)) {
					List<String> columns = new ArrayList<String>();
					
					columns.add(nodeLabel);
					TransformedGraphNode node = transformedGraph.getNode(nodeLabel);
					for (Collection<String> coll : node.getAttributeQueries().values()) {
						columns.addAll(coll);
					}
					
					node2columns.put(nodeLabel, columns);
				}
				
				List<String> columns = node2columns.get(nodeLabel);
				
				if (imTables[i] == null) {
					imTables[i] = new GTable<String>(columns);
				}
				
				String[] newRow = new String [columns.size()];
				newRow[0] = ele.getExtensionId();
				for (int j = 1; j < columns.size(); j++)
					newRow[j] = "bxx" + nodeLabel + j;
				imTables[i].addRow(newRow);
			}
		}
		
		List<GTable<String>> matchTables = new ArrayList<GTable<String>>();
		for (GTable<String> table : imTables)
			if (table != null)
				matchTables.add(table);
		
		log.debug("match tables: " + matchTables);
		
		// step 3: structure-based refinement
		QueryExecution qe = new QueryExecution(q, m_index);
		
		qe.setMatchTables(matchTables);
		
		Set<String> constants = new HashSet<String>();
		Set<String> entityNodes = new HashSet<String>();
		Map<String,Set<String>> entity2constants = new HashMap<String,Set<String>>();
		for (GraphEdge<QueryNode> edge : queryGraph.edges()) {
			// assume edges with constants to be already processed
			if (Util.isConstant(queryGraph.getTargetNode(edge).getSingleMember()) && !edge.getLabel().equals(RDF.TYPE.toString())) {
				qe.imVisited(edge);
				
				constants.add(queryGraph.getTargetNode(edge).getSingleMember());
				entityNodes.add(queryGraph.getSourceNode(edge).getSingleMember());
				
				if (!entity2constants.containsKey(queryGraph.getSourceNode(edge).getName()))
					entity2constants.put(queryGraph.getSourceNode(edge).getName(), new HashSet<String>());
				entity2constants.get(queryGraph.getSourceNode(edge).getName()).add(queryGraph.getTargetNode(edge).getName());
			}
		}

		m_matcher.setQueryExecution(qe);
		
		timings.start(Timings.STEP_IM);
		m_matcher.match();
		timings.end(Timings.STEP_IM);
		
		// TODO print intermediate results
		
//		List<String> refinedColumns = new ArrayList<String>();
//		refinedColumns.addAll(constants);
//		refinedColumns.addAll(entityNodes);
		
//		GTable<KeywordElement> refinedResults = new GTable<KeywordElement>(refinedColumns);
//		for (KeywordElement[] row : asmResult) {
//			boolean found = true;
//			KeywordElement[] newRow = new KeywordElement [refinedResults.columnCount()];
//			int j = 0;
//			for (String[] matchedRow : qe.getIndexMatches()) {
//				found = true;
//				for (int i = 0; i < row.length; i++) {
//					if (row[i] == null)
//						continue;
//					
//					int matchedCol = qe.getIndexMatches().getColumn(asmResult.getColumnName(i));
//					newRow[refinedResults.getColumn(asmResult.getColumnName(i))] = row[i];
//					
//					if (!matchedRow[matchedCol].equals(row[i].getExtensionId())) {
//						found = false;
//						break;
//					}
//				}
//				j++;
//				if (found)
//					break;
//			}
//			
//			if (found) {
//				for (String constant : constants)
//					newRow[refinedResults.getColumn(constant)] = null;
//				refinedResults.addRow(newRow);
//			}
//		}
//		
//		log.debug(asmResult + " " + refinedResults);
		
		// step 4: result computation
		// add entites from remaining extensions to an intermediate result set
		
		List<EvaluationClass> classes = new ArrayList<EvaluationClass>();
		classes.add(new EvaluationClass(qe.getIndexMatches()));
//		log.debug(qe.getIndexMatches().toDataString());
		
		for (GraphEdge<QueryNode> edge : queryGraph.edges()) {
			// assume edges with constants to be already processed
			if (Util.isConstant(queryGraph.getTargetNode(edge).getSingleMember()) && !edge.getLabel().equals(RDF.TYPE.toString())) {
				qe.imVisited(edge);
				qe.visited(edge);
				List<EvaluationClass> newClasses = new ArrayList<EvaluationClass>();
				for (EvaluationClass ec : classes) {
					newClasses.addAll(ec.addMatch(queryGraph.getTargetNode(edge).getName(), true, null, null));
				}
				classes.addAll(newClasses);
				newClasses.clear();
				for (EvaluationClass ec : classes) {
					newClasses.addAll(ec.addMatch(queryGraph.getSourceNode(edge).getName(), false, null, null));
				}
				classes.addAll(newClasses);
			} 
		}
		
		int rows = 0, rows2 = 0;
		for (EvaluationClass ec : classes) {
//			log.debug(ec.getMatches());
//			log.debug(ec.getMappings());
			
			for (String entityNode : entityNodes) {
				if (!ec.getMappings().hasColumn(entityNode))
					continue;
				
				List<String> columns = new ArrayList<String>(entity2constants.get(entityNode));
				columns.add(entityNode);
				GTable<String> table = new GTable<String>(columns);
				int col = table.getColumn(entityNode);

				for (String entity : extNode2entity.get(ec.getMatch(entityNode) + entityNode)) {
					String[] row = new String [entity2constants.get(entityNode).size() + 1];
					row[col] = entity;
					for (String constant : entity2constants.get(entityNode))
						row[table.getColumn(constant)] = constant;
					table.addRow(row);
				}
				ec.getResults().add(table);
				rows += table.rowCount();
//				log.debug(table.toDataString(false));
			}
			
//			GTable<String> table = new GTable<String>(refinedResults.getColumnNames());
//			for (KeywordElement[] row : refinedResults) {
//				boolean fits = true;
//				for (String entityNode : entityNodes) {
//					if (!row[refinedResults.getColumn(entityNode)].getExtensionId().equals(ec.getMatch(entityNode))) {
//						fits = false;
//						break;
//					}
//				}
//				
//				if (fits) {
//					String[] newRow = new String [refinedResults.columnCount()];
//					for (int i = 0; i < newRow.length; i++) {
//						if (entityNodes.contains(refinedResults.getColumnName(i))) {
//							newRow[i] = row[i].getUri();
//						}
//						else
//							newRow[i] = refinedResults.getColumnName(i);
//					}
//					table.addRow(newRow);
//				}
//			}
//			ec.getResults().add(table);
//			log.debug(table.rowCount());
//			rows2 += table.rowCount();
			
//			log.debug(table.toDataString(false));
//			log.debug("----");
		}
//		log.debug(rows + " " + rows2);
		qe.setEvaluationClasses(classes);
		
		m_validator.setQueryExecution(qe);

		timings.start(Timings.STEP_DM);
		m_validator.validateIndexMatches();
		timings.end(Timings.STEP_DM);
		
		m_index.getCollector().addTimings(m_validator.getTimings());
		
		qe.finished();
		log.debug(qe.getResult());
		
		m_index.getCollector().logStats();
		
		return qe.getResult().getRows();
	}

	public long[] getTimings() {
		return null;
	}

	public void clearCaches() throws StorageException {
	}
}

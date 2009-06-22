package edu.unika.aifb.graphindex.query;

import java.io.IOException;
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
import edu.unika.aifb.graphindex.query.matcher_v2.SmallIndexGraphMatcher;
import edu.unika.aifb.graphindex.query.matcher_v2.SmallIndexMatchesValidator;
import edu.unika.aifb.graphindex.query.model.Query;
import edu.unika.aifb.graphindex.storage.ExtensionStorage;
import edu.unika.aifb.graphindex.storage.NeighborhoodStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.ExtensionStorage.IndexDescription;
import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.StatisticsCollector;
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.graphindex.util.TypeUtil;
import edu.unika.aifb.graphindex.util.Util;
import edu.unika.aifb.graphindex.vp.LuceneStorage;
import edu.unika.aifb.graphindex.vp.VPQueryEvaluator;
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
	private ExtensionStorage m_es;
	private int m_cutoff;
	private NeighborhoodStorage m_ns;
	private EntityLoader m_el;
	private int m_nk = 2;
	private VPQueryEvaluator m_evaluator;
	private StatisticsCollector m_collector;
	
	private static final Logger log = Logger.getLogger(IncrementalQueryEvaluator.class);
	
	public IncrementalQueryEvaluator(StructureIndexReader reader, EntityLoader el, NeighborhoodStorage ns, StatisticsCollector collector, int nk) throws StorageException {
		m_indexReader = reader;
		m_index = reader.getIndex();
		m_ns = ns;
		m_es = m_index.getExtensionManager().getExtensionStorage();
		
		for (String ig : reader.getGraphNames()) {
			m_matcher = new SmallIndexGraphMatcher(m_index, ig);
			m_matcher.initialize();
			break;
		}

		m_validator = new SmallIndexMatchesValidator(m_index, m_index.getCollector());
		m_el = el;
		m_nk = nk;
	}
	
	public void setCutoff(int cutoff) {
		m_cutoff = cutoff;
	}
	
	public void setNeighborhoodSize(int nk) {
		m_nk  = nk;
	}
	
	public List<String[]> evaluate(Query q) throws StorageException, IOException {
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

		counters.set(Counters.ES_CUTOFF, m_cutoff);
		log.info("evaluating...");
		timings.start(Timings.TOTAL_QUERY_EVAL);
		
		Graph<QueryNode> queryGraph = q.getGraph();
		TransformedGraph transformedGraph = new TransformedGraph(queryGraph);
		
		counters.set(Counters.QUERY_EDGES, queryGraph.edgeCount());
		counters.set(Counters.QUERY_NODES, queryGraph.nodeCount());
		
		// step 1: entity search
		timings.start(Timings.STEP_ES);
		m_el.setCutoff(m_cutoff);
		transformedGraph = m_el.loadEntities(transformedGraph, m_ns);
		timings.end(Timings.STEP_ES);
		
		Map<String,Set<KeywordElement>> esSets = new HashMap<String,Set<KeywordElement>>();
		for (TransformedGraphNode tgn : transformedGraph.getNodes()) {
			if (tgn.getEntities() != null)
				esSets.put(tgn.getNodeName(), new HashSet<KeywordElement>(tgn.getEntities()));
			else
				esSets.put(tgn.getNodeName(), new HashSet<KeywordElement>());
		}
		
		// step 2: approximate structure matching
		ApproximateStructureMatcher asm = new ApproximateStructureMatcher(transformedGraph, m_nk, m_ns);
		asm.setTimings(timings);
		asm.setCounters(counters);
		
		timings.start(Timings.STEP_ASM);
		GTable<KeywordElement> asmResult = asm.matching();
		timings.end(Timings.STEP_ASM);
		
		counters.set(Counters.ASM_RESULT_SIZE, asmResult.rowCount());
		log.debug("asm result table: " + asmResult);

		timings.start(Timings.STEP_IM);
		
		Set<String> entityNodes = new HashSet<String>();
		Set<String> constants = new HashSet<String>();
		
		for (GraphEdge<QueryNode> edge : queryGraph.edges()) {
			if (Util.isConstant(queryGraph.getTargetNode(edge).getSingleMember())) {
				entityNodes.add(queryGraph.getSourceNode(edge).getSingleMember());
				constants.add(queryGraph.getTargetNode(edge).getSingleMember());
			}
		}
		
		log.debug("entity nodes: " + entityNodes);
		log.debug("constants: " + constants);
		
		if (asmResult.columnCount() < entityNodes.size() || asmResult.rowCount() == 0) {
			log.error("not enough nodes from ASM, probably nk of this index too small for this query");
			timings.end(Timings.STEP_IM);
			timings.end(Timings.TOTAL_QUERY_EVAL);
			return new ArrayList<String[]>();
		}
		
		// result of ASM step, mapping query node labels to extensions
		// list of entities with associated extensions
		
		List<String> columns = new ArrayList<String>();
		columns.addAll(Arrays.asList(asmResult.getColumnNames()));
		columns.addAll(constants);
		GTable<String> resultTable = new GTable<String>(columns);
		GTable<String> matchTable = new GTable<String>(columns);

		Set<String> matchSignatures = new HashSet<String>(asmResult.rowCount() / 4);
		Map<String,List<String[]>> matchRows = new HashMap<String,List<String[]>>(asmResult.rowCount() / 4);
		
		for (KeywordElement[] row : asmResult) {
			String[] resultRow = new String [resultTable.columnCount()];
			String[] matchRow = new String [matchTable.columnCount()];
			
			StringBuilder matchSignature = new StringBuilder();
			for (int i = 0; i < row.length; i++) {
				KeywordElement ele = row[i];
				String node = asmResult.getColumnName(i);
				if (!node.equals(resultTable.getColumnName(i)))
					log.error("whut");
				
				resultRow[i] = ele.getUri();
				matchRow[i] = m_es.getDataItem(IndexDescription.SES, ele.getUri());
				matchSignature.append(matchRow[i]).append("_");
			}
			
			for (int i = row.length; i < resultTable.columnCount(); i++) {
				resultRow[i] = resultTable.getColumnName(i);
				matchRow[i] = "bxx" + i;
			}
			
			resultTable.addRow(resultRow);
			
			String sig = matchSignature.toString();
			if (matchSignatures.add(sig))
				matchTable.addRow(matchRow);
			
			List<String[]> rows = matchRows.get(sig);
			if (rows == null) {
				rows = new ArrayList<String[]>(asmResult.rowCount() / 2);
				matchRows.put(sig, rows);
			}
			rows.add(resultRow);
		}
		
		q.setSelectVariables(new ArrayList<String>(entityNodes));
		q.createQueryGraph(m_index);

		// step 3: structure-based refinement
		QueryExecution qe = new QueryExecution(q, m_index);
		for (GraphEdge<QueryNode> edge : queryGraph.edges()) {
			if (Util.isConstant(queryGraph.getTargetNode(edge).getSingleMember())) {
				qe.visited(edge);
				qe.imVisited(edge);
			}
		}
		
		qe.setMatchTables(new ArrayList<GTable<String>>(Arrays.asList(matchTable)));
		
		m_matcher.setQueryExecution(qe);
		m_matcher.match();
		timings.end(Timings.STEP_IM);
		
		log.debug(qe.getIndexMatches());
		
		timings.start(Timings.STEP_DM);
		List<EvaluationClass> classes = new ArrayList<EvaluationClass>();
		classes.add(new EvaluationClass(qe.getIndexMatches()));
		
		for (String constant : constants) {
			List<EvaluationClass> newClasses = new ArrayList<EvaluationClass>();
			for (EvaluationClass ec : classes) {
				newClasses.addAll(ec.addMatch(constant, true, null, null));
			}
			classes.addAll(newClasses);					
		}
		
		for (String node : entityNodes) {
			List<EvaluationClass> newClasses = new ArrayList<EvaluationClass>();
			for (EvaluationClass ec : classes) {
				newClasses.addAll(ec.addMatch(node, false, null, null));
			}
			classes.addAll(newClasses);					
		}
		
		GTable<String> resultsAfterIM = new GTable<String>(resultTable, false);
		int rowsAfterIM = 0;
		for (EvaluationClass ec : classes) {
			StringBuilder sig = new StringBuilder();
			for (String col : asmResult.getColumnNames())
				sig.append(ec.getMatch(col)).append("_");
			
			GTable<String> table = new GTable<String>(resultTable.getColumnNames());
			table.addRows(matchRows.get(sig.toString()));
			resultsAfterIM.addRows(matchRows.get(sig.toString()));
			ec.getResults().add(table);
			
			rowsAfterIM += table.rowCount();
		}
		
		counters.set(Counters.IM_RESULT_SIZE, rowsAfterIM);
		
		qe.setEvaluationClasses(classes);
		
		m_validator.setQueryExecution(qe);
		m_validator.validateIndexMatches();
		
		qe.finished();
		log.debug(qe.getResult());
		timings.end(Timings.STEP_DM);
		timings.end(Timings.TOTAL_QUERY_EVAL);
		
		counters.set(Counters.RESULTS, qe.getResult() != null ? qe.getResult().rowCount() : 0);
		
		double pES= 0, pASM = 0, pSBR = 0;
		for (String node : entityNodes) {
			Set<String> esEntities = new HashSet<String>(), asmEntities = new HashSet<String>();
			Set<String> imEntites = new HashSet<String>(), finalEntities = new HashSet<String>();
			
			for (KeywordElement ele : esSets.get(node)) //transformedGraph.getNode(node).getEntities())
				esEntities.add(ele.getUri());
			
			entitiesForColumn(resultTable, node, asmEntities);
			entitiesForColumn(resultsAfterIM, node, imEntites);
			if (qe.getResult() != null)
				entitiesForColumn(qe.getResult(), node, finalEntities);
			
			log.debug("node: " + node + " " + esEntities.size() + " " + asmEntities.size() + " " + imEntites.size() + " " + finalEntities.size());
//			log.debug(esEntities.containsAll(asmEntities));
//			log.debug(asmEntities.containsAll(imEntites));
//			log.debug(imEntites.containsAll(finalEntities));
			
			pES += finalEntities.size() / (double)esEntities.size();
			pASM += finalEntities.size() / (double)asmEntities.size();
			pSBR += finalEntities.size() / (double)imEntites.size();
		}
		pES /= entityNodes.size();
		pASM /= entityNodes.size();
		pSBR /= entityNodes.size();
		log.debug(pES + " " + pASM + " " + pSBR);
		
		counters.set(Counters.INC_PRCS_ES, pES);
		counters.set(Counters.INC_PRCS_ASM, pASM);
		counters.set(Counters.INC_PRCS_SBR, pSBR);
		
		if (qe.getResult() != null)
			return qe.getResult().getRows();
		else
			return null;
	}
	
	public void entitiesForColumn(GTable<String> table, String colName, Set<String> entities) {
		int col = table.getColumn(colName);
		for (String[] row : table)
			entities.add(row[col]);
	}
	
	public long[] getTimings() {
		return null;
	}

	public void clearCaches() throws StorageException {
	}
}

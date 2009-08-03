package edu.unika.aifb.graphindex.searcher.structured.incremental;

/**
 * Copyright (C) 2009 GŸnter Ladwig (gla at aifb.uni-karlsruhe.de)
 * 
 * This file is part of the graphindex project.
 *
 * graphindex is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2
 * as published by the Free Software Foundation.
 * 
 * graphindex is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with graphindex.  If not, see <http://www.gnu.org/licenses/>.
 */

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

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.index.StructureIndex;
import edu.unika.aifb.graphindex.query.QueryEdge;
import edu.unika.aifb.graphindex.query.QueryGraph;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.entity.ApproximateStructureMatcher;
import edu.unika.aifb.graphindex.searcher.entity.EntityLoader;
import edu.unika.aifb.graphindex.searcher.entity.EntitySearcher;
import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordElement;
import edu.unika.aifb.graphindex.searcher.keyword.model.TransformedGraph;
import edu.unika.aifb.graphindex.searcher.keyword.model.TransformedGraphNode;
import edu.unika.aifb.graphindex.searcher.structured.QueryExecution;
import edu.unika.aifb.graphindex.searcher.structured.StructuredQueryEvaluator;
import edu.unika.aifb.graphindex.searcher.structured.sig.EvaluationClass;
import edu.unika.aifb.graphindex.searcher.structured.sig.IndexGraphMatcher;
import edu.unika.aifb.graphindex.searcher.structured.sig.IndexMatchesValidator;
import edu.unika.aifb.graphindex.searcher.structured.sig.SmallIndexGraphMatcher;
import edu.unika.aifb.graphindex.searcher.structured.sig.SmallIndexMatchesValidator;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.NeighborhoodStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.StatisticsCollector;
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.graphindex.util.TypeUtil;
import edu.unika.aifb.graphindex.util.Util;

public class IncrementalQueryEvaluator extends StructuredQueryEvaluator {

	private StructureIndex m_si;
	private IndexGraphMatcher m_matcher;
	private IndexMatchesValidator m_validator;
	private EntitySearcher m_searcher;
	private int m_cutoff;
	private NeighborhoodStorage m_ns;
	private EntityLoader m_el;
	private int m_nk = 2;
	private StatisticsCollector m_collector;
	
	private static final Logger log = Logger.getLogger(IncrementalQueryEvaluator.class);
	
	public IncrementalQueryEvaluator(IndexReader reader, NeighborhoodStorage ns, StatisticsCollector collector, int nk) throws StorageException, IOException {
		super(reader);
		m_ns = ns;
		
		m_matcher = new SmallIndexGraphMatcher(reader);
		m_matcher.initialize();

		m_validator = new SmallIndexMatchesValidator(reader);
		m_el = new EntityLoader(reader);
		m_nk = nk;
		
		m_si = reader.getStructureIndex();
	}
	
	public void setCutoff(int cutoff) {
		m_cutoff = cutoff;
	}
	
	public void setNeighborhoodSize(int nk) {
		m_nk  = nk;
	}
	
	@SuppressWarnings("unchecked")
	public Table<String> evaluate(StructuredQuery q) throws StorageException, IOException {
		Timings timings = new Timings();
		Counters counters = new Counters();
		
		m_matcher.setTimings(timings);
		m_matcher.setCounters(counters);
		m_validator.setTimings(timings);
		m_validator.setCounters(counters);
		m_idxReader.getCollector().addTimings(timings);
		m_idxReader.getCollector().addCounters(counters);
		Table.timings = timings;
		Tables.timings = timings;

		counters.set(Counters.ES_CUTOFF, m_cutoff);
		log.info("evaluating...");
		timings.start(Timings.TOTAL_QUERY_EVAL);
		
		QueryGraph queryGraph = q.getQueryGraph();
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
		Table<KeywordElement> asmResult = asm.matching();
		timings.end(Timings.STEP_ASM);
		
		counters.set(Counters.ASM_RESULT_SIZE, asmResult.rowCount());
		log.debug("asm result table: " + asmResult);

		timings.start(Timings.STEP_IM);
		
		Set<String> entityNodes = new HashSet<String>();
		Set<String> constants = new HashSet<String>();
		
		for (QueryEdge edge : queryGraph.edgeSet()) {
			if (Util.isConstant(edge.getTarget().getLabel())) {
				entityNodes.add(edge.getSource().getLabel());
				constants.add(edge.getTarget().getLabel());
			}
		}
		
		log.debug("entity nodes: " + entityNodes);
		log.debug("constants: " + constants);
		
		if (asmResult.columnCount() < entityNodes.size() || asmResult.rowCount() == 0) {
			log.error("not enough nodes from ASM, probably nk of this index too small for this query");
			timings.end(Timings.STEP_IM);
			timings.end(Timings.TOTAL_QUERY_EVAL);
			return new Table<String>(q.getSelectVariableLabels());
		}
		
		// result of ASM step, mapping query node labels to extensions
		// list of entities with associated extensions
		
		List<String> columns = new ArrayList<String>();
		columns.addAll(Arrays.asList(asmResult.getColumnNames()));
		columns.addAll(constants);
		Table<String> resultTable = new Table<String>(columns);
		Table<String> matchTable = new Table<String>(columns);

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
				matchRow[i] = m_si.getExtension(ele.getUri());
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
		
//		q.setSelectVariables(new ArrayList<String>(entityNodes));
//		q.createQueryGraph(m_index);

		// step 3: structure-based refinement
		QueryExecution qe = new QueryExecution(q, m_idxReader);
		for (QueryEdge edge : queryGraph.edgeSet()) {
			if (Util.isConstant(edge.getTarget().getLabel())) {
				qe.visited(edge);
				qe.imVisited(edge);
			}
		}
		
		qe.setMatchTables(new ArrayList<Table<String>>(Arrays.asList(matchTable)));
		
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
		
		Table<String> resultsAfterIM = new Table<String>(resultTable, false);
		int rowsAfterIM = 0;
		for (EvaluationClass ec : classes) {
			StringBuilder sig = new StringBuilder();
			for (String col : asmResult.getColumnNames())
				sig.append(ec.getMatch(col)).append("_");
			
			Table<String> table = new Table<String>(resultTable.getColumnNames());
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
			return qe.getResult();
		else
			return new Table<String>(q.getSelectVariableLabels());
	}
	
	public void entitiesForColumn(Table<String> table, String colName, Set<String> entities) {
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

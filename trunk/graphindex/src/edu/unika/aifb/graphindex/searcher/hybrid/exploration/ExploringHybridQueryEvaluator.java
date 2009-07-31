package edu.unika.aifb.graphindex.searcher.hybrid.exploration;

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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.index.StructureIndex;
import edu.unika.aifb.graphindex.query.HybridQuery;
import edu.unika.aifb.graphindex.query.PrunedQuery;
import edu.unika.aifb.graphindex.query.QNode;
import edu.unika.aifb.graphindex.query.QueryEdge;
import edu.unika.aifb.graphindex.query.QueryGraph;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.hybrid.HybridQueryEvaluator;
import edu.unika.aifb.graphindex.searcher.keyword.KeywordSearcher;
import edu.unika.aifb.graphindex.searcher.keyword.exploration.EdgeElement;
import edu.unika.aifb.graphindex.searcher.keyword.exploration.GraphElement;
import edu.unika.aifb.graphindex.searcher.keyword.exploration.NodeElement;
import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordElement;
import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordSegment;
import edu.unika.aifb.graphindex.searcher.structured.QueryExecution;
import edu.unika.aifb.graphindex.searcher.structured.VPEvaluator;
import edu.unika.aifb.graphindex.searcher.structured.sig.EvaluationClass;
import edu.unika.aifb.graphindex.searcher.structured.sig.SmallIndexMatchesValidator;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.Timings;

public class ExploringHybridQueryEvaluator extends HybridQueryEvaluator {
	private StructureIndex m_si;
	private VPEvaluator m_eval;
	private HybridExploringIndexMatcher m_matcher;
	private KeywordSearcher m_searcher;
	private SmallIndexMatchesValidator m_validator;

	private static final Logger log = Logger.getLogger(ExploringHybridQueryEvaluator.class);
	
	public ExploringHybridQueryEvaluator(IndexReader idxReader) throws IOException, StorageException {
		super(idxReader);
		
		m_si = idxReader.getStructureIndex();
		m_eval = new VPEvaluator(idxReader);
		m_matcher = new HybridExploringIndexMatcher(idxReader);
		m_matcher.initialize();
		m_searcher = new KeywordSearcher(idxReader);
		m_validator = new SmallIndexMatchesValidator(idxReader);
	}
	
	protected Map<KeywordSegment,Collection<KeywordElement>> search(String query, KeywordSearcher searcher, Timings timings) {
		List<String> list = KeywordSearcher.getKeywordList(query);
		log.debug("keyword list: " + list);
		Map<KeywordSegment,Collection<KeywordElement>> res = searcher.searchKeywordElements(list);
		return res;
	}
	
	protected void explore(HybridQuery query, Map<KeywordSegment,Collection<KeywordElement>> entities, HybridExploringIndexMatcher matcher, List<GTable<String>> indexMatches,
			List<StructuredQuery> queries, List<Map<String,Set<KeywordSegment>>> selectMappings, Map<KeywordSegment,List<GraphElement>> segment2elements,
			Map<String,Set<String>> ext2entities, Timings timings, Counters counters) throws StorageException, IOException {
		
		for (KeywordSegment ks : entities.keySet()) {
			Set<String> nodes = new HashSet<String>();
			Set<String> edges = new HashSet<String>();
//			log.debug(ks);
			List<String> keywords = new ArrayList<String>(ks.getKeywords());
			Collections.sort(keywords);
			String id = keywords.toString();
//			log.debug(id);
			for (KeywordElement ele : entities.get(ks)) {
//				log.debug(" " + ele + " " + ele.getType());
				if (ele.getType() == KeywordElement.CONCEPT || ele.getType() == KeywordElement.ENTITY) {
					String ext = ele.getExtensionId();
					
					Set<String> extEntities = ext2entities.get(ext + id);
					if (extEntities == null) {
						extEntities = new HashSet<String>(50);
						ext2entities.put(ext + id, extEntities);
					}
					extEntities.add(ele.getUri());
//					log.debug(ext + id + ": " + extEntities);
//					log.debug(ext + " " + ks.toString() + " " + extEntities.size());
					nodes.add(ext);
				}
				else if (ele.getType() == KeywordElement.RELATION || ele.getType() == KeywordElement.ATTRIBUTE) {
					edges.add(ele.getUri());
				}
				else
					log.error("unknown type...");
			}

			List<GraphElement> elements = new ArrayList<GraphElement>(nodes.size() + edges.size());
			for (String node : nodes) {
				elements.add(new NodeElement(node));
			}
			
			for (String uri : edges) {
				elements.add(new EdgeElement(null, uri, null));
			}
			
			segment2elements.put(ks, elements);
			
			log.debug("segment: " + ks.getKeywords() + ", elements: " + elements.size());
		}
		
		List<GraphElement> elements = new ArrayList<GraphElement>();
		GTable<String> structuredResults = m_eval.evaluate(query.getStructuredQuery());
		int i = 0;
		for (String[] row : structuredResults) {
			Set<NodeElement> nodes = new HashSet<NodeElement>();
			for (QNode s : query.getStructuredQuery().getVariables())
				nodes.add(new NodeElement(m_si.getExtension(row[structuredResults.getColumn(s.getLabel())])));
			GTable<String> table = new GTable<String>(structuredResults, false);
			table.addRow(row);
			StructuredMatchElement element = new StructuredMatchElement("structured-element-" + i, query, nodes, table);
			i++;
			
			elements.add(element);
		}
		
		segment2elements.put(new KeywordSegment("STRUCTURED"), elements);

		matcher.setKeywords(segment2elements);
		matcher.match();
		
		matcher.indexMatches(indexMatches, queries, selectMappings, true);
		
		log.debug("queries: " + queries.size());
	}

	public GTable<String> evaluate(HybridQuery query) throws StorageException, IOException {
		Timings timings = new Timings();
		Counters counters = new Counters();
		
		log.info("evaluating...");
		timings.start(Timings.TOTAL_QUERY_EVAL);

		timings.start(Timings.STEP_KWSEARCH);
		Map<KeywordSegment,Collection<KeywordElement>> decomposition = search(query.getKeywordQuery().getQuery(), m_searcher, timings);
		timings.end(Timings.STEP_KWSEARCH);

		List<GTable<String>> indexMatches = new ArrayList<GTable<String>>();
		List<StructuredQuery> queries = new ArrayList<StructuredQuery>();
		List<Map<String,Set<KeywordSegment>>> selectMappings = new ArrayList<Map<String,Set<KeywordSegment>>>();
		Map<KeywordSegment,List<GraphElement>> segment2elements = new HashMap<KeywordSegment,List<GraphElement>>();
		Map<String,Set<String>> ext2entities = new HashMap<String,Set<String>>();

		timings.start(Timings.STEP_EXPLORE);
		explore(query, decomposition, m_matcher, indexMatches, queries, selectMappings, segment2elements, ext2entities, timings, counters);
		timings.end(Timings.STEP_EXPLORE);
		
		timings.start(Timings.STEP_IQA);
		
		counters.set(Counters.QT_QUERIES, queries.size());
		
//		int numberOfQueries = m_allQueries ? indexMatches.size() : Math.min(1, indexMatches.size());
		int numberOfQueries = 1;
		
		for (int i = 0; i < numberOfQueries; i++) {
			PrunedQuery q = new PrunedQuery(queries.get(i), m_idxReader.getStructureIndex());
			counters.set(Counters.QT_QUERY_EDGES, q.getQueryGraph().edgeCount());
			log.debug(q);

			QueryExecution qe = new QueryExecution(q, m_idxReader);
			QueryGraph queryGraph = qe.getQueryGraph();
			
			GTable<String> indexMatch = indexMatches.get(i);
			qe.setIndexMatches(indexMatch);
			log.debug(indexMatch);
			
			Map<String,Set<KeywordSegment>> select2ks = selectMappings.get(i);
			
			List<EvaluationClass> classes = new ArrayList<EvaluationClass>();
			classes.add(new EvaluationClass(indexMatch));
			
			for (QueryEdge edge : queryGraph.edgeSet()) {
				String src = edge.getSource().getLabel();
				String trg = edge.getTarget().getLabel();
				
				if (q.getSelectVariableLabels().contains(src)) {
					List<EvaluationClass> newClasses = new ArrayList<EvaluationClass>();
					for (EvaluationClass ec : classes) {
						newClasses.addAll(ec.addMatch(src, false, null, null));
					}
					classes.addAll(newClasses);					
				}
				
				if (q.getSelectVariableLabels().contains(trg)) {
					List<EvaluationClass> newClasses = new ArrayList<EvaluationClass>();
					for (EvaluationClass ec : classes) {
						newClasses.addAll(ec.addMatch(trg, false, null, null));
					}
					classes.addAll(newClasses);					
				}
				
				if (edge.getLabel().startsWith("???")) {
					qe.visited(edge);
				}
			}
//			log.debug("ext2entities: " + ext2entities.keySet());
			for (Iterator<EvaluationClass> j = classes.iterator(); j.hasNext(); ) {
				EvaluationClass ec = j.next();
				log.debug(ec);
				for (String selectNode : q.getSelectVariableLabels()) {
					if (!ec.getMappings().hasColumn(selectNode))
						continue;
					if (q.isRemovedNode(selectNode))
						continue;

					boolean stop = false;
					String ksCol = "";
					for (KeywordSegment ks : select2ks.get(selectNode)) {
						ksCol += ks.toString().replaceAll(" ", "_");
//						log.debug("ks: " + ks + ", " + getKSId(ks) + ": " + ec.getMatch(selectNode) + getKSId(ks));
						if (ext2entities.get(ec.getMatch(selectNode) + getKSId(ks)) == null) {
							j.remove();
							stop = true;
//							log.debug("stop");
							break;
						}
					}
					
					if (stop)
						break;

					List<String> columns = new ArrayList<String>();
					columns.add(ksCol);
					columns.add(selectNode);
					GTable<String> table = new GTable<String>(columns);
					int col = table.getColumn(selectNode);
	
					for (KeywordSegment ks : select2ks.get(selectNode)) {
						for (String entity : ext2entities.get(ec.getMatch(selectNode) + getKSId(ks))) {
							String[] row = new String [2];
							row[col] = entity;
							row[0] = ksCol;
							table.addRow(row);
						}
					}
					ec.getResults().add(table);
//					log.debug(table.toDataString());
				}
//					log.debug(ec);
			}
			
			log.debug(classes);
			qe.setEvaluationClasses(classes);
			m_validator.setQueryExecution(qe);
			
			if (classes.size() > 0)
				m_validator.validateIndexMatches();
			
			log.debug("result: " + qe.getResult());
			
			if (qe.getResult() != null)
				counters.inc(Counters.RESULTS, qe.getResult().rowCount());
		}

		timings.end(Timings.STEP_IQA);
		
		timings.end(Timings.TOTAL_QUERY_EVAL);
		
		return null;
	}

	private String getKSId(KeywordSegment ks) {
		List<String> keywords = new ArrayList<String>(ks.getKeywords());
		Collections.sort(keywords);
		return keywords.toString();
	}
}

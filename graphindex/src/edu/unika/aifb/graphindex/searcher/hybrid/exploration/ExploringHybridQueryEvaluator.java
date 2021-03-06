package edu.unika.aifb.graphindex.searcher.hybrid.exploration;

/**
 * Copyright (C) 2009 G�nter Ladwig (gla at aifb.uni-karlsruhe.de)
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.semanticweb.yars.nx.namespace.RDF;
import org.semanticweb.yars.nx.namespace.RDFS;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.index.StructureIndex;
import edu.unika.aifb.graphindex.model.impl.Entity;
import edu.unika.aifb.graphindex.query.HybridQuery;
import edu.unika.aifb.graphindex.query.PrunedQuery;
import edu.unika.aifb.graphindex.query.QNode;
import edu.unika.aifb.graphindex.query.QueryEdge;
import edu.unika.aifb.graphindex.query.QueryGraph;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.hybrid.HybridQueryEvaluator;
import edu.unika.aifb.graphindex.searcher.keyword.KeywordSearcher;
import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordElement;
import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordSegment;
import edu.unika.aifb.graphindex.searcher.keyword.model.SQueryKeywordElement;
import edu.unika.aifb.graphindex.searcher.structured.QueryExecution;
import edu.unika.aifb.graphindex.searcher.structured.TranslatedQueryEvaluator;
import edu.unika.aifb.graphindex.searcher.structured.VPEvaluator;
import edu.unika.aifb.graphindex.searcher.structured.sig.EvaluationClass;
import edu.unika.aifb.graphindex.searcher.structured.sig.SmallIndexMatchesValidator;
import edu.unika.aifb.graphindex.storage.NeighborhoodStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.keyword.BloomFilter;
import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.Statistics;
import edu.unika.aifb.graphindex.util.Timings;

public class ExploringHybridQueryEvaluator extends HybridQueryEvaluator {
	private StructureIndex m_si;
	private VPEvaluator m_eval;
	private ExploringIndexMatcher m_matcher;
	private KeywordSearcher m_searcher;
//	private SmallIndexMatchesValidator m_validator;
	private TranslatedQueryEvaluator m_tqe;
	private boolean m_doOverlap = false;

	private static final int MAX_INTERPRETATIONS = 10;

	private static final Logger log = Logger.getLogger(ExploringHybridQueryEvaluator.class);
	
	public ExploringHybridQueryEvaluator(IndexReader idxReader) throws IOException, StorageException {
		super(idxReader);
		
		m_si = idxReader.getStructureIndex();
		m_eval = new VPEvaluator(idxReader);
		m_matcher = new ExploringIndexMatcher(idxReader);
		m_matcher.initialize();
		m_searcher = new KeywordSearcher(idxReader);
//		m_validator = new SmallIndexMatchesValidator(idxReader);
		m_tqe = new TranslatedQueryEvaluator(idxReader);
	}
	
	protected Map<KeywordSegment,Collection<KeywordElement>> search(String query, KeywordSearcher searcher, boolean doOverlap, Timings timings) throws StorageException, IOException {
		List<String> list = KeywordSearcher.getKeywordList(query);
//		log.debug("keyword list: " + list);
		Map<KeywordSegment,Collection<KeywordElement>> res = searcher.searchKeywordElements(list, doOverlap);
		return res;
	}
	
	
	private String getKeywordElementId(String ext, KeywordElement ele) throws StorageException, IOException {
		if (ele.getAttributeUri().equals("http://xmlns.com/foaf/0.1/name"))
			ele.setAttributeUri(RDFS.LABEL.toString());
		
		String id = ext + "__" + ele.getAttributeUri();
		
		Table<String> t = m_idxReader.getDataIndex().getTriples(ele.getUri(), RDF.TYPE.toString(), null);
		TreeSet<String> types = new TreeSet<String>();
		for (String[] row : t)
			types.add(row[2]);
		id += "__" + types.toString();
//		TreeSet<String> props = new TreeSet<String>(ele.getInProperties());
//		props.addAll(ele.getOutProperties());
//		id += "__" + props.toString();
//		props = new TreeSet<String>(ele.getOutProperties());
//		id += "__" + props.toString();
		
		return id;
	}
	
	protected void explore(HybridQuery query, int k, Map<KeywordSegment,Collection<KeywordElement>> entities, ExploringIndexMatcher matcher, 
			List<TranslatedQuery> queries, Timings timings, Counters counters) throws StorageException, IOException {

		Map<KeywordSegment,List<KeywordElement>> segment2elements = new HashMap<KeywordSegment,List<KeywordElement>>();

		Set<KeywordElement> structuredElements = new HashSet<KeywordElement>();
		if (query.getEntities() != null && query.getEntities().size() > 0) {
			List<KeywordElement> elements = new ArrayList<KeywordElement>();
			Map<String,KeywordElement> id2element = new HashMap<String,KeywordElement>();

			for (String entity : query.getEntities()) {
				String ext = m_si.getExtension(entity);
				
				KeywordElement element = id2element.get(ext);
				
				if (element == null) {
					element = new SQueryKeywordElement(new Entity(ext), KeywordElement.ENTITY, 1.0, "?ATTACH");
					elements.add(element);
				}
				else {
					
				}
				
				Set<String> inProperties = new HashSet<String>();
				Set<String> outProperties = new HashSet<String>();
				
				m_searcher.getProperties(entity, inProperties, outProperties);
				
				element.entities.add(entity);
				element.addInProperties(inProperties);
				element.addOutProperties(outProperties);
				
				structuredElements.add(new KeywordElement(new Entity(entity), KeywordElement.ENTITY, null));
			}
			
			segment2elements.put(new KeywordSegment("STRUCTURED"), elements);
		}
		
		Set<KeywordElement> keywordNodeElements = new HashSet<KeywordElement>();
		for (KeywordSegment ks : entities.keySet()) {
			List<KeywordElement> elements = new ArrayList<KeywordElement>(entities.get(ks).size());
			Map<String,NodeElement> label2node = new HashMap<String,NodeElement>();
			Map<String,KeywordElement> extAttribute2Element = new HashMap<String,KeywordElement>();
			
			for (KeywordElement ele : entities.get(ks)) {
				if (ele.getType() == KeywordElement.CONCEPT) {
					String ext = ele.getExtensionId();
					
					KeywordElement schemaElement = extAttribute2Element.get(ext + "__concept");
					
					if (schemaElement == null) {
						schemaElement = new KeywordElement(new Entity(ext), KeywordElement.CONCEPT, ele.getMatchingScore(), null, ele.getNeighborhoodStorage());
						schemaElement.addKeywords(ks.getAllKeywords());
						
						elements.add(schemaElement);
						
						extAttribute2Element.put(ext + "__concept", schemaElement);
					}
					else {
						schemaElement.setMatchingScore(Math.max(schemaElement.getMatchingScore(), ele.getMatchingScore()));
					}
					
					schemaElement.addInProperties(Arrays.asList(RDF.TYPE.toString()));
					schemaElement.entities.add(ele.getUri());

					keywordNodeElements.add(ele);
				}
				else if (ele.getType() == KeywordElement.ENTITY) {
					if (structuredElements.size() > 0) {
						boolean found = false;
						for (KeywordElement sele : structuredElements) {
							if (ele.isReachable(sele)) {
								found = true;
								break;
							}
						}
						if (!found)
							continue;
					}
					
					String ext = ele.getExtensionId();
					
//					KeywordElement schemaElement = extAttribute2Element.get(ext + "__" + ele.getAttributeUri());
					KeywordElement schemaElement = extAttribute2Element.get(getKeywordElementId(ext, ele));
					
					if (schemaElement == null) {
						schemaElement = new KeywordElement(new Entity(ext), KeywordElement.ENTITY, ele.getMatchingScore(), null, ele.getNeighborhoodStorage());
						schemaElement.addKeywords(ks.getAllKeywords());
						schemaElement.setAttributeUri(ele.getAttributeUri());
						
						elements.add(schemaElement);
						
//						extAttribute2Element.put(ext + "__" + ele.getAttributeUri(), schemaElement);
						extAttribute2Element.put(getKeywordElementId(ext, ele), schemaElement);
					}
					else {
						schemaElement.setMatchingScore(Math.max(schemaElement.getMatchingScore(), ele.getMatchingScore()));
					}
					
					schemaElement.entities.add(ele.getUri());
					schemaElement.addOutProperties(ele.getOutProperties());
					schemaElement.addInProperties(ele.getInProperties());

					keywordNodeElements.add(ele);
				}
				else if (ele.getType() == KeywordElement.RELATION || ele.getType() == KeywordElement.ATTRIBUTE) {
					elements.add(ele);
				}
				else
					log.error("unknown type...");
			}

			segment2elements.put(ks, elements);
			
			log.debug("segment: " + ks + ", elements: " + elements.size());
		}

		double inMax = 0.0, outMax = 0.0;
		Map<String,Double> inprops = new HashMap<String,Double>(), outprops = new HashMap<String,Double>();
		for (KeywordSegment ks : segment2elements.keySet()) {
			for (KeywordElement ele : segment2elements.get(ks)) {
				for (String property : ele.getInPropertyWeights().keySet()) {
					Double w = inprops.get(property) == null ? 0.0 : inprops.get(property);
					w += ele.getInPropertyWeights().get(property);
					inMax = Math.max(inMax, w);
					inprops.put(property, w);
				}
				
				for (String property : ele.getOutPropertyWeights().keySet()) {
					Double w = outprops.get(property) == null ? 0.0 : outprops.get(property);
					w += ele.getOutPropertyWeights().get(property);
					outMax = Math.max(outMax, w);
					outprops.put(property, w);
				}
			}
		}

		for (KeywordSegment ks : segment2elements.keySet()) {
			for (KeywordElement ele : segment2elements.get(ks)) {
				for (String property : ele.getInPropertyWeights().keySet())
					ele.getInPropertyWeights().put(property, inprops.get(property) / inMax);
				
				for (String property : ele.getOutPropertyWeights().keySet())
					ele.getOutPropertyWeights().put(property, outprops.get(property) / outMax);
			}
		}

//		log.debug(inprops);
//		log.debug(outprops);

		Map<String,Set<QNode>> ext2var = new HashMap<String,Set<QNode>>();
		Table<String> structuredResults = null;
		Table<String> queryIndexMatches = null;
		
		if (query.getStructuredQuery() != null) {
			List<KeywordElement> elements = new ArrayList<KeywordElement>();
			structuredResults = m_eval.evaluate(query.getStructuredQuery());

			queryIndexMatches = new Table<String>(structuredResults, false);
			
			Set<String> sqExts = new HashSet<String>();
			for (String[] row : structuredResults) {
				// first check if any of the entities is in the neighborhood of a keyword matched entity
				boolean found = false;
				for (QNode s : query.getStructuredQuery().getVariables()) {
					if (query.getAttachNode() != null || query.getAttachNode().equals(s)) {
						String entity = row[structuredResults.getColumn(s.getLabel())];
	
						for (KeywordElement ele : keywordNodeElements) {
							if (ele.isReachable(new KeywordElement(new Entity(entity), KeywordElement.ENTITY, null))) {
								found = true;
								break;
							}
						}
						
						if (found)
							break;
					}
				}
				
				if (found) {
//					log.debug("row connected");
					String[] extRow = new String[queryIndexMatches.columnCount()];
					for (QNode s : query.getStructuredQuery().getQueryGraph().vertexSet()) {
						if (s.isVariable()) {
							String ext = m_si.getExtension(row[structuredResults.getColumn(s.getLabel())]);
//							sqExts.add(ext);
							elements.add(new SQueryKeywordElement(new Entity(ext), KeywordElement.ENTITY, 1.0, s.getLabel()));
							extRow[queryIndexMatches.getColumn(s.getLabel())] = ext;
  							
							// record for which variables an extension appears
							Set<QNode> vars = ext2var.get(ext);
							if (vars == null) {
								vars = new HashSet<QNode>();
								ext2var.put(ext, vars);
							}
							vars.add(s);
						}
						else
							extRow[queryIndexMatches.getColumn(s.getLabel())] = s.getLabel();
					}
					queryIndexMatches.addRow(extRow);
				}
			}
			
//			for (String ext : sqExts) {
//				KeywordElement element = new SQueryKeywordElement(new Entity(ext), KeywordElement.ENTITY, 1.0, null, null);
//				elements.add(element);
//			}
			
			segment2elements.put(new KeywordSegment("STRUCTURED"), elements);
		}
		
		if (k == 1)
			k = 3;
		matcher.setKeywords(segment2elements, query);
		matcher.setK(Math.min(k, MAX_INTERPRETATIONS));
		matcher.match();
		
		queries.addAll(matcher.indexMatches(query.getStructuredQuery(), ext2var));
		log.debug("queries: " + queries.size());
		
//		if (query.getStructuredQuery() != null) {
//			// join the index matches of the structured part to those of the keyword part
//			for (int i = 0; i < queries.size(); i++) {
//				TranslatedQuery q = queries.get(i);
//				Table<String> indexMatches = q.getIndexMatches();
//				
//				indexMatches.sort(q.getConnectingNode().getLabel());
//				queryIndexMatches.sort(q.getConnectingNode().getLabel(), true);
//				
//				indexMatches = Tables.mergeJoin(indexMatches, queryIndexMatches, q.getConnectingNode().getLabel());
//				q.setIndexMatches(indexMatches);
//				
//				q.addResult(structuredResults);
//			}
//		}
	}
	
	public Table<String> evaluate(HybridQuery query) throws StorageException, IOException {
		List<TranslatedQuery> queries = evaluate(query, 1, 1);
		if (queries.size() > 0 && queries.get(0) != null)
			return queries.get(0).getResult();
		else
			return new Table<String>();
	}

	private String getKSId(KeywordSegment ks) {
		List<String> keywords = new ArrayList<String>(ks.getKeywords());
		Collections.sort(keywords);
		return keywords.toString();
	}
	
	public void setDoNeighborhoodJoin(boolean enable) {
		m_doOverlap  = enable;
	}

	@Override
	public List<TranslatedQuery> evaluate(HybridQuery query, int numberOfQueries, int queryResults) throws StorageException, IOException {
		Timings timings = new Timings();
		Counters counters = new Counters();
		
		if (numberOfQueries < 0)
			numberOfQueries = MAX_INTERPRETATIONS;
		if (queryResults < 0)
			queryResults = MAX_INTERPRETATIONS;
		
		log.info("evaluating...");
		timings.start(Timings.TOTAL_QUERY_EVAL);

//		Statistics.start(ExploringHybridQueryEvaluator.class, Statistics.Timing.HY_SEARCH);
		Map<KeywordSegment,Collection<KeywordElement>> decomposition = search(query.getKeywordQuery().getQuery(), m_searcher, m_doOverlap, timings);
//		Statistics.end(ExploringHybridQueryEvaluator.class, Statistics.Timing.HY_SEARCH);

		List<TranslatedQuery> queries = new ArrayList<TranslatedQuery>();

//		Statistics.start(ExploringHybridQueryEvaluator.class, Statistics.Timing.HY_EXPLORE);
		explore(query, numberOfQueries, decomposition, m_matcher, queries, timings, counters);
//		Statistics.end(ExploringHybridQueryEvaluator.class, Statistics.Timing.HY_EXPLORE);

		timings.start(Timings.STEP_IQA);
		
		counters.set(Counters.QT_QUERIES, queries.size());
		
		numberOfQueries = Math.min(numberOfQueries, queries.size());
		
		for (int i = 0; i < numberOfQueries; i++) {
//			log.debug("------- query " + i + "/" + queries.size());
			TranslatedQuery translated = queries.get(i);
			counters.set(Counters.QT_QUERY_EDGES, translated.getQueryGraph().edgeCount());
//			log.debug(translated);

			if (i < queryResults) {
				Table<String> res = m_tqe.evaluate(translated);
				if (res != null)
					translated.setResult(res);
//				QueryExecution qe = new QueryExecution(translated, m_idxReader);
//	
//				qe.setIndexMatches(translated.getIndexMatches());
//				
//				List<EvaluationClass> classes = new ArrayList<EvaluationClass>();
//				classes.add(new EvaluationClass(translated.getIndexMatches()));
//				
//				for (QNode var : translated.getSelectVariables()) {
//					List<EvaluationClass> newClasses = new ArrayList<EvaluationClass>();
//					for (EvaluationClass ec : classes) {
//						newClasses.addAll(ec.addMatch(var.getLabel(), false, null, null));
//					}
//					classes.addAll(newClasses);					
//				}
//				
//				log.debug("visited edges:");
//				for (QueryEdge edge : translated.getStructuredEdges()) {
//					qe.visited(edge);
//					log.debug(" " + edge);
//				}
//				for (QueryEdge edge : translated.getAttributeEdges()) {
//					qe.visited(edge);
//					log.debug(" " + edge);
//				}
//	
//				for (Iterator<EvaluationClass> j = classes.iterator(); j.hasNext(); ) {
//					EvaluationClass ec = j.next();
//					ec.getResults().addAll(translated.getResults());
//				}
//	
//				qe.setEvaluationClasses(classes);
//				m_validator.setQueryExecution(qe);
//				
//				if (classes.size() > 0)
//					m_validator.validateIndexMatches();
//
//				log.debug("result: " + qe.getResult());
//				if (qe.getResult() != null)
//					translated.setResult(qe.getResult());
			}
		}

		timings.end(Timings.STEP_IQA);
		
		timings.end(Timings.TOTAL_QUERY_EVAL);

//		Statistics.print();
//		Statistics.reset();

		return queries;
	}
}

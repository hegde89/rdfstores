package edu.unika.aifb.graphindex.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
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
import edu.unika.aifb.keywordsearch.KeywordSegement;
import edu.unika.aifb.keywordsearch.TransformedGraph;
import edu.unika.aifb.keywordsearch.TransformedGraphNode;
import edu.unika.aifb.keywordsearch.search.ApproximateStructureMatcher;
import edu.unika.aifb.keywordsearch.search.EntitySearcher;
import edu.unika.aifb.keywordsearch.search.KeywordSearcher;

public abstract class ExploringQueryEvaluator  {

	
	private static final Logger log = Logger.getLogger(ExploringQueryEvaluator.class);
	protected boolean m_allQueries;
	
	public ExploringQueryEvaluator() throws StorageException {
	}
	
	protected Map<KeywordSegement,Collection<KeywordElement>> search(String query, KeywordSearcher searcher, Timings timings) {
		List<String> list = KeywordSearcher.getKeywordList(query);
		log.debug("keyword list: " + list);
		Map<KeywordSegement,Collection<KeywordElement>> res = searcher.searchKeywordElements(list);
		return res;
	}
	
	protected void explore(Map<KeywordSegement,Collection<KeywordElement>> entities, ExploringIndexMatcher matcher, List<GTable<String>> indexMatches,
			List<Query> queries, List<Map<String,Set<KeywordSegement>>> selectMappings, Map<KeywordSegement,List<GraphElement>> segment2elements,
			Map<String,Set<String>> ext2entities, Timings timings, Counters counters) throws StorageException {
		
		for (KeywordSegement ks : entities.keySet()) {
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
		

		matcher.setKeywords(segment2elements);
		matcher.match();
		
		matcher.indexMatches(indexMatches, queries, selectMappings, true);
		
		log.debug("queries: " + queries.size());
	}
	
	public abstract void evaluate(String query) throws StorageException;

	public void setExecuteAllQueries(boolean allQueries) {
		m_allQueries = allQueries;
	}
}

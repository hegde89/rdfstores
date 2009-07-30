package edu.unika.aifb.graphindex.searcher.keyword.exploration;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.RDF;

import edu.unika.aifb.graphindex.data.GTable;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.entity.ApproximateStructureMatcher;
import edu.unika.aifb.graphindex.searcher.entity.EntitySearcher;
import edu.unika.aifb.graphindex.searcher.keyword.KeywordQueryEvaluator;
import edu.unika.aifb.graphindex.searcher.keyword.KeywordSearcher;
import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordElement;
import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordSegment;
import edu.unika.aifb.graphindex.searcher.keyword.model.TransformedGraph;
import edu.unika.aifb.graphindex.searcher.keyword.model.TransformedGraphNode;
import edu.unika.aifb.graphindex.searcher.structured.sig.SmallIndexGraphMatcher;
import edu.unika.aifb.graphindex.searcher.structured.sig.SmallIndexMatchesValidator;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Counters;
import edu.unika.aifb.graphindex.util.Timings;
import edu.unika.aifb.graphindex.util.Util;

public abstract class ExploringQueryEvaluator extends KeywordQueryEvaluator {

	
	private static final Logger log = Logger.getLogger(ExploringQueryEvaluator.class);
	protected boolean m_allQueries;
	
	public ExploringQueryEvaluator(IndexReader idxReader) throws StorageException {
		super(idxReader);
	}
	
	protected Map<KeywordSegment,Collection<KeywordElement>> search(String query, KeywordSearcher searcher, Timings timings) {
		List<String> list = KeywordSearcher.getKeywordList(query);
		log.debug("keyword list: " + list);
		Map<KeywordSegment,Collection<KeywordElement>> res = searcher.searchKeywordElements(list);
		return res;
	}
	
	protected void explore(Map<KeywordSegment,Collection<KeywordElement>> entities, ExploringIndexMatcher matcher, List<GTable<String>> indexMatches,
			List<StructuredQuery> queries, List<Map<String,Set<KeywordSegment>>> selectMappings, Map<KeywordSegment,List<GraphElement>> segment2elements,
			Map<String,Set<String>> ext2entities, Timings timings, Counters counters) throws StorageException {
		
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
		

		matcher.setKeywords(segment2elements);
		matcher.match();
		
		matcher.indexMatches(indexMatches, queries, selectMappings, true);
		
		log.debug("queries: " + queries.size());
	}
	
	public void setExecuteAllQueries(boolean allQueries) {
		m_allQueries = allQueries;
	}
}

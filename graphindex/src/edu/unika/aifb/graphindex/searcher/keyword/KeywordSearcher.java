package edu.unika.aifb.graphindex.searcher.keyword;

/**
 * Copyright (C) 2009 Lei Zhang (beyondlei at gmail.com)
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SetBasedFieldSelector;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.HitCollector;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SimilarityDelegator;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocCollector;
import org.apache.lucene.search.BooleanClause.Occur;

import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.model.IAttribute;
import edu.unika.aifb.graphindex.model.IEntity;
import edu.unika.aifb.graphindex.model.INamedConcept;
import edu.unika.aifb.graphindex.model.IRelation;
import edu.unika.aifb.graphindex.model.impl.Attribute;
import edu.unika.aifb.graphindex.model.impl.Entity;
import edu.unika.aifb.graphindex.model.impl.NamedConcept;
import edu.unika.aifb.graphindex.model.impl.Relation;
import edu.unika.aifb.graphindex.query.KeywordQuery;
import edu.unika.aifb.graphindex.searcher.keyword.model.Constant;
import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordElement;
import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordSegment;
import edu.unika.aifb.graphindex.storage.NeighborhoodStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.TypeUtil;

public class KeywordSearcher {

	private edu.unika.aifb.graphindex.index.IndexReader idxReader;
	private IndexReader reader; 
	private IndexReader valueReader;
	private IndexSearcher searcher;
	private IndexSearcher valueSearcher;
	private NeighborhoodStorage ns;
	
	private double maxScore = 1.0;
	
	public static final double ENTITY_THRESHOLD = 0.8;
	public static final double SCHEMA_THRESHOLD = 0.5;
	public static int MAX_KEYWORDRESULT_SIZE = 100;
	
	private static final Logger log = Logger.getLogger(KeywordSearcher.class);
	
	public KeywordSearcher(edu.unika.aifb.graphindex.index.IndexReader idxReader) throws StorageException {
		this.idxReader = idxReader;
		try {
			reader = IndexReader.open(idxReader.getIndexDirectory().getDirectory(IndexDirectory.KEYWORD_DIR));
			searcher = new IndexSearcher(reader);

			valueReader = IndexReader.open(idxReader.getIndexDirectory().getDirectory(IndexDirectory.VALUE_DIR));
			valueSearcher = new IndexSearcher(valueReader);
			
			// change the default scoring model in Lucene by removing queryWeight 
			valueSearcher.setSimilarity(new SimilarityDelegator(valueSearcher.getSimilarity()) {
				public float idf(int docFreq, int numDocs) {
					return (float) (Math.sqrt(Math.log(numDocs / (double) (docFreq + 1)) + 1.0));
				}

				public float queryNorm(float sumOfSquaredWeights) {
					return (float) 1.0f;
				}
			});
			
			ns = idxReader.getNeighborhoodStorage();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void setCutoff(int cutoff) {
		MAX_KEYWORDRESULT_SIZE = cutoff;
	}
	
	public Map<KeywordSegment, Collection<KeywordElement>> searchKeywordElements(
			List<String> queries) throws StorageException, IOException {
		
		return searchKeywordElements(queries, true);
	}
	
	public Map<KeywordSegment, Collection<KeywordElement>> searchKeywordElements(List<String> queries, 
			boolean doOverlap) throws StorageException, IOException {
		
		resetMaxScore();
		Map<String, Collection<KeywordElement>> conceptsAndRelations = new HashMap<String, Collection<KeywordElement>>();
		Map<String, Collection<KeywordElement>> attributes = new HashMap<String, Collection<KeywordElement>>();
		// Collection<Set<KeywordSegement>> partitions = new ArrayList<Set<KeywordSegement>>();
		
		SortedSet<KeywordSegment> segments;
		try {
			segments = parseQueries(queries, conceptsAndRelations, attributes);
		} catch (IOException e) {
			throw new StorageException(e);
		}
		
		Map<String, Collection<KeywordElement>> keywords2Entities = new HashMap<String, Collection<KeywordElement>>();
		Map<KeywordElement, KeywordSegment> entities2Segement = new HashMap<KeywordElement, KeywordSegment>();
		Map<KeywordSegment, Collection<KeywordElement>> segments2Entities = new TreeMap<KeywordSegment, Collection<KeywordElement>>();
		
//		segments = new TreeSet<KeywordSegment>();
//		for (String keyword : queries) {
//			KeywordSegment ks = new KeywordSegment(keyword);
//			segments.add(ks);
//		}
		log.debug("segments: " + segments);

		try {
			searchElementsByKeywords(segments, attributes, entities2Segement, segments2Entities, keywords2Entities);
		} catch (IOException e) {
			throw new StorageException(e);
		}
		
		int size = 0;
		for(Collection<KeywordElement> coll : segments2Entities.values()) {
			size += coll.size();
		}
		log.debug("------------- Before NeighborhoodJoin ------------ "	+ 
				"Size_of_segements:" + segments2Entities.size() + "   Size_of_elements:" + size);	
		for(KeywordSegment segement : segments2Entities.keySet()) {
			log.debug(segement + " " + segments2Entities.get(segement).size());
		}

		if (doOverlap) {
			overlapNeighborhoods(keywords2Entities, segments2Entities);
	
			size = 0;
			for(Collection<KeywordElement> coll : segments2Entities.values()) {
				size += coll.size();
			}
			log.debug("------------- After NeighborhoodJoin ------------ " + 
					"Size_of_segements:" + segments2Entities.size()	+ "   Size_of_elements:" + size);
			for(KeywordSegment segement : segments2Entities.keySet()) {
				log.debug(segement + " " + segments2Entities.get(segement).size());
			}
		}
		
		log.debug("retrieving extension info");
		for (KeywordSegment ks : segments2Entities.keySet()) {
			Collection<KeywordElement> list = segments2Entities.get(ks);
			for (KeywordElement element : list) {
				element.setMatchingScore(element.getMatchingScore()/maxScore);
				if (element.getExtensionId() == null)
					element.setExtensionId(idxReader.getStructureIndex().getExtension(element.getUri()));
			}	
		}
		log.debug("...done");
		
		for(String keyword : conceptsAndRelations.keySet()) {
			segments2Entities.put(new KeywordSegment(keyword), conceptsAndRelations.get(keyword));
		}

//		attributes.keySet().removeAll(keywords2Entities.keySet());
		for(String keyword : attributes.keySet()) {
			segments2Entities.put(new KeywordSegment(keyword), attributes.get(keyword));
		}

		return segments2Entities;
	}
	
	private Collection<Map<KeywordSegment,Collection<KeywordElement>>> decompose(
			Map<KeywordSegment,Collection<KeywordElement>> segmentsWithEntities, 
			Set<String> keywords, Map<String, 
			Collection<KeywordElement>> conceptsAndRelations) {
		
		Collection<Map<KeywordSegment,Collection<KeywordElement>>> partitions = new ArrayList<Map<KeywordSegment,Collection<KeywordElement>>>();
		Set<KeywordSegment> allSegements = segmentsWithEntities.keySet();
		
		Iterator<Set<KeywordSegment>> iter = KeywordPartitioner.getPartitionIterator(allSegements, keywords);
		
		while(iter.hasNext()) {
			Set<KeywordSegment> segements = iter.next();
		
			Map<KeywordSegment,Collection<KeywordElement>> partition = new HashMap<KeywordSegment,Collection<KeywordElement>>();
			for(KeywordSegment segement : segements) {
				partition.put(segement, segmentsWithEntities.get(segement));
			}
			
			for(String keyword : conceptsAndRelations.keySet()) {
				partition.put(new KeywordSegment(keyword), conceptsAndRelations.get(keyword));
			}
			
			partitions.add(partition);
		}
		
		return partitions;
	}
	
	private SortedSet<KeywordSegment> parseQueries(List<String> queries, 
			Map<String, Collection<KeywordElement>> conceptsAndRelations, 
			Map<String, Collection<KeywordElement>> attributes) throws IOException {
		
		Collection<List<String>> keywordCompounds = new ArrayList<List<String>>();  
		Map<Integer, String> locationofSchemaKeyword = new TreeMap<Integer, String>();
		
		searchSchema(queries, conceptsAndRelations, attributes, locationofSchemaKeyword);
		
		SortedSet<KeywordSegment> segements;   
		if(locationofSchemaKeyword.size() != 0) {
			Iterator<Integer> iter = locationofSchemaKeyword.keySet().iterator();
			
			int from;
			int to = -1;
			while(iter.hasNext()) {
				from = to;
				to = iter.next();
				if(from + 1 != to) {
					List<String> sublist = queries.subList(from + 1, to);
					keywordCompounds.add(sublist);
				}
			}
			from = to;
			to = queries.size();
			if(from + 1 != to) {
				List<String> sublist = queries.subList(from + 1, to);
				keywordCompounds.add(sublist);
			}
			
			segements = KeywordPartitioner.getOrderedSegements(keywordCompounds);
		}
		else {
			segements = KeywordPartitioner.getOrderedSegements(queries);
		}
		
		return segements;
	}
	
	private void overlapNeighborhoods(Map<String, Collection<KeywordElement>> keywords2Entities,
			Map<KeywordSegment, Collection<KeywordElement>> segments2Entities) {
		
		Set<String> keywords = keywords2Entities.keySet();
		for(String keyword : keywords2Entities.keySet()) {
			for(KeywordElement ele : keywords2Entities.get(keyword)) {
				Collection<String> containedkeywords = ele.getKeywords();
				for(String joinKey : keywords) {
					if(!containedkeywords.contains(joinKey)) {
						Collection<KeywordElement> coll = keywords2Entities.get(joinKey);
						Collection<KeywordElement> reachables = ele.getReachable(coll);	
						if(reachables != null && reachables.size() != 0) {
							for(KeywordElement reachable : reachables) {
								Collection<String> keywords1 = ele.getReachableKeywords();
								Collection<String> keywords2 = reachable.getReachableKeywords();
								reachable.addReachableKeywords(keywords1);
								ele.addReachableKeywords(keywords2);
							}
						}
					}
				}
			}	
		}
		
		maxScore = 1.0f;
		Iterator<KeywordSegment> iterSeg = segments2Entities.keySet().iterator();
		while(iterSeg.hasNext()) {
			Collection<KeywordElement> elements = segments2Entities.get(iterSeg.next());
			Iterator<KeywordElement> iterEle = elements.iterator();
			while(iterEle.hasNext()) {
				KeywordElement ele = iterEle.next();
				if(!ele.getReachableKeywords().containsAll(keywords)) {
					iterEle.remove();
				}	
				else {
					maxScore = Math.max(maxScore, ele.getMatchingScore());
				}
			}
			if(elements.size() == 0)
				iterSeg.remove();
		}
	}
	
	// when we index k-hops neighborhoods, where k is the maximal chain length of the query, this method can be used. 
//	private void overlapNeighborhoods(Map<String, Collection<KeywordElement>> keywordsWithEntities, 
//			Map<KeywordSegement, Collection<KeywordElement>> segementsWithEntities) {
//		
//		int min = 0;
//		String filterKeyword = null;
//		Set<String> keywords = keywordsWithEntities.keySet();
//		for(String keyword : keywords) {
//			if(min == 0) {
//				min = keywordsWithEntities.get(keyword).size();
//				filterKeyword = keyword;
//			}
//			else {
//				int num = keywordsWithEntities.get(keyword).size();
//				if(min > num) {
//					min = num;
//					filterKeyword = keyword;
//				}
//			}
//		}
//		if(filterKeyword != null) {
//			Collection<KeywordElement> remain = new HashSet<KeywordElement>();
//			for(KeywordElement filterElement : keywordsWithEntities.get(filterKeyword)) {
//				Collection<String> containedkeywords = filterElement.getKeywords();
//				Collection<KeywordElement> joinEle = new HashSet<KeywordElement>();
//				boolean reachAllKeywords = true;
//				for(String joinKey : keywords) {
//					if(!containedkeywords.contains(joinKey)) {
//						Collection<KeywordElement> coll = keywordsWithEntities.get(joinKey);
//						Collection<KeywordElement> reachables = filterElement.getReachable(coll);	
//						if(reachables != null && reachables.size() != 0) {
//							joinEle.addAll(reachables);
//						}
//						else
//							reachAllKeywords = false;
//					}
//				}
//				if(reachAllKeywords == true) {
//					remain.addAll(joinEle);
//					remain.add(filterElement);
//				}
//			}	
//			Iterator<KeywordSegement> iterSeg = segementsWithEntities.keySet().iterator();
//			while(iterSeg.hasNext()) {
//				Collection<KeywordElement> elements = segementsWithEntities.get(iterSeg.next());
//				Iterator<KeywordElement> iterEle = elements.iterator();
//				while(iterEle.hasNext()) {
//					if(!remain.contains(iterEle.next()))
//						iterEle.remove();
//				}
//				if(elements.size() == 0)
//					iterSeg.remove();
//			}
//		}
//	}
	
	private void searchElementsByKeywords(SortedSet<KeywordSegment> segements, 
			Map<String, Collection<KeywordElement>> attributes, 
			Map<KeywordElement, KeywordSegment> entities2Segement, 
			Map<KeywordSegment, Collection<KeywordElement>> segments2Entities,	
			Map<String, Collection<KeywordElement>> keywords2Entities) throws IOException { 
		
		if(attributes != null && attributes.size() != 0)
			searchEntitiesByAttributesAndValues(segements, attributes, entities2Segement, segments2Entities, keywords2Entities);
		
		searchEntitiesByValues(segements, attributes, entities2Segement, segments2Entities, keywords2Entities);
	}
	
//	public void searchElementsByKeywordCompounds(Map<String, Set<String>> queries, Map<KeywordElement, KeywordSegment> entitiesWithSegement, 
//			Map<KeywordSegment, Collection<KeywordElement>> segementsWithEntities,	Map<String, Collection<KeywordElement>> keywordsWithentities) throws IOException { 
//		Map<String, Collection<KeywordElement>> attributesAndRelations = new HashMap<String, Collection<KeywordElement>>();
//		
//		searchSchema(queries.keySet(), attributesAndRelations);
//		
//		if(attributesAndRelations != null && attributesAndRelations.size() != 0)
//			searchEntitiesByCompounds(queries, attributesAndRelations, entitiesWithSegement, segementsWithEntities, keywordsWithentities);
//	}
	
	private Collection<String> searchSchema(List<String> queries, 
			Map<String, Collection<KeywordElement>> conceptsAndRelations, 
			Map<String, Collection<KeywordElement>> attributes,	
			Map<Integer, String> locationofSchemaKeyword) throws IOException {

		Map<String,Collection<KeywordElement>> keywordElements = getSchemaKeywordElements(queries);

		Set<String> queriesWithResults = new HashSet<String>();
		for (int i = 0; i < queries.size(); i++) {
			String keyword = queries.get(i);
			if (keywordElements.get(keyword) == null)
				continue;
			
			locationofSchemaKeyword.put(i, keyword);
			queriesWithResults.add(keyword);
			
			for(KeywordElement resource : keywordElements.get(keyword)) {
				if(resource.getType() == KeywordElement.ATTRIBUTE) {
					Collection<KeywordElement> coll = attributes.get(keyword);
					if(coll == null) {
						coll = new HashSet<KeywordElement>(); 
						attributes.put(keyword, coll);
					} 
					coll.add(resource);
				}
				else if(resource.getType() == KeywordElement.CONCEPT || resource.getType() == KeywordElement.RELATION){
					Collection<KeywordElement> coll = conceptsAndRelations.get(keyword);
					if(coll == null) {
						coll = new HashSet<KeywordElement>(); 
						conceptsAndRelations.put(keyword, coll);
					} 
					coll.add(resource);
				}
			}
			
		}
		
		return queriesWithResults;
	}
	
	private Map<String,Collection<KeywordElement>> getSchemaKeywordElements(Collection<String> queries) throws IOException {
		StandardAnalyzer analyzer = new StandardAnalyzer();
		QueryParser parser = new QueryParser(Constant.SCHEMA_FIELD, analyzer);
		parser.setDefaultOperator(QueryParser.AND_OPERATOR);
		
		Map<String,Collection<KeywordElement>> keywords = new HashMap<String,Collection<KeywordElement>>();
		for(String keyword : queries) {
			Query q = null;
			try {
				q = parser.parse(keyword);
			} catch (ParseException e) {
				e.printStackTrace();
				continue;
			}
			
			Collection<KeywordElement> tmp = searchSchemaWithClause(q, keyword);
			
			if(tmp != null && tmp.size() > 0)
				keywords.put(keyword, tmp);
		}
		
		return keywords;
	}
	
	private Collection<KeywordElement> searchSchemaWithClause(Query clause, String keyword) throws IOException {
		Collection<KeywordElement> result = new HashSet<KeywordElement>();
		
		Hits hits = searcher.search(clause);
//			/********* add fuzzy query funtion here **************/
//			if (hits == null || hits.length() == 0){
//				Set<Term> terms = new HashSet<Term>();
//				clause.extractTerms(terms);
//				//if clause query is a term query
//				if(terms.size() != 0){
//					BooleanQuery query = new BooleanQuery();
//					for(Term term : terms) {
//						query.add(new FuzzyQuery(term, 0.8f, 1), Occur.MUST);
//					}
//					hits = searcher.search(query);
//				}
//			}
//			/************************************************/

		for (int i = 0; i < Math.min(hits.length(), MAX_KEYWORDRESULT_SIZE); i++) {
			Document doc = hits.doc(i);
			float score = hits.score(i);
			
 			if (score >= SCHEMA_THRESHOLD) {
				String type = doc.get(Constant.TYPE_FIELD);
				if(type == null) {
					log.error("type is null!");
					continue;
				}

				KeywordElement ele = null;
				if(type.equals(TypeUtil.CONCEPT)){
					INamedConcept con = new NamedConcept(pruneString(doc.get(Constant.URI_FIELD)), doc.get(Constant.EXTENSION_FIELD));
					ele = new KeywordElement(con, KeywordElement.CONCEPT, doc, score, keyword, ns);
				}
				else if(type.equals(TypeUtil.RELATION)){
					IRelation rel = new Relation(pruneString(doc.get(Constant.URI_FIELD)));
					ele = new KeywordElement(rel, KeywordElement.RELATION, score, keyword, ns);
				}
				else if(type.equals(TypeUtil.ATTRIBUTE)){
					IAttribute attr = new Attribute(pruneString(doc.get(Constant.URI_FIELD)));
					ele = new KeywordElement(attr, KeywordElement.ATTRIBUTE, score, keyword, ns);
				}
				
				if (ele != null)
					result.add(ele);
			}
			else 
				break;
		}				

		return result;
	}
	
	private void searchEntitiesByAttributesAndValues(SortedSet<KeywordSegment> segements, 
			Map<String, Collection<KeywordElement>> attributes, 
			Map<KeywordElement, KeywordSegment> entities2Segement, 
			Map<KeywordSegment, Collection<KeywordElement>> segments2Entities,
			Map<String, Collection<KeywordElement>> keywords2Entities) throws IOException {
		
		StandardAnalyzer analyzer = new StandardAnalyzer();

		Set<KeywordSegment> segementsWithResults = new HashSet<KeywordSegment>();
		for (String keywordForAttribute : attributes.keySet()) {
			for (KeywordElement attribute : attributes.get(keywordForAttribute)) {
				QueryParser parser = new QueryParser(Constant.CONTENT_FIELD, analyzer);
				parser.setDefaultOperator(QueryParser.AND_OPERATOR);
				
				for (KeywordSegment segement : segements) {
					try {
						BooleanQuery q = new BooleanQuery();
						Query valueQuery = parser.parse(segement.getQuery());
						Query attributeQuery = new TermQuery(new Term(Constant.ATTRIBUTE_FIELD, attribute.getUri()));
						q.add(valueQuery, Occur.MUST);
						q.add(attributeQuery, Occur.MUST);
						boolean hasResults = searchEntitiesWithClause(q, segement, keywordForAttribute,	
								entities2Segement, segments2Entities, keywords2Entities);

						if (hasResults)
							segementsWithResults.add(segement);
					} catch (ParseException e) {
						e.printStackTrace();
						continue;
					}
				}
			}
		}
	}
	
	private void searchEntitiesByValues(SortedSet<KeywordSegment> segements, 
			Map<String, Collection<KeywordElement>> attributes, 
			Map<KeywordElement, KeywordSegment> entities2Segement, 
			Map<KeywordSegment, Collection<KeywordElement>> segments2Entities,
			Map<String, Collection<KeywordElement>> keywords2Entities) throws IOException {

		StandardAnalyzer analyzer = new StandardAnalyzer();
		
		for (KeywordSegment segement : segements) {
			try {
				QueryParser qp = new QueryParser(Constant.CONTENT_FIELD, analyzer);
				qp.setDefaultOperator(QueryParser.AND_OPERATOR);
				Query q = qp.parse(segement.getQuery());
				
				searchEntitiesWithClause(q, segement, null, entities2Segement, segments2Entities, keywords2Entities);
				
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
	}		
	
//	private void searchEntitiesByCompounds(Map<String, Set<String>> queries, Map<String, Collection<KeywordElement>> attributesAndRelations, 
//			Map<KeywordElement, KeywordSegment> entitiesWithSegement, Map<KeywordSegment, Collection<KeywordElement>> segementsWithEntities,
//			Map<String, Collection<KeywordElement>> keywordsWithentities) {
//		try {
//			StandardAnalyzer analyzer = new StandardAnalyzer();
//			for (String keywordForAttributeAndRelation : queries.keySet()) {
//				Collection<KeywordElement> elements = attributesAndRelations.get(keywordForAttributeAndRelation);
//				if(elements != null && elements.size() != 0)
//				for (KeywordElement attributeAndRelation : attributesAndRelations.get(keywordForAttributeAndRelation)) {
//					QueryParser parser = new QueryParser(attributeAndRelation.getResource().getUri(), analyzer);
//					parser.setDefaultOperator(QueryParser.AND_OPERATOR);
//					for(String keywordForValueAndEntityID : queries.get(keywordForAttributeAndRelation)) {
//						Query q = parser.parse(keywordForValueAndEntityID);
//						String compound = keywordForAttributeAndRelation + SEPARATOR + keywordForValueAndEntityID;
//						KeywordSegment segement = new KeywordSegment(compound);
//						searchEntitiesWithClause(q, segement, null, entitiesWithSegement, segementsWithEntities, keywordsWithentities);
//					}	
//				}
//			}
//		} catch (ParseException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
		
	private boolean searchEntitiesWithClause(Query clause, KeywordSegment segement, String additionalKeyword, 
			Map<KeywordElement, KeywordSegment> entities2Segment, 
			Map<KeywordSegment, Collection<KeywordElement>> segments2Entities,
			Map<String, Collection<KeywordElement>> keywords2Entities) throws IOException {
		
		String attributeUri;
	    ScoreDoc[] docHits = getTopValueDocuments(clause, MAX_KEYWORDRESULT_SIZE);
	    if (docHits.length == 0 || docHits[0] == null)
	    	return false;

	    Set<String> loadFieldNames = new HashSet<String>();
//	    loadFieldNames.add(Constant.URI_FIELD);
//	    loadFieldNames.add(Constant.TYPE_FIELD);
//	    loadFieldNames.add(Constant.EXTENSION_FIELD);
	    
	    Set<String> lazyFieldNames = new HashSet<String>();
//	    lazyFieldNames.add(Constant.NEIGHBORHOOD_FIELD);
	    lazyFieldNames.add(Constant.IN_PROPERTIES_FIELD);
	    lazyFieldNames.add(Constant.OUT_PROPERTIES_FIELD);
	    
	    SetBasedFieldSelector fieldSelector = new SetBasedFieldSelector(loadFieldNames, lazyFieldNames);

//	    float maxScore = docHits[0].score;
	    maxScore = Math.max(maxScore, docHits[0].score);
	    	
	   	for (int i = 0; i < docHits.length; i++) {
	   		Document valueDoc = valueReader.document(docHits[i].doc);
	   		
	   		String uri = valueDoc.getFieldable(Constant.URI_FIELD).stringValue();
	   		attributeUri = valueDoc.getFieldable(Constant.ATTRIBUTE_FIELD).stringValue();
	   		String ext = valueDoc.getFieldable(Constant.EXTENSION_FIELD).stringValue();
//	   		float score = docHits[i].score / maxScore;
	   		float score = docHits[i].score;
	   		
//	   		Document doc = reader.document(docHits[i].doc, fieldSelector);

	   		TermDocs td = reader.termDocs(new Term(Constant.URI_FIELD, uri));
	   		if (!td.next())
	   			continue;
	   		Document doc = reader.document(td.doc(), fieldSelector); 

//		   	if(score < ENTITY_THRESHOLD)
//		   		break;
	   		
//    		IEntity ent = new Entity(pruneString(uri), doc.getFieldable(Constant.EXTENSION_FIELD).stringValue());
    		IEntity ent = new Entity(pruneString(uri), null);
    		KeywordElement ele = new KeywordElement(ent, KeywordElement.ENTITY, doc, score, ns);
    		ele.setExtensionId(ext);
    		KeywordSegment ks = new KeywordSegment(segement.getKeywords());
    		
    		ele.setAttributeUri(attributeUri);
    		
    		if (additionalKeyword != null) 
    			ks.addAttributeKeyword(additionalKeyword);
    			
    		KeywordSegment oks = entities2Segment.get(ele);
    		
    		if (oks == null || !oks.contains(ks)) {
    			ele.setKeywords(ks.getAllKeywords());
    			ele.addReachableKeywords(ks.getAllKeywords());
    			entities2Segment.put(ele, ks);
    			Collection<KeywordElement> coll = segments2Entities.get(ks);
    			if(coll == null) {
    				coll = new HashSet<KeywordElement>();
    				segments2Entities.put(ks, coll);
    			}
    			coll.add(ele);
    			
    			for(String keyword : ks.getAllKeywords()) {
    				coll = keywords2Entities.get(keyword);
    				if(coll == null) {
	    				coll = new HashSet<KeywordElement>();
	    				keywords2Entities.put(keyword, coll);
	    			}
	    			coll.add(ele);
    			}
    		}
    	}
	   	
		return true;
	}
	
	private String pruneString(String str) {
		return str.replaceAll("\"", "");
	}
	
	private ScoreDoc[] getDocuments(Query q) throws StorageException, IOException {
		final List<ScoreDoc> docs = new ArrayList<ScoreDoc>();
			searcher.search(q, new HitCollector() {
				public void collect(int docId, float score) {
					docs.add(new ScoreDoc(docId, score));
				}
			});
		
		return docs.toArray(new ScoreDoc[docs.size()]);
	}
	
	private ScoreDoc[] getTopDocuments(Query q, int top) throws IOException {
		ScoreDoc[] docs;
		TopDocCollector collector = new TopDocCollector(top);  
		searcher.search(q, collector);
		docs = collector.topDocs().scoreDocs;
		if (docs.length > 0)
			log.debug(q + " " + docs.length);
		return docs;
	}

	private ScoreDoc[] getTopValueDocuments(Query q, int top) throws IOException {
		ScoreDoc[] docs;
		TopDocCollector collector = new TopDocCollector(top);  
		valueSearcher.search(q, collector);
		docs = collector.topDocs().scoreDocs;
		log.debug(q + " " + docs.length);
		return docs;
	}
	
	private void resetMaxScore() {
		maxScore = 1.0;
	}
	
	public static void main(String[] args) throws Exception {
		OptionParser op = new OptionParser();
		op.accepts("o", "output directory")
			.withRequiredArg().ofType(String.class).describedAs("directory");

		OptionSet os = op.parse(args);
		
		if (!os.has("o")) {
			op.printHelpOn(System.out);
			return;
		}

		String directory = (String)os.valueOf("o");
		
		Scanner scanner = new Scanner(System.in);
		while (true) {
			System.out.println("Please input the keywords:");
			String line = scanner.nextLine();

			// an index is accessed through an IndexReader object, which is
			// passed
			// to all evaluators and searchers
			edu.unika.aifb.graphindex.index.IndexReader ir = new edu.unika.aifb.graphindex.index.IndexReader(new IndexDirectory(directory));

			// a keyword query, DirectExploringQueryEvaluator is the only
			// currently usable for keyword queries
//			KeywordQuery kq = new KeywordQuery("q1", line);
//			KeywordQueryEvaluator kwEval = new ExploringKeywordQueryEvaluator(ir);
//			System.out.println(kwEval.evaluate(kq).toDataString());
			
			KeywordSearcher searcher = new KeywordSearcher(ir);
			Map<KeywordSegment, Collection<KeywordElement>> results = searcher.searchKeywordElements(getKeywordList(line));
			int i = 0;
			for(KeywordSegment ks : results.keySet()) {
				System.out.println(++i + ": " + ks);
				for(KeywordElement ke : results.get(ks)) {
					System.out.println(ke.getUri() + " | " + ke.getMatchingScore());
				}
				System.out.println();
			} 
		}
	} 
	
	 public static LinkedList<String> getKeywordList(String line) {
		LinkedList<String> ll = new LinkedList<String>();

		// Boolean set to true if a " is opened
		Boolean opened = false;
		// Temporary string
		String acc = "";
		// Browse the string
		for (int i = 0; i < line.length(); i++) {
			// Get the character
			String str = String.valueOf(line.charAt(i));
			// If it is an opening "
			if (str.equals("\"") && !opened) {
				opened = true;
				continue;
			}
			// If it is a closing "
			if (str.equals("\"") && opened) {
				opened = false;
				// Put the acc string into the list
				ll.add(acc);
				acc = "";
				continue;
			}
			// If it is a space not between "
			if (str.equals(" ") && !opened) {
				if (acc != "") {
					ll.add(acc);
					acc = "";
				}
				continue;
			}
			// If it is a space between "
			if (str.equals(" ") && opened) {
				acc += " ";
				continue;
			}
			// Else, add the char
			acc += str;
		}
		if (!acc.equals(""))
			ll.add(acc);

		return ll;
	}
}

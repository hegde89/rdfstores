package edu.unika.aifb.graphindex.searcher.entity;

/**
 * Copyright (C) 2009 Lei Zhang (beyondlei at gmail.com)
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SetBasedFieldSelector;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.HitCollector;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocCollector;
import org.apache.lucene.search.BooleanClause.Occur;
import org.openrdf.model.vocabulary.RDFS;

import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.model.IEntity;
import edu.unika.aifb.graphindex.model.impl.Entity;
import edu.unika.aifb.graphindex.searcher.keyword.model.Constant;
import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordElement;
import edu.unika.aifb.graphindex.searcher.keyword.model.TransformedGraph;
import edu.unika.aifb.graphindex.searcher.keyword.model.TransformedGraphNode;
import edu.unika.aifb.graphindex.storage.NeighborhoodStorage;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.lucene.LuceneNeighborhoodStorage;
import edu.unika.aifb.graphindex.util.TypeUtil;

public class EntitySearcher {
	
	private NeighborhoodStorage ns;
	private IndexSearcher searcher;
	private IndexReader reader; 
	
	private static final double ENTITY_THRESHOLD = 0.5;
	private static final double SCHEMA_THRESHOLD = 0.8;
	private static final int MAX_KEYWORDRESULT_SIZE = 10;
	
	private static final String SEPARATOR = ":";
	
	private static final Logger log = Logger.getLogger(EntitySearcher.class);
	
	public EntitySearcher(edu.unika.aifb.graphindex.index.IndexReader idxReader) throws StorageException {
		try {
			reader = IndexReader.open(idxReader.getIndexDirectory().getDirectory(IndexDirectory.KEYWORD_DIR));
			searcher = new IndexSearcher(reader);
			ns = new LuceneNeighborhoodStorage(idxReader.getIndexDirectory().getDirectory(IndexDirectory.NEIGHBORHOOD_DIR));
			ns.initialize(false, true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void warmUp(Set<String> queries) throws StorageException {
		for (String query : queries) {
			String[] t = query.split(" ", 2);
			Query q = new PrefixQuery(new Term(t[0], t[1]));
			ScoreDoc[] docIds = getDocuments(q);
//			log.debug("warmup: " + q + " => " + docIds.length + " doc ids");
		}

	}
	
	public TransformedGraph searchEntities(TransformedGraph graph, int cutOff) {
		for(TransformedGraphNode node : graph.getNodes()) {
			if(node.getType() == TransformedGraphNode.ENTITY_QUERY_NODE) {
				Map<String, Collection<String>> attributeQueries = node.getAttributeQueries();
				if(attributeQueries != null && attributeQueries.keySet().size() != 0)
					node.setEntities(searchEntities(attributeQueries, node.getTypeQueries(), cutOff));
			}	
			else if(node.getType() == TransformedGraphNode.ENTITY_NODE) {
				node.setEntities(searchEntities(node.getUriQuery()));
			}	
			if (node.getEntities() != null)
				log.debug("variable: " + node.getNodeName() + ", entities: " + node.getEntities().size());
			else
				log.debug("variable: " + node.getNodeName() + ", no entities");
		}
		
		return graph;
	}
	
	public TransformedGraph searchEntities(TransformedGraph graph) {
		for(TransformedGraphNode node : graph.getNodes()) {
			if(node.getType() == TransformedGraphNode.ENTITY_QUERY_NODE) {
				Map<String, Collection<String>> attributeQueries = node.getAttributeQueries();
				if(attributeQueries != null && attributeQueries.keySet().size() != 0)
					node.setEntities(searchEntities(attributeQueries, node.getTypeQueries(), 0));
			}	
			else if(node.getType() == TransformedGraphNode.ENTITY_NODE) {
				node.setEntities(searchEntities(node.getUriQuery()));
			}	
			if (node.getEntities() != null)
				log.debug("variable: " + node.getNodeName() + ", entities: " + node.getEntities().size());
			else
				log.debug("variable: " + node.getNodeName() + ", no entities");
		}
		
		return graph;
	}
	
	public boolean isType(String entity, String concept) {
		TermQuery tq = new TermQuery(new Term(Constant.URI_FIELD, entity));
		try {
			ScoreDoc[] docHits = getDocuments(tq);
			Set<String> loadFieldNames = new HashSet<String>();
		    loadFieldNames.add(Constant.URI_FIELD);
		    loadFieldNames.add(Constant.TYPE_FIELD);
		    loadFieldNames.add(Constant.EXTENSION_FIELD);
		    loadFieldNames.add(Constant.CONCEPT_FIELD);
		    Set<String> lazyFieldNames = new HashSet<String>();
		    lazyFieldNames.add(Constant.NEIGHBORHOOD_FIELD);
		    SetBasedFieldSelector fieldSelector = new SetBasedFieldSelector(loadFieldNames, lazyFieldNames);
			
		    for(ScoreDoc docHit : docHits) {
		    	Document doc = reader.document(docHit.doc, fieldSelector);
		    	String type = doc.getFieldable(Constant.TYPE_FIELD).stringValue();
				if(type == null) {
					System.err.println("type is null!");
					continue;
				}

				if(type.equals(TypeUtil.ENTITY)){
					if(doc.getFieldable(Constant.CONCEPT_FIELD).stringValue().equals(concept))
						return true; 
				}
		    }
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public Collection<KeywordElement> searchEntities(Map<String, Collection<String>> attributeQueries, Collection<String> typeQueries, int cutOff) {
		Map<String, Collection<String>> attributes = new HashMap<String, Collection<String>>();
		Collection<String> concepts = new ArrayList<String>();
		Collection<KeywordElement> entities = new ArrayList<KeywordElement>();
		
		searchConcepts(searcher, typeQueries, concepts);
		searchAttributes(searcher, attributeQueries.keySet(), attributes);
		if(attributes != null && attributes.size() != 0)
			searchEntitiesByAttributeVauleCompounds(searcher, attributeQueries, attributes, concepts, entities, cutOff);
		
		return entities;
	}
	
	public Collection<KeywordElement> searchEntities(String uriQuery) {
		Collection<KeywordElement> entities = new ArrayList<KeywordElement>();
		
		searchEntitiesByUri(searcher, uriQuery, entities);
		
		return entities;
	}

	public void searchConcepts(IndexSearcher searcher, Collection<String> queries, Collection<String> concepts) {
		if (queries != null && queries.size() != 0) {
			try {
				// search schema elements
				StandardAnalyzer analyzer = new StandardAnalyzer();
				for (String keyword : queries) {
					Query q;
					if (keyword.startsWith(Constant.URI_PREFIX)) {
						concepts.add(keyword);
					} else {
						QueryParser parser = new QueryParser(Constant.SCHEMA_FIELD, analyzer);
						q = parser.parse(keyword);
						Collection<String> tmp = searchConceptWithClause(searcher, q);
						if (tmp != null && tmp.size() != 0) {
							concepts.addAll(tmp);
						}
					}
				}
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public Collection<String> searchConceptWithClause(IndexSearcher searcher, Query clause) {
		Collection<String> result = new HashSet<String>();
		try {
			Hits hits = searcher.search(clause);
			/********* add fuzzy query funtion here **************/
			if (hits == null || hits.length() == 0){
				Set<Term> terms = new HashSet<Term>();
				clause.extractTerms(terms);
				//if clause query is a term query
				if(terms.size() != 0){
					BooleanQuery query = new BooleanQuery();
					for(Term term : terms) {
						query.add(new FuzzyQuery(term, 0.8f, 1), Occur.MUST);
					}
					hits = searcher.search(query);
					log.debug(query + " " + hits);
				}
			}
			/************************************************/

			for(int i = 0; i < hits.length(); i++){
				Document doc = hits.doc(i);
				float score = hits.score(i);
				if(score >= SCHEMA_THRESHOLD){
					String type = doc.get(Constant.TYPE_FIELD);
					if(type == null) {
						System.err.println("type is null!");
						continue;
					}

					if(type.equals(TypeUtil.CONCEPT)){
						result.add(doc.get(Constant.URI_FIELD));
					}
				}
			}				
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	public void searchAttributes(IndexSearcher searcher, Collection<String> queries, Map<String, Collection<String>> attributes) {
		try {
			// search schema elements
			StandardAnalyzer analyzer = new StandardAnalyzer();
			for(String keyword : queries) {
				Query q;
				Collection<String> tmp = null;
				if(keyword.startsWith(Constant.URI_PREFIX)) {
					tmp = new ArrayList<String>();	
					if(keyword.equals(RDFS.LABEL.stringValue()))
						tmp.add(Constant.LABEL_FIELD);
					else
						tmp.add(keyword);
				}
				else if(keyword.equals(Constant.LABEL_FIELD) || keyword.equals(Constant.LOCALNAME_FIELD) || keyword.equals(Constant.CONCEPT_FIELD)) {
					tmp = new ArrayList<String>();	
					tmp.add(keyword);
				}
				else {	
					QueryParser parser = new QueryParser(Constant.SCHEMA_FIELD, analyzer);
					q = parser.parse(keyword);
					tmp = searchAttributesWithClause(searcher, q);
				}	
				
				if(tmp != null && tmp.size() != 0) {
					for (String resource : tmp) {
						Collection<String> coll = attributes.get(keyword);
						if (coll == null) {
							coll = new ArrayList<String>();
							attributes.put(keyword, coll);
						}
						coll.add(resource);
					}
				}
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Collection<String> searchAttributesWithClause(IndexSearcher searcher, Query clause) {
		Collection<String> result = new HashSet<String>();
		try {
			Hits hits = searcher.search(clause);
			/********* add fuzzy query funtion here **************/
			if (hits == null || hits.length() == 0){
				Set<Term> terms = new HashSet<Term>();
				clause.extractTerms(terms);
				//if clause query is a term query
				if(terms.size() != 0){
					BooleanQuery query = new BooleanQuery();
					for(Term term : terms) {
						query.add(new FuzzyQuery(term, 0.8f, 1), Occur.MUST);
					}
					hits = searcher.search(query);
					log.debug(query + " " + hits);
				}
			}
			/************************************************/

			for(int i = 0; i < hits.length(); i++){
				Document doc = hits.doc(i);
				float score = hits.score(i);
				if(score >= SCHEMA_THRESHOLD){
					String type = doc.get(Constant.TYPE_FIELD);
					if(type == null) {
						System.err.println("type is null!");
						continue;
					}

					if(type.equals(TypeUtil.ATTRIBUTE)){
						result.add(doc.get(Constant.URI_FIELD));
					}
				}
			}				
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	private void searchEntitiesByUri(IndexSearcher searcher, String query, Collection<KeywordElement> entities) {
		TermQuery tq = new TermQuery(new Term(Constant.URI_FIELD, query));
		try {
			ScoreDoc[] docHits = getDocuments(tq);
			Set<String> loadFieldNames = new HashSet<String>();
		    loadFieldNames.add(Constant.URI_FIELD);
		    loadFieldNames.add(Constant.TYPE_FIELD);
		    loadFieldNames.add(Constant.EXTENSION_FIELD);
		    loadFieldNames.add(Constant.CONCEPT_FIELD);
		    Set<String> lazyFieldNames = new HashSet<String>();
		    lazyFieldNames.add(Constant.NEIGHBORHOOD_FIELD);
		    SetBasedFieldSelector fieldSelector = new SetBasedFieldSelector(loadFieldNames, lazyFieldNames);
			
		    for(ScoreDoc docHit : docHits) {
		    	Document doc = reader.document(docHit.doc, fieldSelector);
		    	float score = docHit.score;
		    	String type = doc.getFieldable(Constant.TYPE_FIELD).stringValue();
				if(type == null) {
					System.err.println("type is null!");
					continue;
				}

				if(type.equals(TypeUtil.ENTITY)){
					IEntity ent = new Entity(pruneString(doc.getFieldable(Constant.URI_FIELD).stringValue()), doc.getFieldable(Constant.EXTENSION_FIELD).stringValue());
					KeywordElement ele = new KeywordElement(ent, KeywordElement.ENTITY, doc, score, ns);
					entities.add(ele);
				}
		    }
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void searchEntitiesByAttributeVauleCompounds(IndexSearcher searcher, Map<String, Collection<String>> queries, 
			Map<String, Collection<String>> attributes, Collection<String> concepts, Collection<KeywordElement> entities, int cutOff) {
		BooleanQuery entityQuery = new BooleanQuery(); 
		try {
			StandardAnalyzer analyzer = new StandardAnalyzer();
			for (String keywordForAttribute : queries.keySet()) {
				BooleanQuery attributeQuery = new BooleanQuery(); 
				Collection<String> attributeElements = attributes.get(keywordForAttribute);
				if(attributeElements != null && attributeElements.size() != 0)
				for (String attribute : attributeElements) {
					QueryParser parser = new QueryParser(attribute, analyzer);
					BooleanQuery bq = new BooleanQuery(); 
					for(String value : queries.get(keywordForAttribute)){
						Query q = parser.parse(value);
						if (q instanceof BooleanQuery) {
							BooleanQuery bquery = (BooleanQuery) q;
							for (BooleanClause clause : bquery.getClauses()) {
								clause.setOccur(Occur.MUST);
							}
						}
						bq.add(q, BooleanClause.Occur.MUST);
					}
					attributeQuery.add(bq, BooleanClause.Occur.SHOULD);
				}
				entityQuery.add(attributeQuery, BooleanClause.Occur.MUST);
			}
			if(concepts != null && concepts.size() != 0) {
				BooleanQuery typeQuery = new BooleanQuery(); 
				for(String type : concepts) {
					TermQuery tq = new TermQuery(new Term(Constant.CONCEPT_FIELD, type));
					typeQuery.add(tq, BooleanClause.Occur.MUST);
				}
				entityQuery.add(typeQuery, BooleanClause.Occur.MUST);
			}
			
			searchEntitiesWithClause(searcher, entityQuery, entities, cutOff); 
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Collection<KeywordElement> searchEntitiesWithClause(IndexSearcher searcher, Query query, Collection<KeywordElement> result, int cutOff) {
		try {
			Set<String> loadFieldNames = new HashSet<String>();
		    loadFieldNames.add(Constant.URI_FIELD);
		    loadFieldNames.add(Constant.TYPE_FIELD);
		    loadFieldNames.add(Constant.EXTENSION_FIELD);
		    Set<String> lazyFieldNames = new HashSet<String>();
		    lazyFieldNames.add(Constant.NEIGHBORHOOD_FIELD);
		    SetBasedFieldSelector fieldSelector = new SetBasedFieldSelector(loadFieldNames, lazyFieldNames);
			
		    ScoreDoc[] docHits;
		    if(cutOff > 0)
				docHits = getTopDocuments(query, cutOff);
			else
				docHits = getDocuments(query);

		   	for(ScoreDoc docHit : docHits) {
		   		Document doc = reader.document(docHit.doc, fieldSelector);
		   		float score = docHit.score;
		   		String type = doc.getFieldable(Constant.TYPE_FIELD).stringValue();
		   		if(type == null) {
		   			System.err.println("type is null!");
		   			continue;
		   		}

	    		if(type.equals(TypeUtil.ENTITY)){
	    			IEntity ent = new Entity(pruneString(doc.getFieldable(Constant.URI_FIELD).stringValue()), doc.getFieldable(Constant.EXTENSION_FIELD).stringValue());
	    			KeywordElement ele = new KeywordElement(ent, KeywordElement.ENTITY, doc, score, ns);
	    			result.add(ele);
	    		}
	    	}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	
	public ScoreDoc[] getDocuments(Query q) throws StorageException {
		final List<ScoreDoc> docs = new ArrayList<ScoreDoc>();
		try {
			searcher.search(q, new HitCollector() {
				public void collect(int docId, float score) {
					docs.add(new ScoreDoc(docId, score));
				}
			});
		} catch (IOException e) {
			throw new StorageException(e);
		}
		
		return docs.toArray(new ScoreDoc[docs.size()]);
	}
	
	public ScoreDoc[] getTopDocuments(Query q, int top) throws StorageException {
		ScoreDoc[] docs;
		try {
			TopDocCollector collector = new TopDocCollector(top);  
			searcher.search(q, collector);
			docs = collector.topDocs().scoreDocs;
		} catch (IOException e) {
			throw new StorageException(e);
		}
		
		return docs;
	}
	
	private String pruneString(String str) {
		return str.replaceAll("\"", "");
	}
	
	public static Map<String, Collection<String>> parseQueries(Collection<String> queries) {
		Map<String, Collection<String>> keywordCompounds = new HashMap<String, Collection<String>>();
		for(String query : queries) {
			String[] strs = query.trim().split(SEPARATOR);
			if(strs.length == 2) {
				Collection<String> values = keywordCompounds.get(strs[0]);
				if(values == null) {
					values = new HashSet<String>();
					keywordCompounds.put(strs[0], values);
				}
				values.add(strs[1]);
			}
			else {
				log.error("--------------------- ERROR! ---------------------");
			}
		}
		
		return keywordCompounds;
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

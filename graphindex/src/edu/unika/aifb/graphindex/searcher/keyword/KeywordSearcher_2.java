package edu.unika.aifb.graphindex.searcher.keyword;

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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocCollector;
import org.apache.lucene.search.BooleanClause.Occur;

import edu.unika.aifb.graphindex.model.IAttribute;
import edu.unika.aifb.graphindex.model.IEntity;
import edu.unika.aifb.graphindex.model.INamedConcept;
import edu.unika.aifb.graphindex.model.IRelation;
import edu.unika.aifb.graphindex.model.impl.Attribute;
import edu.unika.aifb.graphindex.model.impl.Entity;
import edu.unika.aifb.graphindex.model.impl.NamedConcept;
import edu.unika.aifb.graphindex.model.impl.Relation;
import edu.unika.aifb.graphindex.searcher.keyword.model.Constant;
import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordElement;
import edu.unika.aifb.graphindex.searcher.keyword.model.KeywordSegement;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.TypeUtil;

public class KeywordSearcher_2 {
	
	private IndexReader reader; 
	private IndexSearcher searcher;
	private Set<String> allAttributes;
	private Set<String> allRelations;
	
	private static final double ENTITY_THRESHOLD = 0.5;
	private static final double SCHEMA_THRESHOLD = 0.8;
	private static final int MAX_KEYWORDRESULT_SIZE = 50;
	
	private static final String SEPARATOR = ":";
	
	private static final Logger log = Logger.getLogger(KeywordSearcher_2.class);
	
	public KeywordSearcher_2(String indexDir) {
		this.allAttributes = new HashSet<String>();
		this.allRelations = new HashSet<String>();
		try {
			reader = IndexReader.open(indexDir);
			searcher = new IndexSearcher(reader);
			searchAllAttributes(searcher, allAttributes);
			searchAllRelations(searcher, allRelations);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Map<KeywordSegement,Collection<KeywordElement>> searchKeywordElements(Collection<String> queries) {
		Collection<String> keywordQueries = new HashSet<String>();
		Map<String, Collection<String>> keywordCompoundQueries = new HashMap<String, Collection<String>>();
		parseQueries(queries,keywordQueries, keywordCompoundQueries);
		
		Map<String, Collection<KeywordElement>> conceptsAndRelations = new HashMap<String, Collection<KeywordElement>>();
		Map<KeywordElement, KeywordSegement> entitiesWithSegement = new HashMap<KeywordElement, KeywordSegement>();
		searchElementsByKeywords(keywordQueries, conceptsAndRelations, entitiesWithSegement);
		searchElementsByKeywordCompounds(keywordCompoundQueries, entitiesWithSegement);
		
//		for(KeywordElement ele : entitiesWithSegement.keySet()) {
//			System.out.println(ele.getResource() + "\t" + entitiesWithSegement.get(ele));
//		}
		
		Map<String, Collection<KeywordElement>> entities = new HashMap<String, Collection<KeywordElement>>();
		for(KeywordElement ele : entitiesWithSegement.keySet()) {
			Set<String> keywords = entitiesWithSegement.get(ele).getKeywords();
			ele.setKeywords(keywords);
			for(String keyword : keywords) {
				Collection<KeywordElement> coll = entities.get(keyword);
				if(coll == null) {
					coll = new HashSet<KeywordElement>(); 
					entities.put(keyword, coll);
				} 
				coll.add(ele);
			}
		}
		
		overlapNeighborhoods(entities, entitiesWithSegement);

		Map<KeywordSegement, Collection<KeywordElement>> results = new HashMap<KeywordSegement, Collection<KeywordElement>>();
		for(String keyword : conceptsAndRelations.keySet()) {
			
		}
		
		return results;
	}
	
	private void overlapNeighborhoods(Map<String, Collection<KeywordElement>> entities, Map<KeywordElement, KeywordSegement> entitiesWithSegement) {
		
		
		
		
		
//		Map<String, Collection<KeywordElement>> entitiesToBeRemoved = new HashMap<String, Collection<KeywordElement>>();
//		for(String keyword : entities.keySet()) {
//			for(KeywordElement ele : entities.get(keyword)) {
//				if (ele.getType() == KeywordElement.ENTITY && ele.getResource() instanceof IEntity) {
//					IEntity entity = (IEntity)ele.getResource();
//					Collection<Collection<KeywordElement>> colls = new HashSet<Collection<KeywordElement>>();
//					for (String key : entities.keySet()) {
//						if(!keyword.equals(key) && !keyword.contains(key))
//							colls.add(entities.get(key));
//					}
//					if(!entity.isAllReachable(colls)) {
//						Collection<KeywordElement> coll = entitiesToBeRemoved.get(keyword);
//						if (coll == null) {
//							coll = new HashSet<KeywordElement>();
//							entitiesToBeRemoved.put(keyword, coll);
//						}
//						coll.add(ele);
//					}
//				}
//				else {
//					log.error("--------------------- ERROR! ---------------------");
//					Collection<KeywordElement> coll = entitiesToBeRemoved.get(keyword);
//					if (coll == null) {
//						coll = new HashSet<KeywordElement>();
//						entitiesToBeRemoved.put(keyword, coll);
//					}
//					coll.add(ele);
//				}
//			}
//		}
//		
//		for(String keyword : entitiesToBeRemoved.keySet()) {
//			entities.get(keyword).removeAll((entitiesToBeRemoved.get(keyword)));
//			if(entities.get(keyword).size() == 0) {
//				entities.remove(keyword);
//			}
//		}
	}
	
	public void parseQueries(Collection<String> queries, Collection<String> keywords, Map<String, Collection<String>> keywordCompounds) {
		for(String query : queries) {
			String[] strs = query.trim().split(SEPARATOR);
			if(strs.length == 1) {
				keywords.add(strs[0]);
			}
			else if(strs.length == 2) {
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
	}
	
	public void searchElementsByKeywords(Collection<String> queries, 
			Map<String, Collection<KeywordElement>> conceptsAndRelations, Map<KeywordElement, KeywordSegement> entities) { 
		Map<String, Collection<KeywordElement>> attributes = new HashMap<String, Collection<KeywordElement>>();
		
		searchSchema(searcher, queries, conceptsAndRelations, attributes);
		if(attributes != null && attributes.size() != 0)
			searchEntitiesByAttributesAndValues(searcher, queries, attributes, entities);
		searchEntitiesByValues(searcher, queries, attributes, entities);
		
	}
	
	public void searchElementsByKeywordCompounds(Map<String, Collection<String>> queries, 
			Map<KeywordElement, KeywordSegement> entities) { 
		Map<String, Collection<KeywordElement>> attributesAndRelations = new HashMap<String, Collection<KeywordElement>>();
		
		searchSchema(searcher, queries.keySet(), attributesAndRelations);
		if(attributesAndRelations != null && attributesAndRelations.size() != 0)
			searchEntitiesByCompounds(searcher, queries, attributesAndRelations, entities);
	}
	
	public void searchSchema(IndexSearcher searcher, Collection<String> queries, 
			Map<String, Collection<KeywordElement>> conceptsAndRelations, Map<String, Collection<KeywordElement>> attributes) {
		Set<String> queriesWithResults = new HashSet<String>();
		try {
			// search schema elements
			StandardAnalyzer analyzer = new StandardAnalyzer();
			QueryParser parser = new QueryParser(Constant.SCHEMA_FIELD, analyzer);
			for(String keyword : queries) {
				Query q = parser.parse(keyword);
				if(q instanceof BooleanQuery) {
					BooleanQuery bquery = (BooleanQuery)q;
					for(BooleanClause clause :  bquery.getClauses()) {
						clause.setOccur(Occur.MUST);
					}
				}
				Collection<KeywordElement> tmp = searchSchemaWithClause(searcher, q, keyword);
				if(tmp != null && tmp.size() != 0) {
					queriesWithResults.add(keyword);
					for(KeywordElement resource : tmp) {
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
			}
			queries.removeAll(queriesWithResults);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void searchSchema(IndexSearcher searcher, Collection<String> queries, Map<String, Collection<KeywordElement>> attributesAndRelations) {
		try {
			// search schema elements
			StandardAnalyzer analyzer = new StandardAnalyzer();
			QueryParser parser = new QueryParser(Constant.SCHEMA_FIELD, analyzer);
			for(String keyword : queries) {
				Query q = parser.parse(keyword);
				if(q instanceof BooleanQuery) {
					BooleanQuery bquery = (BooleanQuery)q;
					for(BooleanClause clause :  bquery.getClauses()) {
						clause.setOccur(Occur.MUST);
					}
				}
				Collection<KeywordElement> tmp = searchSchemaWithClause(searcher, q, keyword);
				if(tmp != null && tmp.size() != 0) {
					for(KeywordElement resource : tmp) {
						if(resource.getType() == KeywordElement.ATTRIBUTE || resource.getType() == KeywordElement.RELATION) {
							Collection<KeywordElement> coll = attributesAndRelations.get(keyword);
							if(coll == null) {
								coll = new HashSet<KeywordElement>(); 
								attributesAndRelations.put(keyword, coll);
							} 
							coll.add(resource);
						}
					}
				}
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Collection<KeywordElement> searchSchemaWithClause(IndexSearcher searcher, Query clause, String keyword) {
		Collection<KeywordElement> result = new HashSet<KeywordElement>();
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
				}
			}
			/************************************************/

			for(int i = 0; i < Math.min(hits.length(), MAX_KEYWORDRESULT_SIZE); i++){
				Document doc = hits.doc(i);
				float score = hits.score(i);
				if(score >= SCHEMA_THRESHOLD){
					String type = doc.get(Constant.TYPE_FIELD);
					if(type == null) {
						System.err.println("type is null!");
						continue;
					}

					if(type.equals(TypeUtil.CONCEPT)){
						INamedConcept con = new NamedConcept(pruneString(doc.get(Constant.URI_FIELD)), doc.get(Constant.EXTENSION_FIELD));
						KeywordElement ele = new KeywordElement(con, KeywordElement.CONCEPT, score, keyword);
						result.add(ele);
					}
					else if(type.equals(TypeUtil.RELATION)){
						IRelation rel = new Relation(pruneString(doc.get(Constant.URI_FIELD)));
						KeywordElement ele = new KeywordElement(rel, KeywordElement.RELATION, score, keyword);
						result.add(ele);
					}
					else if(type.equals(TypeUtil.ATTRIBUTE)){
						IAttribute attr = new Attribute(pruneString(doc.get(Constant.URI_FIELD)));
						KeywordElement ele = new KeywordElement(attr, KeywordElement.ATTRIBUTE, score, keyword);
						result.add(ele);
					}
				}
				else 
					break;
			}				
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	private void searchEntitiesByAttributesAndValues(IndexSearcher searcher, Collection<String> queries, 
			Map<String, Collection<KeywordElement>> attributes, Map<KeywordElement, KeywordSegement> entities) {
		Set<String> queriesWithResults = new HashSet<String>();
		try {
			StandardAnalyzer analyzer = new StandardAnalyzer();
			for (String keywordForAttribute : attributes.keySet()) {
				for (KeywordElement attribute : attributes.get(keywordForAttribute)) {
					QueryParser parser = new QueryParser(attribute.getResource().getUri(), analyzer);
					for (String keywordForValue : queries) {
						Query q = parser.parse(keywordForValue);
						if (q instanceof BooleanQuery) {
							BooleanQuery bquery = (BooleanQuery) q;
							for (BooleanClause clause : bquery.getClauses()) {
								clause.setOccur(Occur.MUST);
							}
						}
						searchEntitiesWithClause(searcher, q, keywordForValue, keywordForAttribute, entities);
					}
				}
			}
			queries.removeAll(queriesWithResults);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void searchEntitiesByValues(IndexSearcher searcher, Collection<String> queries, 
			Map<String, Collection<KeywordElement>> attributes, Map<KeywordElement, KeywordSegement> entities) {
		try {
			StandardAnalyzer analyzer = new StandardAnalyzer();
			Set<String> fields = new HashSet<String>();
			fields.addAll(allAttributes);
			if (attributes != null && attributes.size() != 0) {
				for (Collection<KeywordElement> coll : attributes.values()) {
					for (KeywordElement ele : coll) {
						fields.remove(ele.getResource().getUri());
					}
				}
			}
			KeywordQueryParser parser = new KeywordQueryParser(fields.toArray(new String[fields.size()]), analyzer);
			for (String keyword : queries) {
				Query q = parser.parse(keyword);
				searchEntitiesWithClause(searcher, q, keyword, null, entities);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}		
	
	private void searchEntitiesByCompounds(IndexSearcher searcher, Map<String, Collection<String>> queries, 
			Map<String, Collection<KeywordElement>> attributesAndRelations, Map<KeywordElement, KeywordSegement> entities) {
		try {
			StandardAnalyzer analyzer = new StandardAnalyzer();
			for (String keywordForAttributeAndRelation : queries.keySet()) {
				Collection<KeywordElement> elements = attributesAndRelations.get(keywordForAttributeAndRelation);
				if(elements != null && elements.size() != 0)
				for (KeywordElement attributeAndRelation : attributesAndRelations.get(keywordForAttributeAndRelation)) {
					QueryParser parser = new QueryParser(attributeAndRelation.getResource().getUri(), analyzer);
					for(String keywordForValueAndEntityID : queries.get(keywordForAttributeAndRelation)) {
						Query q = parser.parse(keywordForValueAndEntityID);
						if (q instanceof BooleanQuery) {
							BooleanQuery bquery = (BooleanQuery) q;
							for (BooleanClause clause : bquery.getClauses()) {
								clause.setOccur(Occur.MUST);
							}
						}
						String compound = keywordForAttributeAndRelation + SEPARATOR + keywordForValueAndEntityID;
						searchEntitiesWithClause(searcher, q, compound, null, entities);
					}	
				}
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
		
	public void searchEntitiesWithClause(IndexSearcher searcher, Query clause, String keyword, String additionalKeyword, 
			Map<KeywordElement, KeywordSegement> result) {
		try {
			Set<String> loadFieldNames = new HashSet<String>();
		    loadFieldNames.add(Constant.URI_FIELD);
		    loadFieldNames.add(Constant.TYPE_FIELD);
		    loadFieldNames.add(Constant.EXTENSION_FIELD);
		    Set<String> lazyFieldNames = new HashSet<String>();
		    lazyFieldNames.add(Constant.NEIGHBORHOOD_FIELD);
		    SetBasedFieldSelector fieldSelector = new SetBasedFieldSelector(loadFieldNames, lazyFieldNames);
			
		    float maxScore = 1;
		    ScoreDoc[] docHits = getTopDocuments(clause, MAX_KEYWORDRESULT_SIZE);
		    if(docHits.length != 0 && docHits[0] != null) {
		    	maxScore = docHits[0].score;
		    }

		   	for(int i = 0; i < docHits.length; i++) {
		   		Document doc = reader.document(docHits[i].doc, fieldSelector);
		   		float score = docHits[i].score/maxScore;
		   		if(score < ENTITY_THRESHOLD)
		   			break;
		   		String type = doc.getFieldable(Constant.TYPE_FIELD).stringValue();
		   		if(type == null) {
		   			System.err.println("type is null!");
		   			continue;
		   		}

	    		if(type.equals(TypeUtil.ENTITY)){
	    			IEntity ent = new Entity(pruneString(doc.getFieldable(Constant.URI_FIELD).stringValue()), doc.getFieldable(Constant.EXTENSION_FIELD).stringValue());
	    			KeywordElement ele = new KeywordElement(ent, KeywordElement.ENTITY, doc, score);
	    			if(result.keySet().contains(ele)) {
	    				KeywordSegement ks = result.get(ele);
	    				ks.addKeyword(keyword);
	    				if(additionalKeyword != null)
	    					ks.addKeyword(additionalKeyword);
	    			}
	    			else {
	    				KeywordSegement ks = new KeywordSegement();
	    				ks.addKeyword(keyword);
	    				if(additionalKeyword != null)
	    					ks.addKeyword(additionalKeyword);
		    			result.put(ele, ks);
	    			}
	    		}
	    	}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void searchAllAttributes(IndexSearcher searcher, Set<String> allAttributes) {
		 Term term = new Term(Constant.TYPE_FIELD, TypeUtil.ATTRIBUTE);
		 Query query = new TermQuery(term);
		 try {
			Hits hits = searcher.search(query);
			if (hits != null || hits.length() != 0)
			for (int i = 0; i < hits.length(); i++) {
				Document doc = hits.doc(i);
				allAttributes.add(doc.get(Constant.URI_FIELD));
			}
			allAttributes.add(Constant.LOCALNAME_FIELD);
			allAttributes.add(Constant.LABEL_FIELD);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void searchAllRelations(IndexSearcher searcher, Set<String> allRelations) {
		Term term = new Term(Constant.TYPE_FIELD, TypeUtil.RELATION);
		Query query = new TermQuery(term);
		try {
			Hits hits = searcher.search(query);
			if (hits != null || hits.length() != 0)
				for (int i = 0; i < hits.length(); i++) {
					Document doc = hits.doc(i);
					allRelations.add(doc.get(Constant.URI_FIELD));
				}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private String pruneString(String str) {
		return str.replaceAll("\"", "");
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
	
	public static void main(String[] args) {
		KeywordSearcher_2 searcher = new KeywordSearcher_2("D://QueryGenerator/BTC/index/aifb/keyword"); 
		
		System.out.println("******************** Input Example ********************");
		System.out.println("name:Thanh publication AIFB");
		System.out.println("******************** Input Example ********************");
		Scanner scanner = new Scanner(System.in);
		while (true) {
			System.out.println("Please input the keywords:");
			String line = scanner.nextLine();
			
//			String tokens[] = line.split(" ");
//			LinkedList<String> keywordList = new LinkedList<String>();
//			for (int i = 0; i < tokens.length; i++) {
//				keywordList.add(tokens[i]);
//			}
			
			LinkedList<String> keywordList = getKeywordList(line);
			
			searcher.searchKeywordElements(keywordList);
			
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

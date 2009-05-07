package edu.unika.aifb.keywordsearch.search;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;

import edu.unika.aifb.graphindex.util.TypeUtil;
import edu.unika.aifb.keywordsearch.Constant;
import edu.unika.aifb.keywordsearch.KeywordElement;
import edu.unika.aifb.keywordsearch.api.IAttribute;
import edu.unika.aifb.keywordsearch.api.IEntity;
import edu.unika.aifb.keywordsearch.api.INamedConcept;
import edu.unika.aifb.keywordsearch.api.IRelation;
import edu.unika.aifb.keywordsearch.impl.Attribute;
import edu.unika.aifb.keywordsearch.impl.Entity;
import edu.unika.aifb.keywordsearch.impl.NamedConcept;
import edu.unika.aifb.keywordsearch.impl.Relation;

public class KeywordSearcher {
	
	private IndexSearcher searcher;
	private Set<String> allAttributes;
	private Set<String> allRelations;
	
	private static final double ENTITY_THRESHOLD = 0.5;
	private static final double SCHEMA_THRESHOLD = 0.8;
	private static final int MAX_KEYWORDRESULT_SIZE = 5;
	
	private static final Logger log = Logger.getLogger(KeywordSearcher.class);
	
	public KeywordSearcher(String indexDir) {
		this.allAttributes = new HashSet<String>();
		this.allRelations = new HashSet<String>();
		try {
			this.searcher = new IndexSearcher(indexDir);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public Map<String,Collection<KeywordElement>> searchElements(Collection<String> queries) {
		Map<String, Collection<KeywordElement>> schemaElements = new HashMap<String, Collection<KeywordElement>>();
		Map<String, Collection<KeywordElement>> attributes = new HashMap<String, Collection<KeywordElement>>();
		Map<String, Collection<KeywordElement>> entities = new HashMap<String, Collection<KeywordElement>>();
		
		
		searchAllAttributes(searcher, allAttributes);
		searchAllRelations(searcher, allRelations);
		searchSchema(searcher, queries, schemaElements, attributes);
		searchEntitiesWithAttributes(searcher, queries, attributes, entities);
		searchEntitiesWithoutAttributes(searcher, queries, attributes, entities);
		overlapNeighborhood(entities);
		
		Set<String> keywords = new HashSet<String>();
		keywords.addAll(schemaElements.keySet());
		keywords.addAll(entities.keySet());
		Map<String, Collection<KeywordElement>> keywordElements = new HashMap<String, Collection<KeywordElement>>();
		for(String keyword : keywords) {
			Collection<KeywordElement> coll = keywordElements.get(keyword);
			if (coll == null) {
				coll = new HashSet<KeywordElement>();
				keywordElements.put(keyword, coll);
			}
			coll.addAll(schemaElements.get(keyword));
			coll.addAll(entities.get(keyword));
		}
		return keywordElements;
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
			allAttributes.add(Constant.LOCALNAME);
			allAttributes.add(Constant.LABEL);
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

	public void searchSchema(IndexSearcher searcher, Collection<String> queries, 
			Map<String, Collection<KeywordElement>> schemaElements, Map<String, Collection<KeywordElement>> attributes) {
		try {
			// search schema elements
			StandardAnalyzer analyzer = new StandardAnalyzer();
			QueryParser parser = new QueryParser(Constant.SCHEMA_FIELD, analyzer);
			for(String query : queries) {
				Query q = parser.parse(query);
				if(q instanceof BooleanQuery) {
					BooleanQuery bquery = (BooleanQuery)q;
					for(BooleanClause clause :  bquery.getClauses()) {
						clause.setOccur(Occur.MUST);
					}
				}
				String keyword = q.toString(Constant.SCHEMA_FIELD);
				Collection<KeywordElement> tmp = searchSchemaWithClause(searcher, q, keyword);
				if(tmp != null && tmp.size() != 0) {
					queries.remove(query);
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
							Collection<KeywordElement> coll = schemaElements.get(keyword);
							if(coll == null) {
								coll = new HashSet<KeywordElement>(); 
								schemaElements.put(keyword, coll);
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
			}				
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	private void searchEntitiesWithAttributes(IndexSearcher searcher, Collection<String> queries, 
			Map<String, Collection<KeywordElement>> attributes, Map<String, Collection<KeywordElement>> entities) {
		Set<String> queriesWithResults = new HashSet<String>();
		try {
			StandardAnalyzer analyzer = new StandardAnalyzer();
			for (String keyword : attributes.keySet()) {
				for (KeywordElement attribute : attributes.get(keyword)) {
					QueryParser parser = new QueryParser(attribute.getResource().getUri(), analyzer);
					for (String query : queries) {
						Query q = parser.parse(query);
						if (q instanceof BooleanQuery) {
							BooleanQuery bquery = (BooleanQuery) q;
							for (BooleanClause clause : bquery.getClauses()) {
								clause.setOccur(Occur.MUST);
							}
						}
						String compound = attribute.getQuery() + ":" + q.toString(attribute.getResource().getUri());
						Collection<KeywordElement> tmp = searchEntitiesWithClause(searcher, q, compound);
						if(tmp != null && tmp.size() != 0) {
							queriesWithResults.add(query);
							for (KeywordElement resource : tmp) {
								Collection<KeywordElement> coll = entities.get(compound);
								if (coll == null) {
									coll = new HashSet<KeywordElement>();
									entities.put(compound, coll);
								}
								coll.add(resource);
							}
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
		
	public void searchEntitiesWithoutAttributes(IndexSearcher searcher, Collection<String> queries, 
			Map<String, Collection<KeywordElement>> attributes, Map<String, Collection<KeywordElement>> entities) {
		try {
			StandardAnalyzer analyzer = new StandardAnalyzer();
			Set<String> fields = new HashSet<String>();
			fields.addAll(allAttributes);
			for (Collection<KeywordElement> coll : attributes.values()) {
				for (KeywordElement ele : coll) {
					fields.remove(ele.getResource().getUri());
				}
			}
			MyQueryParser parser = new MyQueryParser(fields.toArray(new String[fields.size()]), analyzer);
			for (String query : queries) {
				Query q = parser.parse(query);
				if (q instanceof BooleanQuery) {
					BooleanQuery bquery = (BooleanQuery) q;
					for (BooleanClause clause : bquery.getClauses()) {
						clause.setOccur(Occur.MUST);
					}
				}
				Collection<KeywordElement> tmp = searchEntitiesWithClause(searcher, q, query);
				if (tmp != null && tmp.size() != 0) {
					for (KeywordElement resource : tmp) {
						Collection<KeywordElement> coll = entities.get(query);
						if (coll == null) {
							coll = new HashSet<KeywordElement>();
							entities.put(query, coll);
						}
						coll.add(resource);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}		
	
	public Collection<KeywordElement> searchEntitiesWithClause(IndexSearcher searcher, Query clause, String keyword) {
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
				if(score >= ENTITY_THRESHOLD){
					String type = doc.get(Constant.TYPE_FIELD);
					if(type == null) {
						System.err.println("type is null!");
						continue;
					}

					if(type.equals(TypeUtil.ENTITY)){
						IEntity ent = new Entity(pruneString(doc.get(Constant.URI_FIELD)), doc.get(Constant.EXTENSION_FIELD));
						String[] reachableEntities = doc.getValues(Constant.NEIGHBORHOOD_FIELD);
						for(String str : reachableEntities) {
							ent.addReachableEntity(new Entity(str));
						}
						KeywordElement ele = new KeywordElement(ent, KeywordElement.ENTITY, score, keyword);
						result.add(ele);
					}
				}
			}				
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	private void overlapNeighborhood(Map<String, Collection<KeywordElement>> entities) {
		Map<String, Collection<KeywordElement>> entitiesToBePruned = new HashMap<String, Collection<KeywordElement>>();
		for(String keyword : entities.keySet()) {
			for(KeywordElement ele : entities.get(keyword)) {
				if (ele.getType() == KeywordElement.ENTITY && ele.getResource() instanceof IEntity) {
					IEntity entity = (IEntity)ele.getResource();
					Collection<Collection<KeywordElement>> colls = new HashSet<Collection<KeywordElement>>();
					Set<String> keywords = entities.keySet();
					keywords.remove(keyword);
					for (String key : keywords) {
						colls.add(entities.get(key));
					}
					if(!entity.isAllReachable(colls)) {
						Collection<KeywordElement> coll = entitiesToBePruned.get(keyword);
						if (coll == null) {
							coll = new HashSet<KeywordElement>();
							entitiesToBePruned.put(keyword, coll);
						}
						coll.add(ele);
					}
				}
				else {
					log.error("--------------------- ERROR! ---------------------");
					Collection<KeywordElement> coll = entitiesToBePruned.get(keyword);
					if (coll == null) {
						coll = new HashSet<KeywordElement>();
						entitiesToBePruned.put(keyword, coll);
					}
					coll.add(ele);
				}
			}
		}
		
		for(String keyword : entitiesToBePruned.keySet()) {
			entities.get(keyword).removeAll((entitiesToBePruned.get(keyword)));
			if(entities.get(keyword).size() == 0) {
				entities.remove(keyword);
			}
		}
	}

	private String pruneString(String str) {
		return str.replaceAll("\"", "");
	}

}

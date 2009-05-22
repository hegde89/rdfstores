package edu.unika.aifb.keywordsearch.search;

import it.unimi.dsi.util.BloomFilter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Scanner;
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
	
	private static final String SEPARATOR = "::";
	
	private static final Logger log = Logger.getLogger(KeywordSearcher.class);
	
	public KeywordSearcher(String indexDir) {
		this.allAttributes = new HashSet<String>();
		this.allRelations = new HashSet<String>();
		try {
			this.searcher = new IndexSearcher(indexDir);
			searchAllAttributes(searcher, allAttributes);
			searchAllRelations(searcher, allRelations);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Map<String,Collection<KeywordElement>> searchKeywordElements(Collection<String> queries) {
		Collection<String> keywordQueries = new HashSet<String>();
		Map<String, Collection<String>> keywordCompoundQueries = new HashMap<String, Collection<String>>();
		parseQueries(queries,keywordQueries, keywordCompoundQueries);
		
		Map<String, Collection<KeywordElement>> conceptsAndRelations = new HashMap<String, Collection<KeywordElement>>();
		Map<String, Collection<KeywordElement>> entitiesByKeyowrds = searchElementsByKeywords(keywordQueries, conceptsAndRelations);
		Map<String, Collection<KeywordElement>> entitiesByKeyowrdCompounds = searchElementsByKeywordCompounds(keywordCompoundQueries);
		
		Map<String, Collection<KeywordElement>> entities = new HashMap<String, Collection<KeywordElement>>();
		entities.putAll(entitiesByKeyowrds);
		entities.putAll(entitiesByKeyowrdCompounds);
		
		overlapNeighborhoods(entities);
		
		Set<String> keywords = new HashSet<String>();
		keywords.addAll(conceptsAndRelations.keySet());
		keywords.addAll(entities.keySet());
		Map<String, Collection<KeywordElement>> keywordElements = new HashMap<String, Collection<KeywordElement>>();
		for(String keyword : keywords) {
			Collection<KeywordElement> coll = keywordElements.get(keyword);
			if (coll == null) {
				coll = new HashSet<KeywordElement>();
				keywordElements.put(keyword, coll);
			}
			Collection<KeywordElement> elements = conceptsAndRelations.get(keyword);
			if(elements != null && elements.size() != 0)
				coll.addAll(elements);
			elements = entities.get(keyword);
			if(elements != null && elements.size() != 0)
				coll.addAll(elements);
		}
		return keywordElements;
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
	
	public Map<String,Collection<KeywordElement>> searchElementsByKeywords(Collection<String> queries, Map<String, Collection<KeywordElement>> conceptsAndRelations) { 
		Map<String, Collection<KeywordElement>> attributes = new HashMap<String, Collection<KeywordElement>>();
		Map<String, Collection<KeywordElement>> entities = new HashMap<String, Collection<KeywordElement>>();
		
		searchSchema(searcher, queries, conceptsAndRelations, attributes);
		if(attributes != null && attributes.size() != 0)
			searchEntitiesByAttributesAndValues(searcher, queries, attributes, entities);
		searchEntitiesByValues(searcher, queries, attributes, entities);
		
		return entities;
	}
	
	public Map<String,Collection<KeywordElement>> searchElementsByKeywordCompounds(Map<String, Collection<String>> queries) { 
		Map<String, Collection<KeywordElement>> attributesAndRelations = new HashMap<String, Collection<KeywordElement>>();
		Map<String, Collection<KeywordElement>> entities = new HashMap<String, Collection<KeywordElement>>();
		
		searchSchema(searcher, queries.keySet(), attributesAndRelations);
		if(attributesAndRelations != null && attributesAndRelations.size() != 0)
			searchEntitiesByCompounds(searcher, queries, attributesAndRelations, entities);
		
		return entities;
	}
	
	public void searchSchema(IndexSearcher searcher, Collection<String> queries, 
			Map<String, Collection<KeywordElement>> conceptsAndRelations, Map<String, Collection<KeywordElement>> attributes) {
		Set<String> queriesWithResults = new HashSet<String>();
		try {
			// search schema elements
			StandardAnalyzer analyzer = new StandardAnalyzer();
			QueryParser parser = new QueryParser(Constant.SCHEMA_FIELD, analyzer);
			QueryParser uriParser = new QueryParser(Constant.URI_FIELD, analyzer);
			for(String keyword : queries) {
				Query q;
				if(keyword.startsWith(Constant.URI_PREFIX)) {
					q = uriParser.parse(keyword);
				}
				else {		
					q = parser.parse(keyword);
				}	
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
			QueryParser uriParser = new QueryParser(Constant.URI_FIELD, analyzer);
			for(String keyword : queries) {
				Query q;
				if(keyword.startsWith(Constant.URI_PREFIX)) {
					q = uriParser.parse(keyword);
				}
				else {		
					q = parser.parse(keyword);
				}	
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
			}				
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	private void searchEntitiesByAttributesAndValues(IndexSearcher searcher, Collection<String> queries, 
			Map<String, Collection<KeywordElement>> attributes, Map<String, Collection<KeywordElement>> entities) {
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
						Collection<KeywordElement> tmp = searchEntitiesWithClause(searcher, q, keywordForValue, keywordForAttribute);
						if(tmp != null && tmp.size() != 0) {
							queriesWithResults.add(keywordForValue);
							for (KeywordElement resource : tmp) {
								Collection<KeywordElement> coll = entities.get(keywordForValue);
								if (coll == null) {
									coll = new HashSet<KeywordElement>();
									entities.put(keywordForValue, coll);
								}
								coll.add(resource);
								
								coll = entities.get(keywordForAttribute);
								if (coll == null) {
									coll = new HashSet<KeywordElement>();
									entities.put(keywordForAttribute, coll);
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
	
	public void searchEntitiesByValues(IndexSearcher searcher, Collection<String> queries, 
			Map<String, Collection<KeywordElement>> attributes, Map<String, Collection<KeywordElement>> entities) {
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
			MyQueryParser parser = new MyQueryParser(fields.toArray(new String[fields.size()]), analyzer);
			for (String keyword : queries) {
				Query q = parser.parse(keyword);
				Collection<KeywordElement> tmp = searchEntitiesWithClause(searcher, q, keyword, null);
				if (tmp != null && tmp.size() != 0) {
					for (KeywordElement resource : tmp) {
						Collection<KeywordElement> coll = entities.get(keyword);
						if (coll == null) {
							coll = new HashSet<KeywordElement>();
							entities.put(keyword, coll);
						}
						coll.add(resource);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}		
	
	private void searchEntitiesByCompounds(IndexSearcher searcher, Map<String, Collection<String>> queries, 
			Map<String, Collection<KeywordElement>> attributesAndRelations, Map<String, Collection<KeywordElement>> entities) {
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
						Collection<KeywordElement> tmp = searchEntitiesWithClause(searcher, q, compound, null);
						if(tmp != null && tmp.size() != 0) {
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
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
		
	public Collection<KeywordElement> searchEntitiesWithClause(IndexSearcher searcher, Query clause, String keyword, String additionalKeyword) {
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
						byte[] bytes = doc.getBinaryValue(Constant.NEIGHBORHOOD_FIELD);
						ByteArrayInputStream byteArrayInput = new ByteArrayInputStream(bytes);
						ObjectInputStream objectInput = new ObjectInputStream(byteArrayInput);
						BloomFilter reachableEntities = (BloomFilter)objectInput.readObject(); 
						ent.setReachaleEntities(reachableEntities);
						KeywordElement ele = new KeywordElement(ent, KeywordElement.ENTITY, score, keyword);
						if(additionalKeyword != null)
							ele.addKeyword(additionalKeyword);
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
	
	private void overlapNeighborhoods(Map<String, Collection<KeywordElement>> entities) {
		Map<String, Collection<KeywordElement>> entitiesToBeRemoved = new HashMap<String, Collection<KeywordElement>>();
		for(String keyword : entities.keySet()) {
			for(KeywordElement ele : entities.get(keyword)) {
				if (ele.getType() == KeywordElement.ENTITY && ele.getResource() instanceof IEntity) {
					IEntity entity = (IEntity)ele.getResource();
					Collection<Collection<KeywordElement>> colls = new HashSet<Collection<KeywordElement>>();
					for (String key : entities.keySet()) {
						if(!keyword.equals(key) && !keyword.contains(key))
							colls.add(entities.get(key));
					}
					if(!entity.isAllReachable(colls)) {
						Collection<KeywordElement> coll = entitiesToBeRemoved.get(keyword);
						if (coll == null) {
							coll = new HashSet<KeywordElement>();
							entitiesToBeRemoved.put(keyword, coll);
						}
						coll.add(ele);
					}
				}
				else {
					log.error("--------------------- ERROR! ---------------------");
					Collection<KeywordElement> coll = entitiesToBeRemoved.get(keyword);
					if (coll == null) {
						coll = new HashSet<KeywordElement>();
						entitiesToBeRemoved.put(keyword, coll);
					}
					coll.add(ele);
				}
			}
		}
		
		for(String keyword : entitiesToBeRemoved.keySet()) {
			entities.get(keyword).removeAll((entitiesToBeRemoved.get(keyword)));
			if(entities.get(keyword).size() == 0) {
				entities.remove(keyword);
			}
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
	
	public static void main(String[] args) {
		KeywordSearcher searcher = new KeywordSearcher("D://QueryGenerator/BTC/index/test/keyword"); 
		
		System.out.println("******************** Input Example ********************");
		System.out.println("name::Thanh publication AIFB");
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
			
			Map<String,Collection<KeywordElement>> results = searcher.searchKeywordElements(keywordList);
			
			int i = 1;
			for(String keyword : results.keySet()) {
				Collection<KeywordElement> elements = results.get(keyword);
				System.out.println("Keyword " + i++ + ": " + keyword);
				System.out.println("Elements :");
				for(KeywordElement ele : elements) {
					System.out.println(ele);
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

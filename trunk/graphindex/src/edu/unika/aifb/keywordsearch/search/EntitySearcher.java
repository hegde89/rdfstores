package edu.unika.aifb.keywordsearch.search;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;

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
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;

import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.TypeUtil;
import edu.unika.aifb.keywordsearch.Constant;
import edu.unika.aifb.keywordsearch.KeywordElement;
import edu.unika.aifb.keywordsearch.TransformedGraph;
import edu.unika.aifb.keywordsearch.TransformedGraphNode;
import edu.unika.aifb.keywordsearch.api.IEntity;
import edu.unika.aifb.keywordsearch.impl.Entity;

public class EntitySearcher {
	
	private IndexSearcher searcher;
	private IndexReader reader; 
	
	private static final double ENTITY_THRESHOLD = 0.5;
	private static final double SCHEMA_THRESHOLD = 0.8;
	private static final int MAX_KEYWORDRESULT_SIZE = 10;
	
	private static final String SEPARATOR = ":";
	
	private static final Logger log = Logger.getLogger(EntitySearcher.class);
	
	public EntitySearcher(String indexDir) {
		try {
			reader = IndexReader.open(indexDir);
			searcher = new IndexSearcher(reader);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public TransformedGraph searchEntities(TransformedGraph graph) {
		for(TransformedGraphNode node : graph.getNodes()) {
			log.debug("variable: " + node.getNodeName());
			if(node.getType() == TransformedGraphNode.ENTITY_QUERY_NODE) {
				Map<String, Collection<String>> attributeQueries = node.getAttributeQueries();
				if(attributeQueries != null && attributeQueries.keySet().size() != 0)
					node.setEntities(searchEntities(attributeQueries, node.getTypeQueries()));
			}	
			else if(node.getType() == TransformedGraphNode.ENTITY_NODE) {
				node.setEntities(searchEntities(node.getUriQuery()));
			}	
		}
		
		return graph;
	}
	
	public boolean isType(String entity, String concept) {
		TermQuery tq = new TermQuery(new Term(Constant.URI_FIELD, entity));
		try {
			Map<Integer, Float> docIdsAndScores = getDocumentIds(tq);
			Set<String> loadFieldNames = new HashSet<String>();
		    loadFieldNames.add(Constant.URI_FIELD);
		    loadFieldNames.add(Constant.TYPE_FIELD);
		    loadFieldNames.add(Constant.EXTENSION_FIELD);
		    loadFieldNames.add(Constant.CONCEPT_FIELD);
		    Set<String> lazyFieldNames = new HashSet<String>();
		    lazyFieldNames.add(Constant.NEIGHBORHOOD_FIELD);
		    SetBasedFieldSelector fieldSelector = new SetBasedFieldSelector(loadFieldNames, lazyFieldNames);
			
		    for(Integer docId : docIdsAndScores.keySet()) {
		    	Document doc = reader.document(docId, fieldSelector);
		    	String type = doc.getFieldable(Constant.TYPE_FIELD).stringValue();
				if(type == null) {
					System.err.println("type is null!");
					continue;
				}

				if(type.equals(TypeUtil.ENTITY)){
					if(doc.getFieldable(Constant.CONCEPT_FIELD).equals(concept))
						return true; 
				}
		    }
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public Collection<KeywordElement> searchEntities(Map<String, Collection<String>> attributeQueries, Collection<String> typeQueries) {
		Map<String, Collection<String>> attributes = new HashMap<String, Collection<String>>();
		Collection<String> concepts = new HashSet<String>();
		Collection<KeywordElement> entities = new HashSet<KeywordElement>();
		
		searchConcepts(searcher, typeQueries, concepts);
		searchAttributes(searcher, attributeQueries.keySet(), attributes);
		if(attributes != null && attributes.size() != 0)
			searchEntitiesByAttributeVauleCompounds(searcher, attributeQueries, attributes, concepts, entities);
		
		return entities;
	}
	
	public Collection<KeywordElement> searchEntities(String uriQuery) {
		Collection<KeywordElement> entities = new HashSet<KeywordElement>();
		
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
						Collection<String> tmp = searchAttributesWithClause(searcher, q);
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
	
	public void searchAttributes(IndexSearcher searcher, Collection<String> queries, Map<String, Collection<String>> attributes) {
		try {
			// search schema elements
			StandardAnalyzer analyzer = new StandardAnalyzer();
			for(String keyword : queries) {
				Query q;
				Collection<String> tmp = null;
				if(keyword.startsWith(Constant.URI_PREFIX)) {
					tmp = new HashSet<String>();	
					tmp.add(keyword);
				}
				else if(keyword.equals(Constant.LABEL_FIELD) || keyword.equals(Constant.LOCALNAME_FIELD) || keyword.equals(Constant.CONCEPT_FIELD)) {
					tmp = new HashSet<String>();	
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
							coll = new HashSet<String>();
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
			Map<Integer, Float> docIdsAndScores = getDocumentIds(tq);
			Set<String> loadFieldNames = new HashSet<String>();
		    loadFieldNames.add(Constant.URI_FIELD);
		    loadFieldNames.add(Constant.TYPE_FIELD);
		    loadFieldNames.add(Constant.EXTENSION_FIELD);
		    loadFieldNames.add(Constant.CONCEPT_FIELD);
		    Set<String> lazyFieldNames = new HashSet<String>();
		    lazyFieldNames.add(Constant.NEIGHBORHOOD_FIELD);
		    SetBasedFieldSelector fieldSelector = new SetBasedFieldSelector(loadFieldNames, lazyFieldNames);
			
		    for(Integer docId : docIdsAndScores.keySet()) {
		    	Document doc = reader.document(docId, fieldSelector);
		    	float score = docIdsAndScores.get(docId);
		    	String type = doc.getFieldable(Constant.TYPE_FIELD).stringValue();
				if(type == null) {
					System.err.println("type is null!");
					continue;
				}

				if(type.equals(TypeUtil.ENTITY)){
					IEntity ent = new Entity(pruneString(doc.getFieldable(Constant.URI_FIELD).stringValue()), doc.getFieldable(Constant.EXTENSION_FIELD).stringValue());
					KeywordElement ele = new KeywordElement(ent, KeywordElement.ENTITY, doc, score);
					entities.add(ele);
				}
		    }
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void searchEntitiesByAttributeVauleCompounds(IndexSearcher searcher, Map<String, Collection<String>> queries, 
			Map<String, Collection<String>> attributes, Collection<String> concepts, Collection<KeywordElement> entities) {
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
			
			searchEntitiesWithClause(searcher, entityQuery, entities); 
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Collection<KeywordElement> searchEntitiesWithClause(IndexSearcher searcher, Query query, Collection<KeywordElement> result) {
		try {
			Map<Integer, Float> docIdsAndScores = getDocumentIds(query);
			Set<String> loadFieldNames = new HashSet<String>();
		    loadFieldNames.add(Constant.URI_FIELD);
		    loadFieldNames.add(Constant.TYPE_FIELD);
		    loadFieldNames.add(Constant.EXTENSION_FIELD);
		    Set<String> lazyFieldNames = new HashSet<String>();
		    lazyFieldNames.add(Constant.NEIGHBORHOOD_FIELD);
		    SetBasedFieldSelector fieldSelector = new SetBasedFieldSelector(loadFieldNames, lazyFieldNames);
			
		    for(Integer docId : docIdsAndScores.keySet()) {
		    	Document doc = reader.document(docId, fieldSelector);
		    	float score = docIdsAndScores.get(docId);
		    	String type = doc.getFieldable(Constant.TYPE_FIELD).stringValue();
				if(type == null) {
					System.err.println("type is null!");
					continue;
				}

				if(type.equals(TypeUtil.ENTITY)){
					IEntity ent = new Entity(pruneString(doc.getFieldable(Constant.URI_FIELD).stringValue()), doc.getFieldable(Constant.EXTENSION_FIELD).stringValue());
					KeywordElement ele = new KeywordElement(ent, KeywordElement.ENTITY, doc, score);
					result.add(ele);
				}
		    }
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	public Map<Integer, Float> getDocumentIds(Query q) throws StorageException {
		final Map<Integer, Float> docIdsAndScores = new TreeMap<Integer, Float>();
		try {
			searcher.search(q, new HitCollector() {
				public void collect(int docId, float score) {
					docIdsAndScores.put(docId, score);
				}
			});
		} catch (IOException e) {
			throw new StorageException(e);
		}
		log.debug(q + ", docs: " + docIdsAndScores.size());
		
		return docIdsAndScores;
	}
	
	private String pruneString(String str) {
		return str.replaceAll("\"", "");
	}
	
	public static void main(String[] args) {
		EntitySearcher searcher = new EntitySearcher("D://QueryGenerator/BTC/index/aifb-/keyword"); 
		
		System.out.println("******************** Input Example ********************");
		System.out.println("name::Thanh publication AIFB");
		System.out.println("******************** Input Example ********************");
		Scanner scanner = new Scanner(System.in);
		while (true) {
			System.out.println("Please input the keywords:");
			String line = scanner.nextLine();
			
			if(line.trim().equals("exit")) return;
			
			LinkedList<String> keywordList = getKeywordList(line);
			Map<String, Collection<String>> map = parseQueries(keywordList);
			
			long start = System.currentTimeMillis();
			Collection<KeywordElement> results = searcher.searchEntities(map, null);
			log.info("total time: " + (System.currentTimeMillis() - start) + " milliseconds");	
			
			for(KeywordElement ele : results) {
				log.info("Elements :" + ele);
				log.info("\n");
			}
		}
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

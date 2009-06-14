package edu.unika.aifb.keywordsearch.search;

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

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SetBasedFieldSelector;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
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

import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.TypeUtil;
import edu.unika.aifb.keywordsearch.Constant;
import edu.unika.aifb.keywordsearch.KeywordElement;
import edu.unika.aifb.keywordsearch.KeywordSegement;
import edu.unika.aifb.keywordsearch.api.IAttribute;
import edu.unika.aifb.keywordsearch.api.IEntity;
import edu.unika.aifb.keywordsearch.api.INamedConcept;
import edu.unika.aifb.keywordsearch.api.IRelation;
import edu.unika.aifb.keywordsearch.impl.Attribute;
import edu.unika.aifb.keywordsearch.impl.Entity;
import edu.unika.aifb.keywordsearch.impl.NamedConcept;
import edu.unika.aifb.keywordsearch.impl.Relation;

public class KeywordSearcher {
	
	private IndexReader reader; 
	private IndexSearcher searcher;
	private Set<String> allAttributes;
	
	private static final double ENTITY_THRESHOLD = 0.8;
	private static final double SCHEMA_THRESHOLD = 0.8;
	private static final int MAX_KEYWORDRESULT_SIZE = 1000;
	
	private static final String SEPARATOR = ":";
	
	private static final Logger log = Logger.getLogger(KeywordSearcher.class);
	
	public KeywordSearcher(String indexDir) {
		this.allAttributes = new HashSet<String>();
		try {
			reader = IndexReader.open(indexDir);
			searcher = new IndexSearcher(reader);
			searchAllAttributes(allAttributes);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Map<KeywordSegement,Collection<KeywordElement>> searchKeywordElements(List<String> queries) {
		Map<String, Collection<KeywordElement>> conceptsAndRelations = new HashMap<String, Collection<KeywordElement>>();
		Map<String, Collection<KeywordElement>> attributes = new HashMap<String, Collection<KeywordElement>>();
		SortedSet<KeywordSegement> segements = parseQueries(queries, conceptsAndRelations, attributes);
		
		Map<String, Collection<KeywordElement>> keywordsWithEntities = new HashMap<String, Collection<KeywordElement>>();
		Map<KeywordElement, KeywordSegement> entitiesWithSegement = new HashMap<KeywordElement, KeywordSegement>();
		Map<KeywordSegement, Collection<KeywordElement>> segementsWithEntities = new HashMap<KeywordSegement, Collection<KeywordElement>>();  
		searchElementsByKeywords(segements, attributes, entitiesWithSegement, segementsWithEntities, keywordsWithEntities);
		
		int size = 0;
		for(Collection<KeywordElement> coll : segementsWithEntities.values()) {
			size += coll.size();
		}
		System.out.println("------------- Before NeighborhoodJoin ------------ " 
				+ "Size_of_segements:" + segementsWithEntities.size() 
				+ "   Size_of_elements:" + size);	
		for(KeywordSegement segement : segementsWithEntities.keySet()) {
			System.out.println(segement);
			for(KeywordElement ele : segementsWithEntities.get(segement))
				System.out.println(ele.getResource() + "\t" + ele.getMatchingScore());
			System.out.println();	
		}
		
		Set<String> keywords = keywordsWithEntities.keySet();
		overlapNeighborhoods(keywordsWithEntities, segementsWithEntities,  keywords);

		size = 0;
		for(Collection<KeywordElement> coll : segementsWithEntities.values()) {
			size += coll.size();
		}
		System.out.println("------------- After NeighborhoodJoin ------------ " 
				+ "Size_of_segements:" + segementsWithEntities.size() 
				+ "   Size_of_elements:" + size);
		for(KeywordSegement segement : segementsWithEntities.keySet()) {
			System.out.println(segement);
			for(KeywordElement ele : segementsWithEntities.get(segement))
				System.out.println(ele.getResource() + "\t" + ele.getMatchingScore());
			System.out.println();	
		}
		
		for(String keyword : conceptsAndRelations.keySet()) {
			segementsWithEntities.put(new KeywordSegement(keyword), conceptsAndRelations.get(keyword));
		}
		
		return segementsWithEntities;
	}
	
	public SortedSet<KeywordSegement> parseQueries(List<String> queries, Map<String, Collection<KeywordElement>> conceptsAndRelations, 
			Map<String, Collection<KeywordElement>> attributes) {
		Collection<List<String>> keywordCompounds = new ArrayList<List<String>>();  
		Map<Integer, String> locationofSchemaKeyword = new TreeMap<Integer, String>();
		searchSchema(queries, conceptsAndRelations, attributes, locationofSchemaKeyword);
		SortedSet<KeywordSegement> segements;   
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
	
	private void overlapNeighborhoods(Map<String, Collection<KeywordElement>> keywordsWithEntities,
			Map<KeywordSegement, Collection<KeywordElement>> segementsWithEntities, Collection<String> keywords) {
		
		for(String keyword : keywordsWithEntities.keySet()) {
			for(KeywordElement ele : keywordsWithEntities.get(keyword)) {
				Collection<String> reachableKeywords = ele.getReachableKeywords();
				for(String joinKey : keywords) {
					if(!reachableKeywords.contains(joinKey)) {
						Collection<KeywordElement> reachables = ele.getReachable(keywordsWithEntities.get(joinKey));	
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
		
		Iterator<KeywordSegement> iterSeg = segementsWithEntities.keySet().iterator();
		while(iterSeg.hasNext()) {
			Collection<KeywordElement> elements = segementsWithEntities.get(iterSeg.next());
			Iterator<KeywordElement> iterEle = elements.iterator();
			while(iterEle.hasNext()) {
				if(!iterEle.next().getReachableKeywords().containsAll(keywords)) {
					iterEle.remove();
				}	
			}
			if(elements.size() == 0)
				iterSeg.remove();
		}
	}
	
	public void searchElementsByKeywords(SortedSet<KeywordSegement> segements, Map<String, Collection<KeywordElement>> attributes, Map<KeywordElement, KeywordSegement> entitiesWithSegement, 
			Map<KeywordSegement, Collection<KeywordElement>> segementsWithEntities,	Map<String, Collection<KeywordElement>> keywordsWithentities) { 
		
		if(attributes != null && attributes.size() != 0)
			searchEntitiesByAttributesAndValues(segements, attributes, entitiesWithSegement, segementsWithEntities, keywordsWithentities);
		searchEntitiesByValues(segements, attributes, entitiesWithSegement, segementsWithEntities, keywordsWithentities);
		
	}
	
	public void searchElementsByKeywordCompounds(Map<String, Set<String>> queries, Map<KeywordElement, KeywordSegement> entitiesWithSegement, 
			Map<KeywordSegement, Collection<KeywordElement>> segementsWithEntities,	Map<String, Collection<KeywordElement>> keywordsWithentities) { 
		Map<String, Collection<KeywordElement>> attributesAndRelations = new HashMap<String, Collection<KeywordElement>>();
		
		searchSchema(queries.keySet(), attributesAndRelations);
		if(attributesAndRelations != null && attributesAndRelations.size() != 0)
			searchEntitiesByCompounds(queries, attributesAndRelations, entitiesWithSegement, segementsWithEntities, keywordsWithentities);
	}
	
	private Collection<String> searchSchema(List<String> queries, Map<String, Collection<KeywordElement>> conceptsAndRelations, 
			Map<String, Collection<KeywordElement>> attributes,	Map<Integer, String> locationofSchemaKeyword) {
		Set<String> queriesWithResults = new HashSet<String>();
		try {
			// search schema elements
			StandardAnalyzer analyzer = new StandardAnalyzer();
			QueryParser parser = new QueryParser(Constant.SCHEMA_FIELD, analyzer);
			parser.setDefaultOperator(QueryParser.AND_OPERATOR);
			for(int i = 0; i < queries.size(); i++) {
				String keyword = queries.get(i);
				Query q = parser.parse(keyword);
				Collection<KeywordElement> tmp = searchSchemaWithClause(q, keyword);
				if(tmp != null && tmp.size() != 0) {
					locationofSchemaKeyword.put(i, keyword);
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
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return queriesWithResults;
	}
	
	public void searchSchema(Collection<String> queries, Map<String, Collection<KeywordElement>> conceptsAndRelations, 
			Map<String, Collection<KeywordElement>> attributes) {
		Set<String> queriesWithResults = new HashSet<String>();
		try {
			// search schema elements
			StandardAnalyzer analyzer = new StandardAnalyzer();
			QueryParser parser = new QueryParser(Constant.SCHEMA_FIELD, analyzer);
			parser.setDefaultOperator(QueryParser.AND_OPERATOR);
			for(String keyword : queries) {
				Query q = parser.parse(keyword);
				Collection<KeywordElement> tmp = searchSchemaWithClause(q, keyword);
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
	
	public void searchSchema(Collection<String> queries, Map<String, Collection<KeywordElement>> attributesAndRelations) {
		try {
			// search schema elements
			StandardAnalyzer analyzer = new StandardAnalyzer();
			QueryParser parser = new QueryParser(Constant.SCHEMA_FIELD, analyzer);
			parser.setDefaultOperator(QueryParser.AND_OPERATOR);
			for(String keyword : queries) {
				Query q = parser.parse(keyword);
				Collection<KeywordElement> tmp = searchSchemaWithClause(q, keyword);
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
	
	public Collection<KeywordElement> searchSchemaWithClause(Query clause, String keyword) {
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
	
	private void searchEntitiesByAttributesAndValues(SortedSet<KeywordSegement> segements, Map<String, Collection<KeywordElement>> attributes, 
			Map<KeywordElement, KeywordSegement> entitiesWithSegement, Map<KeywordSegement, Collection<KeywordElement>> segementsWithEntities,
			Map<String, Collection<KeywordElement>> keywordsWithentities) {
		Set<KeywordSegement> segementsWithResults = new HashSet<KeywordSegement>();
		try {
			StandardAnalyzer analyzer = new StandardAnalyzer();
			for (String keywordForAttribute : attributes.keySet()) {
				for (KeywordElement attribute : attributes.get(keywordForAttribute)) {
					QueryParser parser = new QueryParser(attribute.getResource().getUri(), analyzer);
					parser.setDefaultOperator(QueryParser.AND_OPERATOR);
					for (KeywordSegement segement : segements) {
						Query q = parser.parse(segement.getQuery());
						boolean hasResults = searchEntitiesWithClause(q, segement, keywordForAttribute, entitiesWithSegement, segementsWithEntities, keywordsWithentities);
						if(hasResults)
							segementsWithResults.add(segement);
					}
				}
			}
//			segements.removeAll(segementsWithResults);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void searchEntitiesByValues(SortedSet<KeywordSegement> segements, Map<String, Collection<KeywordElement>> attributes, 
			Map<KeywordElement, KeywordSegement> entitiesWithSegement, Map<KeywordSegement, Collection<KeywordElement>> segementsWithEntities,
			Map<String, Collection<KeywordElement>> keywordsWithentities) {
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
			for (KeywordSegement segement : segements) {
				Query q = parser.parse(segement.getQuery());
				searchEntitiesWithClause(q, segement, null, entitiesWithSegement, segementsWithEntities, keywordsWithentities);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}		
	
	private void searchEntitiesByCompounds(Map<String, Set<String>> queries, Map<String, Collection<KeywordElement>> attributesAndRelations, 
			Map<KeywordElement, KeywordSegement> entitiesWithSegement, Map<KeywordSegement, Collection<KeywordElement>> segementsWithEntities,
			Map<String, Collection<KeywordElement>> keywordsWithentities) {
		try {
			StandardAnalyzer analyzer = new StandardAnalyzer();
			for (String keywordForAttributeAndRelation : queries.keySet()) {
				Collection<KeywordElement> elements = attributesAndRelations.get(keywordForAttributeAndRelation);
				if(elements != null && elements.size() != 0)
				for (KeywordElement attributeAndRelation : attributesAndRelations.get(keywordForAttributeAndRelation)) {
					QueryParser parser = new QueryParser(attributeAndRelation.getResource().getUri(), analyzer);
					parser.setDefaultOperator(QueryParser.AND_OPERATOR);
					for(String keywordForValueAndEntityID : queries.get(keywordForAttributeAndRelation)) {
						Query q = parser.parse(keywordForValueAndEntityID);
						String compound = keywordForAttributeAndRelation + SEPARATOR + keywordForValueAndEntityID;
						KeywordSegement segement = new KeywordSegement(compound);
						searchEntitiesWithClause(q, segement, null, entitiesWithSegement, segementsWithEntities, keywordsWithentities);
					}	
				}
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
		
	public boolean searchEntitiesWithClause(Query clause, KeywordSegement segement, String additionalKeyword, 
			Map<KeywordElement, KeywordSegement> entitiesWithSegement, Map<KeywordSegement, Collection<KeywordElement>> segementsWithEntities,
			Map<String, Collection<KeywordElement>> keywordsWithentities) {
		try {
			Set<String> loadFieldNames = new HashSet<String>();
		    loadFieldNames.add(Constant.URI_FIELD);
		    loadFieldNames.add(Constant.TYPE_FIELD);
		    loadFieldNames.add(Constant.EXTENSION_FIELD);
		    Set<String> lazyFieldNames = new HashSet<String>();
		    lazyFieldNames.add(Constant.NEIGHBORHOOD_FIELD);
		    SetBasedFieldSelector fieldSelector = new SetBasedFieldSelector(loadFieldNames, lazyFieldNames);
			
		    float maxScore = 1.0f;
		    ScoreDoc[] docHits = getTopDocuments(clause, MAX_KEYWORDRESULT_SIZE);
		    if(docHits.length != 0 && docHits[0] != null) {
		    	maxScore = docHits[0].score;
		    }
		    else 
		    	return false;
		    	
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
	    			KeywordSegement ks = new KeywordSegement(segement.getKeywords());
	    			if(additionalKeyword != null) 
	    				ks.addKeyword(additionalKeyword);
	    			
	    			if(!entitiesWithSegement.keySet().contains(ele)) {
	    				ele.setKeywords(ks.getKeywords());
	    				ele.addReachableKeywords(ks.getKeywords());
	    				entitiesWithSegement.put(ele, ks);
	    				Collection<KeywordElement> coll = segementsWithEntities.get(ks);
	    				if(coll == null) {
	    					coll = new HashSet<KeywordElement>();
	    					segementsWithEntities.put(ks, coll);
	    				}
	    				coll.add(ele);
	    				
	    				for(String keyword : ks.getKeywords()) {
	    					coll = keywordsWithentities.get(keyword);
	    					if(coll == null) {
		    					coll = new HashSet<KeywordElement>();
		    					keywordsWithentities.put(keyword, coll);
		    				}
		    				coll.add(ele);
	    				}
	    			}
	    			else {
	    				KeywordSegement oks = entitiesWithSegement.get(ele);
	    				if(!oks.contains(ks)) {
	    					ele.setKeywords(ks.getKeywords());
		    				ele.addReachableKeywords(ks.getKeywords());
		    				entitiesWithSegement.put(ele, ks);
		    				Collection<KeywordElement> coll = segementsWithEntities.get(ks);
		    				if(coll == null) {
		    					coll = new HashSet<KeywordElement>();
		    					segementsWithEntities.put(ks, coll);
		    				}
		    				coll.add(ele);
		    				
		    				for(String keyword : ks.getKeywords()) {
		    					coll = keywordsWithentities.get(keyword);
		    					if(coll == null) {
			    					coll = new HashSet<KeywordElement>();
			    					keywordsWithentities.put(keyword, coll);
			    				}
			    				coll.add(ele);
		    				}
	    				}
	    			}
	    		}
	    	}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}
	
	private void searchAllAttributes(Set<String> allAttributes) {
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
	
	private void searchAllRelations(Set<String> allRelations) {
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
		KeywordSearcher searcher = new KeywordSearcher("D://QueryGenerator/BTC/index/aifb/keyword"); 
		
		System.out.println("******************** Input Example ********************");
		System.out.println("name:Thanh publication AIFB");
		System.out.println("******************** Input Example ********************");
		Scanner scanner = new Scanner(System.in);
		while (true) {
			System.out.println("Please input the keywords:");
			String line = scanner.nextLine();
			
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

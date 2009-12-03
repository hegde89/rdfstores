package edu.unika.aifb.multipartquery;

import edu.unika.aifb.MappingIndex.MappingIndex;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.*;
import edu.unika.aifb.graphindex.storage.StorageException;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

public class MultiPartQuery extends StructuredQuery{

	private Map<String, StructuredQuery>  queries = new HashMap<String, StructuredQuery>();
	// Path to Mapping index
	MappingIndex midx = null;
	
	public MultiPartQuery(String name) {
		super(name);
	}
	
	public void addQuery(String datasource, StructuredQuery query) {
		queries.put(datasource, query);
	}
	
	private Map<QueryEdge, String> getMap() {
		/* FOR TESTING */
		StructuredQuery sq1 = new StructuredQuery("http://dbpedia.org/resource/Germany");
		StructuredQuery sq2 = new StructuredQuery("http://www4.wiwiss.fu-berlin.de/factbook/resource/Germany");
		StructuredQuery sq3 = new StructuredQuery("Buraimi Airport");
		
		// Build up the query
		//sq1.addEdge("?x", "http://dbpedia.org/ontology/birthplace", "http://dbpedia.org/resource/Germany");
		//sq1.addEdge("?x", "http://dbpedia.org/ontology/birthplace", "?z");
		//sq1.addEdge("?z", "http://dbpedia.org/ontology/birthplace", "?x");
		sq1.addEdge("?x", "http://dbpedia.org/ontology/governmenttype", "http://dbpedia.org/resource/Republic");
		//sq1.setAsSelect("?x");
		sq1.setAsSelect("?x");
		
		// Build up the other query
		//sq2.addEdge("?x", "http://www4.wiwiss.fu-berlin.de/factbook/ns#landboundary", "http://www4.wiwiss.fu-berlin.de/factbook/resource/Germany");
		sq2.addEdge("?x", "http://www4.wiwiss.fu-berlin.de/factbook/ns#landboundary", "http://www4.wiwiss.fu-berlin.de/factbook/resource/Oman");
		sq2.setAsSelect("?x");
		//sq2.setAsSelect("$z");
		System.out.println(sq2.getSelectVariableLabels().size());
		
		//sq3.addEdge("?x", "http://www.w3.org/2000/01/rdf-schema#label", "Buraimi Airport");
		//sq3.addEdge("?x", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://dbpedia.org/ontology/Country");
		sq3.addEdge("?x", "http://airports.dataincubator.org/schema/airport", "http://airports.dataincubator.org/airports/OY75");
		sq3.setAsSelect("?x");
		
		Map<QueryEdge, String> map = new HashMap<QueryEdge, String>();
		map.put(sq1.getQueryGraph().edgeSet().iterator().next(), "C:\\Users\\Christoph\\Desktop\\AIFB\\dbpedia\\index");
		map.put(sq2.getQueryGraph().edgeSet().iterator().next(), "C:\\Users\\Christoph\\Desktop\\AIFB\\factbook\\index");
		map.put(sq3.getQueryGraph().edgeSet().iterator().next(), "C:\\Users\\Christoph\\Desktop\\AIFB\\dibbaAirport\\index");
		return map;
	}
	
	public Map<String, StructuredQuery> getDatasetQueries() {
		Map<QueryEdge, String> map = getMap();
		
		// Build subqueries for each dataset
		Map<String, StructuredQuery> dsQuery = new HashMap<String, StructuredQuery>();
		
		for (Iterator<Entry<QueryEdge, String>> it = map.entrySet().iterator();it.hasNext();){
			Entry<QueryEdge, String> e = it.next();
			
			if (dsQuery.containsKey(e.getValue())) {
				dsQuery.get(e.getValue()).addEdge(e.getKey().getSource(), e.getKey().getProperty(), e.getKey().getTarget());
			} else {
				StructuredQuery sq = new StructuredQuery(e.getValue());
				sq.addEdge(e.getKey().getSource(), e.getKey().getProperty(), e.getKey().getTarget());
				dsQuery.put(e.getValue(), sq);
			}
		}
		return dsQuery;
	}
	
	public MappingIndex getMappingIndex () {
		return midx;
	}
	
	public void setMappingIndex (String path) {
		try {
			midx = new MappingIndex(path, new IndexReader(new IndexDirectory(path)).getIndexConfiguration());
		} catch (IOException e) {
			e.printStackTrace();
		} catch (StorageException e) {
			e.printStackTrace();
		}
	}

}

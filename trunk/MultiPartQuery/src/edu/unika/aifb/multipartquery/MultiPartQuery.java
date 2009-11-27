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
	
	public Map<QueryEdge, String> getMap() {
		/* FOR TESTING */
		StructuredQuery sq1 = new StructuredQuery("http://dbpedia.org/resource/Germany");
		StructuredQuery sq2 = new StructuredQuery("http://www4.wiwiss.fu-berlin.de/factbook/resource/Germany");
		
		// Build up the query
		sq1.addEdge("?x", "http://dbpedia.org/ontology/birthplace", "http://dbpedia.org/resource/Germany");
		sq1.setAsSelect("?x");
		
		// Build up the other query
		sq2.addEdge("?x", "http://www4.wiwiss.fu-berlin.de/factbook/ns#landboundary", "http://www4.wiwiss.fu-berlin.de/factbook/resource/Germany");
		sq2.setAsSelect("?x");
		Map<QueryEdge, String> map = new HashMap<QueryEdge, String>();
		map.put(sq1.getQueryGraph().edgeSet().iterator().next(), "C:\\Users\\Christoph\\Desktop\\AIFB\\dbpedia\\index");
		map.put(sq2.getQueryGraph().edgeSet().iterator().next(), "C:\\Users\\Christoph\\Desktop\\AIFB\\factbook\\index");
		return map;
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

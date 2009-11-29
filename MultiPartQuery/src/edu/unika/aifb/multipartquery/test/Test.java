package edu.unika.aifb.multipartquery.test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.QNode;
import edu.unika.aifb.graphindex.query.QueryEdge;
import edu.unika.aifb.graphindex.query.QueryGraph;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.structured.VPEvaluator;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.multipartquery.MultiPartQuery;
import edu.unika.aifb.multipartquery.MultiPartQueryEvaluator;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// Set the name of the query
		MultiPartQuery mpq = new MultiPartQuery("TestQuery");
		// One query for each data source, the name of the query is the entity of the mapping index
		// and the object of the data source index.
		StructuredQuery sq1 = new StructuredQuery("http://dbpedia.org/resource/Germany");
		StructuredQuery sq2 = new StructuredQuery("http://www4.wiwiss.fu-berlin.de/factbook/resource/Germany");
		
		// Build up the query
		sq1.addEdge("?x", "http://dbpedia.org/ontology/birthplace", "http://dbpedia.org/resource/Germany");
		sq1.setAsSelect("?x");
		
		// Build up the other query
		sq2.addEdge("?x", "http://www4.wiwiss.fu-berlin.de/factbook/ns#landboundary", "http://www4.wiwiss.fu-berlin.de/factbook/resource/Germany");
		sq2.setAsSelect("?x");
		
		// Add both queries to the multipartquery
		/*mpq.addQuery("C:\\Users\\Christoph\\Desktop\\AIFB\\dbpedia\\index", sq1);
		mpq.addQuery("C:\\Users\\Christoph\\Desktop\\AIFB\\factbook\\index", sq2);
		// Set path for the mapping index*/
		mpq.setMappingIndex("C:\\Users\\Christoph\\Desktop\\AIFB\\Mappings");
		
		Map<String, IndexReader> idxReaders = new HashMap<String, IndexReader>();
		
		try {
			idxReaders.put("C:\\Users\\Christoph\\Desktop\\AIFB\\dbpedia\\index", new IndexReader(new IndexDirectory("C:\\Users\\Christoph\\Desktop\\AIFB\\dbpedia\\index")));
			idxReaders.put("C:\\Users\\Christoph\\Desktop\\AIFB\\factbook\\index", new IndexReader(new IndexDirectory("C:\\Users\\Christoph\\Desktop\\AIFB\\factbook\\index")));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Execute the query
		MultiPartQueryEvaluator meq = new MultiPartQueryEvaluator(idxReaders);
		Table<String> result = meq.evaluate(mpq);
		//meq.evaluate();
		System.out.println(result.toDataString());


	}

}

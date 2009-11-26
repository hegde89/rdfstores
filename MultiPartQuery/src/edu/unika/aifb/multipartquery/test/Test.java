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
		// Set path for the mapping index
		mpq.setMappingIndex("C:\\Users\\Christoph\\Desktop\\AIFB\\Mappings");
		
		// Execute the query
		MultiPartQueryEvaluator meq = new MultiPartQueryEvaluator(mpq);
		Table<String> result = meq.evaluate();*/
		
		Map<QueryEdge, String> map = new HashMap<QueryEdge, String>();
		map.put(sq1.getQueryGraph().edgeSet().iterator().next(), "C:\\Users\\Christoph\\Desktop\\AIFB\\dbpedia\\index");
		map.put(sq2.getQueryGraph().edgeSet().iterator().next(), "C:\\Users\\Christoph\\Desktop\\AIFB\\factbook\\index");
		
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
		
		// Subquery execution
		Map<String, Table<String>> result = new HashMap<String, Table<String>>();
		
		for (Iterator<Entry<String, StructuredQuery>> it = dsQuery.entrySet().iterator();it.hasNext();) {
			Entry<String, StructuredQuery> e = it.next();
			
			// Run the query
			try {
				IndexReader indexReader = new IndexReader(new IndexDirectory(e.getKey()));
				VPEvaluator qe = new VPEvaluator(indexReader);
				result.put(e.getKey(), qe.evaluate(e.getValue()));
				
			} catch (StorageException e1) {
				e1.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
		
		// Iterate the map
		for (Iterator<Entry<String, StructuredQuery>> it1 = dsQuery.entrySet().iterator();it1.hasNext();){
			Entry<String, StructuredQuery> e1 = it1.next();
			
			// Iterate the map to get the other entries
			for (Iterator<Entry<String, StructuredQuery>> it2 = dsQuery.entrySet().iterator();it2.hasNext();){
				Entry<String, StructuredQuery> e2 = it2.next();
				
				// Only different entries are interesting
				if (!e1.getKey().equals(e2.getKey())) {
					for (Iterator<QNode> node1_it = e1.getValue().getQueryGraph().vertexSet().iterator();node1_it.hasNext();) {
						
						QNode n1 = node1_it.next();
						
						// Iterate nodes
						for (Iterator<QNode> node2_it = e2.getValue().getQueryGraph().vertexSet().iterator();node2_it.hasNext();) {
							QNode n2 = node2_it.next();
							
							if (n1.equals(n2)) {
								//TODO: Join the result of these two subqueries
								// join(e1.getKey(), e2.getKey, n1);
								System.out.println("Found node " + n1.getLabel() + " in subquerie for " + e1.getKey() + " and " + e2.getKey());
							}
						}
					}
				}
				
			}
		
		}

	}

}

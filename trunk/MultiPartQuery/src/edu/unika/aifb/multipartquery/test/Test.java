package edu.unika.aifb.multipartquery.test;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
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
import edu.unika.aifb.multipartquery.MultiPartQueryResolver;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Map<String, List<String>> dsProperties = new HashMap<String, List<String>>();
//		String[] datasets = new String[3];
//		datasets[0] = "C:\\Users\\Christoph\\Desktop\\AIFB\\dbpedia\\index";
//		datasets[1] = "C:\\Users\\Christoph\\Desktop\\AIFB\\factbook\\index";
//		datasets[2] = "C:\\Users\\Christoph\\Desktop\\AIFB\\dibbaAirport\\index";
		String[] datasets = new String[3];
		datasets[0] = "C:\\Users\\Christoph\\Desktop\\AIFB\\Example\\index";
		datasets[1] = "C:\\Users\\Christoph\\Desktop\\AIFB\\Kuehlschrank\\index";
		datasets[2] = "C:\\Users\\Christoph\\Desktop\\AIFB\\Arbeitgeber\\index";
		
		       
		try{
			//System.out.println("Number of properties read per dataset:");
//			for (int i=0; i<3; i++) {
			for (int i=0; i<3; i++) {
				FileInputStream fstream = new FileInputStream(datasets[i] + "\\properties");
				DataInputStream in = new DataInputStream(fstream);
				BufferedReader br = new BufferedReader(new InputStreamReader(in));
				
				List<String> properties = new LinkedList<String>();
				String strLine;
				
				while ((strLine = br.readLine()) != null)   {
					properties.add(strLine);
				}
				dsProperties.put(datasets[i], properties);
				//System.out.println(datasets[i] + ": " + properties.size());
				in.close();
			}
			
		    
	    }catch (Exception e){
	    	System.err.println("Error: " + e.getMessage());
	    }
		
//	    StructuredQuery sq1 = new StructuredQuery("");
//		sq1.addEdge("?x", "http://dbpedia.org/ontology/governmenttype", "http://dbpedia.org/resource/Republic");
//		sq1.addEdge("?x", "http://www4.wiwiss.fu-berlin.de/factbook/ns#landboundary", "http://www4.wiwiss.fu-berlin.de/factbook/resource/Oman");
//		sq1.addEdge("?x", "http://airports.dataincubator.org/schema/airport", "http://airports.dataincubator.org/airports/OY75");
//		sq1.setAsSelect("?x");
	    
		StructuredQuery sq = new StructuredQuery("");
		sq.addEdge("?buch", "http://example.org/VerlegtBei", "http://springer.com/Verlag");
		sq.addEdge("?buch", "http://example.org/Titel", "?titel");
		sq.addEdge("?buch", "http://example.org/Autor", "?autor");
		sq.addEdge("?autor", "http://example.org/ArbeitetBei", "?institut");
		sq.addEdge("?getraenk", "http://kuehlschrank.org/vorraetig", "Ja");
		sq.addEdge("?autor", "http://kuehlschrank.org/trinkt", "?getraenk");
		sq.addEdge("?getraenk", "http://kuehlschrank.org/kostet", "?preis");
		sq.addEdge("?institut", "http://arbeitgeber.info/ist", "http://arbeitgeber.info/Institut");
		sq.addEdge("?institut", "http://arbeitgeber.info/PartnerVon", "?partner");
		sq.setAsSelect("?titel");
		sq.setAsSelect("?autor");
		sq.setAsSelect("?institut");
		sq.setAsSelect("?getraenk");
		sq.setAsSelect("?preis");
		sq.setAsSelect("?partner");
		
		// Set the name of the query
//		MultiPartQuery mpq = new MultiPartQuery("TestQuery", dsProperties, sq1);
		MultiPartQuery mpq = new MultiPartQuery("TestQuery", dsProperties, sq);

		// Set path for the mapping index
		mpq.setMappingIndex("C:\\Users\\Christoph\\Desktop\\AIFB\\Mappings\\Example");
		
		Map<String, IndexReader> idxReaders = new HashMap<String, IndexReader>();
		
		try {
//			idxReaders.put("C:\\Users\\Christoph\\Desktop\\AIFB\\dbpedia\\index", new IndexReader(new IndexDirectory("C:\\Users\\Christoph\\Desktop\\AIFB\\dbpedia\\index")));
//			idxReaders.put("C:\\Users\\Christoph\\Desktop\\AIFB\\factbook\\index", new IndexReader(new IndexDirectory("C:\\Users\\Christoph\\Desktop\\AIFB\\factbook\\index")));
//			idxReaders.put("C:\\Users\\Christoph\\Desktop\\AIFB\\dibbaAirport\\index", new IndexReader(new IndexDirectory("C:\\Users\\Christoph\\Desktop\\AIFB\\dibbaAirport\\index")));
			idxReaders.put("C:\\Users\\Christoph\\Desktop\\AIFB\\Example\\index", new IndexReader(new IndexDirectory("C:\\Users\\Christoph\\Desktop\\AIFB\\Example\\index")));
			idxReaders.put("C:\\Users\\Christoph\\Desktop\\AIFB\\Kuehlschrank\\index", new IndexReader(new IndexDirectory("C:\\Users\\Christoph\\Desktop\\AIFB\\Kuehlschrank\\index")));
			idxReaders.put("C:\\Users\\Christoph\\Desktop\\AIFB\\Arbeitgeber\\index", new IndexReader(new IndexDirectory("C:\\Users\\Christoph\\Desktop\\AIFB\\Arbeitgeber\\index")));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Execute the query
		MultiPartQueryEvaluator meq = new MultiPartQueryEvaluator(idxReaders);
		Table<String> result = meq.evaluate(mpq);
		System.out.println("\n" + result.toDataString());
	}

}

package edu.unika.aifb.multipartquery;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.structured.QueryEvaluator;
import edu.unika.aifb.graphindex.searcher.structured.VPEvaluator;
import edu.unika.aifb.graphindex.storage.StorageException;

public class MultiPartQueryEvaluator {
	
	private MultiPartQuery querie;
	private Table<String> result[];
	private int size = 0;

	public MultiPartQueryEvaluator (MultiPartQuery mpquery) {
		querie = mpquery;
		// Amount of structured queries
		size = querie.getMap().size();
		result = new Table[size];
	}
	
	//public Table<String> evaluate() throws IOException, StorageException {
	public void evaluate() {
		int i = 0;
		
		try {
			for(Iterator<Entry<String, StructuredQuery>> it = querie.getMap().entrySet().iterator(); it.hasNext();) {
			// Get key-value mapping
			Entry<String, StructuredQuery> e = it.next();
			// Get index reader for directory stored as key
			IndexReader indexReader = new IndexReader(new IndexDirectory(e.getKey()));
			System.out.print(indexReader.getDataIndex().getTriples(null, "http://www4.wiwiss.fu-berlin.de/factbook/ns#landboundary", "http://www4.wiwiss.fu-berlin.de/factbook/resource/Argentina").toDataString());
			
			VPEvaluator qe = new VPEvaluator(indexReader);
			//QueryEvaluator qe = new QueryEvaluator(indexReader);
			// Evaluate query stored as value
			result[i] = qe.evaluate(e.getValue());
			result[i].toDataString();
			System.out.println("DEBUG: " + result[i].getRow(0).length);
			System.out.println("1: " + (result[i].getRow(0))[0] + " | " + (result[i].getRow(0))[1]);
			i++;
			}
		} catch (StorageException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
}

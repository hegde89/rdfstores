package edu.unika.aifb.multipartquery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import edu.unika.aifb.MappingIndex.MappingIndex;
import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.data.Tables;
import edu.unika.aifb.graphindex.data.Tables.JoinedRowValidator;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.structured.QueryEvaluator;
import edu.unika.aifb.graphindex.searcher.structured.VPEvaluator;
import edu.unika.aifb.graphindex.storage.StorageException;

public class MultiPartQueryEvaluator {
	
	private MultiPartQuery query;
	private Table<String> result[];
	private int size = 0;

	public MultiPartQueryEvaluator (MultiPartQuery mpquery) {
		query = mpquery;
		// Amount of structured queries
		size = query.getMap().size();
		result = new Table[size];
	}
	
	//public Table<String> evaluate() throws IOException, StorageException {
	public void evaluate() {
		int i = 0;
		String indexDirectory = query.getMIDXPath();
		Table<String> result1, result2;
		String ds1,ds2, e1;
		
		try {
			for(Iterator<Entry<String, StructuredQuery>> it = query.getMap().entrySet().iterator(); it.hasNext();) {
			// Get key-value mapping
			Entry<String, StructuredQuery> e = it.next();
			// Get index reader for directory stored as key
			IndexReader indexReader = new IndexReader(new IndexDirectory(e.getKey()));

			ds1 = e.getKey();

			//System.out.print(indexReader.getDataIndex().getTriples(null, "http://www4.wiwiss.fu-berlin.de/factbook/ns#landboundary", "http://www4.wiwiss.fu-berlin.de/factbook/resource/Argentina").toDataString());
			//System.out.print(indexReader.getDataIndex().getTriples(null, "http://dbpedia.org/ontology/birthplace", "http://dbpedia.org/resource/Germany").toDataString());
			
			VPEvaluator qe = new VPEvaluator(indexReader);
			//QueryEvaluator qe = new QueryEvaluator(indexReader);
			// Evaluate query stored as value
			result1 = qe.evaluate(e.getValue());
			result1.setColumnName(1, "y");
			
			e1 = result1.getRow(0)[1];
			
			e = it.next();
			indexReader = new IndexReader(new IndexDirectory(e.getKey()));
			ds2 = e.getKey();
			qe = new VPEvaluator(indexReader);
			
			result2 = qe.evaluate(e.getValue());
			result2.setColumnName(1, "x");
			//System.out.println(result[i].toDataString());
			
			MappingIndex midx = new MappingIndex(indexDirectory, new IndexReader(new IndexDirectory(indexDirectory)).getIndexConfiguration());
			Table<String> mtable = midx.getStoTMapping(ds1, ds2, e1);
			mtable.setColumnName(0, "y");
			mtable.setColumnName(1, "x");
			
			List<String> cols = new ArrayList<String>();
			cols.add("y");
			
			Table<String> joinresult = Tables.mergeJoin(result1, mtable, cols);
			
			cols = new ArrayList<String>();
			cols.add("x");
			
			joinresult.setColumnName(2, "x");
			joinresult = Tables.mergeJoin(result2, joinresult, cols);
			
			System.out.println(joinresult.toDataString());
			
			/*for (int j=0; j<result[i].rowCount(); j++) {
				//System.out.println("DEBUG: " + result[i].getRow(j).length);
				System.out.println(j + ": " + (result[i].getRow(j))[0] + " | " + (result[i].getRow(j))[1]);
			}*/
			
			//i++;
			}
			
			
			
			//MappingIndex midx = new MappingIndex(indexDirectory, new IndexReader(new IndexDirectory(indexDirectory)).getIndexConfiguration());
			//Table<String> mtable = midx.getStoTMapping("C:\\Users\\Christoph\\Desktop\\AIFB\\dbpedia\\index", "C:\\Users\\Christoph\\Desktop\\AIFB\\factbook\\index", "http://dbpedia.org/resource/Germany");
			//mtable.setColumnName(0, "y");
			//mtable.setColumnName(1, "x");
			
			//for (int j=0; j<mtable.rowCount(); j++) {
			//	System.out.println(j + ": " + (mtable.getRow(j))[0] + " | " + (mtable.getRow(j))[1]);
			//}
			
			/*for (int j=0; j<result[1].rowCount(); j++) {
				result[1].getRow(j)[1] = mtable.getRow(0)[0];
			}*/
			
			/*result[0].setColumnName(0, "person");
			result[0].setColumnName(1, "y");
			result[1].setColumnName(0, "landboundary");
			result[1].setColumnName(1, "x");*/
			
			//List<String> cols = new ArrayList<String>();
			//cols.add("y");

			//Table<String> joinresult = Tables.mergeJoin(result[0], result[1], cols);
			/*Table<String> joinresult = Tables.mergeJoin(result[0], mtable, cols);
			cols = new ArrayList<String>();
			
			cols.add("x");
			joinresult.setColumnName(2, "x");
			joinresult = Tables.mergeJoin(result[1], joinresult, cols);
			
			System.out.println("Row length: " + joinresult.getRow(1).length);
			
			for (int j=0; j<joinresult.rowCount(); j++) {
				System.out.print("\n" + j + " ");
				for (int k=0; k<joinresult.getRow(j).length; k++) {
					System.out.print(joinresult.getRow(j)[k] + " ");
				}
			}*/
			
		} catch (StorageException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
}

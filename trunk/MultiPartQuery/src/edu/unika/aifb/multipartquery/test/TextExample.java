package edu.unika.aifb.multipartquery.test;

import java.io.IOException;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.index.IndexReader;
import edu.unika.aifb.graphindex.query.StructuredQuery;
import edu.unika.aifb.graphindex.searcher.structured.VPEvaluator;
import edu.unika.aifb.graphindex.storage.StorageException;

public class TextExample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		StructuredQuery sq = new StructuredQuery("");
		sq.addEdge("?buch", "http://example.org/VerlegtBei", "http://springer.com/Verlag");
		sq.addEdge("?buch", "http://example.org/Titel", "?titel");
		sq.addEdge("?buch", "http://example.org/Autor", "?autor");
		sq.addEdge("?autor", "http://example.org/ArbeitetBei", "?institut");
		sq.addEdge("?autor", "http://kuehlschrank.org/trinkt", "?getraenk");
		sq.setAsSelect("?titel");
		sq.setAsSelect("?autor");
		sq.setAsSelect("?institut");
		sq.setAsSelect("?getraenk");
		
		try {
			VPEvaluator qe = new VPEvaluator(new IndexReader(new IndexDirectory("C:\\Users\\Christoph\\Desktop\\AIFB\\Example\\index")));
			Table<String> result = qe.evaluate(sq);
			System.out.println(result.toDataString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (StorageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}

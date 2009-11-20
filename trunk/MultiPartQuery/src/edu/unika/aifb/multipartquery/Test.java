package edu.unika.aifb.multipartquery;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.query.StructuredQuery;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		MultiPartQuery mpq = new MultiPartQuery("TestQuery");
		StructuredQuery sq1 = new StructuredQuery("http://dbpedia.org/resource/Germany");
		StructuredQuery sq2 = new StructuredQuery("http://www4.wiwiss.fu-berlin.de/factbook/resource/Germany");

		sq1.addEdge("?y", "http://dbpedia.org/ontology/birthplace", "http://dbpedia.org/resource/Germany");
		sq1.setAsSelect("?y");

		sq2.addEdge("?x", "http://www4.wiwiss.fu-berlin.de/factbook/ns#landboundary", "http://www4.wiwiss.fu-berlin.de/factbook/resource/Germany");
		sq2.setAsSelect("?x");

		mpq.addQuery("C:\\Users\\Christoph\\Desktop\\AIFB\\dbpedia\\index", sq1);
		mpq.addQuery("C:\\Users\\Christoph\\Desktop\\AIFB\\factbook\\index", sq2);
		mpq.setMappingIndex("C:\\Users\\Christoph\\Desktop\\AIFB\\Mappings");
		
		MultiPartQueryEvaluator meq = new MultiPartQueryEvaluator(mpq);
		Table<String> result = meq.evaluate();
	}

}

package edu.unika.aifb.multipartquery;

import edu.unika.aifb.graphindex.query.StructuredQuery;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		MultiPartQuery mpq = new MultiPartQuery("TestQuery");
		StructuredQuery sq1 = new StructuredQuery("test1");
		StructuredQuery sq2 = new StructuredQuery("test2");
		
		//sq1.addEdge("?x", "dbpedia:birthplace", "?y");
		//sq1.addEdge("http://dbpedia.org/resource/Without", "http://dbpedia.org/ontology/aSide", "?x");
		//sq1.addEdge("?x", "http://dbpedia.org/ontology/aSide", "Without");
		//sq1.addEdge("?y", "http://dbpedia.org/ontology/birthplace", "Germany");
		sq1.addEdge("?y", "http://dbpedia.org/ontology/birthplace", "http://dbpedia.org/resource/Germany");
		sq1.setAsSelect("?y");

		// 20 Ergebnisse...
		//sq2.addEdge("?x", "http://www4.wiwiss.fu-berlin.de/factbook/ns#landboundary", "http://www4.wiwiss.fu-berlin.de/factbook/resource/Argentina");
		sq2.addEdge("?x", "http://www4.wiwiss.fu-berlin.de/factbook/ns#landboundary", "http://www4.wiwiss.fu-berlin.de/factbook/resource/Germany");
		sq2.setAsSelect("?x");
		//				   http://www4.wiwiss.fu-berlin.de/factbook/ns#landboundary    http://www4.wiwiss.fu-berlin.de/factbook/resource/Argentina
		/*http://www4.wiwiss.fu-berlin.de/factbook/ns#landboundary
		http://www4.wiwiss.fu-berlin.de/factbook/resource/Brazil 
		http://www4.wiwiss.fu-berlin.de/factbook/resource/Argentina*/
		
		//sq1.addEdge("?x", "http://dbpedia.org/property/country", "Germany");
		//sq2.addEdge("?y", "factbook:landboundary", "?z");
		mpq.addQuery("C:\\Users\\Christoph\\Desktop\\AIFB\\dbpedia\\index", sq1);
		mpq.addQuery("C:\\Users\\Christoph\\Desktop\\AIFB\\factbook\\index", sq2);
		
		MultiPartQueryEvaluator meq = new MultiPartQueryEvaluator(mpq);
		meq.evaluate();
	}

}

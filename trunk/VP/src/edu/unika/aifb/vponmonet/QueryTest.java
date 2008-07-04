package edu.unika.aifb.vponmonet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class QueryTest {
	private static Properties m_props;
	
	private static String loadQuery(File queryFile) throws IOException {
		BufferedReader in = new BufferedReader(new FileReader(queryFile));
		String query = "";
		String input;
		while ((input = in.readLine()) != null)
			query += input + "\n";
		return query;
	}
	
	public static void main(String[] args) throws SQLException, ClassNotFoundException, ImportException, FileNotFoundException, IOException {
		m_props = new Properties();
		m_props.load(new FileInputStream("config.properties"));
		
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Class.forName("net.sf.log4jdbc.DriverSpy");
		Connection conn = DriverManager.getConnection("jdbc:log4jdbc:monetdb://" + m_props.getProperty("db_host") + "/" + m_props.getProperty("db_name"), 
				m_props.getProperty("db_user"), m_props.getProperty("db_password"));

		OntologyMapping ontoMap = new OntologyMapping();
		ontoMap.loadFromDB(conn);
		
		String sparql = "SELECT ?x ?y WHERE { ?x rdf:type <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor>. ?x <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf> ?y . }";
		
		String lubm_q1 = "PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>\n" + 
			"SELECT ?x WHERE { ?x rdf:type ub:GraduateStudent . ?x ub:takesCourse <http://www.Department0.University0.edu/GraduateCourse0>}";

		String lubm_q2a = "PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>\n" +
			"SELECT ?x ?y ?z WHERE { " +
			"?x rdf:type ub:GraduateStudent . " +
			"?y rdf:type ub:University . " +
			"?z rdf:type ub:Department . " + 
			"?x ub:memberOf ?z . " +
			"?z ub:subOrganizationOf ?y . " +
			"?x ub:undergraduateDegreeFrom ?y . " + 
			"}";

		String lubm_q2 = "PREFIX ub: <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>\n" +
			"SELECT ?x ?y ?z WHERE { " +
			"?x ub:memberOf ?z . ?x rdf:type ub:GraduateStudent . " +
			"?y rdf:type ub:University . " +
			"?z rdf:type ub:Department . " + 
			"?z ub:subOrganizationOf ?y . ?x ub:undergraduateDegreeFrom ?y . " +
			"}";
		
		String dbpedia1 = "SELECT ?x ?y WHERE {" +
			"?x <http://dbpedia.org/property/postalcodetype> ?y ." +
			"}";
		
		String dbpedia2 = "SELECT ?x  WHERE {" +
			"?x <http://dbpedia.org/property/type> \"studio\" ." +
			"}";
		
		String dbpedia3 = "SELECT ?x ?y WHERE {" +
			"?x <http://dbpedia.org/property/ushrProperty> ?y ." +
			"}";
		
		String yago1 = "PREFIX yago: <http://www.mpii.de/yago/resource/>\n" +
			"SELECT ?x ?y ?z WHERE {" +
			"?x yago:actedIn ?y . " +
			"?z yago:created ?y . " +
			"}";
		
		String yago_meta = "SELECT t1.subject, t1.object FROM http___www_mpii_de_yago_resource_actedin AS t1";//, http___www_mpii_de_yago_resource_confidence AS t2, __uri_hashes AS t3, __uri_hashes AS t4, __uri_hashes AS t5";// WHERE " +
//			"t1.id = t2.subject"; 
//			"t1.subject = t3.subject AND t2.object = t4.subject AND t1.object = t5.subject";
		
		String evalq1 = "PREFIX	 lubm: <http://www.example.com/#>\n "+
			"PREFIX	rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n "+
			"SELECT	?x "+
			"WHERE"+
			"{ "+
				"?x	lubm:advisor ?y . "+
				"?y lubm:worksFor ?z . "+
				"?a lubm:memberOf ?z . "+
//				"?b lubm:publicationAuthor ?a . "+
				"<http://www.Department12.University0.edu/AssociateProfessor3/Publication4> lubm:publicationAuthor ?a . "+
			"}";
		String evalq15 = "PREFIX	lubm:	<http://www.example.com/#>" + 
			"PREFIX	rdf:	<http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
			"SELECT	?x ?y WHERE " +
			"{" +
				"?x	lubm:advisor ?y . " +
				"?y	lubm:telephone 'xxx-xxx-xxxx' . " +
				"?x	lubm:memberOf ?z . " +
				"?z	lubm:name 'Department12' . " +
				"?a	lubm:publicationAuthor ?y . " +
				"?a	lubm:name 'Publication0' . " +
			"}";
		
		String lehigh1 = "PREFIX lubm: <http://www.example.com/#>\n" + 
			"PREFIX	rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
			"SELECT	?x WHERE " + 
			"{ " +  
			"<http://www.Department5.University0.edu/AssistantProfessor7> rdf:type ?x . " + 
			"}";
		String lehigh13 = "PREFIX lubm: <http://www.example.com/#>\n" + 
			"PREFIX	rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
			"SELECT	?x WHERE { " + 
			"?x	lubm:teacherOf ?y . " +
			"?z	lubm:takesCourse ?y . " +
			"?z	rdf:type lubm:GraduateStudent . " +
			"<http://www.Department5.University0.edu/GraduateStudent71>	lubm:takesCourse ?y . " +
			"}";
		
		SPARQLQueryTranslator sqt = new SPARQLQueryTranslator(ontoMap);
		sqt.setDbConnection(conn);
		
		for (File f : new File("/Users/gl/Studium/diplomarbeit/evaluation/query/").listFiles()) {
			if (f.getName().startsWith("."))
				continue;
			String q = loadQuery(f);
			
			System.out.print(f + ": ");

			PreparedStatement pst = sqt.translateQuery(q);
			pst.execute();
			int j = 0;
			do {
				ResultSet rst = pst.getResultSet();
				while (rst.next()) {
//					for (int i = 1; i <= rst.getMetaData().getColumnCount(); i++)
//						System.out.print(rst.getObject(i) + " ");
//					System.out.println();
					j++;
				}
//				System.out.println();
			}
			while (pst.getMoreResults());
			System.out.println(j);
			
		}
		System.exit(-1);
//		PreparedStatement pst = sqt.translateQuery(loadQuery(new File("/Users/gl/Studium/diplomarbeit/evaluation china/query/query10.q")));
////		PreparedStatement pst = conn.prepareStatement("SELECT COUNT(*) FROM __uri_hashes");
////		PreparedStatement pst = conn.prepareStatement(evale1q);
//		pst.execute();
//		int j = 0;
//		do {
//			ResultSet rst = pst.getResultSet();
//			while (rst.next()) {
////				for (int i = 1; i <= rst.getMetaData().getColumnCount(); i++)
////					System.out.print(rst.getObject(i) + " ");
////				System.out.println();
//				j++;
//			}
////			System.out.println();
//		}
//		while (pst.getMoreResults());
//		System.out.println(j);
	}

}

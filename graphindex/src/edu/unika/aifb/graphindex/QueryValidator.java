package edu.unika.aifb.graphindex;

import java.io.File;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.semanticweb.kaon2.api.DefaultOntologyResolver;
import org.semanticweb.kaon2.api.KAON2Exception;
import org.semanticweb.kaon2.api.KAON2Manager;
import org.semanticweb.kaon2.api.Namespaces;
import org.semanticweb.kaon2.api.Ontology;
import org.semanticweb.kaon2.api.OntologyManager;
import org.semanticweb.kaon2.api.reasoner.Reasoner;
import org.semanticweb.kaon2.reasoner.sparql.ParseException;
import org.semanticweb.kaon2.reasoner.sparql.SPARQLParser;
import org.semanticweb.kaon2.reasoner.sparql.SPARQLParser.Query;

public class QueryValidator {

	private static DefaultOntologyResolver m_resolver;
	private static OntologyManager m_ontoManager;
	private static Ontology m_ontology;
	private static List<String> m_files;
	
	private static void loadOntology() throws KAON2Exception, InterruptedException {
		m_resolver = new DefaultOntologyResolver();
		m_ontoManager = KAON2Manager.newOntologyManager();
		m_ontoManager.setOntologyResolver(m_resolver);
		
		for (String fileName : m_files)
			m_ontoManager.openOntology(((DefaultOntologyResolver)m_ontoManager.getOntologyResolver()).registerOntology(new File(fileName)), new HashMap<String,Object>());
		
		((DefaultOntologyResolver)m_ontoManager.getOntologyResolver()).registerReplacement("http://example.org/import_ontology", "file:import_ontology.owl");
		m_ontology = m_ontoManager.createOntology("http://example.org/import_ontology", new HashMap<String,Object>());
		
		for (Ontology onto : m_ontoManager.getOntologies())
			m_ontology.addToImports(onto);
	}
	
	public static void main(String[] args) throws KAON2Exception, InterruptedException, ParseException {
		m_files = new ArrayList<String>();
		int j = 0;
//		for (File f : new File("/Users/gl/Studium/diplomarbeit/datasets/lubm/").listFiles()) {
//			if (f.getName().startsWith("University") || f.getName().startsWith("univ")) {
//				m_files.add(f.getAbsolutePath());
//				j++;
//				if (j == 3)
//					break;
//			}
//		}
		m_files.add("/Users/gl/Studium/diplomarbeit/datasets/test.owl");

		loadOntology();
		
//		String query = "SELECT ?x  WHERE { ?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent> . }";
		String query = "SELECT ?x ?y  WHERE { ?x <http://www.owl-ontologies.com/Ontology1219504054.owl#friendOf> ?y . }";

		Reasoner r = m_ontology.createReasoner();
//		SPARQLParser p = new SPARQLParser(new StringReader(query));
//		Query q = p.parseQuery(m_ontology, null);
		org.semanticweb.kaon2.api.reasoner.Query q = r.createQuery(new Namespaces(), query);
		
		q.open();
		while (!q.afterLast()) {
			Object[] tupleBuffer= q.tupleBuffer();
			for (int i = 0; i < tupleBuffer.length; i++){
				System.out.print(tupleBuffer[i] + " ");
			}
			System.out.println();
			q.next();
		}
	}

}

package edu.unika.aifb.vponmonet;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.semanticweb.kaon2.api.DefaultOntologyResolver;
import org.semanticweb.kaon2.api.KAON2Exception;
import org.semanticweb.kaon2.api.KAON2Manager;
import org.semanticweb.kaon2.api.Ontology;
import org.semanticweb.kaon2.api.OntologyManager;

public class ImportTest {
	private static Properties m_props;
	
	public static Ontology loadLUBMDataFiles(OntologyManager manager) throws KAON2Exception, InterruptedException {
		List<File> files = new ArrayList<File>();
		files.add(new File("res/univ-bench.owl"));
		for (File f : new File("res/").listFiles()) {
			if (f.getName().startsWith("University")) {
				files.add(f);
			}
		}
		
		for (File file : files)
			manager.openOntology(((DefaultOntologyResolver)manager.getOntologyResolver()).registerOntology(file), new HashMap<String,Object>());
		
		((DefaultOntologyResolver)manager.getOntologyResolver()).registerReplacement("http://example.org/import_ontology", "file:import_ontology.owl");
		Ontology ontology = manager.createOntology("http://example.org/import_ontology", new HashMap<String,Object>());
		
		for (Ontology onto : manager.getOntologies())
			ontology.addToImports(onto);
		
		return ontology;
	}

	public static Ontology loadLUBMOntologyFile(OntologyManager manager) throws KAON2Exception, InterruptedException {
		return manager.openOntology(((DefaultOntologyResolver)manager.getOntologyResolver()).registerOntology(new File("res/univ-bench.owl")), new HashMap<String,Object>());
	}
	
	private static void ontologyTest(Connection conn, Connection conn2) {
		try {
			DefaultOntologyResolver resolver = new DefaultOntologyResolver();
			OntologyManager manager = KAON2Manager.newOntologyManager();
			manager.setOntologyResolver(resolver);
			
			Ontology ontology = loadLUBMDataFiles(manager);
			
			OntologyImporter oi = new OntologyImporter();
			oi.setDbConnection(conn, conn2);
			oi.setOntology(ontology);
			
			oi.doImport();
			
			oi.getOntologyMapping().saveToDB(conn);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void triplesTest(Properties props) {
		try {
			TriplesImporter ti = new TriplesImporter();
			ti.setDbConnection(m_props.getProperty("db_host"), m_props.getProperty("db_name"), m_props.getProperty("db_user"), m_props.getProperty("db_password"));
			ti.addTriplesFile(new File(m_props.getProperty("dbpedia_dir") + "/tmp.txt"));
			ti.setPropNamesCaseSensitive(false);
//			ti.addTriplesFile(new File("/Users/gl/Studium/diplomarbeit/evaluation china/lehigh5.nt"));
//			ti.doImport();
			ti.createOntologyMapping();
			System.out.println(ti.getOntologyMapping().getProperties());
			
//			ti.getOntologyMapping().saveToDB(conn);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void yagoTest(Properties props) {
		try {
			YagoImporter yi = new YagoImporter();
//			yi.setDbConnection(conn, conn2);
			yi.setDbConnection(m_props.getProperty("db_host"), m_props.getProperty("db_name"), m_props.getProperty("db_user"), m_props.getProperty("db_password"));
			yi.setYagoDir(new File(m_props.getProperty("yago_dir")));
			
			yi.doImport();
			
//			yi.getOntologyMapping().saveToDB(conn);
		} catch (ImportException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException, KAON2Exception, ImportException, InterruptedException, FileNotFoundException, IOException {
		m_props = new Properties();
		m_props.load(new FileInputStream("config.properties"));
		
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Class.forName("net.sf.log4jdbc.DriverSpy");
//		Connection conn = DriverManager.getConnection("jdbc:log4jdbc:monetdb://" + m_props.getProperty("db_host") + "/" + m_props.getProperty("db_name"), 
//				m_props.getProperty("db_user"), m_props.getProperty("db_password"));
//		Connection conn2 = DriverManager.getConnection("jdbc:monetdb://" + m_props.getProperty("db_host") + "/" + m_props.getProperty("db_name"), 
//				m_props.getProperty("db_user"), m_props.getProperty("db_password"));
//		Connection conn = DriverManager.getConnection("jdbc:log4jdbc:monetdb://" + m_props.getProperty("db_host") + "/" + m_props.getProperty("db_name"), dbp);
		
		triplesTest(m_props);
//		ontologyTest(conn, conn2);
//		yagoTest(conn, conn2);
	}
}

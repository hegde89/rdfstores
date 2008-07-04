package edu.unika.aifb.vponmonet.examples;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.semanticweb.kaon2.api.logic.Variable;

import edu.unika.aifb.vponmonet.ImportException;
import edu.unika.aifb.vponmonet.OntologyMapping;
import edu.unika.aifb.vponmonet.SPARQLQueryTranslator;

public class QueryExample {
	public static void main(String[] args) {
		try {
			// load db configuration
			Properties props = new Properties();
			props.load(new FileInputStream("config.properties"));

			// connect to db
			Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
			Connection conn = DriverManager.getConnection("jdbc:monetdb://" + props.getProperty("db_host") + "/" + props.getProperty("db_name"),
					props.getProperty("db_user"), props.getProperty("db_password"));
			
			// load the previously saved ontology mapping
			OntologyMapping ontoMap = new OntologyMapping();
			ontoMap.loadFromDB(conn);
			
			// conjunctive queries only (no metaknowledge)
			String sparql = "SELECT ?x ?y WHERE {" +
				"?x <http://dbpedia.org/property/type> \"studio\" ." +
				"?x <http://dbpedia.org/property/name> ?y . " +
				"}";
			
			// translate the query from sparql to sql
			SPARQLQueryTranslator sqt = new SPARQLQueryTranslator(ontoMap);
			sqt.setDbConnection(conn);
			PreparedStatement pst = sqt.translateQuery(sparql);
			
			// execute query and display results
			long start = System.currentTimeMillis();
			pst.execute();
			long end = System.currentTimeMillis();
			do {
				ResultSet rst = pst.getResultSet();
				while (rst.next()) {
					for (int i = 1; i <= rst.getMetaData().getColumnCount(); i++)
						System.out.print(rst.getObject(i) + " ");
					System.out.println();
				}
			}
			while (pst.getMoreResults());
			System.out.println("evaluated in " + (end - start)/1000.0 + " seconds");
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ImportException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

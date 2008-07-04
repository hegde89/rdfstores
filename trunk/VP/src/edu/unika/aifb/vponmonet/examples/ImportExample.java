package edu.unika.aifb.vponmonet.examples;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import edu.unika.aifb.vponmonet.ImportException;
import edu.unika.aifb.vponmonet.TriplesImporter;

public class ImportExample {

	public static void main(String[] args) {
		try {
			// load db configuration
			Properties props = new Properties();
			props.load(new FileInputStream("config.properties"));

			TriplesImporter ti = new TriplesImporter();
			
			// set db login, two connections will be used
			ti.setDbConnection(props.getProperty("db_host"), props.getProperty("db_name"), 
					props.getProperty("db_user"), props.getProperty("db_password"));
			
			// add files to be imported (in n-triples format)
			// all previously imported data in the database will be deleted
			ti.addTriplesFile(new File(""));
			
			// this will take a while...
			ti.doImport();
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

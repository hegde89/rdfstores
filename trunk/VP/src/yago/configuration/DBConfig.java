package yago.configuration;

import java.io.*;
import java.sql.*;
import java.util.*;

import yago.javatools.*;

/** 
 * This class is part of the YAGO distribution. Its use is subject to the
 * licence agreement at http://mpii.de/yago
 *
 * Returns the database connection from yago.ini
 * 
 * @author Fabian M. Suchanek
 */
public class DBConfig {
  /** Holds the information that is necessary to connect to a database*/ 
  public static class DatabaseParameters {
  	public String system = null;
  	public String host = null;
  	public String port = null;
  	public String user = null;
  	public String password = null;
  	public String inst = null;
  	public String database= null;
  }
  
  /** Maps the name of an ini-file to the relevant parameters*/
  protected static Map<String,DatabaseParameters> databaseParameters=new TreeMap<String, DatabaseParameters>();
  
	public static String entityTable="entities";
	public static String entityName="name";
	public static String entityId="id";
	public static String entityIsConcept="isConcept";
	public static String entityURL="url";
	public static String factTable="facts";
	public static String factId="id";
	public static String factRelation="relation";
	public static String factArg1="arg1";
	public static String factArg2="arg2";
	public static String factWeight="weight";
  public static String degreeTable="entityDegrees";  
  
	/** Returns the database parameters for an ini-File. Initializes from the ini-file, if necessary*/
	public static DatabaseParameters databaseParameters(String inifile) throws IOException {
    DatabaseParameters p=databaseParameters.get(inifile);
    if(p!=null) return(p);
    p=new DatabaseParameters();
    
    Parameters.reset();
    Parameters.init(inifile);

		Parameters.ensureParameters("databaseSystem - either Oracle, Postgres or MySQL",
				"databaseUser - the user name for the YAGO database (also: databaseDatabase, databaseInst,databasePort,databaseHost)",
				"databasePassword - the password for the YAGO database"
		);
        
		// Retrieve the obligatory parameters
		p.system=Parameters.get("databaseSystem").toUpperCase();
		p.user=Parameters.get("databaseUser");    
		p.password=Parameters.get("databasePassword");
		// Retrieve the optional parameters
		try {
			p.host=Parameters.get("databaseHost");
		} catch(Exception e){};
		try {
			p.port=Parameters.get("databasePort");    
		} catch(Exception e){};          
		try {
			p.inst=Parameters.get("databaseSID");
		} catch(Parameters.UndefinedParameterException e) {}
		try {
			p.database=Parameters.get("databaseDatabase");
		} catch(Parameters.UndefinedParameterException e) {}    
    databaseParameters.put(inifile, p);
    return(p);
	}
  
	/** Returns a database as configured in the ini-File */
	public static Database getDatabase(String iniFile) throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException,SQLException  {
    DatabaseParameters p=databaseParameters(iniFile);
		// ------ ORACLE ----------
		if(p.system.equals("ORACLE")) {
			return(new OracleDatabase(p.user,p.password,p.host,p.port,p.inst));
		}
		//  ------ MySQL----------
		if(p.system.equals("MYSQL")) {
			return(new MySQLDatabase(p.user,p.password,p.database,p.host,p.port));
		}
    //  ------ Postgres----------
    if(p.system.equals("POSTGRES")) {
      return(new PostgresDatabase(p.user,p.password,p.database,p.host,p.port));
    }
		throw new RuntimeException("Unsupported database system "+p.system);
	}
  
  /** Returns a database as configured in yago.ini*/
  public static Database getYagoDatabase() throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException,SQLException  {
    return(getDatabase("yago.ini"));
  }

  /** Returns a database as configured in naga.ini*/
  public static Database getNagaDatabase() throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException,SQLException  {
    return(getDatabase("naga.ini"));
  }

  public static void main(String[] arg) throws Exception {
    getYagoDatabase().runInterface();
  }
}

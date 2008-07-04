package edu.unika.aifb.vponmonet;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

public abstract class Importer {

	protected Connection m_conn;
	protected Connection m_hashConn;
	protected OntologyMapping m_ontoMap;
	protected static Logger log;
	protected int m_hashInsertsSucceeded = 0;
	protected int m_hashInsertsAttempted = 0;
	protected String m_host;
	protected String m_database;
	protected String m_user;
	protected String m_password;
	
	public Importer() {
		m_ontoMap = new OntologyMapping();
	}

	public void setDbConnection(Connection conn) {
		m_conn = conn;
		try {
			m_conn.setAutoCommit(false);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void setDbConnection(Connection conn, Connection hashConn) {
		m_conn = conn;
		m_hashConn = hashConn;
		try {
			m_conn.setAutoCommit(false);
			m_hashConn.setAutoCommit(true);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void setDbConnection(String host, String db, String user, String password) {
		m_host = host;
		m_database = db;
		m_user = user;
		m_password = password;
		
		connect();
	}
	
	protected void connect() {
		try {
			m_conn = DriverManager.getConnection("jdbc:log4jdbc:monetdb://" + m_host + "/" + m_database, 
					m_user, m_password);
			m_hashConn = DriverManager.getConnection("jdbc:monetdb://" + m_host + "/" + m_database, 
					m_user, m_password);
			m_conn.setAutoCommit(false);
			m_hashConn.setAutoCommit(true);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	protected void disconnect() {
		try {
			m_conn.close();
			m_hashConn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	protected void dropAllTables() {
		try {
			Statement stmt = m_conn.createStatement();
			ResultSet rst = stmt.executeQuery("SELECT name FROM sys.tables WHERE system = false");
			while (rst.next()) {
				dropTable(rst.getString("name"));
			}
			m_conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}		
	}

	private void dropTable(String tableName) throws SQLException {
		try {
			Statement stmt = m_conn.createStatement();
			stmt.execute("DROP TABLE " + tableName);
		} catch (SQLException e) {
			m_conn.rollback();
		}		
	}

	private void createIndex(String indexName, String tableName, String columns, boolean unique) throws SQLException {
//		log.debug("creating index " + indexName + " on " + tableName + " (" + columns + ")");
//		Statement stmt = m_conn.createStatement();
//		stmt.execute("CREATE" + (unique ? " UNIQUE" : "" ) + " INDEX " + indexName + " ON " + tableName + "(" + columns + ")");
	}

	private void createTable(String tableName, String[] columns) throws SQLException {
		log.debug("creating table " + tableName);
		String sql = "CREATE TABLE " + tableName + "(";
		String addComma = "";
		for (String column : columns) {
			sql += addComma + column;
			addComma = ",";
		}
		sql += ")";
		Statement stmt = m_conn.createStatement();
		stmt.execute(sql);
		m_conn.commit();
	}

	private void createTableFor(String propertyUri) throws SQLException {
		dropTable(m_ontoMap.getPropertyTableName(propertyUri));
		createTable(m_ontoMap.getPropertyTableName(propertyUri), new String[] { 
				"id BIGINT",
				"subject BIGINT NOT NULL",
				"object " + m_ontoMap.getDBTypeForProperty(propertyUri) + " NOT NULL"
		});
		createIndex(m_ontoMap.getPropertyTableName(propertyUri) + "_subject", m_ontoMap.getPropertyTableName(propertyUri), "subject", false);
	}

	private void createSchemaTables() throws SQLException {
		dropTable(OntologyMapping.TYPE_TABLE);
		createTable(OntologyMapping.TYPE_TABLE, new String[] {
				"id BIGINT",
				"subject BIGINT NOT NULL",
				"object BIGINT NOT NULL"
		});
	
		dropTable(OntologyMapping.SUBCLASSOF_TABLE);
		createTable(OntologyMapping.SUBCLASSOF_TABLE, new String[] {
				"id BIGINT",
				"subject BIGINT NOT NULL",
				"object BIGINT NOT NULL"
		});
	
		dropTable(OntologyMapping.SUBPROPERTYOF_TABLE);
		createTable(OntologyMapping.SUBPROPERTYOF_TABLE, new String[] {
				"id BIGINT",
				"subject BIGINT NOT NULL",
				"object BIGINT NOT NULL"
		});
		
		dropTable(OntologyMapping.URI_HASHES_TABLE);
		createTable(OntologyMapping.URI_HASHES_TABLE, new String[]{
				"subject BIGINT NOT NULL PRIMARY KEY",
				"object VARCHAR (255) NOT NULL"
		});
	}

	protected void createTables() throws SQLException, ImportException {
		createSchemaTables();
		for (String propertyUri : m_ontoMap.getPropertyUris()) {
			createTableFor(propertyUri);
		}
		log.info("tables created");
	}
	
	protected long getId(String subject, String property, String object) {
		return URIHash.hash(subject + property + object);
	}
	
	protected long hash(String uri) throws ImportException, SQLException {
		long val = URIHash.hash(uri);
		
		try {
			Statement st = m_hashConn.createStatement();
			m_hashInsertsAttempted++;
			st.executeUpdate("INSERT INTO " + OntologyMapping.URI_HASHES_TABLE + " (subject, object) VALUES ('" + val + "', '" + uri + "')");
			m_hashInsertsSucceeded++;
			st.close();
		} catch (SQLException e) {
			// ignore exceptions, which are thrown when trying
			// to insert an already existing hash value
		}
		
		return val;
	}
	
	protected void insertHashes() {
		log.info("hash inserts (succeeded/attempted): " + m_hashInsertsSucceeded + "/" + m_hashInsertsAttempted);
	}
	
	public OntologyMapping getOntologyMapping() {
		return m_ontoMap;
	}
	
	protected abstract void createOntologyMapping() throws Exception;
	
	protected abstract void importData() throws ImportException;
	
	public void doImport() throws ImportException {
		try {
			createOntologyMapping();
//			dropAllTables();
			createTables();
			getOntologyMapping().saveToDB(m_conn);
			m_conn.commit();
			importData();
			m_conn.commit();
			insertHashes();
			m_conn.commit();
		} catch (SQLException e) {
			throw new ImportException(e);
		} catch (ImportException e) {
			throw e;
		} catch (Exception e) {
			throw new ImportException(e);
		}
	}
}
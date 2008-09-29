package edu.unika.aifb.graphindex.storage.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import edu.unika.aifb.graphindex.data.Triple;
import edu.unika.aifb.graphindex.storage.AbstractExtensionStorage;
import edu.unika.aifb.graphindex.storage.StorageException;

public class MySQLExtensionStorage extends AbstractExtensionStorage {

	private Connection m_conn;
	private String m_prefix;
	
	private final String COL_EXT = "ext";
	private final String COL_SUBJECT = "subject";
	private final String COL_PROPERTY = "property";
	private final String COL_OBJECT = "object";

	public MySQLExtensionStorage() {
	}
	
	public void setPrefix(String prefix) {
		m_prefix = prefix + "_";
	}

	public void initialize(boolean clean, boolean readonly) throws StorageException {
		super.initialize(clean, readonly);
		try {
			Class.forName ("com.mysql.jdbc.Driver");
			Class.forName("net.sf.log4jdbc.DriverSpy");
			m_conn = DriverManager.getConnection("jdbc:log4jdbc:mysql://localhost/graphindex", "graphindex", "blah");

			if (clean) {
				Statement st = m_conn.createStatement();
				st.execute("DROP TABLE IF EXISTS " + m_prefix + "extdata");
				st.execute("DROP TABLE IF EXISTS " + m_prefix + "extensions");
				st.execute("CREATE TABLE " + m_prefix + "extdata (" + COL_EXT + " char(20) not null, " + COL_SUBJECT + " varchar(255) not null, " + COL_PROPERTY + " varchar(255), " + COL_OBJECT + " varchar(255))");
				st.execute("CREATE INDEX " + m_prefix + "ext_index ON " + m_prefix + "extdata (" + COL_EXT + ")");
				st.execute("CREATE TABLE " + m_prefix + "extensions (" + COL_EXT + " char(20) not null)");
//				st.execute("CREATE UNIQUE INDEX " + m_prefix + "extall_index ON " + m_prefix + "exts (ext,uri,edge,parent)");
				st.close();
			}
		} catch (SQLException e) {
			throw new StorageException(e);
		} catch (ClassNotFoundException e) {
			throw new StorageException(e);
		}
	}

	public void close() {
	}
	
	public void optimize() {
		
	}
	
	private void lock() throws SQLException {
		Statement st = m_conn.createStatement();
		st.execute("LOCK TABLES " + m_prefix + "extdata WRITE");
		st.close();
	}
	
	private void unlock() throws SQLException {
		Statement st = m_conn.createStatement();
		st.execute("UNLOCK TABLES");
		st.close();
	}
	
	public void startBulkUpdate() throws StorageException {
		try {
			lock();
		} catch (SQLException e) {
			throw new StorageException(e);
		}
	}

	public void finishBulkUpdate() throws StorageException {
		try {
			unlock();
		} catch (SQLException e) {
			throw new StorageException(e);
		}
	}

	public Set<String> loadExtensionList() throws StorageException {
		Set<String> uris = new HashSet<String>();
		try {
			PreparedStatement pst = m_conn.prepareStatement("SELECT " + COL_EXT + " FROM " + m_prefix + "extensions");
			pst.execute();
			do {
				java.sql.ResultSet rst = pst.getResultSet();
				while (rst.next()) {
					uris.add(rst.getString(1));
				}
			}
			while (pst.getMoreResults());
			pst.close();
		} catch (SQLException e) {
			throw new StorageException(e);
		}
		
		return uris;
	}

	public void saveExtensionList(Set<String> uris) throws StorageException {
		try {
			Statement st = m_conn.createStatement();
			st.execute("TRUNCATE " + m_prefix + "extensions");
			st.close();
			
			PreparedStatement pst = m_conn.prepareStatement("INSERT INTO " + m_prefix + "extensions (" + COL_EXT + ") VALUES (?)");
			for (String uri : uris) {
				pst.setString(1, uri);
				pst.addBatch();
			}
			pst.executeBatch();
			
			pst.close();
		}
		catch (SQLException e) {
			throw new StorageException (e);
		}
	}
	
	private Set<Triple> executeQuery(PreparedStatement pst, String subject, String property, String object) throws SQLException {
		Set<Triple> triples = new HashSet<Triple>();
		
		pst.execute();
		do {
			java.sql.ResultSet rst = pst.getResultSet();
			while (rst.next()) {
				String s = subject == null ? rst.getString(COL_SUBJECT) : subject;
				String p = property == null ? rst.getString(COL_PROPERTY) : property;
				String o = object == null ? rst.getString(COL_OBJECT) : object;
				
				triples.add(new Triple(s, p, o));
			}
		}
		while (pst.getMoreResults());

		pst.close();
		
		return triples;
	}

	public Set<Triple> loadData(String extUri) throws SQLException {
		PreparedStatement pst = m_conn.prepareStatement("SELECT " + COL_SUBJECT + ", " + COL_PROPERTY + ", " + COL_OBJECT + " FROM " + m_prefix + "extdata WHERE " + COL_EXT + " = ?");
		pst.setString(1, extUri);
		
		return executeQuery(pst, null, null, null);
	}
	
	public Set<Triple> loadData(String extUri, String propertyUri) throws SQLException {
		PreparedStatement pst = m_conn.prepareStatement("SELECT " + COL_SUBJECT + ", " + COL_OBJECT + " FROM " + m_prefix + "extdata WHERE " + COL_EXT + " = ? AND " + COL_PROPERTY + " = ?");
		pst.setString(1, extUri);
		pst.setString(2, propertyUri);
		
		return executeQuery(pst, null, propertyUri, null);
	}
	
	public Set<Triple> loadData(String extUri, String propertyUri, String objectValue) throws SQLException {
		PreparedStatement pst = m_conn.prepareStatement("SELECT " + COL_SUBJECT + " FROM " + m_prefix + "extdata WHERE " + COL_EXT + " = ? AND " + COL_PROPERTY + " = ? AND " + COL_OBJECT + " = ?");
		pst.setString(1, extUri);
		pst.setString(2, propertyUri);
		pst.setString(3, objectValue);
		
		return executeQuery(pst, null, propertyUri, objectValue);
	}

	public void saveData(String extUri, Set<Triple> triples) throws SQLException {
		PreparedStatement pst = m_conn.prepareStatement("INSERT INTO " + m_prefix + "extdata (" + COL_EXT + ", " + COL_SUBJECT + ", " + COL_PROPERTY + ", " + COL_OBJECT + ") VALUES (?, ?, ?, ?)");
		for (Triple t : triples) {
			pst.setString(1, extUri);
			pst.setString(2, t.getSubject());
			pst.setString(3, t.getProperty());
			pst.setString(4, t.getObject());
			pst.addBatch();
		}
		
		pst.executeBatch();

		pst.close();
	}

	public void saveData(String extUri, Triple t) throws SQLException {
		PreparedStatement pst = m_conn.prepareStatement("INSERT INTO " + m_prefix + "extdata (" + COL_SUBJECT + ", " + COL_PROPERTY + ", " + COL_OBJECT + ") VALUES (?, ?, ?, ?)");
		pst.setString(1, t.getSubject());
		pst.setString(2, t.getProperty());
		pst.setString(3, t.getObject());
		
		pst.execute();

		pst.close();
	}
}

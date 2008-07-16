package edu.unika.aifb.graphindex.extensions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MySQLExtensionStorage implements ExtensionStorageEngine {

	private Connection m_conn;
	private Set<String> m_existing;
	private String m_prefix;
	private boolean m_init;
	
	public MySQLExtensionStorage(boolean init) {
		try {
			Class.forName ("com.mysql.jdbc.Driver");
			Class.forName("net.sf.log4jdbc.DriverSpy");
			m_conn = DriverManager.getConnection("jdbc:log4jdbc:mysql://localhost/graphindex", "graphindex", "blah");
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		m_existing = new HashSet<String>();
		m_init = init;
	}
	
	public void init() {
		initSchema(m_init);
		loadExisting();
	}
	
	public void lock() {
		try {
			Statement st = m_conn.createStatement();
			st.execute("LOCK TABLES " + m_prefix + "exts WRITE");
			st.close();
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void unlock() {
		try {
			Statement st = m_conn.createStatement();
			st.execute("UNLOCK TABLES");
			st.close();
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private void initSchema(boolean init) {
		try {
			Statement st = m_conn.createStatement();
			if (init) {
				st.execute("DROP TABLE IF EXISTS " + m_prefix + "exts");
				st.execute("CREATE TABLE " + m_prefix + "exts (ext char(20) not null, uri varchar(255) not null, edge varchar(255), parent varchar(255))");
				st.execute("CREATE INDEX " + m_prefix + "ext_index ON " + m_prefix + "exts (ext)");
				st.execute("CREATE UNIQUE INDEX " + m_prefix + "extall_index ON " + m_prefix + "exts (ext,uri,edge,parent)");
			}
			st.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private void loadExisting() {
		try {
			PreparedStatement pst = m_conn.prepareStatement("SELECT ext FROM " + m_prefix + "exts GROUP BY ext");
			pst.execute();
			do {
				java.sql.ResultSet rst = pst.getResultSet();
				while (rst.next()) {
					m_existing.add(rst.getString(1));
				}
			}
			while (pst.getMoreResults());
			pst.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public Extension readExtension(String uri) {
		try {
			Extension ext = new Extension(uri);
			
			PreparedStatement pst = m_conn.prepareStatement("SELECT uri, edge, parent FROM " + m_prefix + "exts WHERE ext = ?");
			pst.setString(1, uri);
			pst.execute();
			do {
				java.sql.ResultSet rst = pst.getResultSet();
				while (rst.next()) {
					ext.add(rst.getString(1), rst.getString(2), rst.getString(3));
				}
			}
			while (pst.getMoreResults());

			pst.close();
			
			return ext;
		} catch (SQLException e) {
			e.printStackTrace();
		}
	
		return null;
	}

	public void storeExtension(Extension ext) {
		try {
			if (extensionExists(ext.getUri()))
				removeExtension(ext.getUri());
			lock();
			PreparedStatement pst = m_conn.prepareStatement("INSERT INTO " + m_prefix + "exts (ext, uri, edge, parent) VALUES (?, ?, ?, ?)");
			for (String edgeUri : ext.getEdgeUris()) {
				for (ExtEntry e : ext.getEntries(edgeUri)) {
					if (e.getParents().size() == 0) {
						pst.setString(1, ext.getUri());
						pst.setString(2, e.getUri());
						pst.setString(3, edgeUri);
						pst.setString(4, null);
						pst.addBatch();
					}
					else {
						for (String parent : e.getParents()) {
							pst.setString(1, ext.getUri());
							pst.setString(2, e.getUri());
							pst.setString(3, edgeUri);
	//						if (e.getParents().size() > 1)
	//							System.out.println("> 1");
	//						else if (e.getParents().size() == 0)
	//							pst.setString(4, null);
	//						else
	//							pst.setString(4, e.getParents().get(0));
							pst.setString(4, parent);
							pst.addBatch();
						}
					}
				}
			}
			pst.executeBatch();
			pst.close();
			unlock();
			m_existing.add(ext.getUri());
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
	}

	public boolean extensionExists(String uri) {
		return m_existing.contains(uri);
//		try {
//			PreparedStatement pst = m_conn.prepareStatement("SELECT COUNT(*) FROM exts WHERE ext = ?");
//			pst.setString(1, uri);
//			pst.execute();
//			java.sql.ResultSet rst = pst.getResultSet();
//			if (rst.next()) {
//				return rst.getInt(1) > 0;
//			}
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//		return false;
	}

	public void removeExtension(String uri) {
		try {
			Statement st = m_conn.createStatement();
			st.execute("DELETE FROM " + m_prefix + "exts WHERE ext =\"" + uri + "\"");
			st.close();
			m_existing.remove(uri);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void removeAllExtensions() {
		try {
			Statement st = m_conn.createStatement();
			st.execute("DELETE FROM " + m_prefix + "exts");
			st.close();
			m_existing.clear();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public int numberOfExtensions() {
		return m_existing.size();
	}

	public void setPrefix(String prefix) {
		m_prefix = prefix + "_";
	}

	public void mergeExtensions(String targetUri, String sourceUri) {
		try {
			lock();
			Statement st = m_conn.createStatement();
			st.execute("UPDATE IGNORE " + m_prefix + "exts SET ext='" + targetUri + "' WHERE ext =\"" + sourceUri + "\"");
			st.close();
			removeExtension(sourceUri);
			unlock();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}

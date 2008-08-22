package edu.unika.aifb.graphindex.graph;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.unika.aifb.graphindex.Util;
import edu.unika.aifb.graphindex.extensions.ExtEntry;

public class MySQLGraphStorage implements GraphStorageEngine {
	private Connection m_conn;
	private Set<String> m_existing;
	private String m_prefix;
	private boolean m_init;
	
	public MySQLGraphStorage(boolean init) {
		m_existing = new HashSet<String>();
		
		try {
			Class.forName ("com.mysql.jdbc.Driver");
			Class.forName("net.sf.log4jdbc.DriverSpy");
			m_conn = DriverManager.getConnection("jdbc:log4jdbc:mysql://localhost/graphindex", "graphindex", "blah");
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		m_init = init;
	}
	
	public void init() {
		initSchema(m_init);
		loadExisting();
	}
	
	public void lock() {
		try {
			Statement st = m_conn.createStatement();
//			st.execute("LOCK TABLES " + m_prefix + "graphs WRITE, " + m_prefix + "graphdata WRITE, " + m_prefix + "vertexlabels WRITE");
			st.execute("LOCK TABLES " + m_prefix + "graphs WRITE, " + m_prefix + "graphdata WRITE");
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
				st.execute("DROP TABLE IF EXISTS " + m_prefix + "graphs");
				st.execute("DROP TABLE IF EXISTS " + m_prefix + "graphdata");
//				st.execute("DROP TABLE IF EXISTS " + m_prefix + "vertexlabels");
				st.execute("CREATE TABLE " + m_prefix + "graphs (name char(50) not null, id int not null, vertices int not null, root varchar(255), type varchar(255), PRIMARY KEY (name))");
				st.execute("CREATE TABLE " + m_prefix + "graphdata (name char(50) not null, source varchar(255) not null, edge varchar(255) not null, target varchar(255) not null, type varchar(255))");
//				st.execute("CREATE TABLE " + m_prefix + "vertexlabels (vertex varchar(255) not null, label text, graph char(50) not null, PRIMARY KEY (vertex))");
				st.execute("CREATE INDEX " + m_prefix + "graphdata_index ON " + m_prefix + "graphdata (name)");
//				st.execute("CREATE INDEX " + m_prefix + "vertexlabels_graph ON " + m_prefix + "vertexlabels (graph)");
			}
			else {
				
			}
			st.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private void loadExisting() {
		try {
			PreparedStatement pst = m_conn.prepareStatement("SELECT name FROM " + m_prefix + "graphs");
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
	
	public String getTableName(String name) {
		return Util.digest(name);
	}
	
	public boolean graphExists(String name) {
		return m_existing.contains(name);
//		try {
//			PreparedStatement pst = m_conn.prepareStatement("SELECT COUNT(*) FROM " + m_prefix + "graphs WHERE name = ?");
//			pst.setString(1, name);
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
	
	public boolean loadStub(Graph g) {
		try {
			PreparedStatement pst = m_conn.prepareStatement("SELECT name, id, vertices, root FROM " + m_prefix + "graphs WHERE name = ?");
			pst.setString(1, g.getName());
			pst.execute();
			do {
				java.sql.ResultSet rst = pst.getResultSet();
				if (rst != null && rst.next()) {
					g.setId(rst.getInt(2));
					g.setNumberOfVertices(rst.getInt(3));
					g.setRoot(rst.getString(4));
					pst.close();
					return true;
				}
			}
			while (pst.getMoreResults());

			pst.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return false;
	}
	
	public void readGraph(Graph g) {
		try {
			PreparedStatement pst = m_conn.prepareStatement("SELECT source, edge, target FROM " + m_prefix + "graphdata WHERE name = ?");
			pst.setString(1, g.getName());
			pst.execute();
			do {
				java.sql.ResultSet rst = pst.getResultSet();
				while (rst.next()) {
					g.addEdge(rst.getString(1), rst.getString(2), rst.getString(3));
				}
			}
			while (pst.getMoreResults());
			pst.close();
			
//			pst = m_conn.prepareStatement("SELECT vertex, label FROM " + m_prefix + "vertexlabels WHERE graph = ?");
//			pst.setString(1, g.getName());
//			pst.execute();
//			do {
//				java.sql.ResultSet rst = pst.getResultSet();
//				while (rst.next()) {
//					String vertex = rst.getString(1);
//					String label = rst.getString(2);
//					g.getVertex(vertex).setCanonicalLabel(label);
//				}
//			}
//			while (pst.getMoreResults());
//			pst.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void writeGraph(Graph g) {
		try {
			lock();
			Statement st = m_conn.createStatement();
//			st.execute("DROP TABLE IF EXISTS " + tableName);
			st.execute("REPLACE INTO " + m_prefix + "graphs (name, id, vertices, root) VALUES('" + g.getName() + "', '" + g.getId() + "', '" + g.numberOfVertices() + "', '" + (g.getRoot() != null ? g.getRoot().getLabel() : "") + "')");
//			st.execute("CREATE TABLE " + tableName + " (source varchar(255) not null, edge varchar(255) not null, target varchar(255) not null, type varchar(255))");
			if (m_existing.contains(g.getName())) {
				st.execute("DELETE FROM " + m_prefix + "graphdata WHERE name = '" + g.getName() + "'");
//				st.execute("DELETE FROM " + m_prefix + "vertexlabels WHERE graph = '" + g.getName() + "'");
			}
			st.close();
			
			PreparedStatement pst = m_conn.prepareStatement("INSERT INTO " + m_prefix + "graphdata (name, source, edge, target) VALUES (?, ?, ?, ?)");
			for (Edge e : g.edges()) {
				pst.setString(1, g.getName());
				pst.setString(2, e.getSource().getLabel());
				pst.setString(3, e.getLabel());
				pst.setString(4, e.getTarget().getLabel());
//				pst.execute();
				pst.addBatch();
			}
			pst.executeBatch();
			pst.close();

//			pst = m_conn.prepareStatement("INSERT INTO " + m_prefix + "vertexlabels (vertex, label, graph) VALUES (?, ?, ?)");
//			for (Vertex v : g.vertices()) {
//				pst.setString(1, v.getLabel());
//				pst.setString(2, v.getCanonicalLabel());
//				pst.setString(3, g.getName());
//				pst.addBatch();
//			}
//			pst.executeBatch();
//			pst.close();
			
			st = m_conn.createStatement();
			st.execute("UNLOCK TABLES");
			st.close();
			m_existing.add(g.getName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	
	public void removeGraph(Graph g) {
		try {
			lock();
			Statement st = m_conn.createStatement();
			st.execute("DELETE FROM " + m_prefix + "graphdata WHERE name = '" + g.getName() + "'");
			st.execute("DELETE FROM " + m_prefix + "graphs WHERE name = '" + g.getName() + "'");
//			st.execute("DELETE FROM " + m_prefix + "vertexlabels WHERE graph = '" + g.getName() + "'");
			st.close();
			unlock();
			m_existing.remove(g.getName());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public List<String> getStoredGraphs() {
		List<String> names = new ArrayList<String>();
		try {
			PreparedStatement pst = m_conn.prepareStatement("SELECT name FROM " + m_prefix + "graphs");
			pst.execute();
			do {
				java.sql.ResultSet rst = pst.getResultSet();
				while (rst.next()) {
					names.add(rst.getString(1));
					m_existing.add(rst.getString(1));
				}
			}
			while (pst.getMoreResults());

			pst.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return names;
	}

	public void setPrefix(String prefix) {
		m_prefix = prefix + "_";
	}
}

package edu.unika.aifb.vponmonet;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.semanticweb.kaon2.api.StringWithLanguage;


public class OntologyMapping {
	public class Property {
		private String m_uri;
		private int m_type;
		private String m_tableName;
		private String m_datatypeUri;
		private boolean m_clob = false;
		
		public static final int TYPE_DATA_PROPERTY = 0;
		public static final int TYPE_OBJECT_PROPERTY = 1;
		
		public Property(String uri, int type) {
			m_uri = uri;
			m_type = type;
		}
		
		public Property(String uri, int type, boolean clob) {
			m_uri = uri;
			m_type = type;
			m_clob = clob;
		}
		
		public String getUri() {
			return m_uri;
		}
		
		public int getType() {
			return m_type;
		}

		public String getTableName() {
			return m_tableName;
		}

		public void setTableName(String name) {
			m_tableName = name;
		}

		public String getDatatypeUri() {
			return m_datatypeUri;
		}

		public void setDatatypeUri(String uri) {
			m_datatypeUri = uri;
		}
		
		public boolean equals(Object o) {
			if (o == this)
				return true;
			if (!(o instanceof Property))
				return false;
			Property other = (Property)o;
			if (other.getUri() == getUri())
				return true;
			return false;
		}
		
		public boolean isCLOB() {
			return m_clob;
		}
		
		public String toString() {
			return "[" + m_uri + " (" + m_type + "), " + m_datatypeUri + " " + m_clob + "]";
		}
	}

	private Map<String,Property> m_pu2po;
	private Set<Property> m_properties;
	private DatatypeMappings m_dtmap;
	public final static String TYPE_TABLE = uri2TableName("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
	public final static String SUBPROPERTYOF_TABLE = uri2TableName("http://www.w3.org/2000/01/rdf-schema#subPropertyOf");
	public final static String SUBCLASSOF_TABLE = uri2TableName("http://www.w3.org/2000/01/rdf-schema#subClassOf");
	public final static String PROPERTY_MAPPING_TABLE = "__property_mapping";
	public final static String URI_HASHES_TABLE = "__uri_hashes";
	
	public OntologyMapping() {
		m_pu2po = new HashMap<String,Property>();
		m_properties = new HashSet<Property>();
		
		m_dtmap = DatatypeMappings.getInstance();
	}
	
	private static String uri2TableName(String uri) {
		// TODO this probably does not capture all valid characters in an URI
		try{
			MessageDigest algorithm = MessageDigest.getInstance("MD5");
			algorithm.update(uri.getBytes());
			byte messageDigest[] = algorithm.digest();
		            
			StringBuffer hexString = new StringBuffer();
			for (int i=0;i<messageDigest.length;i++) {
				hexString.append(Integer.toHexString(0xFF & messageDigest[i]));
			}
			return "_" + hexString.toString(); 
		} catch(NoSuchAlgorithmException nsae){
		}
		
		return uri.replaceAll("\\/|:|\\.|#|\\?|&|\\+|-|~", "_");
	}
	
	public void addProperty(String propertyUri, int type) {
		addProperty(propertyUri, type, false);
	}
	
	public void addProperty(String propertyUri, int type, boolean clob) {
		Property p = new Property(propertyUri, type, clob);
		m_properties.add(p);
		m_pu2po.put(propertyUri, p);
		addTableNameMapping(propertyUri, uri2TableName(propertyUri));
	}
	
	public void addTableNameMapping(String propertyUri, String tableName) {
		m_pu2po.get(propertyUri).setTableName(tableName);
	}
	
	public Set<String> getTableNames() {
		Set<String> names = new HashSet<String>();
		for (Property p : m_properties)
			names.add(p.getTableName());
		return names;
	}
	
	public Set<String> getPropertyUris() {
		Set<String> uris = new HashSet<String>();
		for (Property p : m_properties)
			uris.add(p.getUri());
		return uris;
	}
	
	public Set<Property> getProperties() {
		return m_properties;
	}
	
	public int getPropertyType(String propertyUri) {
		return m_pu2po.get(propertyUri).getType();
	}
	
	public boolean isDataProperty(String propertyUri) {
		return m_pu2po.get(propertyUri).getType() == Property.TYPE_DATA_PROPERTY;
	}
	
	public boolean isObjectProperty(String propertyUri) {
		return m_pu2po.get(propertyUri).getType() == Property.TYPE_OBJECT_PROPERTY;
	}
	
	public void setPropertyType(String propertyUri, String datatypeUri) {
		m_pu2po.get(propertyUri).setDatatypeUri(datatypeUri);
	}
	
	public String getPropertyTableName(String propertyUri) {
		return m_pu2po.get(propertyUri).getTableName();
	}
	
	public String getXSDTypeForProperty(String propertyUri) {
		return m_pu2po.get(propertyUri).getDatatypeUri();
	}
	
	public String getDBTypeForProperty(String propertyUri) {
		if (getProperty(propertyUri).isCLOB())
			return "CLOB";
		return m_dtmap.getDBType(getXSDTypeForProperty(propertyUri));
	}
	
	public int getJDBCTypeForProperty(String propertyUri) {
		if (getProperty(propertyUri).isCLOB())
			return Types.CLOB;
		return m_dtmap.getJDBCType(getXSDTypeForProperty(propertyUri));
	}

	public int getJDBCTypeForXSDType(String datatypeUri) {
		return m_dtmap.getJDBCType(datatypeUri);
	}
	
	public boolean isPropertyMapped(String uri) {
		return m_pu2po.containsKey(uri);
	}
	
	private Property findPropertyByTableName(String table) {
		for (Property p : m_properties)
			if (p.getTableName().equals(table))
				return p;
		return null;
	}

	public String getXSDTypeForTableColumn(String name, String column) {
		if (column.equals("subject"))
			return DatatypeMappings.XSD_NS + "string";
		Property p = findPropertyByTableName(name);
		if (p != null)
			return p.getDatatypeUri();
		else 
			return null;
	}

	public Object convertToDBObject(Object o, String datatypeUri) {
		if (datatypeUri.equals(DatatypeMappings.XSD_URI)) {
			return URIHash.hash((String)o);
		}
		
		if (o instanceof StringWithLanguage) {
			StringWithLanguage s = (StringWithLanguage)o;
			return "\"" + s.getString() + "\"@" + s.getLanguage();
		}
		else if (o instanceof java.util.Date) {
			o = new java.sql.Date(((java.util.Date)o).getTime());
		}
		else if (o instanceof String) {
			try {
				if (datatypeUri.equals(DatatypeMappings.RDFSDatatype.rstring.toString()))
					return o;
				else if (datatypeUri.equals(DatatypeMappings.RDFSDatatype.rboolean.toString()))
					return Boolean.valueOf((String)o);
				else if (datatypeUri.equals(DatatypeMappings.RDFSDatatype.rint.toString()) ||
						datatypeUri.equals(DatatypeMappings.RDFSDatatype.rinteger.toString()) ||
						datatypeUri.equals(DatatypeMappings.RDFSDatatype.rnonNegativeInteger.toString()))
					return Integer.valueOf((String)o);
				else if(datatypeUri.equals(DatatypeMappings.RDFSDatatype.rfloat.toString()))
					return Float.valueOf((String)o);
				else if(datatypeUri.equals(DatatypeMappings.RDFSDatatype.rdecimal.toString()) ||
						datatypeUri.equals(DatatypeMappings.RDFSDatatype.rdouble.toString()))
					return Double.valueOf((String)o);
				else if (datatypeUri.equals(DatatypeMappings.RDFSDatatype.rdate.toString())) {
					String[] t = ((String)o).split("-");
					if (t.length == 3) {
						int year = Integer.valueOf(t[0]);
						int month = Integer.valueOf(t[1]);
						if (year > 9999)
							return null;
						year -= 1900;
						month--;
						return new java.sql.Date((new Date(year, month, Integer.valueOf(t[2]))).getTime());
					}
					else 
						return null;
				}
				else
					return null;
			}
			catch (NumberFormatException e) {
				return null;
			}
		}
		
		return o;
	}
	
	public void loadFromDB(Connection conn) throws SQLException {
		m_pu2po.clear();
		m_properties.clear();
		
		Statement st = conn.createStatement();
		st.execute("SELECT uri, type, tableName, datatypeUri, isClob FROM " + PROPERTY_MAPPING_TABLE);
		do {
			ResultSet rst = st.getResultSet();
			while (rst.next()) {
				addProperty(rst.getString(1), rst.getInt(2), rst.getBoolean(5));
				addTableNameMapping(rst.getString(1), rst.getString(3));
				setPropertyType(rst.getString(1), rst.getString(4));
			}
		}
		while (st.getMoreResults());
	}
	
	public void saveToDB(Connection conn) throws SQLException {
		Statement st;
		try {
			st = conn.createStatement();
			st.execute("DROP TABLE " + PROPERTY_MAPPING_TABLE);
		} catch (SQLException e) {
			conn.rollback();
		}
		
		st = conn.createStatement();
		st.execute("CREATE TABLE " + PROPERTY_MAPPING_TABLE + " (" + 
				"id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, " +
				"uri VARCHAR (255) NOT NULL, " + 
				"type INT NOT NULL, " + 
				"tableName VARCHAR(255) NOT NULL, " + 
				"datatypeUri VARCHAR(255) NOT NULL, " +
				"isClob BOOLEAN NOT NULL" +
				")");
		
		PreparedStatement pst = conn.prepareStatement("INSERT INTO " + PROPERTY_MAPPING_TABLE + " (uri, type, tableName, datatypeUri, isClob) VALUES (?, ?, ?, ?, ?)");
		for (Property p : m_properties) {
			pst.setString(1, p.getUri());
			pst.setInt(2, p.getType());
			pst.setString(3, p.getTableName());
			pst.setString(4, p.getDatatypeUri());
			pst.setBoolean(5, p.isCLOB());
			pst.addBatch();
		}
		try {
			pst.executeBatch();
			conn.commit();
		}
		catch (BatchUpdateException e) {
			e.getNextException().printStackTrace();
		}
	}

	public Property getProperty(String propertyUri) {
		return m_pu2po.get(propertyUri);
	}
	
	public boolean containsObjectProperty(String propertyUri) {
		Property p = getProperty(propertyUri);
		if (p == null || p.getType() == Property.TYPE_DATA_PROPERTY)
			return false;
		return true;
	}

	public boolean containsDataProperty(String propertyUri) {
		Property p = getProperty(propertyUri);
		if (p == null || p.getType() == Property.TYPE_OBJECT_PROPERTY)
			return false;
		return true;
	}
}

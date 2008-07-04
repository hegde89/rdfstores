package edu.unika.aifb.vponmonet;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import yago.javatools.Char;


public class DatatypeMappings {
	private Map<String,Integer> m_uri2jdbc;
	private Map<Integer,String> m_jdbc2uri;
	private Map<Integer,String> m_jdbc2mdb;
	private static DatatypeMappings m_instance = null;
	
	public static final String XSD_NS = "http://www.w3.org/2001/XMLSchema#";
	public static final String XSD_URI = XSD_NS + "anyURI";

	public static enum RDFSDatatype {
		rboolean, rdouble, rdecimal, rnonNegativeInteger, rstring, rdate, rgYear, rduration, ranyURI, rint, rinteger, rdateTime, rfloat, rlong;
		@Override
		public String toString() {
			return XSD_NS + name().substring(1);
		}

		public String asLiteral(String literal) {
			if (literal.startsWith("\""))
				literal = Char.cutLast(literal.substring(1));
			return (Char.encodeAmpersand(literal));
		}
	};

	private DatatypeMappings() {
		m_uri2jdbc = new HashMap<String,Integer>();
		m_jdbc2uri = new HashMap<Integer,String>();
		m_jdbc2mdb = new HashMap<Integer,String>();
		registerDefaultMappings();
	}
	
	public static DatatypeMappings getInstance() {
		if (m_instance == null)
			m_instance = new DatatypeMappings();
		return m_instance;
	}
	
	public void registerDatatypeMapping(String datatypeUri, int jdbcType) {
		m_uri2jdbc.put(datatypeUri, jdbcType);
		m_jdbc2uri.put(jdbcType, datatypeUri);
	}
	
	public void registerJDBCMapping(int jdbcType, String mdbType) {
		m_jdbc2mdb.put(jdbcType, mdbType);
	}
	
	private void registerDefaultMappings() {
		registerDatatypeMapping(XSD_NS + "boolean", Types.BOOLEAN);
		registerDatatypeMapping(XSD_NS + "string", Types.VARCHAR);
		registerDatatypeMapping(XSD_NS + "int", Types.INTEGER);
		registerDatatypeMapping(XSD_NS + "integer", Types.INTEGER);
		registerDatatypeMapping(XSD_NS + "nonNegativeInteger", Types.INTEGER);
		registerDatatypeMapping(XSD_NS + "long", Types.BIGINT);
		registerDatatypeMapping(XSD_NS + "float", Types.REAL);
		registerDatatypeMapping(XSD_NS + "double", Types.DOUBLE);
		registerDatatypeMapping(XSD_NS + "decimal", Types.DOUBLE);
		registerDatatypeMapping(XSD_NS + "dateTime", Types.TIMESTAMP);
		registerDatatypeMapping(XSD_NS + "date", Types.DATE);
		registerDatatypeMapping(XSD_NS + "anyURI", Types.BIGINT);
		
		registerJDBCMapping(Types.BOOLEAN, "BOOLEAN");
		registerJDBCMapping(Types.VARCHAR, "VARCHAR (255)");
		registerJDBCMapping(Types.INTEGER, "INT");
		registerJDBCMapping(Types.BIGINT, "BIGINT");
		registerJDBCMapping(Types.REAL, "REAL");
		registerJDBCMapping(Types.DOUBLE, "DOUBLE");
		registerJDBCMapping(Types.TIMESTAMP, "TIMESTAMP");
		registerJDBCMapping(Types.DATE, "DATE");
	}
	
	public boolean isNumericType(String type) {
		type = type.toLowerCase();
		if (type.equalsIgnoreCase(XSD_NS + "integer") ||
				type.equalsIgnoreCase(XSD_NS + "int") || 
				type.equalsIgnoreCase(XSD_NS + "nonNegativeInteger") ||
				type.equalsIgnoreCase(XSD_NS + "long") ||
				type.equalsIgnoreCase(XSD_NS + "float") ||
				type.equalsIgnoreCase(XSD_NS + "double") ||
				type.equalsIgnoreCase(XSD_NS + "decimal"))
			return true;
		return false;
	}
	
	public boolean isDatatypeRegistered(String type) {
		return m_uri2jdbc.containsKey(type);
	}

	public String getDBType(String datatypeUri) {
		return m_jdbc2mdb.get(m_uri2jdbc.get(datatypeUri));
	}
	
	public int getJDBCType(String datatypeUri) {
		return m_uri2jdbc.get(datatypeUri);
	}
}

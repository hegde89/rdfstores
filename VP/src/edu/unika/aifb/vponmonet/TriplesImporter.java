package edu.unika.aifb.vponmonet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nl.cwi.monetdb.jdbc.MonetClob;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser.DatatypeHandling;
import org.openrdf.rio.ntriples.NTriplesParser;

public class TriplesImporter extends Importer {
	private int m_triplesCount = 0;
	
	private class TypeOccurences {
		private Map<String,Integer> m_types;
		
		public TypeOccurences() {
			m_types = new HashMap<String, Integer>();
		}
		
		public void addOccurence(String type) {
			if (!m_types.containsKey(type))
				m_types.put(type, 0);
			m_types.put(type, m_types.get(type) + 1);
		}
		
		public String getMostFrequentType() {
			String type = null;
			int max = 0;
			for (String t : m_types.keySet()) {
				if (DatatypeMappings.getInstance().isDatatypeRegistered(t) && m_types.get(t) > max) {
					type = t;
					max = m_types.get(t);
				}
			}
			return type;
		}

		public Set<String> getTypes() {
			return m_types.keySet();
		}
	}
	
	private class CreatePropertiesHandler implements RDFHandler {
		
		public Map<String,TypeOccurences> m_props;
		public Map<String,Integer> m_propTypes;
		public Set<String> m_clobProps;
		
		public CreatePropertiesHandler() {
			m_props = new HashMap<String,TypeOccurences>();
			m_propTypes = new HashMap<String, Integer>();
			m_clobProps = new HashSet<String>();
		}

		public void startRDF() throws RDFHandlerException {
		}

		public void endRDF() throws RDFHandlerException {
		}

		public void handleComment(String arg0) throws RDFHandlerException {
		}

		public void handleNamespace(String arg0, String arg1) throws RDFHandlerException {
		}

		public void handleStatement(org.openrdf.model.Statement st) throws RDFHandlerException {
			m_triplesCount++;
			
			String propertyUri = st.getPredicate().toString();
			
			if (!propNamesCaseSensitive())
				propertyUri = propertyUri.toLowerCase();
			
//			if (propertyUri.endsWith("type"))
//				System.out.println(st);

			if (!(st.getSubject() instanceof org.openrdf.model.URI)) {
				log.warn("subject is not an URI, ignored: " + st);
				return;
			}
			
			if (!m_props.containsKey(propertyUri))
				m_props.put(propertyUri, new TypeOccurences());
			
			if (st.getObject() instanceof Literal) {
				Literal l = (Literal)st.getObject();
				if (l.getDatatype() != null) {
					String type = l.getDatatype().toString();
					int i = type.indexOf("#");
					if (i > 0) {
						type = type.substring(0, i + 1) + type.substring(i + 1, i + 2).toLowerCase() + type.substring(i + 2);
					}
					m_props.get(propertyUri).addOccurence(type);
//					if (l.getDatatype().toString().equals(DatatypeMappings.XSD_NS + "string")) {
//						String s = l.getLabel();
//						if (s.length() > 200)
//							m_clobProps.add(propertyUri);
//					}
				}
				else if (l.getLabel() != null) {
					m_props.get(propertyUri).addOccurence(DatatypeMappings.XSD_NS + "string");
//					if (l.getLabel().length() > 200)
//						m_clobProps.add(propertyUri);
				}
				else
					System.out.println("datatype and label null: " + st);
				m_propTypes.put(propertyUri, OntologyMapping.Property.TYPE_DATA_PROPERTY);
			}
			else if (st.getObject() instanceof org.openrdf.model.URI) {
				m_props.get(propertyUri).addOccurence(DatatypeMappings.XSD_NS + "anyURI");
				m_propTypes.put(propertyUri, OntologyMapping.Property.TYPE_OBJECT_PROPERTY);
			}
			else {
				log.warn("unknown object type: " + st.getObject().getClass() + ": " + st);
			}
		}
	}
	
	private class ImportDataHandler implements RDFHandler {

		public int m_insertedTriplesCount = 0;
		public int m_triplesDropped = 0;
		
		public ImportDataHandler(OntologyMapping map) {
		}

		public void startRDF() throws RDFHandlerException {
		}

		public void endRDF() throws RDFHandlerException {
		}

		public void handleComment(String arg0) throws RDFHandlerException {
		}

		public void handleNamespace(String arg0, String arg1) throws RDFHandlerException {
		}

		public void handleStatement(org.openrdf.model.Statement st) throws RDFHandlerException {
			String propertyUri = st.getPredicate().toString();
			
			if (!propNamesCaseSensitive())
				propertyUri = propertyUri.toLowerCase();
			
			if (!m_ontoMap.isPropertyMapped(propertyUri))
				return;
			
			try {
				PreparedStatement pst = m_conn.prepareStatement("INSERT INTO " + m_ontoMap.getPropertyTableName(propertyUri) + 
						" (subject, object) VALUES (?, ?)");
				if (m_ontoMap.getPropertyType(propertyUri) == OntologyMapping.Property.TYPE_DATA_PROPERTY) {
					if (!(st.getObject() instanceof Literal)) {
//						log.warn("object of data property is not a literal, dropping " + st);
						m_triplesDropped++;
						return;
					}
					
					Literal l = (Literal)st.getObject();
					Object o = null;
					
					if (l.getLabel() != null)
						o = m_ontoMap.convertToDBObject(l.getLabel(), m_ontoMap.getXSDTypeForProperty(propertyUri));
					
					if (o != null) {// && m_ontoMap.getJDBCTypeForProperty(propertyUri) != Types.DATE) {
						pst.setLong(1, hash(((org.openrdf.model.URI)st.getSubject()).toString()));
						if (o instanceof String && ((String)o).length() > 230)
							o = ((String)o).substring(0, 230);
//						log.warn(o + " " + m_ontoMap.getJDBCTypeForProperty(propertyUri) + " " + m_ontoMap.getXSDTypeForProperty(propertyUri));
						pst.setObject(2, o, m_ontoMap.getJDBCTypeForProperty(propertyUri));
						try {
							pst.executeUpdate();
						} catch (SQLException e) {
							log.error(e);
							log.error(o + " " + m_ontoMap.getProperty(propertyUri));
							m_conn.rollback();
						}
						m_insertedTriplesCount++;
					}
					else
						m_triplesDropped++;
				}
				else {
					if (!(st.getObject() instanceof org.openrdf.model.URI)) {
//						log.debug("object of object property is not an URI (probably cause be type conflict resolution), dropping " + st);
						m_triplesDropped++;
						return;
					}
					
					pst.setLong(1, hash(((org.openrdf.model.URI)st.getSubject()).toString()));
					pst.setLong(2, hash(((org.openrdf.model.URI)st.getObject()).toString()));
					try {
						pst.executeUpdate();
					} catch (SQLException e) {
						log.error(e);
						log.error(m_ontoMap.getProperty(propertyUri) + " " + st);
						m_conn.rollback();
					}
					m_insertedTriplesCount++;
				}
				pst.close();
				
				if (m_insertedTriplesCount % 40000 == 0) {
					m_conn.commit();
					disconnect();
					connect();
					log.info("reconnected");
					log.debug(Runtime.getRuntime().maxMemory() + " " + Runtime.getRuntime().totalMemory() + " " + Runtime.getRuntime().freeMemory());
					System.gc();
					log.debug(Runtime.getRuntime().maxMemory() + " " + Runtime.getRuntime().totalMemory() + " " + Runtime.getRuntime().freeMemory());
				}
			} catch (SQLException e) {
				e.printStackTrace();
				m_triplesDropped++;
				throw new RDFHandlerException(e);
			} catch (ImportException e) {
				e.printStackTrace();
			}
			if (m_insertedTriplesCount % 1000 == 0) 
				log.info("triples inserted/dropped: " + m_insertedTriplesCount + "/" + m_triplesDropped);
		}
	}

	private List<File> m_triplesFiles;
	private boolean m_propNamesCaseSensitive = true;
	
	public TriplesImporter() {
		super();
		log = Logger.getLogger(TriplesImporter.class);
		m_triplesFiles = new ArrayList<File>();
		m_ontoMap = new OntologyMapping();
	}
	
	public void setTriplesFiles(List<File> files) {
		m_triplesFiles = files;
	}
	
	public void addTriplesFile(File file) {
		m_triplesFiles.add(file);
	}
	
	public void createOntologyMapping() throws Exception {
		CreatePropertiesHandler handler = new CreatePropertiesHandler();
		
		for (File file : m_triplesFiles) {
			NTriplesParser parser = new NTriplesParser();
			parser.setDatatypeHandling(DatatypeHandling.VERIFY);
			parser.setStopAtFirstError(false);
			parser.setRDFHandler(handler);
			
			parser.parse(new BufferedReader(new FileReader(file), 10000000), "");
			
			log.info("triples scanned: " + m_triplesCount);
		}
		
		int conflicts = 0;
		int conflictsSolved = 0;
		for (String propertyUri : handler.m_props.keySet()) {
			Set<String> types = handler.m_props.get(propertyUri).getTypes();
			String finalType = null;
			boolean clob = false;
			
			if (types.size() != 1) {
				conflicts++;

				boolean allNumbers = true;
				for (String type : types) {
					if (!DatatypeMappings.getInstance().isNumericType(type))
						allNumbers = false;
				}
				
				if (allNumbers) {
					if (types.contains(DatatypeMappings.XSD_NS + "decimal") || types.contains(DatatypeMappings.XSD_NS + "Decimal"))
						finalType = DatatypeMappings.XSD_NS + "decimal";
					else if (types.contains(DatatypeMappings.XSD_NS + "double") || types.contains(DatatypeMappings.XSD_NS + "Double"))
						finalType = DatatypeMappings.XSD_NS + "double";
					else if (types.contains(DatatypeMappings.XSD_NS + "float") || types.contains(DatatypeMappings.XSD_NS + "Float"))
						finalType = DatatypeMappings.XSD_NS + "double";
					else if (types.contains(DatatypeMappings.XSD_NS + "long") || types.contains(DatatypeMappings.XSD_NS + "Long"))
						finalType = DatatypeMappings.XSD_NS + "long";
					else
						finalType = DatatypeMappings.XSD_NS + "integer";
				}
				else {
					finalType = handler.m_props.get(propertyUri).getMostFrequentType();
				}
				
				if (finalType != null) {
					if (finalType.equals(DatatypeMappings.XSD_URI))
						handler.m_propTypes.put(propertyUri, OntologyMapping.Property.TYPE_OBJECT_PROPERTY);
					else
						handler.m_propTypes.put(propertyUri, OntologyMapping.Property.TYPE_DATA_PROPERTY);
					conflictsSolved++;
				}
				else
					log.warn(propertyUri + ": " + types + " => " + finalType);
			}
			else
				finalType = (String)types.toArray()[0];
			
			if (finalType != null) {
				if (finalType.equals(DatatypeMappings.XSD_NS + "string") && handler.m_clobProps.contains(propertyUri)) {
					clob = true;
				}
				if (DatatypeMappings.getInstance().getDBType(finalType) != null) {
					m_ontoMap.addProperty(propertyUri, handler.m_propTypes.get(propertyUri), clob);
					m_ontoMap.setPropertyType(propertyUri, finalType);
					
					if (m_ontoMap.getDBTypeForProperty(propertyUri) == null) {
						System.out.println(propertyUri + ":" + m_ontoMap.getXSDTypeForProperty(propertyUri));
					}
				}
			}
		}
		log.info("properties with datatype conflicts/solved conflicts/total properties: " + conflicts + "/" + conflictsSolved + "/" + handler.m_props.keySet().size());
		
		Map<String,String> tables = new HashMap<String,String>();
		for (String p : m_ontoMap.getPropertyUris()) {
			if (tables.containsKey(m_ontoMap.getPropertyTableName(p).toLowerCase())) {
				log.error("duplicate table name: " + m_ontoMap.getPropertyTableName(p) + ", " + p + " " + tables.get(m_ontoMap.getPropertyTableName(p).toLowerCase()));
			}
			else
				tables.put(m_ontoMap.getPropertyTableName(p).toLowerCase(), p);
		}
	}
	
	private void importTriples() throws ImportException, RDFParseException, RDFHandlerException, FileNotFoundException, IOException {
		ImportDataHandler handler = new ImportDataHandler(m_ontoMap);
		
		for (File file : m_triplesFiles) {
			NTriplesParser parser = new NTriplesParser();
			parser.setDatatypeHandling(DatatypeHandling.VERIFY);
			parser.setStopAtFirstError(false);
			parser.setRDFHandler(handler);
			
			parser.parse(new BufferedReader(new FileReader(file), 10000000), "");
			
			log.info("triples inserted: " + handler.m_insertedTriplesCount);
		}
	}
	
	protected void importData() throws ImportException {
		try {
			importTriples();
		} catch (Exception e) {
			throw new ImportException(e);
		} 
	}

	public boolean propNamesCaseSensitive() {
		return m_propNamesCaseSensitive;
	}

	public void setPropNamesCaseSensitive(boolean namesCaseSensitive) {
		m_propNamesCaseSensitive = namesCaseSensitive;
	}
}

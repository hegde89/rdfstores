package edu.unika.aifb.vponmonet;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import yago.datagraph.Relation;
import yago.datagraph.YagoClass;
import yago.javatools.Announce;
import yago.javatools.Char;
import yago.javatools.FileLines;
import yago.javatools.FinalMap;
import yago.javatools.UTF8Reader;

public class YagoImporter extends Importer {

	private File m_yagoDir;
	public static final String YAGO_NS = "http://www.mpii.de/yago/resource/";
	public static final String RDFS_NS = "http://www.w3.org/2000/01/rdf-schema#";
	public static final String RDF_NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
	private List<Object[]> m_confidences;

	/** All rdfs datatypes */
	public static enum RDFSDatatype {
		rboolean, rdouble, rdecimal, rnonNegativeInteger, rstring, rdate, rgYear, rduration, rRESOURCE;
		@Override
		public String toString() {
			return "http://www.w3.org/2001/XMLSchema#" + name().substring(1);
		}

		public String asLiteral(String literal) {
			if (literal.startsWith("\""))
				literal = Char.cutLast(literal.substring(1));
			return (Char.encodeAmpersand(literal));
		}
	};

	/** Maps Yago classes to RDFS types */
	public static final Map<YagoClass,RDFSDatatype> yago2rdfs = new FinalMap<YagoClass,RDFSDatatype> (
			new Object[] { 
					YagoClass.BOOLEAN, RDFSDatatype.rboolean,
					YagoClass.NUMBER, RDFSDatatype.rdouble, 
					YagoClass.RATIONAL, RDFSDatatype.rdouble, 
					YagoClass.INTEGER, RDFSDatatype.rdecimal, 
					YagoClass.NONNEGATIVEINTEGER, RDFSDatatype.rnonNegativeInteger, 
					YagoClass.STRING, RDFSDatatype.rstring, 
					YagoClass.WORD, RDFSDatatype.rstring,
					YagoClass.TLD, RDFSDatatype.rstring, 
					YagoClass.CHAR, RDFSDatatype.rstring, 
					YagoClass.TIMEINTERVAL, RDFSDatatype.rdate, 
					YagoClass.TIMEPOINT, RDFSDatatype.rdate, 
					YagoClass.DATE, RDFSDatatype.rdate,
					YagoClass.YEAR, RDFSDatatype.rgYear, 
					YagoClass.DURATION, RDFSDatatype.rduration, 
					YagoClass.IDENTIFIER, RDFSDatatype.rstring, 
					YagoClass.CALLINGCODE, RDFSDatatype.rstring, 
					YagoClass.ISBN, RDFSDatatype.rstring,
					YagoClass.PROPORTION, RDFSDatatype.rdouble 
			});

	/** Returns the RDFSType for a YagoClass (or RESOURCE if it's a resource) */
	public static RDFSDatatype rdfsTypeforYagoClass(YagoClass y) {
		if (yago2rdfs.containsKey(y))
			return (yago2rdfs.get(y));
		else
			return (RDFSDatatype.rRESOURCE);
	}

	/** Formats a Yago entity as a URI */
	public static String asURI(String entity) {
		entity = Char.encodeAmpersand(entity);
		if (!entity.startsWith("http://"))
			entity = YAGO_NS + entity;
		return entity;
	}

	/** Maps Yago relation names to their RDFS equivalent */
	public static final Map<Relation,String> specialRelationNames = new FinalMap<Relation,String> (
			Relation.MEANS, "rdfs:label xml:lang=\"en\"", 
			Relation.TYPE, RDF_NS + "type", 
			Relation.SUBCLASSOF, RDFS_NS + "subClassOf",
			Relation.SUBPROPERTYOF, RDFS_NS + "subPropertyOf", 
			Relation.DOMAIN, RDFS_NS + "domain", 
			Relation.RANGE, RDFS_NS + "range", 
			Relation.FAMILYNAME, YAGO_NS + "hasFamilyName", 
			Relation.GIVENNAME, YAGO_NS + "hasGivenName",
			Relation.NATIVENAME, YAGO_NS + "hasNativeName");
	  
	public YagoImporter() {
		log = Logger.getLogger(YagoImporter.class);
		m_confidences = new ArrayList<Object[]>();
	}
	
	public void setYagoDir(File yagoDir) {
		m_yagoDir = yagoDir;
	}
	
	private Set<Relation> getRelations() {
		Set<Relation> relations = new HashSet<Relation>();
		
		for (File f : new File(m_yagoDir, "facts").listFiles()) {
			if (!f.isDirectory())
				continue;
			
			Relation r = Relation.valueOf(f.getName());
			if (r != null)
				relations.add(r);
		}
		
		return relations;
	}
	
	@Override
	protected void createOntologyMapping() throws Exception {
		Set<Relation> relations = getRelations();
		m_ontoMap = new OntologyMapping();
		
		for (Relation yagoRelation : relations) {
			if (yagoRelation == Relation.ISCALLED || yagoRelation == Relation.INLANGUAGE || yagoRelation == Relation.MEANS)
				continue;

			String yagoRelationName = yagoRelation.name();
			String s = yagoRelationName;

			RDFSDatatype objectType = rdfsTypeforYagoClass(yagoRelation.range);

			boolean inverted = false;
			if (rdfsTypeforYagoClass(yagoRelation.domain) != RDFSDatatype.rRESOURCE) {
				inverted = true;
				log.debug("inverted: ");
				objectType = rdfsTypeforYagoClass(yagoRelation.domain);
			}
			
			if (specialRelationNames.containsKey(yagoRelation))
				yagoRelationName = specialRelationNames.get(yagoRelation);
			else
				yagoRelationName = YAGO_NS + yagoRelationName;
			
			String type = objectType == RDFSDatatype.rRESOURCE ? DatatypeMappings.XSD_URI : objectType.toString();
			int propertyType = objectType == RDFSDatatype.rRESOURCE ? OntologyMapping.Property.TYPE_OBJECT_PROPERTY : OntologyMapping.Property.TYPE_DATA_PROPERTY; 
			
			log.debug(s + "=>" + yagoRelationName + " " + type);
			
			if (!DatatypeMappings.getInstance().isDatatypeRegistered(type))
				continue;
			
			m_ontoMap.addProperty(yagoRelationName, propertyType);
			m_ontoMap.setPropertyType(yagoRelationName, type);
		}
		
		m_ontoMap.addProperty(YAGO_NS + "confidence", OntologyMapping.Property.TYPE_DATA_PROPERTY);
		m_ontoMap.setPropertyType(YAGO_NS + "confidence", DatatypeMappings.XSD_NS + "float");
		
		for (OntologyMapping.Property p : m_ontoMap.getProperties())
			log.debug(p);
	}
	
	private void addConfidenceValue(long id, long idHash, String value) {
		Object[] o = new Object[] {id, idHash, value};
		m_confidences.add(o);
	}
	
	private void insertConfidenceValues() throws SQLException {
		PreparedStatement cpst = m_conn.prepareStatement("INSERT INTO " + m_ontoMap.getPropertyTableName(YAGO_NS + "confidence") +
			"(id, subject, object) VALUES (?,?,?)");
		for (Object[] o : m_confidences) {
			cpst.setLong(1, (Long)o[0]);
			cpst.setLong(2, (Long)o[1]);
			cpst.setFloat(3, Float.valueOf((String)o[2]));
			cpst.executeUpdate();
		}
		cpst.close();
		m_confidences = new ArrayList<Object[]>();
	}

	@Override
	protected void importData() throws ImportException {
		Announce.setAnnounce(false);
		
		int statements = 0;
		Object lastObject = null;
		
		try {
			for (File factDir : new File(m_yagoDir, "facts").listFiles()) {
				if (!factDir.isDirectory())
					continue;
				
				Relation yagoRelation = Relation.valueOf(factDir.getName());
				
//				if (yagoRelation != Relation.BORNONDATE)
//					continue;
				
				if (yagoRelation == Relation.ISCALLED || yagoRelation == Relation.INLANGUAGE || yagoRelation == Relation.MEANS)
					continue;
	
				String yagoRelationName = yagoRelation.name();
	
				RDFSDatatype objectType = rdfsTypeforYagoClass(yagoRelation.range);
	
				boolean inverted = false;
				if (rdfsTypeforYagoClass(yagoRelation.domain) != RDFSDatatype.rRESOURCE) {
					inverted = true;
					log.debug("inverted: ");
					objectType = rdfsTypeforYagoClass(yagoRelation.domain);
				}
				
				String propertyUri;
				if (specialRelationNames.containsKey(yagoRelation))
					propertyUri = specialRelationNames.get(yagoRelation);
				else
					propertyUri = YAGO_NS + yagoRelationName;
	
				if (!m_ontoMap.isPropertyMapped(propertyUri))
					continue;
			
//			boolean arg1isClass = inverted ? 
//				yagoRelation.range.isSubClassOf(YagoClass.CLASS) : yagoRelation.domain.isSubClassOf(YagoClass.CLASS);
				
				PreparedStatement pst = m_conn.prepareStatement("INSERT INTO " + m_ontoMap.getPropertyTableName(propertyUri) + 
						"(id,subject,object) VALUES (?,?,?)");

				for (File f : factDir.listFiles()) {
//					if (!deductiveClosure && f.getName().equalsIgnoreCase("IsAExtractor.txt"))
//						continue;

					int c = 0;
					for (String l : new FileLines(new UTF8Reader(f, "Parsing " + f.getName()))) {
//						if (c >= 1000)
//							break;
//						c++;
						
						String[] split = l.split("\t");
						if (split.length != 4)
							continue;
						
						String id = split[0];
						String arg1 = split[inverted ? 2 : 1];
						String arg2 = split[inverted ? 1 : 2];
						String confidence = split[3];
						
//						log.debug(id + " " + arg1 + " " + arg2 + " " + confidence);
						
						id = YAGO_NS + "f" + id;
						
						String subject;
						
						if (yagoRelation.domain == YagoClass.FACT && !inverted)
							subject = YAGO_NS + "f" + arg1;
						else 
							subject = asURI(arg1);

						long idHash = hash(id);
						
						pst.clearParameters();
						pst.setLong(1, idHash);
						pst.setLong(2, hash(subject));

						if (m_ontoMap.getPropertyType(propertyUri) == OntologyMapping.Property.TYPE_DATA_PROPERTY) {
							Object o = m_ontoMap.convertToDBObject(objectType.asLiteral(arg2), m_ontoMap.getXSDTypeForProperty(propertyUri));
							if (o != null) {
								lastObject = o;
								pst.setObject(3, o, m_ontoMap.getJDBCTypeForProperty(propertyUri));
								try {
									pst.executeUpdate();
//									pst.addBatch();
								}
								catch (SQLException e) {
									System.out.println(e);
								}
								statements++;
							}
							else {
//								log.warn(yagoRelation + " CONVERSION FAILED '" + objectType.asLiteral(arg2) + "'");
							}
						}
						else {
							pst.setLong(3, hash(asURI(arg2)));
							pst.executeUpdate();
//							pst.addBatch();
							statements++;
						}
						
						addConfidenceValue(hash(id + "_c"), idHash, confidence);
						
						if (statements % 1000 == 0) {
//							pst.executeBatch();
//							pst.clearBatch();
							log.info(statements);
						}
						
						if (statements % 20000 == 0) {
							m_conn.commit();
							
							pst.close();
							
							statements += m_confidences.size();
							insertConfidenceValues();
							
							m_conn.commit();
							
							pst = m_conn.prepareStatement("INSERT INTO " + m_ontoMap.getPropertyTableName(propertyUri) + 
								"(id,subject,object) VALUES (?,?,?)");
							
							log.info(statements);
						}
					}
				}
				
				// insert all left over statements
//				pst.executeBatch();
//				m_conn.commit();
				pst.close();

				log.info(statements);
			}
			
			insertConfidenceValues();
		} catch (FileNotFoundException e) {
			throw new ImportException(e);
		} catch (SQLException e) {
			throw new ImportException(e);
		}
	}
}

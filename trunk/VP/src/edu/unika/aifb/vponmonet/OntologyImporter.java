package edu.unika.aifb.vponmonet;
import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Set;

import org.apache.log4j.Logger;
import org.semanticweb.kaon2.api.KAON2Exception;
import org.semanticweb.kaon2.api.KAON2Manager;
import org.semanticweb.kaon2.api.Ontology;
import org.semanticweb.kaon2.api.owl.axioms.ClassMember;
import org.semanticweb.kaon2.api.owl.axioms.DataPropertyMember;
import org.semanticweb.kaon2.api.owl.axioms.ObjectPropertyMember;
import org.semanticweb.kaon2.api.owl.axioms.SubClassOf;
import org.semanticweb.kaon2.api.owl.elements.DataProperty;
import org.semanticweb.kaon2.api.owl.elements.DataRange;
import org.semanticweb.kaon2.api.owl.elements.Datatype;
import org.semanticweb.kaon2.api.owl.elements.OWLClass;
import org.semanticweb.kaon2.api.owl.elements.ObjectProperty;

public class OntologyImporter extends Importer {
	private Ontology m_ontology;
	private final int INSERT_INTERVAL = 5000;
	
	public OntologyImporter() throws KAON2Exception {
		super();
		log = Logger.getLogger(OntologyImporter.class);
	}
	
	protected void insertSchema() throws ImportException, KAON2Exception {
		int classMember = 0;
		int subClassOf = 0;
		int subPropertyOf = 0;
		int statements = 0;
		
		try {
			PreparedStatement pst = m_conn.prepareStatement("INSERT INTO " + OntologyMapping.TYPE_TABLE + 
					" (subject,object) VALUES (?,?)");
			for (ClassMember cm : m_ontology.createAxiomRequest(ClassMember.class).getAll()) {
				if (cm.getDescription() instanceof OWLClass) {
					pst.setLong(1, hash(cm.getIndividual().getURI()));
					pst.setLong(2, hash(((OWLClass)cm.getDescription()).getURI()));
		
					pst.addBatch();
		
					statements++;
					classMember++;
				}
				
				if (statements % INSERT_INTERVAL == 0) {
					int[] results = pst.executeBatch();
					pst.clearBatch();
					for (int r : results)
						if (r != 1)
							log.error("y: " + r);
					System.out.println(statements);
				}
			}
			pst.executeBatch();
		
			statements = 0;
			pst = m_conn.prepareStatement("INSERT INTO " + OntologyMapping.SUBCLASSOF_TABLE + 
				" (subject,object) VALUES (?,?)");
			for (SubClassOf sc : m_ontology.createAxiomRequest(SubClassOf.class).getAll()) {
				if (sc.getSubDescription() instanceof OWLClass && sc.getSuperDescription() instanceof OWLClass) {
					pst.setLong(1, hash(((OWLClass)sc.getSubDescription()).getURI()));
					pst.setLong(2, hash(((OWLClass)sc.getSuperDescription()).getURI()));
		
					pst.addBatch();
					subClassOf++;
					statements++;
					
					if (statements % INSERT_INTERVAL == 0) {
						int[] results = pst.executeBatch();
						pst.clearBatch();
						for (int r : results)
							if (r != 1)
								log.error("y: " + r);
						System.out.println(statements);
					}
				}
			}
			pst.executeBatch();
		}
		catch (BatchUpdateException e) {
			SQLException c = e.getNextException();
			e.printStackTrace();
//			System.out.println("-----------------------");
//			while (c != null) {
//				c.printStackTrace();
//				c = c.getNextException();
//			}
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
		
		log.info("type table entries: " + classMember);
		log.info("subClassOf table entries: " + subClassOf);
	}

	private void insertData() throws SQLException, KAON2Exception, ImportException {
		
		int objectPropertyInstances = 0;
		int dataPropertyInstances = 0;
		int statements = 0;
		for (String propertyUri : m_ontoMap.getPropertyUris()) {
			PreparedStatement pst = m_conn.prepareStatement("INSERT INTO " + m_ontoMap.getPropertyTableName(propertyUri) + 
					"(subject, object) VALUES (?, ?)");

			if (m_ontoMap.isDataProperty(propertyUri)) {
				DataProperty p = KAON2Manager.factory().dataProperty(propertyUri);
				for (DataPropertyMember dpm : p.getDataPropertyMembers(m_ontology)) {
					pst.setLong(1, hash(dpm.getSourceIndividual().getURI()));
					
//					System.out.println(dpm.getTargetValue() + " " + dpm.getTargetValue().getValue().getClass().getCanonicalName());
					Object o = m_ontoMap.convertToDBObject(dpm.getTargetValue().getValue(), m_ontoMap.getXSDTypeForProperty(p.getURI()));
					pst.setObject(2, o, m_ontoMap.getJDBCTypeForProperty(p.getURI()));
					
					pst.addBatch();
					statements++;
					
					dataPropertyInstances++;
					
					if (statements % INSERT_INTERVAL == 0) {
						int[] results = pst.executeBatch();
						pst.clearBatch();
						for (int r : results)
							if (r != 1)
								log.error("a: " + r);
						System.out.println(statements);
					}
				}
			}
			else {
				ObjectProperty p = KAON2Manager.factory().objectProperty(propertyUri);
				for (ObjectPropertyMember opm : p.getObjectPropertyMembers(m_ontology)) {
					pst.setLong(1, hash(opm.getSourceIndividual().getURI()));
					pst.setLong(2, hash(opm.getTargetIndividual().getURI()));
					
					pst.addBatch();
					statements++;
					
					objectPropertyInstances++;
					
					if (statements % INSERT_INTERVAL == 0) {
						int[] results = pst.executeBatch();
						pst.clearBatch();
						for (int r : results)
							if (r != 1)
								log.error("b: " + r);
						System.out.println(statements);
					}
				}
			}
			int[] results = pst.executeBatch();
			for (int r : results)
				if (r != 1)
					log.error("c: " + r);
		}
		
		log.info("data property instances: " + dataPropertyInstances);
		log.info("object property instances: " + objectPropertyInstances);
	}

	public void createOntologyMapping() throws Exception {
		m_ontoMap = new OntologyMapping();
		
		for (DataProperty p : m_ontology.createEntityRequest(DataProperty.class).getAll()) {
			m_ontoMap.addProperty(p.getURI(), OntologyMapping.Property.TYPE_DATA_PROPERTY);
			String datatypeUri = "";
			Set<DataRange> ranges = p.getRangeDataRanges(m_ontology);
			if (ranges.size() > 1)
				throw new ImportException("data property " + p + " has more than one data range");
			else if (ranges.size() == 0) {
				datatypeUri = DatatypeMappings.XSD_NS + "string";
				log.warn("data property " + p.getURI() + " has no datatype information, assuming " + datatypeUri);
			}
			else {
				Object o = ranges.toArray()[0];
				if (!(o instanceof Datatype))
					throw new ImportException("data range of data property " + p + " is not a datatype object");
				datatypeUri = ((Datatype)o).getURI();
			}
			
			log.info("data property " + p.getURI() + " has type " + datatypeUri);
			m_ontoMap.setPropertyType(p.getURI(), datatypeUri);
		}
		
		for (ObjectProperty p : m_ontology.createEntityRequest(ObjectProperty.class).getAll()) {
			m_ontoMap.addProperty(p.getURI(), OntologyMapping.Property.TYPE_OBJECT_PROPERTY);
			m_ontoMap.setPropertyType(p.getURI(), DatatypeMappings.XSD_URI);
		}
	}
	
	public Ontology getOntology() {
		return m_ontology;
	}
	
	public void setOntology(Ontology onto) throws ImportException {
		m_ontology = onto;
	}
	
	protected void importData() throws ImportException {
		try {
			insertSchema();
			insertData();
		} catch (KAON2Exception e) {
			throw new ImportException(e);
		} catch (SQLException e) {
			throw new ImportException(e);
		}
	}
}

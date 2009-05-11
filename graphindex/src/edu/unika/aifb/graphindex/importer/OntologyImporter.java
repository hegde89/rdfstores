package edu.unika.aifb.graphindex.importer;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.semanticweb.kaon2.api.Axiom;
import org.semanticweb.kaon2.api.DefaultOntologyResolver;
import org.semanticweb.kaon2.api.KAON2Exception;
import org.semanticweb.kaon2.api.KAON2Manager;
import org.semanticweb.kaon2.api.Ontology;
import org.semanticweb.kaon2.api.OntologyManager;
import org.semanticweb.kaon2.api.owl.axioms.ClassMember;
import org.semanticweb.kaon2.api.owl.axioms.DataPropertyMember;
import org.semanticweb.kaon2.api.owl.axioms.EntityAnnotation;
import org.semanticweb.kaon2.api.owl.axioms.ObjectPropertyMember;
import org.semanticweb.kaon2.api.owl.axioms.SubClassOf;
import org.semanticweb.kaon2.api.owl.axioms.SubDataPropertyOf;
import org.semanticweb.kaon2.api.owl.axioms.SubObjectPropertyOf;
import org.semanticweb.kaon2.api.owl.elements.DataProperty;
import org.semanticweb.kaon2.api.owl.elements.DataRange;
import org.semanticweb.kaon2.api.owl.elements.Datatype;
import org.semanticweb.kaon2.api.owl.elements.OWLClass;
import org.semanticweb.kaon2.api.owl.elements.ObjectProperty;

public class OntologyImporter extends Importer {

	private DefaultOntologyResolver m_resolver;
	private OntologyManager m_ontoManager;
	private Ontology m_ontology;
	private Map<String,String> m_datatypes;
	
	private static final String RDFS_NS = "http://www.w3.org/2000/01/rdf-schema#";
	private static final String RDF_NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
	private static final String XSD_NS = "http://www.w3.org/2001/XMLSchema#";
	private static final String SUBCLASSOF = RDFS_NS + "subClassOf";
	private static final String RDF_TYPE = RDF_NS + "type";
	
	public OntologyImporter() {
		super();
		log = Logger.getLogger(OntologyImporter.class);
		m_datatypes = new HashMap<String,String>();
	}
	
	private void loadOntology() throws KAON2Exception, InterruptedException {
		m_resolver = new DefaultOntologyResolver();
		m_ontoManager = KAON2Manager.newOntologyManager();
		m_ontoManager.setOntologyResolver(m_resolver);

		for (String fileName : m_files) {
			m_ontoManager.openOntology(((DefaultOntologyResolver)m_ontoManager.getOntologyResolver()).registerOntology(new File(fileName)), new HashMap<String,Object>());
			log.debug("opened " + fileName);
		}
		
		((DefaultOntologyResolver)m_ontoManager.getOntologyResolver()).registerReplacement("http://example.org/import_ontology", "file:import_ontology.owl");
		m_ontology = m_ontoManager.createOntology("http://example.org/import_ontology", new HashMap<String,Object>());
		
		for (Ontology onto : m_ontoManager.getOntologies())
			m_ontology.addToImports(onto);
	}
	
	private String getDatatype(DataProperty p) throws KAON2Exception {
		String datatypeUri = m_datatypes.get(p.getURI());
		if (datatypeUri == null) {
			Set<DataRange> ranges = p.getRangeDataRanges(m_ontology);
			if (ranges.size() > 1) {
				m_datatypes.put(p.getURI(), "##");
			}
			else if (ranges.size() == 0) {
				datatypeUri = XSD_NS + "string";
			}
			else {
				Object o = ranges.toArray()[0];
				if (o instanceof Datatype)
					datatypeUri = ((Datatype)o).getURI();
				else
					m_datatypes.put(p.getURI(), "##");
			}
		}
		else {
			if (datatypeUri.equals("##"))
				datatypeUri = null;
		}
		
		return datatypeUri;
	}
	
	@Override
	public void doImport() {
		try {
			loadOntology();
			log.info("ontologies loaded");
			
			int axioms = 0, totalAxioms = 0;
			Set<String> classes = new HashSet<String>();
			
			for (Axiom a : m_ontology.createAxiomRequest().getAll()) {
				totalAxioms++;
				classes.add(a.getClass().getCanonicalName());
				if (a instanceof EntityAnnotation)
					continue;
				
				if (!(a instanceof OWLClass || a instanceof SubClassOf || a instanceof ObjectPropertyMember || a instanceof ClassMember
						|| a instanceof DataPropertyMember || a instanceof SubObjectPropertyOf || a instanceof SubDataPropertyOf))
					continue;
				
				if (a instanceof SubClassOf) {
					SubClassOf sco = (SubClassOf)a;
					if (sco.getSubDescription() instanceof OWLClass && sco.getSuperDescription() instanceof OWLClass) {
						OWLClass sub = (OWLClass)sco.getSubDescription();
						OWLClass sup = (OWLClass)sco.getSuperDescription();
						
						m_sink.triple(sub.getURI(), SUBCLASSOF, sup.getURI(), null);
						axioms++;
					}
				}
				
				if (a instanceof SubObjectPropertyOf) {
					SubObjectPropertyOf sop = (SubObjectPropertyOf)a;
//					log.debug(sop);
				}
				
				if (a instanceof SubDataPropertyOf) {
					
				}
				
				if (a instanceof ObjectPropertyMember) {
					ObjectPropertyMember opm = (ObjectPropertyMember)a;
					m_sink.triple(opm.getSourceIndividual().getURI(), ((ObjectProperty)opm.getObjectProperty()).getURI(), opm.getTargetIndividual().getURI(), null);
					axioms++;
				}
				
				if (a instanceof DataPropertyMember) {
					DataPropertyMember dpm = (DataPropertyMember)a;
					String datatype = getDatatype((DataProperty)dpm.getDataProperty());
//					log.debug(dpm);
					if (datatype != null) {
						m_sink.triple(dpm.getSourceIndividual().getURI(), ((DataProperty)dpm.getDataProperty()).getURI(), dpm.getTargetValue().getValue().toString(), null);
						axioms++;
					}
				}
				
				if (a instanceof ClassMember) {
					ClassMember cm = (ClassMember)a;
					if (cm.getDescription() instanceof OWLClass) {
						OWLClass c = (OWLClass)cm.getDescription();
						m_sink.triple(cm.getIndividual().getURI(), RDF_TYPE, c.getURI(), null);
						axioms++;
					}
				}
				
//				log.debug(a + " " + a.getClass());
			}
			log.debug("axioms: " + axioms + "/" + totalAxioms);
//			log.debug(classes);
			
//			for (Axiom a : m_ontology.createAxiomRequest().setCondition("superDescription", OWLClass.OWL_THING).getAll())
//				log.debug(a);
			
			m_ontoManager.close();
			m_ontology = null;
			m_resolver = null;
			m_datatypes.clear();
			
		} catch (KAON2Exception e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}

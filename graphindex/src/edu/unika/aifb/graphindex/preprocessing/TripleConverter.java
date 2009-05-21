package edu.unika.aifb.graphindex.preprocessing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import edu.unika.aifb.graphindex.importer.TripleSink;
import edu.unika.aifb.graphindex.util.TypeUtil;
import edu.unika.aifb.graphindex.util.Util;

public class TripleConverter implements TripleSink {

	private String m_outputDirectory;
	private Map<Long,String> m_propertyHashes;
	private Map<Long,String> m_hashes;
	private Set<String> m_edgeSet = new HashSet<String>();
	private TreeSet<String> m_conSet, m_relSet, m_attrSet, m_entSet;
	private PrintWriter m_out;
	private int m_triples;
	private static final Logger log = Logger.getLogger(TripleConverter.class);
	

	
	public TripleConverter(String outputDirectory) throws IOException {
		m_outputDirectory = outputDirectory;
		m_out = new PrintWriter(new BufferedWriter(new FileWriter(outputDirectory + "/input.ht")));
		m_propertyHashes = new HashMap<Long,String>();
		m_hashes = new HashMap<Long,String>();
		m_triples = 0;
		
		m_conSet = new TreeSet<String>();
		m_relSet = new TreeSet<String>();
		m_attrSet = new TreeSet<String>();
		m_entSet = new TreeSet<String>();
	}
	
	public void triple(String s, String p, String o, String objectType) {
		if (TypeUtil.getSubjectType(p, o).equals(TypeUtil.ENTITY)
				&& TypeUtil.getObjectType(p, o).equals(TypeUtil.CONCEPT)
				&& TypeUtil.getPredicateType(p, o).equals(TypeUtil.TYPE)) {
			m_conSet.add(o);
			m_entSet.add(s);
		}
		if (TypeUtil.getSubjectType(p, o).equals(TypeUtil.ENTITY)
				&& TypeUtil.getObjectType(p, o).equals(TypeUtil.ENTITY)
				&& TypeUtil.getPredicateType(p, o).equals(TypeUtil.RELATION)) {
			m_relSet.add(p);
			m_entSet.add(s);
			m_entSet.add(o);
		}
		if (TypeUtil.getSubjectType(p, o).equals(TypeUtil.ENTITY)
				&& TypeUtil.getObjectType(p, o).equals(TypeUtil.LITERAL)
				&& TypeUtil.getPredicateType(p, o).equals(TypeUtil.ATTRIBUTE)) {
			m_attrSet.add(p);
			m_entSet.add(s);
		}
		
		m_edgeSet.add(p);
		
		long sh = Util.hash(s);
		long ph = Util.hash(p);
		long oh = Util.hash(o);
		
		m_out.println(sh + "\t" + ph + "\t" + oh + "\t" + (o.startsWith("http") ? "e" : "d"));
		
		if (!m_propertyHashes.containsKey(ph))
			m_propertyHashes.put(ph, p);
	
		if (!m_hashes.containsKey(sh))
			m_hashes.put(sh, s);

		if (!m_hashes.containsKey(oh))
			m_hashes.put(oh, o);
		
		m_triples++;

		if (m_triples % 1000000 == 0)
			log.debug("triples: " + m_triples);
	}
	
	public void write() throws IOException {
		m_out.close();
		
		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(m_outputDirectory + "/hashes")));
		for (long hash : m_hashes.keySet()) {
			out.println(hash + "\t" + m_hashes.get(hash));
		}
		out.close();
		
		out = new PrintWriter(new BufferedWriter(new FileWriter(m_outputDirectory + "/propertyhashes")));
		for (long hash : m_propertyHashes.keySet()) {
			out.println(hash + "\t" + m_propertyHashes.get(hash));
		}
		out.close();
		
		out = new PrintWriter(new BufferedWriter(new FileWriter(m_outputDirectory + "/concepts")));
		for (String con : m_conSet) {
			out.println(con);
		}
		out.close();
		
		out = new PrintWriter(new BufferedWriter(new FileWriter(m_outputDirectory + "/relations")));
		for (String rel : m_relSet) {
			out.println(rel);
		}
		out.close();
		
		out = new PrintWriter(new BufferedWriter(new FileWriter(m_outputDirectory + "/attributes")));
		for (String attr : m_attrSet) {
			out.println(attr);
		}
		out.close();
		
		out = new PrintWriter(new BufferedWriter(new FileWriter(m_outputDirectory + "/entities")));
		for (String ent : m_entSet) {
			out.println(ent);
		}
		out.close();
	}

	public Set<String> getEdgeSet() {
		return m_edgeSet;
	}
}

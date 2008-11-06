package edu.unika.aifb.graphindex.preprocessing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.importer.TripleSink;
import edu.unika.aifb.graphindex.util.Util;

public class TripleConverter implements TripleSink {

	private String m_outputDirectory;
	private Map<Long,String> m_propertyHashes;
	private Map<Long,String> m_hashes;
	private PrintWriter m_out;
	private int m_triples;
	private static final Logger log = Logger.getLogger(TripleConverter.class);
	
	public TripleConverter(String outputDirectory) throws IOException {
		m_outputDirectory = outputDirectory;
		m_out = new PrintWriter(new BufferedWriter(new FileWriter(outputDirectory + "/input.ht")));
		m_propertyHashes = new HashMap<Long,String>();
		m_hashes = new HashMap<Long,String>();
		m_triples = 0;
	}
	
	public void triple(String s, String p, String o) {
		long sh = Util.hash(s);
		long ph = Util.hash(p);
		long oh = Util.hash(o);
		
		m_out.println(sh + "\t" + ph + "\t" + oh);
		
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
	}
}

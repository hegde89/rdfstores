package edu.unika.aifb.graphindex.data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.Util;
import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.TripleSink;

public class VertexListBuilder implements TripleSink {

	private VertexCollection m_vertices;
	private Iterator<File> m_componentFiles;
	private Importer m_importer;
	private static final Logger log = Logger.getLogger(VertexListBuilder.class);
	private int m_triples;
	private Set<String> m_edges;
	private Set<Long> m_edgeHashes;
	
	public VertexListBuilder(Importer importer, String prefix) throws NumberFormatException, IOException {
		File prefixDir = new File(prefix).getParentFile();
		String namePrefix = new File(prefix).getName();
		
		List<File> componentFiles = new ArrayList<File>();
		for (File file : prefixDir.listFiles())
			if (file.getName().startsWith(namePrefix + ".component"))
				componentFiles.add(file);
		
		Collections.sort(componentFiles, new Comparator<File>() {
			public int compare(File o1, File o2) {
				return ((Long)o1.length()).compareTo(o2.length()) * -1;
			}
		});
		
		m_componentFiles = componentFiles.iterator();
		
		m_importer = importer;
		importer.setTripleSink(this);
		
		m_edges = new HashSet<String>();
		m_edgeHashes = new HashSet<Long>();
	}
	
	public void triple(String src, String edge, String dst) {
		long sh = Util.hash(src);
		IVertex sv = m_vertices.getVertex(sh);
		if (sv == null)
			return;
		
		long oh = Util.hash(dst);
		IVertex ov = m_vertices.getVertex(oh);
		if (ov == null) {
			throw new UnsupportedOperationException("if the subject is in the current component the object should be too");
		}
		
		long eh = Util.hash(edge);

		sv.addToImage(eh, ov);
		m_edges.add(edge);
		m_edgeHashes.add(eh);
		
		if (m_triples % 500000 == 0)
			log.info(" triples: " + m_triples + ", " + Util.memory());
			
		m_triples++;
	}
	
	public Set<String> getEdges() {
		return m_edges;
	}
	
	public void loadVertexList(File componentFile) throws NumberFormatException, IOException {
		long start = System.currentTimeMillis();
		
		m_vertices = VertexFactory.collection();
		m_vertices.loadFromComponentFile(componentFile.getAbsolutePath());
		
		log.info("adding triples");
		m_triples = 0;
		m_importer.doImport();
		
		log.info("triples in component: " + m_triples);
		long duration = (System.currentTimeMillis() - start) / 1000;
		log.info("vertex list created in " + duration + " seconds");
	}
	
	private void saveVertexList(String fileName) throws IOException {
		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(fileName)));

		out.println("edges");
		for (String edgeLabel : m_edges)
			out.println(edgeLabel);

		for (IVertex v : m_vertices) {
			out.println("v " + v.getId());
			for (long edgeLabel : m_edgeHashes) {
				List<IVertex> i = v.getImage(edgeLabel);
				if (i != null) {
					out.println("e " + edgeLabel + " " + i.size());
					for (IVertex iv : i) {
						out.println(iv.getId());
					}
				}
			}
		}
		
		out.close();
	}
	
	public void write(String outPrefix) throws NumberFormatException, IOException {
		for ( ; m_componentFiles.hasNext(); ) {
			File componentFile = m_componentFiles.next();
			String vertexListFile = componentFile.getAbsolutePath().replaceFirst("\\.component", ".vertexlist.component");
			
			loadVertexList(componentFile);
			saveVertexList(vertexListFile);
		}
	}
}

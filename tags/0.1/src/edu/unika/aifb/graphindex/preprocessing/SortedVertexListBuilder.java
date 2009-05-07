package edu.unika.aifb.graphindex.preprocessing;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.data.IVertex;
import edu.unika.aifb.graphindex.data.VertexCollection;
import edu.unika.aifb.graphindex.data.VertexFactory;
import edu.unika.aifb.graphindex.importer.HashedTripleSink;
import edu.unika.aifb.graphindex.importer.Importer;
import edu.unika.aifb.graphindex.importer.ParsingTripleConverter;
import edu.unika.aifb.graphindex.importer.TripleSink;
import edu.unika.aifb.graphindex.util.Util;

public class SortedVertexListBuilder implements HashedTripleSink {

	private VertexCollection m_vertices;
	private Iterator<File> m_componentFiles;
	private Importer m_importer;
	private int m_triples;
	private Set<String> m_edges;
	private Set<Long> m_edgeHashes;
	private String m_prefix;
	
	private IVertex m_currentVertex;
	private Map<Long,List<IVertex>> m_currentImage;
	
	private static final Logger log = Logger.getLogger(SortedVertexListBuilder.class);

	public SortedVertexListBuilder(Importer importer, String componentDirectory) throws NumberFormatException, IOException {
		File componentDir = new File(componentDirectory);
		
		List<File> componentFiles = new ArrayList<File>();
		File[] cf = componentDir.listFiles();
		if(cf != null)
		for (File file : componentDir.listFiles())
			if (file.getName().startsWith("component") && !file.getName().endsWith("vertexlist"))
				componentFiles.add(file);
		
		// largest components first
		Collections.sort(componentFiles, new Comparator<File>() {
			public int compare(File o1, File o2) {
				return ((Long)o1.length()).compareTo(o2.length()) * -1;
			}
		});
		
		m_componentFiles = componentFiles.iterator();
		
		m_importer = importer;
		importer.setTripleSink(new ParsingTripleConverter(this));
		
		m_edges = new HashSet<String>();
		m_edgeHashes = new HashSet<Long>();
	}
	
	public void triple(long s, long p, long o) {
		if (m_currentVertex == null) {
			m_currentVertex = m_vertices.getVertex(s);
			m_currentImage = new HashMap<Long,List<IVertex>>();
		}
		else if (s != m_currentVertex.getId()) {
			if (m_currentImage != null) {
				for (long label : m_currentImage.keySet())
					m_currentVertex.setImage(label, m_currentImage.get(label));
			}
			
			m_currentImage = new HashMap<Long,List<IVertex>>();
			m_currentVertex = m_vertices.getVertex(s);
		}
		
		if (m_currentVertex == null)
			return;
		
		IVertex ov = m_vertices.getVertex(o);
		if (ov == null) {
			throw new UnsupportedOperationException("if the subject is in the current component the object should be too");
		}
		
//		m_currentVertex.addToImage(p, ov);
		List<IVertex> image = m_currentImage.get(p);
		if (image == null) {
			image = new ArrayList<IVertex>();
			m_currentImage.put(p, image);
		}
		image.add(ov);
		m_edgeHashes.add(p);
		
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
		
		if (m_currentVertex != null && m_currentImage != null) {
			for (long label : m_currentImage.keySet()) 
				m_currentVertex.setImage(label, m_currentImage.get(label));
		}
		
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
	
	public void write() throws NumberFormatException, IOException {
		for ( ; m_componentFiles.hasNext(); ) {
			File componentFile = m_componentFiles.next();
			String vertexListFile = componentFile.getAbsolutePath() + ".vertexlist";
			
			loadVertexList(componentFile);
			saveVertexList(vertexListFile);
		}
	}
}

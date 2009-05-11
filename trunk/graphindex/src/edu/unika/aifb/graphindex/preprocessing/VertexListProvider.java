package edu.unika.aifb.graphindex.preprocessing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
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
import edu.unika.aifb.graphindex.data.MapVertexCollection;
import edu.unika.aifb.graphindex.data.VertexCollection;
import edu.unika.aifb.graphindex.data.VertexFactory;


public class VertexListProvider {
	private Iterator<File> m_componentFiles;
	private File m_componentFile;
	private Set<String> m_edges;
	private static final Logger log = Logger.getLogger(VertexListProvider.class);

	public VertexListProvider(String componentDirectory) {
		File componentDir = new File(componentDirectory);
		
		List<File> componentFiles = new ArrayList<File>();
		for (File file : componentDir.listFiles())
			if (file.getName().startsWith("component") && !file.getName().endsWith("vertexlist") && !file.getName().contains("."))
				componentFiles.add(file);
		
		Collections.sort(componentFiles, new Comparator<File>() {
			public int compare(File o1, File o2) {
				return ((Long)o1.length()).compareTo(o2.length()) * -1;
			}
		});
		
		m_componentFiles = componentFiles.iterator();
	}

	public List<IVertex> getInverted() throws IOException {
		m_edges = new HashSet<String>();
		VertexCollection vc = VertexFactory.collection();
		vc.loadFromComponentFile(m_componentFile.getAbsolutePath());

		BufferedReader in = new BufferedReader(new FileReader(m_componentFile.getAbsolutePath() + ".vertexlist"));
		String input;
		
		IVertex currentVertex = null;
		long currentLabel = 0;
		
		boolean inEdgeList = false;
		boolean inImage = false;
		int vertices = 0, triples = 0;
		
		log.info("loading inverted vertex list");
		long t1 = 0, t2 = 0, t3 = 0, t4 = 0;
		long ts = 0;
		while ((input = in.readLine()) != null) {
			input = input.trim();
			
			if (input.equals("edges")) {
				inEdgeList = true;
				continue;
			}
			
			if (input.startsWith("v")) {
				inEdgeList = false;
				inImage = true;
				
				ts = System.currentTimeMillis();
				currentVertex = vc.getVertex(Long.parseLong(input.substring(input.lastIndexOf(" ") + 1)));
				t1 += System.currentTimeMillis() - ts;
				
				if (input.charAt(1) == 'd')
					currentVertex.setDataValue(true);
				
				vertices++;
				
				continue;
			}
			
			if (input.startsWith("e") && inImage) {
				ts = System.currentTimeMillis();
				String[] t = input.split(" ");
				long edgeLabel = Long.parseLong(t[1]);
				currentLabel = edgeLabel;
				t2 += System.currentTimeMillis() - ts;
				continue;
			}
			
			if (inEdgeList) {
				m_edges.add(input);
			}
			else if (inImage) {
				ts = System.currentTimeMillis();
				IVertex target = vc.getVertex(Long.parseLong(input));
				t3 += System.currentTimeMillis() - ts;

				ts = System.currentTimeMillis();
				target.addToImage(currentLabel, currentVertex);
				t4 += System.currentTimeMillis() - ts;
				triples++;
			}
			
			if (triples % 1000000 == 0 && triples > 0)
				log.debug(" inverted loaded " + triples + " triples");
		}
		log.debug("t1: " + t1 + ", t2: " + t2 + ", t3: " + t3 + ", t4: " + t4);
		log.info("inverted vertex list loaded: " + vertices + " vertices, " + triples + " triples");
		
		return vc.toList();
	}
	
	public boolean nextComponent() throws IOException {
		if (!m_componentFiles.hasNext())
			return false;
		
		m_componentFile = m_componentFiles.next();
		return true;
	}
	
	public List<IVertex> getList() throws IOException {
		
		m_edges = new HashSet<String>();
		VertexCollection vc = VertexFactory.collection();
		vc.loadFromComponentFile(m_componentFile.getAbsolutePath());

		BufferedReader in = new BufferedReader(new FileReader(m_componentFile.getAbsolutePath() + ".vertexlist"));
		String input;
		
		IVertex currentVertex = null;
		long currentLabel = 0;
		Map<Long,List<IVertex>> currentImage = null;
		
		boolean inEdgeList = false;
		boolean inImage = false;
		int vertices = 0, triples = 0;
		
		log.info("loading vertex list");
		while ((input = in.readLine()) != null) {
			input = input.trim();
			
			if (input.equals("edges")) {
				inEdgeList = true;
				continue;
			}
			
			if (input.startsWith("v")) {
				inEdgeList = false;
				inImage = true;
				
				if (currentImage != null) {
					for (long label : currentImage.keySet()) {
						currentVertex.setImage(label, currentImage.get(label));
					}
				}
				
				currentImage = new HashMap<Long,List<IVertex>>();
				currentVertex = vc.getVertex(Long.parseLong(input.substring(input.lastIndexOf(" ") + 1)));

				if (input.charAt(1) == 'd')
					currentVertex.setDataValue(true);
				
				vertices++;
				
				continue;
			}
			
			if (input.startsWith("e") && inImage) {
				String[] t = input.split(" ");
				long edgeLabel = Long.parseLong(t[1]);
				int size = Integer.parseInt(t[2]);
				currentImage.put(edgeLabel, new ArrayList<IVertex>(size));
				currentLabel = edgeLabel;
				continue;
			}
			
			if (inEdgeList) {
				m_edges.add(input);
			}
			else if (inImage) {
				IVertex target = vc.getVertex(Long.parseLong(input));
				
				currentImage.get(currentLabel).add(target);
				
				triples++;
			}
			
			if ((triples % 1000000 == 0 && triples > 0))
				log.debug(" loaded " + vertices + " vertices, " + triples + " triples");
		}
		
		log.info("vertex list loaded: " + vertices + " vertices, " + triples + " triples");
		
		return vc.toList();
	}
	
	/* (non-Javadoc)
	 * @see edu.unika.aifb.graphindex.preprocessing.IVertexListProvider#getComponentFile()
	 */
	public File getComponentFile() {
		return m_componentFile;
	}
	
	/* (non-Javadoc)
	 * @see edu.unika.aifb.graphindex.preprocessing.IVertexListProvider#getEdges()
	 */
	public Set<String> getEdges() {
		return m_edges;
	}
}

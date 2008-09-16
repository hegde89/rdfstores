package edu.unika.aifb.graphindex.data;

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


public class VertexListProvider {
	private Iterator<File> m_componentFiles;
	private Set<String> m_edges;
	private static final Logger log = Logger.getLogger(VertexListProvider.class);

	public VertexListProvider(String prefix) {
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
	}
	
	public List<IVertex> nextComponent() throws IOException {
		if (!m_componentFiles.hasNext())
			return null;
		
		File componentFile = m_componentFiles.next();
		
		m_edges = new HashSet<String>();
		VertexCollection vc = VertexFactory.collection();
		vc.loadFromComponentFile(componentFile.getAbsolutePath());

		BufferedReader in = new BufferedReader(new FileReader(componentFile.getAbsolutePath().replaceFirst("\\.component", ".vertexlist.component")));
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
//				String[] t = input.split(" ");
//				long edgeLabel = Long.parseLong(t[0]);
//				IVertex target = vc.getVertex(Long.parseLong(t[1]));
//				currentVertex.addToImage(Long.parseLong(t[0]), target);
				
				IVertex target = vc.getVertex(Long.parseLong(input));
				
//				List<IVertex> image = currentImage.get(edgeLabel);
//				if (image == null) {
//					image = new ArrayList<IVertex>();
//					currentImage.put(edgeLabel, image);
//				}
//				image.add(target);
				
				currentImage.get(currentLabel).add(target);
				
				triples++;
			}
			
			if ((triples % 250000 == 0 && triples > 0))// || triples > 3500000)
				log.debug(" loaded " + vertices + " vertices, " + triples + " triples");
		}
		
		log.info("vertex list loaded: " + vertices + " vertices, " + triples + " triples");
		
		return vc.toList();
	}
	
	public Set<String> getEdges() {
		return m_edges;
	}
}

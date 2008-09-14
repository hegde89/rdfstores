package edu.unika.aifb.graphindex;

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

import edu.unika.aifb.graphindex.graph.IVertex;
import edu.unika.aifb.graphindex.graph.LVertex;
import edu.unika.aifb.graphindex.graph.LVertexM;
import edu.unika.aifb.graphindex.importer.Importer;

public class LVertexSetBuilder implements TripleSink {

	private Map<Long,IVertex> m_vertices;
	private Iterator<File> m_componentFiles;
	private File m_hashesFile;
	private Importer m_importer;
	private static final Logger log = Logger.getLogger(LVertexSetBuilder.class);
	private int m_triples;
	private Map<Long,String> m_h2v;
	private boolean m_reload = false;
	private Set<String> m_edges;
	
	public LVertexSetBuilder(Importer importer, String prefix, boolean reloadForEachComponent, boolean loadHashes) throws NumberFormatException, IOException {
		File prefixDir = new File(prefix).getParentFile();
		String namePrefix = new File(prefix).getName();
		m_hashesFile = new File(prefix + ".hashes");
		
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
		
		m_vertices = new HashMap<Long,IVertex>();
		m_edges = new HashSet<String>();
		m_h2v = new HashMap<Long,String>();
		m_reload = reloadForEachComponent;
		
//		if (loadHashes)
//			loadHashes();
	}
	
	public Map<Long,String> getHashes() {
		return m_h2v;
	}
	
//	private void loadHashes() throws NumberFormatException, IOException {
//		BufferedReader in = new BufferedReader(new FileReader(m_hashesFile));
//		String input;
//		while ((input = in.readLine()) != null) {
//			String[] t = input.split("\t");
//			long hash = Long.parseLong(t[0]);
//			m_h2v.put(hash, t[1]); 
//		}
//		log.debug("hashes loaded, " + Util.memory());
//	}
	
	public void triple(String src, String edge, String dst) {
		long sh = Util.hash(src);
		long oh = Util.hash(dst);
		
		if (!m_vertices.containsKey(sh) || !m_vertices.containsKey(oh))
			return;
		
		m_edges.add(edge);
		m_vertices.get(sh).addToImage(Util.hash(edge), m_vertices.get(oh));
		m_triples++;
	}
	
	public Set<String> getEdges() {
		return m_edges;
	}
	
	public List<IVertex> nextComponent() throws NumberFormatException, IOException {
		if (!m_componentFiles.hasNext())
			return null;
		File componentFile = m_componentFiles.next();
		
		BufferedReader in = new BufferedReader(new FileReader(componentFile));
		
		String input;
		int componentSize = 0;
		while ((input = in.readLine()) != null)
			componentSize++;
		in.close();
		
		log.debug("component size: " + componentSize);
		
		m_edges.clear();
		m_vertices = new HashMap<Long,IVertex>(componentSize + 20, 1.0f);
		log.debug("vertex map initial capacity: " + (componentSize + 20));
		
		log.info("loading " + componentFile);
		in = new BufferedReader(new FileReader(componentFile));
		while ((input = in.readLine()) != null) {
 			long hash = Long.parseLong(input);

			if (m_vertices.size() % 500000 == 0)
				log.info("vertex objects created: " + m_vertices.size() + ", " + Util.memory());

//			m_vertices.put(hash, new LVertex(hash));
			m_vertices.put(hash, new LVertexM(hash));
		}
		log.debug("after loading only vertices: " + Util.memory());

		m_triples = 0;
		m_importer.doImport();
		
//		log.info(Util.memory());
		List<IVertex> vertices = new ArrayList<IVertex>(m_vertices.values());
//		log.info(Util.memory());
		m_vertices = new HashMap<Long,IVertex>();
		log.info("triples in component: " + m_triples);
//		log.info(Util.memory());
//		System.gc();
//		log.info(Util.memory());

		
		return vertices;
	}
}

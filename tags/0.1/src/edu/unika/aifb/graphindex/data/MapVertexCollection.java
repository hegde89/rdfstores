package edu.unika.aifb.graphindex.data;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.util.Util;

public class MapVertexCollection implements VertexCollection {
	private Map<Long,IVertex> m_vertices;
	private static final Logger log = Logger.getLogger(MapVertexCollection.class);

	public MapVertexCollection() {
	}
	
	public int size() {
		return m_vertices.size();
	}
	
	public IVertex addVertex(long id) {
		IVertex v = m_vertices.get(id);
		if (v == null) {
			v = VertexFactory.vertex(id);
			m_vertices.put(id, v);
		}
		return v;
	}

	public IVertex getVertex(long id) {
		return m_vertices.get(id);
	}

	public void loadFromComponentFile(String fileName) throws IOException {
		String input;
		int componentSize = 0;
		BufferedReader in = new BufferedReader(new FileReader(fileName));
		while ((input = in.readLine()) != null)
			componentSize++;
		in.close();
		
		log.debug("component size: " + componentSize);
		
		m_vertices = new HashMap<Long,IVertex>(componentSize + 20, 1.0f);
		log.debug("vertex map initial capacity: " + (componentSize + 20));
		
		log.info("loading " + fileName);
		in = new BufferedReader(new FileReader(fileName));
		while ((input = in.readLine()) != null) {
 			long hash = Long.parseLong(input);

			if (m_vertices.size() % 500000 == 0)
				log.info("vertex objects created: " + m_vertices.size() + ", " + Util.memory());

			addVertex(hash);
		}
		log.debug("after loading vertices: " + Util.memory());
	}

	public List<IVertex> toList() {
		return new ArrayList<IVertex>(m_vertices.values());
	}

	public Iterator<IVertex> iterator() {
		return m_vertices.values().iterator();
	}
}

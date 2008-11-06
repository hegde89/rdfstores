package edu.unika.aifb.graphindex.data;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.util.Util;

public class ListVertexCollection implements VertexCollection {
	List<IVertex> m_vertexList;
	private Comparator<IVertex> m_vlistComparator;
	private static final Logger log = Logger.getLogger(ListVertexCollection.class);
	
	public ListVertexCollection() {
		m_vlistComparator = new Comparator<IVertex>() {
			public int compare(IVertex o1, IVertex o2) {
				return ((Long)o1.getId()).compareTo(o2.getId());
			}
		};
	}
	
	public int size() {
		return m_vertexList.size();
	}
	
	public IVertex addVertex(long id) {
		IVertex v = getVertex(id);
		if (v == null) {
			v = VertexFactory.vertex(id);
			m_vertexList.add(v);
		}
		return v;
	}

	public IVertex getVertex(long id) {
		int idx = Collections.binarySearch(m_vertexList, new LVertexM(id), m_vlistComparator);
		if (idx < 0)
			return null;
		return m_vertexList.get(idx);
	}

	public void loadFromComponentFile(String fileName) throws IOException {
		BufferedReader in = new BufferedReader(new FileReader(fileName));
		String input;
		int componentSize = 0;
		while ((input = in.readLine()) != null)
			componentSize++;
		in.close();
		
		log.debug("component size: " + componentSize);
		m_vertexList = new ArrayList<IVertex>(componentSize + 1);
		
		log.info("loading " + fileName);
		in = new BufferedReader(new FileReader(fileName));
		while ((input = in.readLine()) != null) {
 			long hash = Long.parseLong(input);

			if (m_vertexList.size() % 2500000 == 0)
				log.info("vertex objects created: " + m_vertexList.size() + ", " + Util.memory());

			addVertex(hash);
		}
//		log.debug("after loading vertices: " + Util.memory());
		
		Collections.sort(m_vertexList, m_vlistComparator);
		
		System.gc();
		log.debug("vertex list loaded and sorted, " + Util.memory());
	}

	public List<IVertex> toList() {
		return m_vertexList;
	}

	public Iterator<IVertex> iterator() {
		return m_vertexList.iterator();
	}
}

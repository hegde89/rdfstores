/**
 * 
 */
package edu.unika.aifb.graphindex.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.unika.aifb.graphindex.algorithm.rcp.RCPFast;
import edu.unika.aifb.graphindex.util.Util;

public class LVertex extends AbstractVertex {
	private Map<Long,List<IVertex>> m_image;
	
	public LVertex(long id) {
		super(id);
		m_image = new HashMap<Long,List<IVertex>>();
	}
	
	public Set<Long> getEdgeLabels() {
		return m_image.keySet();
	}
	
	public List<IVertex> getImage(long label) {
		return m_image.get(label);
	}
	
	public void addToImage(long label, IVertex v) {
		List<IVertex> image = m_image.get(label);
		if (image == null) {
			image = new ArrayList<IVertex>();
			m_image.put(label, image);
		}
//		if (!image.contains(v))
			image.add(v);
//		else
//			System.out.println("blök");
	}
	
	public void setImage(long label, List<IVertex> image) {
		m_image.put(label, image);
	}
}
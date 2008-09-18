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

import edu.unika.aifb.graphindex.Util;
import edu.unika.aifb.graphindex.algorithm.rcp.RCPFast;

public class LVertex extends AbstractVertex {
	private Map<Long,Integer> m_info;
	private Map<Long,Integer> m_sInfo;
	private Map<Long,List<IVertex>> m_image;
	
	public LVertex(long id) {
		super(id);
		m_image = new HashMap<Long,List<IVertex>>();
		m_info = new HashMap<Long,Integer>();
		m_sInfo = new HashMap<Long,Integer>();
	}
	
	public Map<Long,Integer> getInfo() {
		return m_info;
	}

	public Map<Long,Integer> getSInfo() {
		return m_sInfo;
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
		if (!image.contains(v))
			image.add(v);
	}
	
	public void setImage(long label, List<IVertex> image) {
		m_image.put(label, image);
	}
	
	public void clearInfo() {
		m_info = new HashMap<Long,Integer>();
		m_sInfo = new HashMap<Long,Integer>();
	}
	
	public int getInfo(long label) {
		return m_info.get(label);
	}

	public int getSInfo(long label) {
		return m_sInfo.get(label);
	}

	public void incInfo(long label) {
		if (!m_info.containsKey(label))
			m_info.put(label, 1);
		else
			m_info.put(label, m_info.get(label) + 1);
	}

	public void incSInfo(long label) {
		if (!m_sInfo.containsKey(label))
			m_sInfo.put(label, 1);
		else
			m_sInfo.put(label, m_sInfo.get(label) + 1);
	}
	
	public void decSInfo(long label) {
		if (m_sInfo.containsKey(label))
			m_sInfo.put(label, m_sInfo.get(label) - 1);
	}

	public void setInfo(long label, int val) {
		m_info.put(label, val);
	}

	public void setSInfo(long label, int val) {
		m_sInfo.put(label, val);
	}
}
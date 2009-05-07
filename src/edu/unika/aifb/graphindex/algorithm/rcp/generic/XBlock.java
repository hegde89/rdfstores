/**
 * 
 */
package edu.unika.aifb.graphindex.algorithm.rcp.generic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class XBlock<V,E> {
	private class Info {
		public E label;
		public int sval;
		
		public Info(E label, int sval) {
			this.label = label;
			this.sval = sval;
		}
	}
	private LinkedList<Block<V,E>> m_blocks;
	private Map<GVertex<V,E>,List<Info>> m_info;
	
	public XBlock() {
		m_blocks = new LinkedList<Block<V,E>>();
		m_info = new HashMap<GVertex<V,E>,List<Info>>();
	}
	
	public XBlock(Block<V,E> b) {
		this();
		addBlock(b);
	}
	
	public void calcInfo() {
		for (Block<V,E> block : m_blocks) {
			for (GVertex<V,E> v : block) {
				for (E label : v.getEdgeLabels()) {
					for (GVertex<V,E> x : v.getImage(label)) {
						List<Info> infos = m_info.get(x);
						if (infos == null) {
							infos = new ArrayList<Info>();
							m_info.put(x, infos);
						}
						boolean found = false;
						for (Info i : infos) {
							if (i.label.equals(label)) {
								i.sval++;
								found = true;
							}
						}
						if (!found)
							infos.add(new Info(label, 1));
					}
				}
			}
		}
	}
	
	public Integer getInfo(GVertex<V,E> v, E label) {
		List<Info> infos = m_info.get(v);
		if (infos == null)
			return null;
		
		for (Info i : infos) {
			if (i.label.equals(label))
				return i.sval;
		}
		return null;
	}
	
	public void decInfo(GVertex<V,E> v, E label, int dec) {
		List<Info> infos = m_info.get(v);
		if (infos == null)
			return;
		for (Iterator<Info> it = infos.iterator(); it.hasNext(); ) {
			Info i = it.next();
			if (i.label.equals(label)) {
				i.sval -= dec;
				if (i.sval == 0) {
					it.remove();
				}
			}
		}
		if (infos.size() == 0)
			m_info.remove(v);
	}
	
	public Block<V,E> getFirstBlock() {
		return m_blocks.getFirst();
	}
	
	public Block<V,E> getSecondBlock() {
		return m_blocks.get(1);
	}
	
	public void addBlock(Block<V,E> b) {
		m_blocks.add(b);
		b.setXBlock(this);
	}

	public void remove(Block<V,E> block) {
		m_blocks.remove(block);
	}
	
	public boolean isCompound() {
		return m_blocks.size() > 1;
	}
	
	public int numberOfBlocks() {
		return m_blocks.size();
	}
	
	public int numberOfVertices() {
		return 0;
	}
	
	public String toString() {
		String s = "(xb ";
		String comma = "";
		for (Block<V,E> b : m_blocks) {
			s += comma + b;
			comma = ", ";
		}
		return s + ")";
	}

	public List<Block<V,E>> getBlocks() {
		return m_blocks;
	}
}
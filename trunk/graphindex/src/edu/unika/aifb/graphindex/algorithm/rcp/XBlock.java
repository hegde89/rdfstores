/**
 * 
 */
package edu.unika.aifb.graphindex.algorithm.rcp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.unika.aifb.graphindex.data.IVertex;


public class XBlock {
	private class Info {
		public long label;
		public int sval;
		
		public Info(long label, int sval) {
			this.label = label;
			this.sval = sval;
		}
	}
	private LinkedList<Block> m_blocks;
	private Map<IVertex,List<Info>> m_info;
	
	public XBlock() {
		m_blocks = new LinkedList<Block>();
		m_info = new HashMap<IVertex,List<Info>>();
	}
	
	public XBlock(Block b) {
		this();
		addBlock(b);
	}
	
	public void calcInfo() {
		for (Block block : m_blocks) {
			for (IVertex v : block) {
				for (long label : v.getEdgeLabels()) {
					for (IVertex x : v.getImage(label)) {
						List<Info> infos = m_info.get(x);
						if (infos == null) {
							infos = new ArrayList<Info>();
							m_info.put(x, infos);
						}
						boolean found = false;
						for (Info i : infos) {
							if (i.label == label) {
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
	
	public Integer getInfo(IVertex v, long label) {
		List<Info> infos = m_info.get(v);
		if (infos == null)
			return null;
		
		for (Info i : infos) {
			if (i.label == label)
				return i.sval;
		}
		return null;
	}
	
	public void decInfo(IVertex v, long label, int dec) {
		List<Info> infos = m_info.get(v);
		if (infos == null)
			return;
		for (Iterator<Info> it = infos.iterator(); it.hasNext(); ) {
			Info i = it.next();
			if (i.label == label) {
				i.sval -= dec;
				if (i.sval == 0) {
					it.remove();
				}
			}
		}
		if (infos.size() == 0)
			m_info.remove(v);
	}
	
	public Block getFirstBlock() {
		return m_blocks.getFirst();
	}
	
	public Block getSecondBlock() {
		return m_blocks.get(1);
	}
	
	public void addBlock(Block b) {
		m_blocks.add(b);
		b.setXBlock(this);
	}

	public void remove(Block block) {
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
		for (Block b : m_blocks) {
			s += comma + b;
			comma = ", ";
		}
		return s + ")";
	}

	public List<Block> getBlocks() {
		return m_blocks;
	}
}
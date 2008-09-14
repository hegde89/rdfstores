package edu.unika.aifb.graphindex.graph;

import edu.unika.aifb.graphindex.Util;
import edu.unika.aifb.graphindex.algorithm.RCP.Block;

public class SVertex {
	private String m_label;
	private Block m_block;
	
	public SVertex(String label) {
		m_label = label;
//		System.out.println("new vertex " + label);
	}
	
	public String getLabel() {
		return m_label;
	}
	
	public void setBlock(Block block) {
		m_block = block;
//		System.out.println(m_label + " block set to " + m_block.getName());
	}
	
	public Block getBlock() {
		return m_block;
	}
	
	public String toString() {
		return Util.truncateUri(m_label) + "(" + (m_block != null ? m_block.getName() : null) + ")";
	}
	
	@Override
	public int hashCode() {
		return m_label.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SVertex other = (SVertex)obj;
//		if (other.getLabel() == getLabel()) // label is interned
//			return true;
		if (other.getLabel().equals(getLabel()))
			return true;
		return false;
	}
}

/**
 * 
 */
package edu.unika.aifb.graphindex.algorithm.rcp;

import java.util.LinkedList;
import java.util.List;


public class XBlock {
	private LinkedList<Block> m_blocks;
	
	public XBlock() {
		m_blocks = new LinkedList<Block>();
	}
	
	public XBlock(Block b) {
		this();
		addBlock(b);
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
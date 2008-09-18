/**
 * 
 */
package edu.unika.aifb.graphindex.algorithm.rcp;

import java.util.ArrayList;
import java.util.List;

import edu.unika.aifb.graphindex.data.IVertex;

public class Partition {
	List<Block> m_blocks;
	
	public Partition() {
		m_blocks = new ArrayList<Block>();
	}
	
	public void add(Block block) {
		m_blocks.add(block);
	}
	
	public void remove(Block block) {
		m_blocks.remove(block);
	}
	
	public List<Block> getBlocks() {
		return m_blocks;
	}
	
	private boolean stable(Block other) {
		for (Block b : m_blocks) {
			if (!b.stable(other)) {
//				System.out.println(b + " not stable to " + other);
				return false;
			}
		}
		return true;
	}
	
	public boolean stable(List<IVertex> vertices) {
		for (Block b : m_blocks) {
			if (!b.stable(vertices))
				return false;
		}
		return true;
	}
	
	public boolean stable() {
		for (Block b : m_blocks) {
			if (!stable(b))
				return false;
		}
		return true;
	}
	
	public String toString() {
		String s = "";
		String comma = "";
		for (Block b : m_blocks) {
			s += comma + b;
			comma = ", ";
		}
		return s;
	}
}
/**
 * 
 */
package edu.unika.aifb.graphindex.algorithm.rcp.generic;

import java.util.ArrayList;
import java.util.List;

import edu.unika.aifb.graphindex.data.IVertex;

public class Partition<V,E> {
	List<Block<V,E>> m_blocks;
	
	public Partition() {
		m_blocks = new ArrayList<Block<V,E>>();
	}
	
	public void add(Block<V,E> block) {
		m_blocks.add(block);
	}
	
	public void remove(Block<V,E> block) {
		m_blocks.remove(block);
	}
	
	public List<Block<V,E>> getBlocks() {
		return m_blocks;
	}
	
//	private boolean stable(Block<V,E> other) {
//		for (Block<V,E> b : m_blocks) {
//			if (!b.stable(other)) {
////				System.out.println(b + " not stable to " + other);
//				return false;
//			}
//		}
//		return true;
//	}
//	
//	public boolean stable(List<IVertex> vertices) {
//		for (Block b : m_blocks) {
//			if (!b.stable(vertices))
//				return false;
//		}
//		return true;
//	}
//	
//	public boolean stable() {
//		for (Block b : m_blocks) {
//			if (!stable(b))
//				return false;
//		}
//		return true;
//	}
	
	public String toString() {
		String s = "";
		String comma = "";
		for (Block<V,E> b : m_blocks) {
			s += comma + b;
			comma = ", ";
		}
		return s;
	}
}
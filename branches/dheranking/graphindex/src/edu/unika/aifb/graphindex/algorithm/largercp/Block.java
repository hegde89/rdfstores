package edu.unika.aifb.graphindex.algorithm.largercp;

/**
 * Copyright (C) 2009 GŸnter Ladwig (gla at aifb.uni-karlsruhe.de)
 * 
 * This file is part of the graphindex project.
 *
 * graphindex is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2
 * as published by the Free Software Foundation.
 * 
 * graphindex is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with graphindex.  If not, see <http://www.gnu.org/licenses/>.
 */

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.unika.aifb.graphindex.index.DataIndex;
import edu.unika.aifb.graphindex.storage.StorageException;

public class Block {
	public static DataIndex m_gs;
	public static BlockCache m_bc;
	public static int m_blockId = 0;

	private int m_size;
	private XBlock m_parent;
	private int m_id = ++m_blockId;
	private String m_name = "b" + m_id;
	private Block m_splitBlock;
	
	public Block() {
	}
	
	public Block getSplitBlock() {
		return m_splitBlock;
	}
	
	public void setSplitBlock(Block splitBlock) {
		m_splitBlock = splitBlock;
	}
	
	public XBlock getXBlock() {
		return m_parent;
	}
	
	public void setXBlock(XBlock xblock) {
		m_parent = xblock;
	}
	
	public void add(String node) {
		m_bc.setBlock(node, this);
		m_bc.addNode(this, node);
		m_size++;
	}
	
	public void remove(String node) {
		m_bc.removeNode(this, node);
		m_size--;
	}
	
	public void moveNode(String node, Block toBlock) {
		remove(node);
		toBlock.add(node);
	}
	
	public void setSize(int size) {
		m_size = size;
	}
	
	public int size() {
		return m_size;
	}
	
	public Set<String> getNodes() {
		return m_bc.getNodes(this);
	}
	
	public Set<String> image(String property, boolean preimage) throws StorageException {
		Set<String> image = new HashSet<String>(5000);
		
		for (String node : m_bc.getNodes(this)) {
			image.addAll(m_gs.getImage(node, property, preimage));
		}
		
		return image;
	}
	
	public String toString() {
		String s = m_name + "[" + m_size + "]";
		return s;
	}
	
	public String getName() {
		return m_name;
	}
	
	public int getId() {
		return m_id;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_name == null) ? 0 : m_name.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Block other = (Block)obj;
		if (m_name == null) {
			if (other.m_name != null)
				return false;
		} else if (!m_name.equals(other.m_name))
			return false;
		return true;
	}
}
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;

import edu.unika.aifb.graphindex.index.DataIndex;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Util;


public class XBlock {
	public static Environment m_env;
	public static DataIndex m_gs;
	private static int m_xbId = 0;
	
	private LinkedList<Block> m_blocks;
	private int m_id;
	private Database m_db;
	
	public XBlock() throws DatabaseException {
		m_blocks = new LinkedList<Block>();
		m_id = m_xbId++;
		
		DatabaseConfig config = new DatabaseConfig();
		config.setTransactional(false);
		config.setAllowCreate(true);
		config.setSortedDuplicates(false);
		config.setDeferredWrite(true);
		
		m_db = m_env.openDatabase(null, "xb" + m_id, config);
	}
	
	public int getId() {
		return m_id;
	}
	
	public XBlock(Block b) throws DatabaseException {
		this();
		addBlock(b);
	}
	
	public void close() throws DatabaseException {
		m_db.close();
	}
	
	private String getDbKey(String property, String node) {
		return property + "__" + node;
	}
	
	private Integer getVal(String property, String node) {
		try {
			DatabaseEntry out = new DatabaseEntry();
			m_db.get(null, new DatabaseEntry(getDbKey(property, node).getBytes()), out, null);
			if (out.getData() != null)
				return Util.bytesToInt(out.getData());
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private void putVal(String property, String node, int val) {
		try {
			m_db.put(null, new DatabaseEntry(getDbKey(property, node).getBytes()), new DatabaseEntry(Util.intToBytes(val)));
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}
	
	private void removeKey(String property, String node) {
		try {
			m_db.delete(null, new DatabaseEntry(getDbKey(property, node).getBytes()));
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}
	
	public void calcInfo(boolean preimage) throws StorageException {
		for (Block block : m_blocks) {
			Set<String> blockNodes = block.getNodes();
			System.out.println("block size: " + blockNodes.size());
			int nodes = 0;
			for (String node : blockNodes) {
//				System.out.print(node);
				Map<String,Set<String>> image = m_gs.getImage(node, preimage);
//				System.out.print(" image");
				for (String property : image.keySet()) {
					for (String x : image.get(property)) {
						Integer i = getVal(property, x);
						if (i == null)
							i = 1;
						putVal(property, x, i);
//						List<Info> infos = m_info.get(x);
//						if (infos == null) {
//							infos = new ArrayList<Info>();
//							m_info.put(x, infos);
//						}
//						boolean found = false;
//						for (Info i : infos) {
//							if (i.label == label) {
//								i.sval++;
//								found = true;
//							}
//						}
//						if (!found)
//							infos.add(new Info(label, 1));
					}
				}
//				System.out.println(" done");
				nodes++;
				if (nodes % 10000 == 0)
					System.out.println(" " + nodes);
			}
		}
	}
	
	public Integer getInfo(String v, String property) {
		return getVal(property, v);
//		List<Info> infos = m_info.get(v);
//		if (infos == null)
//			return null;
//		
//		for (Info i : infos) {
//			if (i.label == property)
//				return i.sval;
//		}
//		return null;
	}
	
	public void decInfo(String v, String property, int dec) {
		Integer val = getVal(property, v);
		if (val != null) {
			val -= dec;
			if (val == 0)
				removeKey(property, v);
			else
				putVal(property, v, val);
		}
//		List<Info> infos = m_info.get(v);
//		if (infos == null)
//			return;
//		for (Iterator<Info> it = infos.iterator(); it.hasNext(); ) {
//			Info i = it.next();
//			if (i.label == label) {
//				i.sval -= dec;
//				if (i.sval == 0) {
//					it.remove();
//				}
//			}
//		}
//		if (infos.size() == 0)
//			m_info.remove(v);
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
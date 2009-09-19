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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.PreloadConfig;
import com.sleepycat.je.dbi.Operation;

import edu.unika.aifb.graphindex.index.IndexDirectory;
import edu.unika.aifb.graphindex.util.StringSplitter;
import edu.unika.aifb.graphindex.util.Util;

public class BlockCache {
	private Database m_nodeDb;
	private Database m_blockDb;
	
	private final static Logger log = Logger.getLogger(BlockCache.class);
	
	private Map<Integer,Block> m_blocks;
	private Map<Integer,Set<String>> m_nodeCache;
	boolean m_nodeCacheActive = false;
	
	public BlockCache(IndexDirectory idxDirectory) throws DatabaseException, IOException {
		EnvironmentConfig config = new EnvironmentConfig();
		config.setTransactional(false);
		config.setAllowCreate(false);

		Environment env = new Environment(idxDirectory.getDirectory(IndexDirectory.BDB_DIR), config);
		initialize(env);
	}

	public BlockCache(Environment env) throws DatabaseException {
		initialize(env);
	}
	
	private void initialize(Environment env) throws DatabaseException {
		DatabaseConfig config = new DatabaseConfig();
		config.setTransactional(false);
		config.setAllowCreate(true);
		config.setSortedDuplicates(false);
		config.setDeferredWrite(true);
		
		m_nodeDb = env.openDatabase(null, "nbc", config);
		
		config = new DatabaseConfig();
		config.setTransactional(false);
		config.setAllowCreate(true);
		config.setSortedDuplicates(false);
		config.setDeferredWrite(true);
		
		m_blockDb = env.openDatabase(null, "bbc", config);
		
		m_blocks = new HashMap<Integer,Block>();
		m_nodeCache = new HashMap<Integer,Set<String>>();
		
		PreloadConfig pc = new PreloadConfig();
		pc.setMaxMillisecs(2000);
		m_nodeDb.preload(pc);
	}
	
	public void close() throws DatabaseException {
		m_nodeDb.close();
		m_blockDb.close();
	}
	
	public int getBlockCount() {
		return m_blocks.keySet().size();
	}
	
	public void setNodeCacheActive(boolean active) {
		m_nodeCacheActive = active;
		if (!active)
			syncNodeCache();
	}
	
	public void syncNodeCache() {
		for (int block : m_nodeCache.keySet()) {
			putNodes(m_blocks.get(block), m_nodeCache.get(block));
		}
		m_nodeCache = new HashMap<Integer,Set<String>>();
	}
	
	public Block createBlock() {
		Block b = new Block();
		m_blocks.put(b.getId(), b);
		return b;
	}
	
	public void setBlock(String node, Block block) {
		try {
			m_nodeDb.put(null, new DatabaseEntry(node.getBytes()), new DatabaseEntry(Util.intToBytes(block.getId())));
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}
	
	public long getNodeCount() {
		try {
			return m_nodeDb.count();
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	public Block getBlock(String node) {
		try {
			DatabaseEntry out = new DatabaseEntry();
			m_nodeDb.get(null, new DatabaseEntry(node.getBytes()), out, null);
			if (out.getData() == null)
				System.out.println(node);
			return m_blocks.get(Util.bytesToInt(out.getData()));
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public String getBlockName(String node) {
		try {
			DatabaseEntry out = new DatabaseEntry();
			m_nodeDb.get(null, new DatabaseEntry(node.getBytes()), out, null);
			if (out.getData() == null)
				return null;
			return "b" + Util.bytesToInt(out.getData());
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public Set<String> getNodes(Block block) {
		Set<String> nodes = m_nodeCache.get(block.getId());
		if (m_nodeCacheActive && nodes != null)
			return nodes;
		else
			nodes = new HashSet<String>();
		try {
			DatabaseEntry out = new DatabaseEntry();
			m_blockDb.get(null, new DatabaseEntry(Util.intToBytes(block.getId())), out, null);
			
			if (out.getData() != null) {
				StringSplitter sp = new StringSplitter(new String(out.getData()), "\n");
				String s;
				while ((s = sp.next()) != null)
					nodes.add(s);
			}
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
		if (m_nodeCacheActive)
			m_nodeCache.put(block.getId(), nodes);
		
		return nodes;
	}
	
	public void putNodes(Block block, Set<String> nodes) {
		if (m_nodeCacheActive) {
			m_nodeCache.put(block.getId(), nodes);
			return;
		}
		try {
			StringBuffer sb = new StringBuffer();
			for (String s : nodes)
				sb.append(s).append("\n");
			m_blockDb.put(null, new DatabaseEntry(Util.intToBytes(block.getId())), new DatabaseEntry(sb.toString().getBytes()));
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}

	public void addNode(Block block, String node) {
		Set<String> nodes = getNodes(block);
		nodes.add(node);
		putNodes(block, nodes);
	}

	public void removeNode(Block block, String node) {
		Set<String> nodes = getNodes(block);
		nodes.remove(node);
		putNodes(block, nodes);
		
	}
	
	public void removeBlock(Block block) {
		try {
			m_blocks.remove(block.getId());
			m_nodeCache.remove(block.getId());
			m_blockDb.delete(null, new DatabaseEntry(Util.intToBytes(block.getId())));
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}

	public void addNodesToBlock(Block b) {
	    try {
			Cursor c = m_nodeDb.openCursor(null, null);
			DatabaseEntry foundKey = new DatabaseEntry();
		    DatabaseEntry foundData = new DatabaseEntry();
		    Set<String> nodes = new HashSet<String>();
			while (c.getNext(foundKey, foundData, null) == OperationStatus.SUCCESS) {
				int id = Util.bytesToInt(foundData.getData());
				if (id == b.getId())
					nodes.add(new String(foundKey.getData()));
			}
			c.close();
			putNodes(b, nodes);
			b.setSize(nodes.size());
			log.debug(nodes.size() + " added to " + b);
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}
}

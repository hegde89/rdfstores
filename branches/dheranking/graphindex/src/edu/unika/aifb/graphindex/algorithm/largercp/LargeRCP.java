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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;

import edu.unika.aifb.graphindex.data.Table;
import edu.unika.aifb.graphindex.index.DataIndex;
import edu.unika.aifb.graphindex.index.DataIndex.NodeListener;
import edu.unika.aifb.graphindex.storage.DataField;
import edu.unika.aifb.graphindex.storage.IndexDescription;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Util;

public class LargeRCP {
	private BlockCache m_bc;
	private boolean m_ignoreDataValues = false;
	private Set<String> m_allEdges;
	private Set<String> m_forwardEdges, m_backwardEdges;
	private DataIndex m_gs;
	private Environment m_env;
	private String m_tempDir;
	private static final Logger log = Logger.getLogger(LargeRCP.class);
	
	public LargeRCP(DataIndex gs, Environment env, Set<String> fw, Set<String> bw, Set<String> allEdges) throws EnvironmentLockedException, DatabaseException {
		m_forwardEdges = fw;
		m_backwardEdges = bw;
		m_allEdges = allEdges;
		m_allEdges.addAll(m_forwardEdges);
		m_allEdges.addAll(m_backwardEdges);
		
		m_gs = gs;
		m_env = env;
		
		XBlock.m_env = m_env;
		XBlock.m_gs = gs;

		m_bc = new BlockCache(m_env);
	}
	
	public void setIgnoreDataValues(boolean ignore) {
		m_ignoreDataValues = ignore;
	}
	
	public void setTempDir(String tempDir) {
		m_tempDir = tempDir;
	}

	private boolean refinePartitionSimple(List<Block> blocks, Collection<String> nodes, boolean useMove) {
		// TODO check if nodes should be moved multiple times in one refinement step, on subsequent properties
		List<Block> splitBlocks = new LinkedList<Block>();
		log.debug("image size: " + nodes.size());
		int nodeCount = 0;
		float[] intervals = {0.2f, 0.4f, 0.6f, 0.8f};
		int interval = 0;
		m_bc.setNodeCacheActive(true);
		for (String node : nodes) {
//			if (m_ignoreDataValues && !Util.isEntity(node))
//				continue;
			
			Block block = m_bc.getBlock(node);
			Block splitBlock = block.getSplitBlock();
			if (splitBlock == null) {
				splitBlock = m_bc.createBlock();
				block.setSplitBlock(splitBlock);
				splitBlocks.add(block);
			}

			splitBlock.add(node);
			
			nodeCount++;
			if (interval < intervals.length && nodeCount > intervals[interval] * (float)nodes.size()) {
				interval++;
//				log.debug(nodeCount);
			}
		}
		
		for (Block block : splitBlocks) {
			Block splitBlock = block.getSplitBlock();
			blocks.add(splitBlock);
			block.setSplitBlock(null);
			
			if (block.size() == splitBlock.size()) {
				m_bc.removeBlock(block);
				blocks.remove(block);
			}
			else {
				block.setSize(block.size() - splitBlock.size());
				Set<String> blockNodes = m_bc.getNodes(block);
				blockNodes.removeAll(m_bc.getNodes(splitBlock));
				m_bc.putNodes(block, blockNodes);
//				log.debug("block: "+ block + " " + block.getNodes().size());
//				log.debug("splitblock: "+ splitBlock + " " + splitBlock.getNodes().size());
			}
		}
		
		m_bc.setNodeCacheActive(false);
		
		return true;
	}

	private Set<String> moved = new HashSet<String>();
	/**
	 * Refines the partition <code>p</code> with respect to the vertex set <code>image</code>, which in most
	 * cases is the image of a block in <code>p</code>. This operation splits blocks in <code>p</code> so that
	 * all blocks are either subsets of <code>image</code> or disjunct from <code>image</code>, ie. all blocks
	 * are stable to <code>image</code>.<p>
	 * 
	 * XBlocks which were made compound by splitting are added to <code>w</code>.
	 * 
	 * @param p
	 * @param image
	 * @param w
	 */
	private void refinePartition(List<Block> blocks, Collection<String> nodes, Splitters w) {
		List<Block> splitBlocks = new LinkedList<Block>();
//		log.debug("image size: " + nodes.size());
		int nodeCount = 0;
		float[] intervals = {0.3f, 0.6f};
		int interval = 0;
		m_bc.setNodeCacheActive(true);
		for (String node : nodes) {
//			if (m_ignoreDataValues && !Util.isEntity(node))
//				continue;
			
			Block block = m_bc.getBlock(node);
			Block splitBlock = block.getSplitBlock();
			if (splitBlock == null) {
				splitBlock = m_bc.createBlock();
				block.setSplitBlock(splitBlock);
				splitBlocks.add(block);
			}
			splitBlock.add(node);
			
			nodeCount++;
			if (nodes.size() > 10000 & interval < intervals.length && nodeCount > intervals[interval] * (float)nodes.size()) {
				interval++;
//				log.debug(nodeCount);
			}
		}
		
		for (Block block : splitBlocks) {
			Block splitBlock = block.getSplitBlock();
			block.getXBlock().addBlock(splitBlock);
			blocks.add(splitBlock);
			block.setSplitBlock(null);
			
			if (block.size() == splitBlock.size()) {
				block.getXBlock().remove(block);
				m_bc.removeBlock(block);
				blocks.remove(block);
			}
			else {
				block.setSize(block.size() - splitBlock.size());
				Set<String> blockNodes = m_bc.getNodes(block);
				blockNodes.removeAll(m_bc.getNodes(splitBlock));
				m_bc.putNodes(block, blockNodes);
				
				if (!w.contains(block.getXBlock()))
					w.add(block.getXBlock());
			}
		}
		
		m_bc.setNodeCacheActive(false);
	}
	
	private void createPartitionSimple(List<Block> blocks, List<String> edges, int pathLength, boolean forward) throws StorageException, IOException {
		int steps = 0;
		
		log.debug("forward: " + forward);
		
		if (blocks.size() == 0) {
			final Block b = m_bc.createBlock();
			blocks.add(b);

			// start
			// init block cache, set one block for all nodes; this block will be "empty", i.e. the blockDb won't contain
			// the nodes, which will be fixed after the first splitting
			for (String property : m_allEdges)
				m_gs.iterateNodes(property, new NodeListener() {
					public void node(String node) {
						m_bc.setBlock(node, b);
					}
				});

			b.setSize((int)m_bc.getNodeCount());
			log.debug("nodes: " + b.size());
			
			// load image directly from the graph (image == all nodes with incoming edges)
			moved = new HashSet<String>();
			for (String property : edges) {
				log.debug(property);
				Set<String> image = m_gs.getObjectNodes(property);
				refinePartitionSimple(blocks, image, false);
				log.debug("blocks: " + blocks.size());
			}
			moved = new HashSet<String>();
			
			// add all nodes in the start block to the start block
			m_bc.addNodesToBlock(b);
			
			steps++;
			log.debug("steps: " + steps + ", blocks: " + blocks.size());
		}
		
		String blockFile = m_tempDir + "/blocks";
		while (steps < pathLength) {
			PrintWriter pw = new PrintWriter(new FileWriter(blockFile));
			for (Block b : blocks) {
//				pw.println("block:" + b.getName());
				Set<String> blockNodes = b.getNodes();
				for (String node : blockNodes)
					pw.println(node);
			}
			pw.close();
			
			int blockCount = blocks.size();
			
			for (String property : edges) {
				m_bc.setNodeCacheActive(true);
				
				log.debug(property);

//				BufferedReader in = new BufferedReader(new FileReader(blockFile));
				String input;
				String currentBlock = null;
//				Set<String> image = new HashSet<String>();
//				while ((input = in.readLine()) != null) {
//					input = input.trim();
//					
//					if (input.startsWith("block:")) {
//						if (currentBlock != null) {
//							
//							refine(blocks, image);
//						}
//						
//						currentBlock = input;
//						image = new HashSet<String>();
//					}
//					else {
//						image.addAll(m_gs.getImage(input, property, forward));
//					}
//				}
//				in.close();
//				
				Table<String> table = null;
				if (!forward) {
					table = m_gs.getIndexStorage(IndexDescription.POS).getTable(IndexDescription.POS, new DataField[] { DataField.OBJECT }, property);
				}
				else {
					table = m_gs.getIndexStorage(IndexDescription.PSO).getTable(IndexDescription.PSO, new DataField[] { DataField.SUBJECT }, property);
				}
				
//				log.debug("image size: " + image.size());
				HashSet<String> image = new HashSet<String>(table.rowCount() / 4 + 1);
				for (String[] row : table)
					image.add(row[0]);

				if (image != null && image.size() > 0) {
					log.debug("image size of " + currentBlock + ": " + image.size());
					refine(blocks, image);
				}
				
				m_bc.setNodeCacheActive(false);

				log.debug("blocks: " + blocks.size());
			}
			
			new File(blockFile).delete();
			
			steps++;
			log.debug("steps: " + steps + ", blocks: " + blocks.size());
			
			if (blockCount == blocks.size()) {
				log.debug("no split, done");
				break;
			}
			
			System.gc();
			log.debug(Util.memory());
		}
	}

	private void refine(List<Block> blocks, Set<String> image) {
		List<Block> splitBlocks = new ArrayList<Block>();
		for (String imageNode : image) {
			Block block = m_bc.getBlock(imageNode);
			
			Block splitBlock = block.getSplitBlock();
			if (splitBlock == null) {
				splitBlock = m_bc.createBlock();
				block.setSplitBlock(splitBlock);
				splitBlocks.add(block);
			}
			splitBlock.add(imageNode);
		}

		int blocksSplit = 0;
		for (Block block : splitBlocks) {
			Block splitBlock = block.getSplitBlock();
			blocks.add(splitBlock);
			block.setSplitBlock(null);
			
			if (block.size() == splitBlock.size()) {
				m_bc.removeBlock(block);
				blocks.remove(block);
			}
			else {
				block.setSize(block.size() - splitBlock.size());
				Set<String> bnodes = new HashSet<String>(m_bc.getNodes(block));
				bnodes.removeAll(m_bc.getNodes(splitBlock));
				m_bc.putNodes(block, bnodes);
				
				blocksSplit++;
			}
		}
		if (blocksSplit > 0)
			log.debug(blocksSplit + " blocks split");
	}
	
	private void createPartition(List<Block> blocks, List<String> properties, int pathLength, boolean preimage) throws StorageException, DatabaseException {
		Splitters w = new Splitters();
		XBlock startXB = new XBlock();
		Set<XBlock> cbs = new HashSet<XBlock>();
		
		if (blocks.size() == 0) {
			if (preimage)
				log.error("wrong start");
			
			final Block b = m_bc.createBlock();
			blocks.add(b);
			startXB.addBlock(b);
			
			// start
			// init block cache, set one block for all nodes; this block will be "empty", i.e. the blockDb won't contain
			// the nodes, which will be fixed after the first splitting
			for (String property : properties)
				m_gs.iterateNodes(property, new NodeListener() {
					public void node(String node) {
						m_bc.setBlock(node, b);
					}
				});

			b.setSize((int)m_bc.getNodeCount());
			log.debug("nodes: " + b.size());
			
			// load image directly from the graph (image == all nodes with incoming edges)
			int movedIn = 0;
			moved = new HashSet<String>();
			for (String property : properties) {
				log.debug(property);
				Set<String> image = m_gs.getObjectNodes(property);
				refinePartition(blocks, image, w);
				movedIn++;
			}
			moved = new HashSet<String>();
			
			// add all nodes in the start block to the start block
			m_bc.addNodesToBlock(b);
		}
		else {
			log.debug(blocks.size() + " " + m_bc.getBlockCount());
			for (Block b : blocks)
				startXB.addBlock(b);
		}
		
		if (w.size() == 0)
			w.add(startXB);
		cbs.add(startXB);
		
		System.gc();

		log.debug("path length: " + pathLength);
		log.debug("blocks: " + blocks.size());
		log.debug("setup complete, " + Util.memory());
		
		long start = System.currentTimeMillis();
		int steps = 0;
		
		startXB.calcInfo(preimage);
		
		steps++;
		log.debug(blocks.size());
			
		while (w.size() > 0 && (pathLength == -1 || steps < pathLength)) {
			XBlock s = w.remove();
			
			Block b;
			if (s.getFirstBlock().size() <= s.getSecondBlock().size())
				b = s.getFirstBlock();
			else
				b = s.getSecondBlock();
			
			s.remove(b);
			
			if (s.isCompound())
				w.add(s);
			
			XBlock s_ = new XBlock(b);
//			s_.calcInfo(); // TODO is not really necessary, as we compute info for b below
			cbs.add(s_);
			log.debug(b);
			List<String> b_ = new ArrayList<String>(b.size());
			b_.addAll(b.getNodes());
			
			for (String property : properties) {
//				log.debug("LABEL " + label);

				// calculate E(B) and LD
				Set<String> imageB = new HashSet<String>();
				Map<String,Integer> ld = new HashMap<String,Integer>();
				for (String node : b_) {
					Set<String> image = m_gs.getImage(node, property, preimage);
					
					imageB.addAll(image);
					
					for (String y : image) {
						if (!ld.containsKey(y))
							ld.put(y, 1);
						else
							ld.put(y, ld.get(y) + 1);
					}
				}

				refinePartition(blocks, imageB, w);
//				log.debug("blocks1: " + blocks.size());

				// calculate E(B) - E(S - B)
				Set<String> imageBSB = new HashSet<String>();
				for (String v : imageB) {
					Integer sval = s.getInfo(v, property);
					if (sval == null)
						continue;
					
					int val = sval;
					
					if (!ld.containsKey(v))
						continue;
					
					// because B is a subset of S, if the number of incoming
					// edges from B equals the number of incoming edges from S,
					// all incoming edges of the vertex are from B, ie. the
					// the vertex is part of E(B) - E(S - B)
					if (val == (int)ld.get(v))
						imageBSB.add(v);
				}

				refinePartition(blocks, imageBSB, w);

				// update info map of S
				for (String v : ld.keySet())
					s.decInfo(v, property, ld.get(v));
			}
	
//			log.debug("e " + p.stable(b_));
			
			steps++;
			
			long duration = (System.currentTimeMillis() - start) / 1000;
			if (steps % 500 == 0)
				log.info(" steps: " + steps + ", psize: " + blocks.size() + ", duration: " + duration + " seconds, " + Util.memory());
		}

		log.info("partition size: " + blocks.size());
		log.info("steps: " + steps);

		for (XBlock xb : cbs)
			xb.close();
	}
	
	public void createIndexGraph(int pathLength) throws StorageException, IOException, InterruptedException, DatabaseException {
		log.debug("ignore data values: " + m_ignoreDataValues);
		log.debug("---------------------------------- starting backward bisim");
		log.debug("backward edges: " + m_backwardEdges.size());
//		Set<String> nodes = m_gs.getNodes();
//		log.debug("nodes: " + nodes.size() + ", " + Util.memory());
		
		Block.m_bc = m_bc;
		Block.m_gs = m_gs;
		
//		Block b = m_bc.createBlock();
//		
//		for (String node : nodes) {
//			if (!m_ignoreDataValues || Util.isEntity(node))
//				b.add(node);
//		}
//		log.debug("start block size: " + b.size());
//		
//		nodes = null;
		System.gc();
		
		List<Block> blocks = new ArrayList<Block>();
//		blocks.add(b);

		List<String> properties = new ArrayList<String>(m_backwardEdges);
		Collections.sort(properties);
//		createPartition(blocks, properties, pathLength, false);
		if (properties.size() > 0)
			createPartitionSimple(blocks, properties, pathLength, false);

		System.gc();

		log.debug("---------------------------------- starting forward bisim");
		log.debug("forward edges: " + m_forwardEdges.size());
		log.debug(Util.memory());
		
		properties = new ArrayList<String>(m_forwardEdges);
		Collections.sort(properties);
//		createPartition(blocks, properties, pathLength, true);
		if (properties.size() > 0)
			createPartitionSimple(blocks, properties, pathLength, true);

//		writePartition(p, partitionFile, graphFile, blockFile, true);
//		if (m_ignoreDataValues)
//			writeDataEdges(partitionFile, vertices, true);

		m_bc.close();
	}
}

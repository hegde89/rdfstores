package edu.unika.aifb.graphindex.algorithm.largercp;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
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

import edu.unika.aifb.graphindex.StructureIndex;
import edu.unika.aifb.graphindex.data.HashValueProvider;
import edu.unika.aifb.graphindex.data.IVertex;
import edu.unika.aifb.graphindex.graph.IndexGraph;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.NamedGraph;
import edu.unika.aifb.graphindex.preprocessing.VertexListProvider;
import edu.unika.aifb.graphindex.storage.GraphManager;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;
import edu.unika.aifb.graphindex.storage.lucene.LuceneGraphStorage;
import edu.unika.aifb.graphindex.util.LineSortFile;
import edu.unika.aifb.graphindex.util.Util;

public class LargeRCP {
	private BlockCache m_bc;
	private boolean m_ignoreDataValues = false;
	private Set<String> m_forwardEdges, m_backwardEdges;
	private LuceneGraphStorage m_gs;
	private Environment m_env;
	private static final Logger log = Logger.getLogger(LargeRCP.class);
	
	public LargeRCP(LuceneGraphStorage gs, Environment env, Set<String> fw, Set<String> bw) throws EnvironmentLockedException, DatabaseException {
		m_forwardEdges = fw;
		m_backwardEdges = bw;
		
		m_gs = gs;
		m_env = env;
		
		XBlock.m_env = m_env;
		XBlock.m_gs = gs;

		m_bc = new BlockCache(m_env);
	}
	
	public void setIgnoreDataValues(boolean ignore) {
		m_ignoreDataValues = ignore;
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
			if (m_ignoreDataValues && !Util.isEntity(node))
				continue;
			
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
				log.debug(nodeCount);
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
			if (m_ignoreDataValues && !Util.isEntity(node))
				continue;
			
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
				log.debug(nodeCount);
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
	
	private void createPartitionSimple(List<Block> blocks, List<String> edges, int pathLength, boolean forward) throws StorageException {
		int steps = 0;
		
		if (blocks.size() == 0) {
			Block b = m_bc.createBlock();
			blocks.add(b);

			// start
			// init block cache, set one block for all nodes; this block will be "empty", i.e. the blockDb won't contain
			// the nodes, which will be fixed after the first splitting
			m_gs.addNodesToBC(m_bc, b, m_ignoreDataValues);
			b.setSize((int)m_bc.getNodeCount());
			log.debug("nodes: " + b.size());
			
			// load image directly from the graph (image == all nodes with incoming edges)
			moved = new HashSet<String>();
			for (String property : edges) {
				log.debug(property);
				Set<String> image = m_gs.getNodes(1, property);
				refinePartitionSimple(blocks, image, false);
				log.debug("blocks: " + blocks.size());
			}
			moved = new HashSet<String>();
			
			// add all nodes in the start block to the start block
			m_bc.addNodesToBlock(b);
			
			steps++;
			log.debug("steps: " + steps + ", blocks: " + blocks.size());
		}
		
		while (steps < pathLength) {
			for (String property : edges) {
				m_bc.setNodeCacheActive(true);
				
				log.debug(property);
				int maxBlockId = Block.m_blockId;
				List<Block> splitBlocks = new ArrayList<Block>();
				List<Block> oldBlocks = new ArrayList<Block>(blocks);
				for (Block b : oldBlocks) {
					log.debug(b);
					int nodeCount = 0;
					float[] intervals = {0.2f, 0.4f, 0.6f, 0.8f};
					int interval = 0;

					Set<String> blockNodes = b.getNodes();
					for (String node : blockNodes) {
						Set<String> image = m_gs.getImage(node, property, forward);

						for (String imageNode : image) {
							if (m_ignoreDataValues && !Util.isEntity(imageNode))
								continue;
							
							Block block = m_bc.getBlock(imageNode);
							if (block.getId() > maxBlockId)
								 // node was already moved, because it was part of the image of a previous node in this block
								continue;
							
							Block splitBlock = block.getSplitBlock();
							if (splitBlock == null) {
								splitBlock = m_bc.createBlock();
								block.setSplitBlock(splitBlock);
								splitBlocks.add(block);
							}
							splitBlock.add(imageNode);
						}

						nodeCount++;
						if (blockNodes.size() > 10000 && interval < intervals.length && nodeCount > intervals[interval] * (float)blockNodes.size()) {
							interval++;
							log.debug(nodeCount);
						}
					}
				}
				
				for (Block block : splitBlocks) {
					Block splitBlock = block.getSplitBlock();
					blocks.add(splitBlock);
					block.setSplitBlock(null);
					
					if (block.size() == splitBlock.size()) {
						blocks.remove(block);
					}
					else {
						block.setSize(block.size() - splitBlock.size());
						Set<String> blockNodes = new HashSet<String>(m_bc.getNodes(block));
						blockNodes.removeAll(m_bc.getNodes(splitBlock));
						m_bc.putNodes(block, blockNodes);
					}
				}
				
				m_bc.setNodeCacheActive(false);
				log.debug("blocks: " + blocks.size());
			}
			
			steps++;
			log.debug("steps: " + steps + ", blocks: " + blocks.size());
		}
	}
	
	private void createPartition(List<Block> blocks, List<String> properties, int pathLength, boolean preimage) throws StorageException, DatabaseException {
		Splitters w = new Splitters();
		XBlock startXB = new XBlock();
		Set<XBlock> cbs = new HashSet<XBlock>();
		
		if (blocks.size() == 0) {
			if (preimage)
				log.error("wrong start");
			
			Block b = m_bc.createBlock();
			blocks.add(b);
			startXB.addBlock(b);
			
			// start
			// init block cache, set one block for all nodes; this block will be "empty", i.e. the blockDb won't contain
			// the nodes, which will be fixed after the first splitting
			m_gs.addNodesToBC(m_bc, b, m_ignoreDataValues);
			b.setSize((int)m_bc.getNodeCount());
			log.debug("nodes: " + b.size());
			
			// load image directly from the graph (image == all nodes with incoming edges)
			int movedIn = 0;
			moved = new HashSet<String>();
			for (String property : properties) {
				log.debug(property);
				Set<String> image = m_gs.getNodes(1, property);
				refinePartition(blocks, image, w);
				movedIn++;
			}
			moved = new HashSet<String>();
			
			// add all nodes in the start block to the start block
			m_bc.addNodesToBlock(b);
			
			for (Block block : blocks)
				log.debug(block);
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
		log.debug(m_gs.m_docCacheMisses + "/" + m_gs.m_docCacheHits);
		log.debug("path length: " + pathLength);
		log.debug("blocks: " + blocks.size());
		log.debug("setup complete, " + Util.memory());
		
		long start = System.currentTimeMillis();
		int steps = 0;
		
		startXB.calcInfo(preimage);
		log.debug(m_gs.m_docCacheMisses + "/" + m_gs.m_docCacheHits);
		
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

		List<String> edges = new ArrayList<String>(m_backwardEdges);
		Collections.sort(edges);
		createPartition(blocks, edges, pathLength, false);
//		createPartitionSimple(blocks, edges, pathLength, false);

		System.gc();

		log.debug("---------------------------------- starting forward bisim");
		log.debug("forward edges: " + m_forwardEdges.size());
		log.debug(Util.memory());
		
		edges = new ArrayList<String>(m_forwardEdges);
		Collections.sort(edges);
		createPartition(blocks, edges, pathLength, true);
//		createPartitionSimple(blocks, edges, pathLength, false);

//		writePartition(p, partitionFile, graphFile, blockFile, true);
//		if (m_ignoreDataValues)
//			writeDataEdges(partitionFile, vertices, true);

		m_bc.close();
	}
	
	private String getTripleString(String s, String p, String o) {
		StringBuilder sb = new StringBuilder();
		sb.append(s).append("__").append(p).append("__").append(o);
		return sb.toString();
	}
	
	private void writeDataEdges(String partitionFile, List<IVertex> vertices, boolean inverted) throws IOException {
		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(partitionFile, true)));
		
		int edges = 0;
		for (IVertex v : vertices) {
			for (long label : v.getEdgeLabels()) {
				for (IVertex y : v.getImage(label)) {
					if (v.isDataValue()) {
						if (inverted) {
							// switch around if inverted
							out.println(y.getBlock().getName() + "\t" + y.getId() + "\t" + label + "\t" + v.getId() + "\t");
						}
						else {
							out.println(v.getBlock().getName() + "\t" + v.getId() + "\t" + label + "\t" + y.getId() + "\t");
						}
						edges++;
					}
				}
			}
		}
		
		log.debug("data edges written: " + edges);
		out.close();
	}

//	private void writePartition(Partition p, String partitionFile, String graphFile, String blockFile, boolean inverted) throws IOException, InterruptedException {
//		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(partitionFile)));
//		PrintWriter graph = new PrintWriter(new BufferedWriter(new FileWriter(graphFile)));
//		PrintWriter block = new PrintWriter(new BufferedWriter(new FileWriter(blockFile)));
//	
//		log.info("writing block file...");
//		for(Block b : p.getBlocks()) {
//			for (IVertex v : b) {
//				block.println(b.getName() + "\t" + m_hashes.getValue(v.getId()));
//			}
//		}
//		
//		log.info("writing partition file...");
//		int blocks = 0;
//		int edges = 0;
//		int vertices = 0;
//		for (Block b : p.getBlocks()) {
//			for (IVertex v : b) {
//				vertices++;
//				if (v.isDataValue())
//					log.debug("data");
//				for (Long label : v.getEdgeLabels()) {
//					if (!m_backwardEdges.contains(m_hashes.getValue(label)) && !m_forwardEdges.contains(m_hashes.getValue(label)))
//						continue;
//
//					for (IVertex y : v.getImage(label)) {
//						// subject extension, subject, property, object, object extension
//						if (inverted) {
//							// switch around if inverted
//							out.println(y.getBlock().getName() + "\t" + y.getId() + "\t" + label + "\t" + v.getId() + "\t" + b.getName());
//							graph.println(y.getBlock().getName() + "\t" + m_hashes.getValue(label) + "\t" + b.getName());
//						}
//						else {
//							out.println(b.getName() + "\t" + v.getId() + "\t" + label + "\t" + y.getId() + "\t" + y.getBlock().getName());
//							graph.println(b.getName() + "\t" + m_hashes.getValue(label) + "\t" + y.getBlock().getName());
//						}
//						edges++;
//					}
//				}
//			}
//			
//			blocks++;
//
//			if (blocks % 5000 == 0)
//				log.debug(" blocks processed: " + blocks);
//		}
//		block.close();
//		out.close();
//		graph.close();
//		
//		log.debug("entity edges written: " + edges);
//		log.debug("vertices in partition: " + vertices);
//		
//		LineSortFile blsf = new LineSortFile(blockFile, blockFile);
//		blsf.setDeleteWhenStringRepeated(true);
//		blsf.sortFile();
//		
////		LineSortFile plsf = new LineSortFile(partitionFile, partitionFile);
////		plsf.setDeleteWhenStringRepeated(true);
////		plsf.sortFile();
//		
//		// uniq only filters repeated lines: sort so that duplicate lines are consecutive
//		Process process = Runtime.getRuntime().exec("sort -o " + graphFile + " " + graphFile);
//		process.waitFor();
//		process = Runtime.getRuntime().exec("uniq " + graphFile + " " + graphFile + ".uniq");
//		process.waitFor();
//		File f = new File(graphFile);
//		f.delete();
//		f = new File(graphFile + ".uniq");
//		f.renameTo(new File(graphFile));
//		log.debug("graph uniq");
//		
////		LineSortFile glsf = new LineSortFile(graphFile, graphFile);
////		glsf.setDeleteWhenStringRepeated(true);
////		glsf.sortFile();
//	}
}

package edu.unika.aifb.graphindex.algorithm.rcp;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

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
import edu.unika.aifb.graphindex.util.Util;

public class RCPFast2 {
	private GraphManager m_gm;
	private HashValueProvider m_hashes;
	private Set<String> m_forwardEdges, m_backwardEdges;
	private static final Logger log = Logger.getLogger(RCPFast2.class);
	
	public RCPFast2(StructureIndex index, HashValueProvider provider) {
		m_forwardEdges = index.getForwardEdges();
		m_backwardEdges = index.getBackwardEdges();
		m_gm = index.getGraphManager();
		m_hashes = provider;
	}
	
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
	private void refinePartition(Partition p, Set<IVertex> image, Splitters w, Integer currentIteration) {
		List<Block> splitBlocks = new LinkedList<Block>();
		for (IVertex x : image) {
			if (currentIteration != null && x.getMovedIn() == currentIteration) {
				continue;
			}
			
			Block block = x.getBlock();
			if (block.getSplitBlock() == null) {
				Block splitBlock = new Block();
				block.setSplitBlock(splitBlock);
				splitBlocks.add(block);
			}
			
			block.remove(x);
			block.getSplitBlock().add(x);
			x.setBlock(block.getSplitBlock());
			
			if (currentIteration != null)
				x.setMovedIn(currentIteration);
		}
		
		for (Block sb : splitBlocks) {
			if (sb.size() == 0) {
				Block newBlock = sb.getSplitBlock();
				sb.setSplitBlock(null);
				sb.getXBlock().addBlock(newBlock);
				sb.getXBlock().remove(sb);
				p.remove(sb);
				p.add(newBlock);
			}
			else {
				Block newBlock = sb.getSplitBlock();
				sb.setSplitBlock(null);
				sb.getXBlock().addBlock(newBlock);
				p.add(newBlock);
				
				if (!w.contains(sb.getXBlock()))
					w.add(sb.getXBlock());
			}
		}
	}
	
	/**
	 * Calculate the image set of a set of vertices. Only edges with the specified
	 * label will be taken into account.
	 * 
	 * @param vertices
	 * @param label
	 * @return
	 */
	private Set<IVertex> imageOf(Collection<IVertex> vertices, long label) {
		Set<IVertex> image = new HashSet<IVertex>();
		for (IVertex v :vertices) {
			if (v.getImage(label) == null)
				continue;
			image.addAll(v.getImage(label));
		}
		return image;
	}
	
	private Partition createPartition(Partition p, List<IVertex> vertices, Set<String> edges) {
		Splitters w = new Splitters();
		XBlock startXB = new XBlock();
		Set<XBlock> cbs = new HashSet<XBlock>();
		Set<Long> labels = new TreeSet<Long>();

		for (String edge : edges) 
			labels.add(new Long(Util.hash(edge)));
		
		for (Block b : p.getBlocks())
			startXB.addBlock(b);
		
		w.add(startXB);
		
		int movedIn = 0;
		
		System.gc();
		log.debug("blocks: " + p.getBlocks().size());
		log.debug("setup complete, " + Util.memory());
		
		long start = System.currentTimeMillis();
		int steps = 0;
		
		startXB.calcInfo();
		
		List<IVertex> b_;
		if (p.getBlocks().size() == 1) {
			b_ = new ArrayList<IVertex>(p.getBlocks().get(0).size());
			for (Iterator<IVertex> i = p.getBlocks().get(0).iterator(); i.hasNext(); )
				b_.add(i.next());
			
			for (long label : labels) {
				refinePartition(p, imageOf(b_, label), w, movedIn);
				movedIn++;
			}
		}
		
		steps++;
			
		while (w.size() > 0) {
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
			s_.calcInfo(); // TODO is not really necessary, as we compute info for b below
			cbs.add(s_);
			
			b_ = new ArrayList<IVertex>(b.size());
			for (Iterator<IVertex> i = b.iterator(); i.hasNext(); )
				b_.add(i.next());
			
			for (long label : labels) {
//				log.debug("LABEL " + label);

				// calculate E(B) and LD
				Set<IVertex> imageB = new HashSet<IVertex>();
				Map<IVertex,Integer> ld = new HashMap<IVertex,Integer>();
				for (IVertex v : b_) {
					if (v.getImage(label) == null)
						continue;
					
					imageB.addAll(v.getImage(label));
					
					for (IVertex y : v.getImage(label)) {
						if (!ld.containsKey(y))
							ld.put(y, 1);
						else
							ld.put(y, ld.get(y) + 1);
					}
				}

				refinePartition(p, imageB, w, null);

				// calculate E(B) - E(S - B)
				Set<IVertex> imageBSB = new HashSet<IVertex>();
				for (IVertex v : imageB) {
					Integer sval = s.getInfo(v, label);
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

				refinePartition(p, imageBSB, w, null);

				// update info map of S
				for (IVertex v : ld.keySet())
					s.decInfo(v, label, ld.get(v));
			}
	
//			log.debug("e " + p.stable(b_));
			
			steps++;
			
			long duration = (System.currentTimeMillis() - start) / 1000;
			if (steps % 500 == 0)
				log.info(" steps: " + steps + ", psize: " + p.getBlocks().size() + ", duration: " + duration + " seconds, " + Util.memory());
		}
//		log.debug(p.stable());
//		log.debug(p);
		log.info("partition size: " + p.getBlocks().size());
		log.info("steps: " + steps);

//		purgeSelfloops(p);
		
		return p;
	}
	
	private void purgeSelfloops(Partition p) {
		List<Block> newBlocks = new LinkedList<Block>();
		for (Block b : p.getBlocks()) {
			IVertex v = b.m_head;
			boolean hasSelfloop = false;
			for (IVertex v2 : b) {
				if (v != v2) {
					for (long label : v.getEdgeLabels()) {
						if (v.getImage(label).contains(v2)) {
							hasSelfloop = true;
							break;
						}
					}
				}
			}
			
			if (hasSelfloop) {
				Block nb = new Block();
				int i = 0;
				int x = b.size() / 2;
				for (IVertex n : b) {
					n.getBlock().remove(n);
					nb.add(n);
					i++;
					if (i >= x)
						break;
				}
				newBlocks.add(nb);
			}
		}
		for (Block nb : newBlocks)
			p.add(nb);
	}
	
	public IndexGraph createIndexGraph(VertexListProvider vlp, String partitionFile, String graphFile) throws StorageException, IOException, InterruptedException {
		log.debug("---------------------------------- starting backward bisim");
		log.debug("backward edges: " + m_backwardEdges);
		List<IVertex> vertices = vlp.getList();
		if (vertices.size() < 20)
			return null;
		
		Block startBlock = new Block();
		for (IVertex v : vertices)
			startBlock.add(v);

		Partition p = new Partition();
		p.add(startBlock);
		
		p = createPartition(p, vertices, m_backwardEdges);

		Map<Long,Block> id2block = new HashMap<Long,Block>();
		for (Block b : p.getBlocks()) {
			Block nb = new Block();
			for (IVertex v : b) {
				id2block.put(v.getId(), nb);
			}
		}
		
		vertices = null;
		p = null;
		System.gc();
		log.debug("---------------------------------- starting forward bisim");
		log.debug("forward edges: " + m_forwardEdges);
		log.debug(Util.memory());
		
		vertices = vlp.getInverted();
		for (IVertex v : vertices)
			id2block.get(v.getId()).add(v);
		
		p = new Partition();
		p.addAll(new HashSet<Block>(id2block.values()));
		
		p = createPartition(p, vertices, m_forwardEdges);
		

		writePartition(p, partitionFile, graphFile, true);
//		IndexGraph g = createIndexGraph(p, true);
//		log.debug(g.edgeCount());
		
//		writePartition(p, partitionFile, false);
//		IndexGraph g = createIndexGraph(p, false);
		
		return null;
	}
	
	private String getTripleString(String s, String p, String o) {
		StringBuilder sb = new StringBuilder();
		sb.append(s).append("__").append(p).append("__").append(o);
		return sb.toString();
	}
	
	private IndexGraph createIndexGraph(Partition p, boolean inverted) throws StorageException, FileNotFoundException {
		log.info("creating index graph...");
		int blocks = 0;
		NamedGraph<String,LabeledEdge<String>> g = m_gm.graph();
		int edges = 0;
		Set<String> triples = new HashSet<String>();
		for (Block b : p.getBlocks()) {
			g.addVertex(b.getName());

			for (IVertex v : b) {
				for (Long label : v.getEdgeLabels()) {
					if (!m_backwardEdges.contains(m_hashes.getValue(label)) && !m_forwardEdges.contains(m_hashes.getValue(label)))
						continue;
					
					for (IVertex y : v.getImage(label)) {
						g.addVertex(y.getBlock().getName());
						if (inverted) {
							String s = getTripleString(y.getBlock().getName(), m_hashes.getValue(label), b.getName());
							if (!triples.contains(s)) {
								g.addEdge(y.getBlock().getName(), b.getName(), new LabeledEdge<String>(y.getBlock().getName(), b.getName(), m_hashes.getValue(label)));
								edges++;
								triples.add(s);
							}
						}
						else {
							String s = getTripleString(b.getName(), m_hashes.getValue(label), y.getBlock().getName());
							if (!triples.contains(s)) {
								g.addEdge(b.getName(), y.getBlock().getName(), new LabeledEdge<String>(b.getName(), y.getBlock().getName(), m_hashes.getValue(label)));
								edges++;
								triples.add(s);
							}
						}
					}
				}
			}
			
			blocks++;

			if (blocks % 5000 == 0)
				log.debug(" blocks processed: " + blocks);
		}
		log.debug("edges added: " + edges + ", in graph: " + g.edgeSet().size());
		
		return new IndexGraph(g);
	}

	private void writePartition(Partition p, String partitionFile, String graphFile, boolean inverted) throws IOException, InterruptedException {
		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(partitionFile)));
		PrintWriter graph = new PrintWriter(new BufferedWriter(new FileWriter(graphFile)));
	
		log.info("writing partition file...");
		int blocks = 0;
		for (Block b : p.getBlocks()) {
			for (IVertex v : b) {
				for (Long label : v.getEdgeLabels()) {
					if (!m_backwardEdges.contains(m_hashes.getValue(label)) && !m_forwardEdges.contains(m_hashes.getValue(label)))
						continue;

					for (IVertex y : v.getImage(label)) {
						if (inverted) {
							out.println(v.getBlock().getName() + "\t" + y.getId() + "\t" + label + "\t" + v.getId() + "\t" + b.getName());
							graph.println(y.getBlock().getName() + "\t" + m_hashes.getValue(label) + "\t" + b.getName());
						}
						else {
							// extension, subject, property, object, subject extension
							out.println(y.getBlock().getName() + "\t" + v.getId() + "\t" + label + "\t" + y.getId() + "\t" + b.getName());
							graph.println(b.getName() + "\t" + m_hashes.getValue(label) + "\t" + y.getBlock().getName());
						}
					}
				}
			}
			
			blocks++;

			if (blocks % 5000 == 0)
				log.debug(" blocks processed: " + blocks);
		}
		out.close();
		graph.close();
		
		// sort partition file by extension uri, property uri, object
		Process process = Runtime.getRuntime().exec("sort -n -k 1.2,1n -k 3,3n -k 4,4n -o " + partitionFile + " " + partitionFile);
		process.waitFor();
		log.debug("sorted");
		
		process = Runtime.getRuntime().exec("sort -o " + graphFile + " " + graphFile);
		process.waitFor();
		process = Runtime.getRuntime().exec("uniq " + graphFile + " " + graphFile + ".uniq");
		process.waitFor();
		File f = new File(graphFile);
		f.delete();
		f = new File(graphFile + ".uniq");
		f.renameTo(new File(graphFile));
		log.debug("graph uniq");
	}
}

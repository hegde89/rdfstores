package edu.unika.aifb.graphindex.algorithm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.HashValueProvider;
import edu.unika.aifb.graphindex.Util;
import edu.unika.aifb.graphindex.algorithm.RCP.Block;
import edu.unika.aifb.graphindex.algorithm.RCP.Partition;
import edu.unika.aifb.graphindex.graph.IVertex;
import edu.unika.aifb.graphindex.graph.LVertex;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.NamedGraph;
import edu.unika.aifb.graphindex.graph.SVertex;
import edu.unika.aifb.graphindex.storage.Extension;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.GraphManager;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;
import edu.unika.aifb.graphindex.storage.Triple;

public class RCPFast {
	private static int block_id = 0;

	private class Splitters {
		LinkedList<XBlock> m_splitters;
		
		public Splitters() {
			m_splitters = new LinkedList<XBlock>();
		}
		
		public void add(XBlock xblock) {
			m_splitters.addLast(xblock);
		}
		
		public boolean contains(XBlock xblock) {
			return m_splitters.contains(xblock);
		}
		
		public XBlock remove() {
			return m_splitters.removeFirst();
		}
		
		public int size() {
			return m_splitters.size();
		}
		
		public String toString() {
			String s = "";
			String comma = "";
			for (XBlock b : m_splitters) {
				s += comma + b;
				comma = ", ";
			}
			return s;
		}
	}
	
	public class Partition {
		private List<Block> m_blocks;
		
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
				if (!b.stable(other))
					return false;
			}
			return true;
		}
		
		public boolean stable(Set<LVertex> vertices) {
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

	public class Block implements Iterable<IVertex> {
		private IVertex m_head;
		private IVertex m_tail;
//		private Block m_next, m_prev;
		private int m_size;
		private XBlock m_parent;
		private String m_name = "b" + ++block_id;
		private Block m_splitBlock;
		
		public Block() {
			m_head = null;
			m_tail = null;
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
		
		public void add(IVertex v) {
			if (m_head == null) {
				m_head = v;
				m_tail = v;
				v.setNext(null);
				v.setPrev(null);
			}
			else {
				m_tail.setNext(v);
				v.setPrev(m_tail);
				v.setNext(null);
				m_tail = v;
			}
			v.setBlock(this);
			m_size++;
		}
		
		public void remove(IVertex v) {
			if (v.getBlock() != this)
				return;
			
			if (v.getPrev() != null)
				v.getPrev().setNext(v.getNext());
			if (v.getNext() != null)
				v.getNext().setPrev(v.getPrev());
			if (m_head == v)
				m_head = v.getNext();
			if (m_tail == v)
				m_tail = v.getPrev();
			v.setNext(null);
			v.setPrev(null);
			v.setBlock(null);
			m_size--;
		}
		
		public int size() {
//			int s = 0;
//			for (Iterator<LVertex> i = iterator(); i.hasNext(); ) {
//				i.next();
//				s++;
//			}
//			System.out.println(m_size == s);
			return m_size;
		}
		
		public Map<Long,Set<IVertex>> image() {
			Map<Long,Set<IVertex>> image = new HashMap<Long,Set<IVertex>>();
			
			for (IVertex v : this) {
				for (long label : v.getEdgeLabels()) {
					if (!image.containsKey(label))
						image.put(label, new HashSet<IVertex>());
					image.get(label).addAll(v.getImage(label));
				}
			}
			
			return image;
		}
		
		public boolean stable(Set<LVertex> vertices) {
			Map<Long,Set<IVertex>> bimage = new HashMap<Long,Set<IVertex>>();
			
			for (IVertex v : vertices) {
				for (long label : v.getEdgeLabels()) {
					if (!bimage.containsKey(label))
						bimage.put(label, new HashSet<IVertex>());
					bimage.get(label).addAll(v.getImage(label));
				}
			}
//			log.debug(this.size() + " ownImage: " + image.size() + ", b: " + b.size());
			
			for (long label : bimage.keySet()) {
				boolean foundOneNotInImage = false;
				boolean foundOneInImage = false;
				for (IVertex v : this) {
					if (bimage.get(label).contains(v)) {
						if (foundOneNotInImage)
							return false;
						foundOneInImage = true;
					}
					else {
						if (foundOneInImage)
							return false;
						foundOneNotInImage = true;
					}
				}
			}
			
			return true;
		}
		
		public boolean stable(Block b) {
			Map<Long,Set<IVertex>> bimage = b.image();
//			log.debug(this.size() + " ownImage: " + image.size() + ", b: " + b.size());
			
			for (long label : bimage.keySet()) {
				boolean foundOneNotInImage = false;
				boolean foundOneInImage = false;
				for (IVertex v : this) {
					if (bimage.get(label).contains(v)) {
						if (foundOneNotInImage)
							return false;
						foundOneInImage = true;
					}
					else {
						if (foundOneInImage)
							return false;
						foundOneNotInImage = true;
					}
				}
			}
			
			return true;
 		}
		
		public String toString() {
			String s = m_name + "[";
			String comma = "";
			if (m_head != null) {
				IVertex cur = m_head;
				do {
					s += comma + cur;
					comma = ",";
					cur = cur.getNext();
				}
				while (cur != null);
			}
			return s + "]";
		}
		
		public Iterator<IVertex> iterator() {
			return new Iterator<IVertex> () {
				private IVertex cur = m_head;
				public boolean hasNext() {
					return cur != null;
				}

				public IVertex next() {
					IVertex ret = cur;
					cur = cur.getNext();
					return ret;
				}

				public void remove() {
					throw new UnsupportedOperationException("remove not supported");
				}
			};
		}

		public String getName() {
			return m_name;
		}
	}
	
	private GraphManager m_gm;
	private ExtensionManager m_em;
	public static Map<Long,String> m_h2v;
	private HashValueProvider m_hashes;
	private static final Logger log = Logger.getLogger(RCPFast.class);
	
	public RCPFast(Map<Long,String> h2v, HashValueProvider provider) {
		m_h2v = h2v;
		m_gm = StorageManager.getInstance().getGraphManager();
		m_em = StorageManager.getInstance().getExtensionManager();
		m_hashes = provider;
	}
	
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
	
	private Set<IVertex> imageOf(Collection<IVertex> vertices, long label) {
		Set<IVertex> image = new HashSet<IVertex>();
		for (IVertex v :vertices) {
			if (v.getImage(label) == null)
				continue;
			image.addAll(v.getImage(label));
		}
		return image;
	}
	
	public NamedGraph<String,LabeledEdge<String>> createIndex(List<IVertex> vertices) throws StorageException {
		Splitters w = new Splitters();
		XBlock startXB = new XBlock();
		Partition p = new Partition();
		Block startBlock = new Block();
		Set<XBlock> cbs = new HashSet<XBlock>();
		
		Set<Long> labels = new TreeSet<Long>();
		for (IVertex v : vertices) {
//			labels.addAll(v.getImage().keySet());
			labels.addAll(m_hashes.getEdges());
			startBlock.add(v);
		}
		
		p.add(startBlock);
		startXB.addBlock(startBlock);
		w.add(startXB);
		
		int movedIn = 0, clearedIn = 0;
		
		System.gc();
		log.debug("setup complete, " + Util.memory());
		log.debug("starting refinement process...");
		
		long start = System.currentTimeMillis();
		int steps = 0;
		while (w.size() > 0) {
			steps++;
			XBlock xb = w.remove();
			
			if (!xb.isCompound()) {
				Block bl = xb.getFirstBlock();
				
				List<IVertex> b_ = new ArrayList<IVertex>(bl.size());
				for (Iterator<IVertex> i = bl.iterator(); i.hasNext(); )
					b_.add(i.next());
//				log.debug("b_: " + b_);
				
				for (long label : labels) {
					refinePartition(p, imageOf(b_, label), w, movedIn);
					
					movedIn++;
				}
			}
			else {
				XBlock s = xb;
//				log.debug("compound splitter: " + s);
//				log.debug("w: " + w);
				
				Block b;
				if (s.getFirstBlock().size() <= s.getSecondBlock().size())
					b = s.getFirstBlock();
				else
					b = s.getSecondBlock();
				
				s.remove(b);
				if (s.isCompound())
					w.add(s);
				
				cbs.add(new XBlock(b));
				
//				log.debug("b: " + b);
//				log.debug("w: " + w);
				
				List<IVertex> b_ = new ArrayList<IVertex>(b.size());
				for (Iterator<IVertex> i = b.iterator(); i.hasNext(); )
					b_.add(i.next());
//				log.debug("b_" + b_);
				
				// calc info_s
//				Set<LVertex> cleared = new HashSet<LVertex>();
//				for (LVertex v : vertices) {
//					v.getInfo().clear();
//					v.getSInfo().clear();
//					cleared.add(v);
//				}
//				log.debug("-------------------------");
//				log.debug(clearedIn);
				for (IVertex v : b_) {
					for (long label : labels) {
						if (v.getImage(label) == null)
							continue;
						
						for (IVertex x : v.getImage(label)) {
//							log.debug(x + " " + clearedIn);
							if (x.getClearedIn() < clearedIn) {
								x.setClearedIn(clearedIn);
								x.clearInfo();
//								log.debug("cleared " + x);
							}
							x.incSInfo(label);
						}
					}
				}
//				log.debug("cleared: " + cleared);
//				Set<LVertex> restb = new HashSet<LVertex>();
				
				for (Block block : s.getBlocks()) {
//					for (LVertex v : block) {
//						restb.add(v);
//						if (!cleared.contains(v)) {
//							v.getInfo().clear();
//							v.getSInfo().clear();
//						}
//					}
					
					for (IVertex v : block) {
						for (long label : labels) {
							if (v.getImage(label) == null)
								continue;
							
							for (IVertex x : v.getImage(label)) {
								if (x.getClearedIn() < clearedIn) {
									x.setClearedIn(clearedIn);
									x.clearInfo();
//									log.debug("- cleared " + x);
								}
								x.incSInfo(label);
							}
						}
					}
				}
				
				clearedIn++;
				
//				log.debug("b restb " + b_ + " " + restb);
				
				for (long label : labels) {
//					log.debug("LABEL " + label);
					
					Set<IVertex> imageB = new HashSet<IVertex>();
					for (IVertex v : b_) {
						if (v.getImage(label) == null)
							continue;
						
						imageB.addAll(v.getImage(label));
						
						for (IVertex y : v.getImage(label))
							y.incInfo(label);
					}
					
//					log.debug("imageB: " + imageB);
					
					refinePartition(p, imageB, w, null);

//					int size = imageB.size();
//					Set<LVertex> imageRSB = imageOf(restb, label);
//					imageB.removeAll(imageRSB);

					Set<IVertex> imageBSB = new HashSet<IVertex>();
					for (IVertex y : b_) {
						if (y.getImage(label) == null)
							continue;
						
						for (IVertex x : y.getImage(label)) {
							if (x.getInfo(label) == x.getSInfo(label))
								imageBSB.add(x);
//							log.debug(x + " " + x.getInfo().get(label) + " " + x.getSInfo().get(label) + " " + x.getClearedIn());
						}
					}
					
//					if (!imageB.equals(imageBSB)) {
//						log.debug(imageB + " " + imageRSB);
//						log.debug(imageBSB);
//						log.debug("false");
//					}
					
//					int size = p.getBlocks().size();
					refinePartition(p, imageBSB, w, null);
//					if (p.getBlocks().size() != size)
//						log.debug("bsb refinement affected p");
				}
				
			}
			
			movedIn++;
			
			long duration = (System.currentTimeMillis() - start) / 1000;
			if (duration > 30) {
				log.info(" steps: " + steps + ", psize: " + p.getBlocks().size() + ", duration: " + duration + " seconds, " + Util.memory());
			}
		}
		log.info("partition size: " + p.m_blocks.size());
		log.info("steps: " + steps);

		purgeSelfloops(p);
		
		return createIndexGraph(p);
	}

	private void purgeSelfloops(Partition p) {
		List<Block> newBlocks = new LinkedList<Block>();
		for (Block b : p.getBlocks()) {
			IVertex v = b.m_head;
			boolean hasSelfloop = false;
			for (IVertex v2 : b) {
				if (v != v2) {
//					for (long label : v.getImage().keySet()) {
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
	
	private NamedGraph<String,LabeledEdge<String>> createIndexGraph(Partition p) throws StorageException {
		NamedGraph<String,LabeledEdge<String>> g = m_gm.graph();
		for (Block b : p.getBlocks()) {
			g.addVertex(b.getName());

			for (IVertex v : b) {
				for (Long label : v.getEdgeLabels()) {
					for (IVertex y : v.getImage(label)) {
						g.addVertex(y.getBlock().getName());
						g.addEdge(b.getName(), y.getBlock().getName(), new LabeledEdge<String>(b.getName(), y.getBlock().getName(), m_hashes.getValue(label)));

//						log.debug(m_hashes.getValue(v.getId()) + " " + m_hashes.getValue(label) + " " + m_hashes.getValue(y.getId()));
						Extension extension = m_em.extension(y.getBlock().getName());
						extension.addTriple(new Triple(m_hashes.getValue(v.getId()), m_hashes.getValue(label), m_hashes.getValue(y.getId())));
					}
				}
			}
		}
		
		return g;
	}
}

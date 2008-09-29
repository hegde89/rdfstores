package edu.unika.aifb.graphindex.algorithm.rcp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import edu.unika.aifb.graphindex.Util;
import edu.unika.aifb.graphindex.data.HashValueProvider;
import edu.unika.aifb.graphindex.data.IVertex;
import edu.unika.aifb.graphindex.data.LVertex;
import edu.unika.aifb.graphindex.data.Triple;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.NamedGraph;
import edu.unika.aifb.graphindex.graph.SVertex;
import edu.unika.aifb.graphindex.storage.Extension;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.GraphManager;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;

public class RCPFast {
	private GraphManager m_gm;
	private ExtensionManager m_em;
	private HashValueProvider m_hashes;
	private static final Logger log = Logger.getLogger(RCPFast.class);
	
	public RCPFast(HashValueProvider provider) {
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
		labels.addAll(m_hashes.getEdges());

		for (IVertex v : vertices) {
//			labels.addAll(v.getImage().keySet());
			startBlock.add(v);
		}
		
		p.add(startBlock);
		startXB.addBlock(startBlock);
		w.add(startXB);
		
		int movedIn = 0, clearedIn = 0;
		
		System.gc();
		log.debug("setup complete, " + Util.memory());
		log.debug("starting refinement process...");
		
		boolean first = true;
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
							}
							x.incSInfo(label);
						}
					}
				}
//				log.debug("cleared: " + cleared);
//				Set<LVertex> restb = new HashSet<LVertex>();
				
				for (Block block : s.getBlocks()) {
					for (IVertex v : block) {
						for (long label : labels) {
							if (v.getImage(label) == null)
								continue;
							
							for (IVertex x : v.getImage(label)) {
								if (x.getClearedIn() < clearedIn) {
									x.setClearedIn(clearedIn);
									x.clearInfo();
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
				
//				log.debug(p.stable(b_));
			}
			
			movedIn++;
			
			long duration = (System.currentTimeMillis() - start) / 1000;
//			if (duration > 30) {
				log.info(" steps: " + steps + ", psize: " + p.getBlocks().size() + ", duration: " + duration + " seconds, " + Util.memory());
//			}
		}
//		log.debug(p.stable());
		log.info("partition size: " + p.m_blocks.size());
		log.info("steps: " + steps);

		purgeSelfloops(p);
//		log.debug(p.stable());
		
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
//						extension.addTriple(new Triple(m_hashes.getValue(v.getId()), m_hashes.getValue(label), m_hashes.getValue(y.getId())));
					}
				}
			}
		}
		
		return g;
	}
}

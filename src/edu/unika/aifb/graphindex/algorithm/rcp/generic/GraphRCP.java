package edu.unika.aifb.graphindex.algorithm.rcp.generic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.ClassBasedEdgeFactory;
import org.jgrapht.graph.DirectedMultigraph;

import edu.unika.aifb.graphindex.graph.Graph;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.QueryNode;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.util.Util;

/**
 * @author gl
 *
 * @param <V>
 * @param <E>
 */
/**
 * @author gl
 *
 * @param <V>
 * @param <E>
 */
public class GraphRCP<V extends Comparable<V>,E> {
	private LabelProvider<V,E> m_labelProvider;
	private static final Logger log = Logger.getLogger(GraphRCP.class);
	
	public GraphRCP(LabelProvider<V,E> provider) {
		m_labelProvider = provider;
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
	private void refinePartition(Partition<V,E> p, Set<GVertex<V,E>> image, Splitters<V,E> w, Integer currentIteration) {
		List<Block<V,E>> splitBlocks = new LinkedList<Block<V,E>>();
		for (GVertex<V,E> x : image) {
			if (currentIteration != null && x.getMovedIn() == currentIteration) {
				continue;
			}
			
			Block<V,E> block = x.getBlock();
			if (block.getSplitBlock() == null) {
				Block<V,E> splitBlock = new Block<V,E>();
				block.setSplitBlock(splitBlock);
				splitBlocks.add(block);
			}
			
			block.remove(x);
			block.getSplitBlock().add(x);
			x.setBlock(block.getSplitBlock());
			
			if (currentIteration != null)
				x.setMovedIn(currentIteration);
		}
		
		for (Block<V,E> sb : splitBlocks) {
			if (sb.size() == 0) {
				Block<V,E> newBlock = sb.getSplitBlock();
				sb.setSplitBlock(null);
				sb.getXBlock().addBlock(newBlock);
				sb.getXBlock().remove(sb);
				p.remove(sb);
				p.add(newBlock);
			}
			else {
				Block<V,E> newBlock = sb.getSplitBlock();
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
	private Set<GVertex<V,E>> imageOf(Collection<GVertex<V,E>> vertices, E label) {
		Set<GVertex<V,E>> image = new HashSet<GVertex<V,E>>();
		for (GVertex<V,E> v :vertices) {
			if (v.getImage(label) == null)
				continue;
			image.addAll(v.getImage(label));
		}
		return image;
	}
	
	private Partition<V,E> processPartition(Partition<V,E> p, Set<E> labels) {
		Splitters<V,E> w = new Splitters<V,E>();
		XBlock<V,E> startXB = new XBlock<V,E>();
		Set<XBlock<V,E>> cbs = new HashSet<XBlock<V,E>>();

		for (Block<V,E> b : p.getBlocks())
			startXB.addBlock(b);
		w.add(startXB);
		
		int movedIn = 0;
		
//		System.gc();
//		log.debug("setup complete, " + Util.memory());
//		log.debug("starting refinement process...");
		
		long start = System.currentTimeMillis();
		int steps = 0;
		
		startXB.calcInfo();

		if (p.getBlocks().size() == 1) {
			Block<V,E> startBlock = p.getBlocks().get(0);
			List<GVertex<V,E>> b_ = new ArrayList<GVertex<V,E>>(startBlock.size());
			for (Iterator<GVertex<V,E>> i = startBlock.iterator(); i.hasNext(); )
				b_.add(i.next());
			
			for (E label : labels) {
				refinePartition(p, imageOf(b_, label), w, movedIn);
				movedIn++;
			}
			steps++;
		}
		
			
		while (w.size() > 0) {
			XBlock<V,E> s = w.remove();
			
			Block<V,E> b;
			if (s.getFirstBlock().size() <= s.getSecondBlock().size())
				b = s.getFirstBlock();
			else
				b = s.getSecondBlock();
			
			s.remove(b);
			
			if (s.isCompound())
				w.add(s);
			
			XBlock<V,E> s_ = new XBlock<V,E>(b);
			s_.calcInfo(); // TODO is not really necessary, as we compute info for b below
			cbs.add(s_);
			
			List<GVertex<V,E>> b_ = new ArrayList<GVertex<V,E>>(b.size());
			for (Iterator<GVertex<V,E>> i = b.iterator(); i.hasNext(); )
				b_.add(i.next());
			
			for (E label : labels) {
//				log.debug("LABEL " + label);

				// calculate E(B) and LD
				Set<GVertex<V,E>> imageB = new HashSet<GVertex<V,E>>();
				Map<GVertex<V,E>,Integer> ld = new HashMap<GVertex<V,E>,Integer>();
				for (GVertex<V,E> v : b_) {
					if (v.getImage(label) == null)
						continue;
					
					imageB.addAll(v.getImage(label));
					
					for (GVertex<V,E> y : v.getImage(label)) {
						if (!ld.containsKey(y))
							ld.put(y, 1);
						else
							ld.put(y, ld.get(y) + 1);
					}
				}

				refinePartition(p, imageB, w, null);

				// calculate E(B) - E(S - B)
				Set<GVertex<V,E>> imageBSB = new HashSet<GVertex<V,E>>();
				for (GVertex<V,E> v : imageB) {
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
				for (GVertex<V,E> v : ld.keySet())
					s.decInfo(v, label, ld.get(v));
			}
	
//			log.debug("e " + p.stable(b_));
			
			steps++;
			
			long duration = (System.currentTimeMillis() - start) / 1000;
//			if (steps % 500 == 0)
//				log.info(" steps: " + steps + ", psize: " + p.getBlocks().size() + ", duration: " + duration + " seconds, " + Util.memory());
		}
//		log.debug(p.stable());
//		log.info("partition size: " + p.m_blocks.size());
//		log.info("steps: " + steps);

		purgeSelfloops(p);
		
		return p;
	}
	
	private void purgeSelfloops(Partition<V,E> p) {
		List<Block<V,E>> newBlocks = new LinkedList<Block<V,E>>();
		for (Block<V,E> b : p.getBlocks()) {
			GVertex<V,E> v = b.m_head;
			boolean hasSelfloop = false;
			for (GVertex<V,E> v2 : b) {
				if (v != v2) {
					for (E label : v.getEdgeLabels()) {
						if (v.getImage(label).contains(v2)) {
							hasSelfloop = true;
							break;
						}
					}
				}
			}
			
			if (hasSelfloop) {
				Block<V,E> nb = new Block<V,E>();
				int i = 0;
				int x = b.size() / 2;
				for (GVertex<V,E> n : b) {
					n.getBlock().remove(n);
					nb.add(n);
					i++;
					if (i >= x)
						break;
				}
				newBlocks.add(nb);
			}
		}
		for (Block<V,E> nb : newBlocks)
			p.add(nb);
	}
	
	public Graph<QueryNode> createQueryGraph(DirectedGraph<V,LabeledEdge<V>> graph) throws StorageException {
		List<GVertex<V,E>> vertices = new ArrayList<GVertex<V,E>>();
		Map<V,GVertex<V,E>> v2o = new HashMap<V,GVertex<V,E>>();
		for (V v : graph.vertexSet()) {
			GVertex<V,E> vertex = new GVertexImpl<V,E>(v);
			v2o.put(v, vertex);
			vertices.add(vertex);
		}
		
		for (V v : graph.vertexSet()) {
			GVertex<V,E> vertex = v2o.get(v);
			for (LabeledEdge<V> e : graph.outgoingEdgesOf(v))
				vertex.addToImage((E)e.getLabel(), v2o.get(e.getDst()));
		}
		
		Set<E> labels = new HashSet<E>();
		for (LabeledEdge<V> e : graph.edgeSet())
			labels.add((E)e.getLabel()); // TODO

		Partition<V,E> p = new Partition<V,E>();

//		Block<V,E> b = new Block<V,E>();
//		for (GVertex<V,E> v : vertices)
//			b.add(v);
//		p.add(b);
		
		for (GVertex<V,E> v : vertices) {
			Block<V,E> b = new Block<V,E>();
			b.add(v);
			p.add(b);
		}
		
		DirectedGraph<QueryNode,LabeledEdge<QueryNode>> q;
		do {
			p = processPartition(p, labels);
			q = createQueryGraph(p);
			
			p = splitNodes(graph, q, p); // returns null when done, q contains graph
		}
		while (p != null);
		
		return new Graph<QueryNode>(q);
	}
	
	
	/**
	 * 
	 * 
	 * @param pg
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private Partition<V,E> splitNodes(DirectedGraph<V,LabeledEdge<V>> graph, DirectedGraph<QueryNode,LabeledEdge<QueryNode>> pg, Partition<V,E> p) {
		// TODO terminology confusing
		boolean modified = false;
		for (QueryNode qn : pg.vertexSet()) {
			if (pg.inDegreeOf(qn) < 2)
				continue;
			
			Map<QueryNode,Integer> n2inc = new HashMap<QueryNode,Integer>();
			Map<String,Set<LabeledEdge<QueryNode>>> l2e = new HashMap<String,Set<LabeledEdge<QueryNode>>>();
			Set<QueryNode> sources = new HashSet<QueryNode>();
			
			// find nodes that have multiple edges to the current node
			for (LabeledEdge<QueryNode> e : pg.incomingEdgesOf(qn)) {
				if (e.getSrc().size() <= 1) // ignore source nodes with only one member
					continue;
				
				if (n2inc.containsKey(e.getSrc())) {
					n2inc.put(e.getSrc(), n2inc.get(e.getSrc()) + 1);
					sources.add(e.getSrc());
				}
				else
					n2inc.put(e.getSrc(), 1);
				
				// group edges by label, used for splitting later
				if (!l2e.containsKey(e.getLabel())) {
					l2e.put(e.getLabel(), new HashSet<LabeledEdge<QueryNode>>());
				}
				l2e.get(e.getLabel()).add(e);
			}

			// if there are no source nodes with multiple edges to the current node, skip to next node
			if (sources.size() == 0)
				continue;
			
			// all nodes in sources have at least two edges to the current node
			// because of the way the rcp algorithm works, all nodes in a source
			// node have at least one successor in the current node

			// splitting a source node is accomplished by separating nodes contained
			// in the source by their label to nodes in the current node
			
			modified = true;
			
			for (QueryNode srcNode : sources) {
				Block<V,E> srcBlock = null;
				for (Block<V,E> b : p.getBlocks()) {
					if (b.getName().equals(srcNode.getName()))
						srcBlock = b;
				}
				
				Map<V,Set<String>> sn2l = new HashMap<V,Set<String>>();
				
				Set<V> srcBlockNodes = new HashSet<V>();
				for (GVertex<V,E> v : srcBlock) {
					srcBlockNodes.add(v.getLabel());
					for (LabeledEdge<V> e : graph.outgoingEdgesOf(v.getLabel())) {
						if (!sn2l.containsKey(v.getLabel()))
							sn2l.put(v.getLabel(), new HashSet<String>());
						sn2l.get(v.getLabel()).add(e.getLabel());
					}
				}
				
				Map<String,Set<V>> l2sn = new HashMap<String,Set<V>>(); // label 2 source nodes
				for (String v : qn.getMembers()) {
					for (LabeledEdge<V> e : graph.incomingEdgesOf((V)v)) {
						if (!srcBlockNodes.contains(e.getSrc()))
							continue;
						
						if (!l2sn.containsKey(e.getLabel()))
							l2sn.put(e.getLabel(), new HashSet<V>());
						l2sn.get(e.getLabel()).add(e.getSrc());
					}
				}
				
				for (Set<V> sn : l2sn.values())
					srcBlockNodes.removeAll(sn);
				
				for (V v : srcBlockNodes) {
					boolean found = false;
					for (LabeledEdge<V> e : graph.outgoingEdgesOf(v)) {
						for (V sn : sn2l.keySet()) {
							if (!sn.equals(v) && sn2l.get(sn).contains(e.getLabel())) {
								found = true;
								for (Set<V> sns : l2sn.values())
									if (sns.contains(sn))
										sns.add(v);
							}
						}
					}
					if (!found)
						throw new UnsupportedOperationException("blök");
				}
				
				List<GVertex<V,E>> b_ = new ArrayList<GVertex<V,E>>(srcBlock.size());
				for (GVertex<V,E> v : srcBlock)
					b_.add(v);
				
				for (Iterator<Set<V>> i = l2sn.values().iterator(); i.hasNext(); ) {
					Set<V> sn = i.next();
					if (!i.hasNext()) // skip last set of edges, their source nodes stay in the block
						continue;
					
					Block<V,E> b = new Block();
					for (GVertex<V,E> v : b_) {
						if (sn.contains(v.getLabel())) {
							srcBlock.remove(v);
							b.add(v);
						}
					}
					p.add(b);
				}
			}
		}
		
		return modified ? p : null;
	}
	
	private DirectedGraph<QueryNode,LabeledEdge<QueryNode>> createQueryGraph(Partition<V,E> p) throws StorageException {
		Map<String,List<String>> b2l = new HashMap<String,List<String>>();
		DirectedGraph<QueryNode,LabeledEdge<QueryNode>> g = new DirectedMultigraph<QueryNode,LabeledEdge<QueryNode>>(new ClassBasedEdgeFactory<QueryNode,LabeledEdge<QueryNode>>((Class<? extends LabeledEdge<QueryNode>>)LabeledEdge.class));
		Map<String,QueryNode> b2qn = new HashMap<String,QueryNode>();
		for (Block<V,E> b : p.getBlocks()) {
			QueryNode qn = b2qn.get(b.getName());
			if (qn == null) {
				qn = new QueryNode(b.getName());
				b2qn.put(b.getName(), qn);
				g.addVertex(qn);
			}

			List<String> vertices = new ArrayList<String>();
			for (GVertex<V,E> v : b) {
				String vl = m_labelProvider.getVertexLabel(v.getLabel());
				for (E label : v.getEdgeLabels()) {
					for (GVertex<V,E> y : v.getImage(label)) {
						QueryNode targetQn = b2qn.get(y.getBlock().getName());
						if (targetQn == null) {
							targetQn = new QueryNode(y.getBlock().getName());
							b2qn.put(y.getBlock().getName(), targetQn);
							g.addVertex(targetQn);
						}
						g.addEdge(qn, targetQn, new LabeledEdge<QueryNode>(qn, targetQn, m_labelProvider.getEdgeLabel(label)));
					}
				}
				qn.addMember(vl);
			}
		}
//		log.debug("b2l: " + b2l);
		
		return g;
	}
}

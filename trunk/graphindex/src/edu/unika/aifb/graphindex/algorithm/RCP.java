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

import org.apache.log4j.Logger;
import org.jgrapht.DirectedGraph;

import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.NamedGraph;
import edu.unika.aifb.graphindex.graph.SVertex;
import edu.unika.aifb.graphindex.storage.Extension;
import edu.unika.aifb.graphindex.storage.GraphManager;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;
import edu.unika.aifb.graphindex.storage.Triple;

public class RCP {
	public class Partition {
		private List<Block> m_blocks;
		private Set<String> m_uniqueLabels;
		
		public Partition() {
			m_blocks = new LinkedList<Block>();
			setUniqueLabels(new HashSet<String>());
		}
		
		public void addBlock(Block block) {
			m_blocks.add(block);
		}

		public List<Block> getBlocks() {
			return m_blocks;
		}

		public void setUniqueLabels(Set<String> m_uniqueLabels) {
			this.m_uniqueLabels = m_uniqueLabels;
		}

		public Set<String> getUniqueLabels() {
			return m_uniqueLabels;
		}

		public String toString() {
			return m_blocks.toString();
		}
	}
	
	public class Block {
		private Set<SVertex> m_vertices;
		private String m_name;
		public Block newBlock = null;
		private XBlock m_xblock;
		
		public Block() {
			m_vertices = new HashSet<SVertex>();
			m_name = "b" + ++m_id;
		}
		
		public Block(SVertex vertex) {
			this();
			addVertex(vertex);
		}
		
		public Block(Set<SVertex> vertices) {
			m_vertices = vertices;
			m_name = "b" + ++m_id;
			for (SVertex v : vertices)
				v.setBlock(this);
		}
		
		public String getName() {
			return m_name;
		}
		
		public XBlock getXBlock() {
			return m_xblock;
		}
		
		public void setXBlock(XBlock xb) {
			m_xblock = xb;
		}

		public void addVertex(SVertex v) {
			m_vertices.add(v);
			v.setBlock(this);
		}
		
		public void addVertices(Collection<SVertex> vs) {
			m_vertices.addAll(vs);
			for (SVertex v : vs)
				v.setBlock(this);
		}
		
		public void removeVertex(SVertex v) {
			m_vertices.remove(v);
		}

		public Set<SVertex> getVertices() {
			return m_vertices;
		}
		
		public int size() {
			return m_vertices.size();
		}
		
		public String toString() {
			return m_name + getVertices().toString();
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
	
	private class XBlock {
		private List<Block> m_blocks;
		
		public XBlock() {
			m_blocks = new LinkedList<Block>();
		}
		
		public void addBlock(Block b) {
			m_blocks.add(b);
			b.setXBlock(this);
		}
		
		public Block getBlock(int i) {
			return m_blocks.get(i);
		}
		
		public Block removeBlock(int i) {
			return m_blocks.remove(i);
		}
		
		public void removeBlock(Block b) {
			m_blocks.remove(b);
		}
		
		public boolean isCompound() {
			return m_blocks.size() > 1;
		}
		
		public String toString() {
			return m_blocks.toString().replaceAll("^\\[", "(").replaceAll("\\]$", ")");
		}
	}
	
	private class Tuple {
		public String e, v;
		public Tuple(String e, String v) {
			this.e = e;
			this.v = v;
		}
		
		public String toString() {
			return "(" + e + "," + v + ")";
		}
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((e == null) ? 0 : e.hashCode());
			result = prime * result + ((v == null) ? 0 : v.hashCode());
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
			Tuple other = (Tuple)obj;
			if (e == null) {
				if (other.e != null)
					return false;
			} else if (!e.equals(other.e))
				return false;
			if (v == null) {
				if (other.v != null)
					return false;
			} else if (!v.equals(other.v))
				return false;
			return true;
		}
	}
	
	private int m_id = 0;
	private DirectedGraph<SVertex,LabeledEdge<SVertex>> m_graph;
	private static final Logger log = Logger.getLogger(RCP.class);
	private GraphManager m_gm;
	
	public RCP(DirectedGraph<SVertex,LabeledEdge<SVertex>> graph) {
		m_gm = StorageManager.getInstance().getGraphManager();
		m_graph = graph;
	}
	
	private void logxb(String msg, List<XBlock> xblocks) { 
		log.debug(msg + xblocks.toString().replaceAll("^\\[|\\]$", ""));
	}
	
	private NamedGraph<String,LabeledEdge<String>> createIndexGraph2(Partition p) throws StorageException {
		NamedGraph<String,LabeledEdge<String>> g = m_gm.graph();
		for (Block b : p.getBlocks()) {
			g.addVertex(b.getName());

			for (SVertex v : b.getVertices()) {
				for (LabeledEdge<SVertex> e : m_graph.outgoingEdgesOf(v)) {
					g.addVertex(e.getDst().getBlock().getName());
					g.addEdge(b.getName(), e.getDst().getBlock().getName(), new LabeledEdge<String>(b.getName(), e.getDst().getBlock().getName(), e.getLabel()));
				}
			}
		}
		
		return g;
	}
	
	private NamedGraph<String,LabeledEdge<String>> createIndexGraph(Partition p) throws StorageException {
		Map<Tuple,Set<Block>> incLabels = new HashMap<Tuple,Set<Block>>();
		
		for (Block b : p.getBlocks()) {
			Set<Tuple> tuples = new HashSet<Tuple>();
			for (SVertex v : b.getVertices())
				for (LabeledEdge<SVertex> e : m_graph.incomingEdgesOf(v))
					tuples.add(new Tuple(e.getLabel(), v.getLabel()));
		
			for (Tuple t : tuples) {
				Set<Block> blocks = incLabels.get(t);
				if (blocks == null) {
					blocks = new HashSet<Block>();
					incLabels.put(t, blocks);
				}
				blocks.add(b);
			}
		}
		
//		log.debug(incLabels);
		
		NamedGraph<String,LabeledEdge<String>> g = m_gm.graph();
		
		for (Block b : p.getBlocks()) {
			Set<Triple> triples = new HashSet<Triple>();

//			log.debug(b);
			for (SVertex v : b.getVertices()) {
				for (LabeledEdge<SVertex> e : m_graph.outgoingEdgesOf(v)) {
					Tuple t = new Tuple(e.getLabel(), m_graph.getEdgeTarget(e).getLabel());
					Set<Block> targets = incLabels.get(t);
					if (targets != null) {
						for (Block dst : targets) {
							g.addEdge(b.getName(), e.getLabel(), dst.getName());
						}
					}
				}
				
				for (LabeledEdge<SVertex> e : m_graph.incomingEdgesOf(v)) {
					triples.add(new Triple(m_graph.getEdgeSource(e).getLabel(), e.getLabel(), v.getLabel()));
				}
				
				triples.add(new Triple("", "", v.getLabel()));
			}
			
			if (StorageManager.getInstance().getExtensionManager() != null) {
				Extension extension = StorageManager.getInstance().getExtensionManager().extension(b.getName());
				extension.addTriples(triples);
			}
		}
//		Util.printDOT("idx.dot", g);
		return g;
	}
	
	private long[] starts = new long[10];
	private long[] times = new long [10];
	
	private void start(int timer) {
		starts[timer] = System.currentTimeMillis();
	}
	
	private void add(int timer) {
		times[timer] += System.currentTimeMillis() - starts[timer];
	}
	
	public void purgeSelfloops(Partition p) {
		List<Block> newBlocks = new LinkedList<Block>();
		for (Block b : p.getBlocks()) {
			SVertex v = b.getVertices().toArray(new SVertex[] {})[0];
			boolean hasSelfloop = false;
			for (SVertex v2 : b.getVertices()) {
				if (v != v2 && m_graph.getAllEdges(v, v2).size() > 0) {
					hasSelfloop = true;
					break;
				}
			}
			
			if (hasSelfloop) {
				Block nb = new Block();
				int i = 0;
				int x = b.getVertices().size() / 2;
				Iterator<SVertex> vertices = b.getVertices().iterator();
				while (i < x) {
					nb.addVertex(vertices.next());
					vertices.remove();
					i++;
				}
				newBlocks.add(nb);
			}
		}
		for (Block nb : newBlocks)
			p.addBlock(nb);
	}
	
	public Partition createPartition(Set<SVertex> vertices) {
//		Map<Tuple<String,String>,Integer> vcs = new HashMap<Tuple<String,String>,Integer>();
//		Map<Tuple<String,LabeledEdge<String>>,Integer> ecs = new HashMap<Tuple<String,LabeledEdge<String>>,Integer>();

		int initialElements = vertices.size();
		
		log.debug("vertices: " + vertices.size());

		Set<String> labels = new HashSet<String>();
		for (LabeledEdge<SVertex> e : m_graph.edgeSet())
			labels.add(e.getLabel());
		
		Partition p = new Partition();
		XBlock xb = new XBlock();
		Block nb = new Block(vertices);
		p.addBlock(nb);
		xb.addBlock(nb);
		
		Set<SVertex> vb = new HashSet<SVertex>(nb.getVertices());
		for (String label : labels) {
			Set<SVertex> image = new HashSet<SVertex>();
			for (SVertex x : vb) {
//				log.debug(x);
				for (LabeledEdge<SVertex> e : m_graph.outgoingEdgesOf(x))
					if (e.getLabel().equals(label)) 
						image.add(m_graph.getEdgeTarget(e));
			}
			
//			log.debug("image " + label + ": " + image);
			
			List<Block> splitBlocks = new LinkedList<Block>();
			for (SVertex v : image) {
//				log.debug(v);
				Block d = v.getBlock();
				
				if (d.newBlock == null) {
					d.newBlock = new Block();
					d.getXBlock().addBlock(d.newBlock);
					splitBlocks.add(d);
					p.addBlock(d.newBlock);
				}
				d.newBlock.addVertex(v);
				d.removeVertex(v);
			}
			
			for (Block d : splitBlocks) {
				if (d.size() == 0) {
					d.getXBlock().removeBlock(d);
					p.getBlocks().remove(d);
				}
				d.newBlock = null;
			}
		}
		
		vb = null;
		
//		log.debug(p);
		
//		for (String label : labels) {
//			p.split(nb, label);
//			if (p.getBlocks().size() > 1)
//				break;
//		}

		if (p.getBlocks().size() == 1) {
			log.debug("huh");
			return null;
		}
		
		List<XBlock> xblocks = new ArrayList<XBlock>();
		xblocks.add(xb);
		
		List<XBlock> compounds = new ArrayList<XBlock>();
		compounds.add(xb);
		
		long start = System.currentTimeMillis();
		long bstart, ts = 0, t3 = 0, t4 = 0, t5a = 0, t5 = 0, t6 = 0, t6a = 0, t6b = 0;
		
		int iterations = 0;
		
		System.gc();
		
		while (compounds.size() > 0) {
//			log.debug("p: " + p);
//			logxb("xblocks: ", xblocks);
//			log.debug("v2b: " + v2b);
			
			// 1
//			log.debug("--- 1 ---");
			bstart = System.currentTimeMillis();
			XBlock s = compounds.remove(0);
			
//			ecs.clear();
//			for (Block sb : s.m_blocks) {
//				for (String v : sb.getVertices()) {
//					for (LabeledEdge<String> e : m_graph.outgoingEdgesOf(v)) {
//							String x = m_graph.getEdgeTarget(e);
//							Tuple<String,LabeledEdge<String>> t = new Tuple<String,LabeledEdge<String>>(e.getLabel(), e);
//							if (ecs.get(t) == null) {
//								ecs.put(t, 0);
//							}
//							ecs.put(t, ecs.get(t) + 1);
//					}
//				}
//			}
			
			Block b;
			if (s.getBlock(0).size() < s.getBlock(1).size()) {
				b = s.removeBlock(0);
			}
			else {
				b = s.removeBlock(1);
			}
			
//			log.debug("chosen b: " + b);
//			log.debug("ecs: " + ecs);
			
			Set<SVertex> s_bVertices = new HashSet<SVertex>();
			for (Block sb : s.m_blocks) {
				s_bVertices.addAll(sb.getVertices());
			}
//			log.debug("s - b: " + s_bVertices);
			
			// 2
//			log.debug("--- 2 ---");
			XBlock s_ = new XBlock();
			s_.addBlock(b);
			xblocks.add(s_);
			
			if (s.isCompound())
				compounds.add(s);

//			logxb("xblocks: ", xblocks);
//			logxb("compounds: ", compounds);
			
			Set<SVertex> b_ = new HashSet<SVertex>(b.getVertices());
			ts += System.currentTimeMillis() - bstart;
			for (String label : labels) {
				// 3
//				log.debug("--- 3 ---");
				bstart = System.currentTimeMillis();
				Set<SVertex> bImage = new HashSet<SVertex>();
				for (SVertex x : b_) {
					for (LabeledEdge<SVertex> e : m_graph.outgoingEdgesOf(x)) {
						if (e.getLabel().equals(label)) {
							bImage.add(m_graph.getEdgeTarget(e));
							
//							Tuple<String,String> t = new Tuple<String,String>(label, m_graph.getEdgeTarget(e));
//							if (vcs.get(t) == null) {
//								vcs.put(t, 0);
//							}
//							vcs.put(t, vcs.get(t) + 1);
						}
					}
				}
				t3 += System.currentTimeMillis() - bstart;
//				log.debug("bimage " + label + ": " + bImage);
//				log.debug("image of b " + b + ": " + bImage);
//				log.debug("vcs: " + vcs);
				
				// 4
//				log.debug("--- 4 ---");
				bstart = System.currentTimeMillis();
				List<Block> splitBlocks = new LinkedList<Block>();
				for (SVertex v : bImage) {
					Block d = v.getBlock();
					
					if (d.newBlock == null) {
						d.newBlock = new Block();
						d.getXBlock().addBlock(d.newBlock);
						splitBlocks.add(d);
						p.addBlock(d.newBlock);
					}
					d.newBlock.addVertex(v);
					d.removeVertex(v);
				}
				
				for (Block d : splitBlocks) {
					if (d.size() == 0) {
						d.getXBlock().removeBlock(d);
						p.getBlocks().remove(d);
					}
					else {
						if (d.getXBlock().isCompound() && !compounds.contains(d.getXBlock()))
							compounds.add(d.getXBlock());
					}
					d.newBlock = null;
				}
				t4 += System.currentTimeMillis() - bstart;
				
//				log.debug("p: " + p);
//				logxb("xblocks: ", xblocks);
			}
			
			bstart = System.currentTimeMillis();
			
			Map<String,Set<SVertex>> imageByLabel = new HashMap<String,Set<SVertex>>();
			
			for (SVertex x : s_bVertices) {
				for (LabeledEdge<SVertex> e : m_graph.outgoingEdgesOf(x)) {
					Set<SVertex> image = imageByLabel.get(e.getLabel());
					if (image == null) {
						image = new HashSet<SVertex>();
						imageByLabel.put(e.getLabel(), image);
					}
					image.add(m_graph.getEdgeTarget(e));
				}
			}
			
//			Map<String,Set<String>> imageByLabel2 = new HashMap<String,Set<String>>();
//			
//			for (String x : s_bVertices) {
//
//				Map<String,Set<String>> label2image = v2label2image.get(x);
//				if (label2image == null) {
//					label2image = new HashMap<String,Set<String>>();
//					v2label2image.put(x, label2image);
//					
//					for (LabeledEdge<String> e : m_graph.outgoingEdgesOf(x)) {
//						Set<String> image = label2image.get(e.getLabel());
//						if (image == null) {
//							image = new HashSet<String>();
//							label2image.put(e.getLabel(), image);
//						}
//						image.add(e.getDst());
//					}
//				}
//				
//				for (String label : label2image.keySet()) {
//					Set<String> image = imageByLabel2.get(label);
//					if (image == null) {
//						image = new HashSet<String>();
//						imageByLabel2.put(label, image);
//					}
//					image.addAll(label2image.get(label));
//				}
//			}
			
//			log.debug(imageByLabel.equals(imageByLabel2));
			
			t5a += System.currentTimeMillis() - bstart;
			
			for (String label : labels) {	
				// 5
				Set<SVertex> imageBSB = imageByLabel.get(label);
				if (imageBSB == null)
					continue;
				
//				int bsbsize = imageBSB.size();
//				imageBSB.removeAll(bimageByLabel.get(label));
//					
//				if (bsbsize != imageBSB.size())
//					log.debug("removed " + bsbsize + "->" + imageBSB.size());
//			
//				if (imageBSB.size() == 0)
//					continue;
				
//				for (String x : b_) {
//					for (LabeledEdge<String> e : m_graph.outgoingEdgesOf(x)) {
//						Tuple<String,String> vt = new Tuple<String,String>(label, m_graph.getEdgeTarget(e));
//						Tuple<String,LabeledEdge<String>> et = new Tuple<String,LabeledEdge<String>>(label, e);
//						log.debug(e + " " + vcs.get(vt) + " " + ecs.get(et));
//						log.debug(x);
//						
//						if (vcs.get(vt) != null && ecs.get(et) != null && vcs.get(vt).equals(ecs.get(et)))
//							imageBSB.add(x);
//					}
//				}
				
//				log.debug("imageBSB: " + imageBSB);
				
				// 6
//				log.debug("--- 6 ---");
				bstart = System.currentTimeMillis();
				List<Block> splitBlocks = new LinkedList<Block>();
				for (SVertex v : imageBSB) {
					start(0);
					Block d = v.getBlock();
					add(0);
					
					start(1);
					if (d.newBlock == null) {
						d.newBlock = new Block();
						d.getXBlock().addBlock(d.newBlock);
						splitBlocks.add(d);
						p.addBlock(d.newBlock);
					}
					add(1);
					
					start(2);
					d.newBlock.addVertex(v);
					add(2);
					
					start(3);
					d.removeVertex(v);
					add(3);
				}
				t6a += System.currentTimeMillis() - bstart;
				bstart = System.currentTimeMillis();
				
				for (Block d : splitBlocks) {
					if (d.size() == 0) {
						d.getXBlock().removeBlock(d);
						p.getBlocks().remove(d);
					}
					else {
						if (d.getXBlock().isCompound() && !compounds.contains(d.getXBlock()))
							compounds.add(d.getXBlock());
					}
					d.newBlock = null;
				}
				t6b += System.currentTimeMillis() - bstart;
				t6 += System.currentTimeMillis() - bstart;
				
//				log.debug("p: " + p);
//				logxb("xblocks: ", xblocks);
			}
			
			
//			log.debug("current csize: " + compounds.size() + ", current psize: " + p.getBlocks().size() + ", duration: " + ((System.currentTimeMillis() - start) / 1000) + " seconds");
			
			if (iterations % 20 == 0) {
				int cblocks = 0, cvertices = 0;
				for (XBlock c : compounds) {
					for (Block cb : c.m_blocks)
						cvertices += cb.getVertices().size();
					cblocks += c.m_blocks.size();
				}
//				log.debug("blocks in c: " + cblocks + ", vertices in c: " + cvertices + ", processed: " + (initialElements - cvertices) + ", vertices/s: " + ((initialElements - cvertices) / ((System.currentTimeMillis() - start) / 1000.0)));
//				log.debug(ts + " " + t3 + " " + t4 + " " + t5a + "/" + t5 + " " + t6a + "/" + t6b);
//				String timing = "";
//				for (int i = 0; i < times.length; i++)
//					timing += i + ":" + times[i] + " ";
//				log.debug(timing);
			}
			if ((System.currentTimeMillis() - start) / 1000 > 30) {
				log.debug("current csize: " + compounds.size() + ", current psize: " + p.getBlocks().size() + ", duration: " + ((System.currentTimeMillis() - start) / 1000) + " seconds");
			}
			log.debug(p.m_blocks.size());
//			log.debug("------- FINISH");
			
			iterations++;
		}
		
//		log.debug(ts + " " + t3 + " " + t4 + " " + t5a + "/" + t5 + " " + t6a + "/" + t6b);
//		for (int i = 0; i < times.length; i++)
//			System.out.print(i + ":" + times[i] + " ");
//		System.out.println();

		log.debug("before purge: " + p.m_blocks.size());
		purgeSelfloops(p);
		log.info(p.getBlocks().size());
		
		List<Block> blocks = new ArrayList<Block>(p.getBlocks());
		Collections.sort(blocks, new Comparator<Block>() {
			public int compare(Block o1, Block o2) {
				return ((Integer)o1.size()).compareTo(o2.size());
			}
		});
		
		for (Block b : blocks) {
			log.debug(b.size());
		}
		
		return p;
	}
	
	public NamedGraph<String,LabeledEdge<String>> createOneIndex(Set<SVertex> vertices) throws StorageException {
		return createIndexGraph2(createPartition(vertices));
	}
}

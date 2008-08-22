package edu.unika.aifb.graphindex.algorithm;

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

import edu.unika.aifb.graphindex.Util;
import edu.unika.aifb.graphindex.graph.LabeledEdge;
import edu.unika.aifb.graphindex.graph.NamedGraph;
import edu.unika.aifb.graphindex.storage.Extension;
import edu.unika.aifb.graphindex.storage.ExtensionManager;
import edu.unika.aifb.graphindex.storage.GraphManager;
import edu.unika.aifb.graphindex.storage.StorageException;
import edu.unika.aifb.graphindex.storage.StorageManager;
import edu.unika.aifb.graphindex.storage.Triple;

public class NaiveOneIndex {
	private static int m_id = 0;
	
	private class Partition {
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

		public void split(Block x, String label) {
			Set<String> image = x.image(label);
			
			if (image.size() == 0)
				return;
			
			List<Block> newBlocks = new LinkedList<Block>();
			for (Iterator<Block> i = m_blocks.iterator(); i.hasNext(); ) {
				Block b = i.next();
				
				Set<String> is = new HashSet<String>(b.getVertices());
				is.retainAll(image);
				if (is.size() == 0)
					continue;
				
				Set<String> ds = new HashSet<String>(b.getVertices());
				ds.removeAll(image);
				
				if (is.size() > 0 && ds.size() > 0) {
					i.remove();
					newBlocks.add(new Block(is));
					newBlocks.add(new Block(ds));
				}
			}
			m_blocks.addAll(newBlocks);
		}

		public boolean stable() {
			for (Block b : getBlocks()) {
				for (Block c : getBlocks()) {
					if (!b.stable(c))
						return false;
				}
			}
			return true;
		}
		
		public String toString() {
			return m_blocks.toString();
		}
	}
	
	private class Block {
		private Set<String> m_vertices;
		private Set<String> m_labels;
		private String m_name;
		
		public Block() {
			m_vertices = new HashSet<String>();
			m_labels = new HashSet<String>();
			m_name = "b" + ++m_id;
		}
		
		public Block(Set<String> vertices) {
			m_vertices = vertices;
			m_labels = new HashSet<String>();
			m_name = "b" + ++m_id;
			initLabels();
		}
		
		private String getName() {
			return m_name;
		}

		private void initLabels() {
			addLabels(m_vertices);
		}
		
		private void addLabels(Collection<String> vertices) {
			for (String v : vertices)
				addLabels(v);
		}
		
		private void addLabels(String v) {
			for (LabeledEdge<String> e : m_graph.outgoingEdgesOf(v)) {
				m_labels.add(e.getLabel());
			}
		}
		
		public Set<String> image(String label) {
			Set<String> image = new HashSet<String>();
			
			if (!m_labels.contains(label))
				return image;
			
			for (String v : m_vertices) {
				for (LabeledEdge<String> e : m_graph.outgoingEdgesOf(v)) {
					if (e.getLabel().equals(label))
						image.add(e.getDst());
				}
			}
			
			return image;
		}

		public boolean stable(Block c) {
			for (String label : m_labels) {
				Set<String> image = image(label);
				boolean subset = image.containsAll(c.getVertices());
				
				Set<String> is = new HashSet<String>(c.getVertices());
				is.retainAll(image);
				if (is.size() > 0 && !subset)
					return false;
			}
			return true;
		}

		public void addVertex(String v) {
			m_vertices.add(v);
			addLabels(v);
		}
		
		public void addVertices(Collection<String> vs) {
			m_vertices.addAll(vs);
			addLabels(vs);
		}

		public Set<String> getVertices() {
			return m_vertices;
		}
		
		public String toString() {
			return getVertices().toString();
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
		
	private GraphManager m_gm;
	private DirectedGraph<String,LabeledEdge<String>> m_graph;
	public int ps_min = Integer.MAX_VALUE, ps_max = 0, ps_avg = 0;
	private static final Logger log = Logger.getLogger(NaiveOneIndex.class);
	
	public NaiveOneIndex(DirectedGraph<String,LabeledEdge<String>> graph) {
		m_graph = graph;
		m_gm = StorageManager.getInstance().getGraphManager();
	}
	
	private Partition initialize(Set<String> vertices) {
		Block b = new Block();
		b.addVertices(vertices);

		Partition p = new Partition();
		p.addBlock(b);
		
		for (String vertex : vertices) {
			for (LabeledEdge<String> e : m_graph.outgoingEdgesOf(vertex)) {
				p.getUniqueLabels().add(e.getLabel());
			}
		}
		
		return p;
	}
	
	private NamedGraph<String,LabeledEdge<String>> createIndexGraph(Partition p) throws StorageException {
		Map<Tuple,Set<Block>> incLabels = new HashMap<Tuple,Set<Block>>();
		
		for (Block b : p.getBlocks()) {
			Set<Tuple> tuples = new HashSet<Tuple>();
			for (String v : b.getVertices())
				for (LabeledEdge<String> e : m_graph.incomingEdgesOf(v))
					tuples.add(new Tuple(e.getLabel(), v));
		
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
			Extension extension = StorageManager.getInstance().getExtensionManager().extension(b.getName());
			
			for (String v : b.getVertices()) {
				for (LabeledEdge<String> e : m_graph.outgoingEdgesOf(v)) {
					Tuple t = new Tuple(e.getLabel(), e.getDst());
					Set<Block> targets = incLabels.get(t);
					if (targets != null) {
						for (Block dst : targets) {
							g.addEdge(b.getName(), e.getLabel(), dst.getName());
						}
					}
				}
				
				for (LabeledEdge<String> e : m_graph.incomingEdgesOf(v)) {
					triples.add(new Triple(e.getSrc(), e.getLabel(), v));
				}
			}
			extension.addTriples(triples);
		}
//		Util.printDOT("idx.dot", g);
		return g;
	}
	
	public NamedGraph<String,LabeledEdge<String>> createOneIndex(Set<String> vertices) throws StorageException {
		Partition p = initialize(vertices);
		
		Set<Set<String>> splitters = new HashSet<Set<String>>();
		
		long start = System.currentTimeMillis();
		boolean done = false;
		while (!done) {
			int psize = p.getBlocks().size();
			List<Block> blocks = new ArrayList<Block>(p.getBlocks());
			for (Block b : blocks) {
				if (!splitters.contains(b.getVertices())) {
					splitters.add(b.getVertices());
					for (String label : p.getUniqueLabels()) {
						p.split(b, label);
					}
				}
			}
			
			if (p.getBlocks().size() != psize) {
				if (p.stable())
					done = true;
			}
			
//			log.debug(p + " " + p.stable());
//			done = true;
		}
		
		long duration = (System.currentTimeMillis() - start) / 1000;
		
		if (p.getBlocks().size() < ps_min)
			ps_min = p.getBlocks().size();
		if (p.getBlocks().size() > ps_max)
			ps_max = p.getBlocks().size();
		ps_avg += p.getBlocks().size();
		
		if (duration > 30) {
			log.debug("psize: " + p.getBlocks().size() + ", duration: " + duration + " seconds");
		}
		
		return createIndexGraph(p);
	}
}

package edu.unika.aifb.graphindex.algorithm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.log4j.Logger;

public class OneIndex {
	private static int m_id = 0;
	private class Node {
		private String m_label;
		private Map<String,List<Node>> m_out;
		private Block m_block;
		
		public Node(String label) {
			m_label = label;
			m_out = new HashMap<String,List<Node>>();
		}
		
		public void addOut(Node dst, String label) {
			List<Node> list = m_out.get(label);
			if (list == null) {
				list = new ArrayList<Node>();
				m_out.put(label, list);
			}
			list.add(dst);
		}
		
		public String getLabel() {
			return m_label;
		}
		
		public Set<String> getEdgeLabels() {
			return m_out.keySet();
		}
		
		public Block getBlock() {
			return m_block;
		}
		
		public void setBlock(Block b) {
			m_block = b;
		}
		
		public Set<Node> image() {
			Set<Node> image = new HashSet<Node>();
			for (List<Node> list : m_out.values())
				image.addAll(list);
			return image;
		}
		
		public Set<Node> image(String label) {
			List<Node> list = m_out.get(label);
			if (list != null)
				return new HashSet<Node>(list);
			else 
				return new HashSet<Node>();
		}
		
		public String toString() {
			return m_label;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((m_label == null) ? 0 : m_label.hashCode());
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
			Node other = (Node)obj;
			if (m_label == null) {
				if (other.m_label != null)
					return false;
			} else if (!m_label.equals(other.m_label))
				return false;
			return true;
		}
	}
	
	private class Block {
		private Set<Node> m_nodes;
		private Splitter m_splitter;
		private String m_name;
		
		public Block() {
			m_name = "b" + ++m_id;
			m_nodes = new HashSet<Node>();
		}
		
		public Block(Collection<Node> nodes) {
			m_name = "b" + ++m_id;
			m_nodes = new HashSet<Node>(nodes);
			for (Node n : m_nodes) 
				n.setBlock(this);
		}
		
		public Set<Node> getNodes() {
			return m_nodes;
		}
		
		public void addNode(Node n) {
			m_nodes.add(n);
		}
		
		public void removeNode(Node n) {
			m_nodes.remove(n);
		}
		
		public Splitter getSplitter() {
			return m_splitter;
		}
		
		public void setSplitter(Splitter s) {
			m_splitter = s;
		}
		
		public int size() {
			return m_nodes.size();
		}
		
		public boolean stable(Block b, String label) {
			
		}
		
		public boolean stable(Block b) {
			
		}
		
		public Set<Node> image() {
			Set<Node> image = new HashSet<Node>();
			for (Node n : m_nodes) 
				image.addAll(n.image());
			return image;
		}
		
		public Set<Node> image(String label) {
			Set<Node> image = new HashSet<Node>();
			for (Node n : m_nodes) 
				image.addAll(n.image(label));
			return image;
		}
		
		public String toString() {
			return m_name + m_nodes.toString().replaceAll("(^\\[)", "(").replaceAll("\\]$", ")");
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((m_name == null) ? 0 : m_name.hashCode());
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
	
	private class Partition {
		private LinkedList<Block> m_blocks;
		
		public Partition() {
			m_blocks = new LinkedList<Block>();
		}
		
		public void addBlock(Block b) {
			m_blocks.add(b);
		}
		
		public String toString() {
			return m_blocks.toString().replaceAll("\\[|\\]", "");
		}
	}
	
	private class Splitter {
		private Splitter m_parent = null;
		private LinkedList<Splitter> m_children;
		private boolean m_inw;
		private Block m_block;
		
		public Splitter() {
			m_children = new LinkedList<Splitter>();
		}
		
		public Splitter(Block block) {
			this();
			m_block = block;
		}
		
		public Splitter(Splitter parent) {
			this();
			m_parent = parent;
		}
		
		public Splitter(Splitter parent, Block block) {
			this(parent);
			m_block = block;
		}
		
		public Block getBlock() {
			return m_block;
		}
		
		public Splitter getParent() {
			return m_parent;
		}
		
		public boolean isInW() {
			return m_inw;
		}
		
		public void setInW(boolean inw) {
			m_inw = inw;
		}
		
		public List<Splitter> getChildren() {
			return m_children;
		}
		
		public String toString() {
			return "x" + "{" + (m_children.size() == 0 ? m_block : m_children) + "}";
		}
	}
	
	private Map<String,Node> m_nodes;
	private Set<String> m_labels;
	private Logger log = Logger.getLogger(OneIndex.class);
	
	public OneIndex() {
		m_nodes = new HashMap<String,Node>();
		m_labels = new HashSet<String>();
	}
	
	private Set<Node> image(Collection<Node> nodes, String label) {
		Set<Node> image = new HashSet<Node>();
		for (Node n : nodes)
			image.addAll(n.image(label));
		return image;
	}
	
	public void addEdge(String src, String dst, String edge) {
		Node s = m_nodes.get(src);
		if (s == null) {
			s = new Node(src);
			m_nodes.put(src, s);
		}
		
		Node d = m_nodes.get(dst);
		if (d == null) {
			d = new Node(dst);
			m_nodes.put(dst, d);
		}
		
		s.addOut(d, edge);
		m_labels.add(edge);
	}
	
	public void calc() {
		addEdge("A", "A", "d");
		addEdge("A", "B", "a");
		addEdge("A", "C", "b");
		addEdge("G", "C", "g");
		addEdge("A", "D", "a");
		addEdge("D", "E", "a");
		addEdge("B", "F", "a");
		addEdge("F", "A", "a");
		addEdge("A", "X", "y");
		addEdge("A", "Y", "y");
		addEdge("A", "Z", "y");
		addEdge("Y", "T", "x");
  		addEdge("Z", "S", "z");
		
		Block initb = new Block(m_nodes.values());
		
		Partition p = new Partition();
		p.addBlock(initb);
		
		Splitter inits = new Splitter(initb);
		
		Queue<Splitter> splitters = new LinkedList<Splitter>();
		inits.setInW(true);
		splitters.add(inits);
		initb.setSplitter(inits);
		
		log.debug("p: " + p);
		log.debug("splitlist: " + splitters);
		
		boolean done = false;
		while (!done) {
			Splitter splitter = splitters.poll();
			
			if (splitter.getChildren().size() > 0) {
				
			}
			else {
				Block b = splitter.getBlock();
				log.debug("b: " + b);
				List<Node> _b = new LinkedList<Node>(b.getNodes());
				
				for (String label : m_labels) {
					Set<Node> image = image(b.getNodes(), label);
					
					log.debug("label: " + label);
					log.debug("image(" + label + "): " + image);
					
					Map<Block,Block> newBlocks = new HashMap<Block,Block>();
					for (Node n : image) {
						Block newBlock = newBlocks.get(n.getBlock());
						if (newBlock == null) {
							newBlock = new Block();
							newBlocks.put(n.getBlock(), newBlock);
						}
						log.debug("  n.block: " + n.getBlock() + "newBlock: " + newBlock);
						n.getBlock().removeNode(n);
						newBlock.addNode(n);
					}
					log.debug("newBlocks: " + newBlocks);
				
					for (Block x : newBlocks.keySet()) {
						Block xi = newBlocks.get(x);
						log.debug("x: " + x + ", xi: " + xi);
						if (xi.size() == x.size()) {
							
						}
						else {
							for (Node n : xi.getNodes()) {
								n.setBlock(xi);
							}
							p.addBlock(xi);
							
							if (x.getSplitter().isInW()) {
								Splitter xis = new Splitter(xi);
								xi.setSplitter(xis);
								xis.setInW(true);
								splitters.add(xis);
							}
							else if (x.getSplitter().getParent() != null) {
								log.debug("comp");
							}
							else {
								log.debug("notinw");
							}
						}
					}
				}
				log.debug("p: " + p);
				log.debug("splitlist: " + splitters);
			}
			
			done = splitters.size() == 0;
		}
	}
	
	public static void main(String[] args) {
		OneIndex oi = new OneIndex();
		oi.calc();
	}
}

package edu.unika.aifb.graphindex.algorithm;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

public class BiSim {
	
	private static int qId = 0, xId = 0;

	private class Count {
		int c;
	}
	
	private class Element {
		public String label;
		public List<Edge> incident;
		public QBlock qblock;
		public int count = 0;
		
		public Element(String label) {
			this.label = label;
			this.incident = new ArrayList<Edge>();
		}
		
		public String toString() {
			return "{" + label + "" + count + ":" + incident.toString().replaceAll("\\[|\\]|\\s", "") + "}";
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((label == null) ? 0 : label.hashCode());
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
			Element other = (Element)obj;
			if (label == null) {
				if (other.label != null)
					return false;
			} else if (!label.equals(other.label))
				return false;
			return true;
		}
	}
	
	private class Edge {
		public Element x;
		public Element y;
		public int count = 0;
		
		public Edge(Element x, Element y) {
			this.x = x;
			this.y = y;
		}
		
		public String toString() {
			return x.label + ">" + y.label + "|" + count;
		}
	}
	
	private class QPartition {
		public List<QBlock> blocks;
		
		public QPartition() {
			this.blocks = new ArrayList<QBlock>();
		}
		
		public String toString() {
			return "qpart " + blocks;
		}
	}
	
	private class QBlock {
		public LinkedList<Element> elements;
		public XBlock xblock;
		public QBlock assoc;
		public String name;
		
		public QBlock() {
			this.elements = new LinkedList<Element>();
			this.name = "q" +qId++;
		}
		
		public String toString() {
			return name + elements;
		}
	}
	
	private class XPartition {
		public List<XBlock> blocks;
		
		public XPartition() {
			this.blocks = new ArrayList<XBlock>();
		}
		
		public String toString() {
			return "xpart " + blocks;
		}
	}
	
	private class XBlock {
		public LinkedList<QBlock> qblocks;
		public String name;
		
		public XBlock() {
			this.qblocks = new LinkedList<QBlock>();
			this.name = "x" + xId++;
		}
		
		public boolean isCompound() {
			return qblocks.size() > 1;
		}
		
		public String toString() {
			return name + "(" + qblocks.toString().replaceAll("(^\\[|\\]$)", "") + ")";
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((name == null) ? 0 : name.hashCode());
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
			XBlock other = (XBlock)obj;
			if (name == null) {
				if (other.name != null)
					return false;
			} else if (!name.equals(other.name))
				return false;
			return true;
		}
	}
	
	private class CompoundSet {
		private List<XBlock> blocks;
		
		public CompoundSet() {
			this.blocks = new ArrayList<XBlock>();
		}
		
		public void add(XBlock block) {
			if (!blocks.contains(block))
				blocks.add(block);
		}
		
		public String toString() {
			return blocks.toString();
		}
	}

	private static final Logger log = Logger.getLogger(BiSim.class);
	
	private void refine(LinkedList<Element> preimage, QPartition qp, CompoundSet cs) {
		List<QBlock> splitBlocks = new ArrayList<QBlock>();
		for (Element x : preimage) {
			QBlock d = x.qblock;
			if (d.assoc == null) {
				d.assoc = new QBlock();
				d.assoc.elements.add(x);
				d.assoc.xblock = d.xblock;
				d.assoc.xblock.qblocks.add(d.assoc);
				x.qblock = d.assoc;
				d.elements.remove(x);
				log.debug("splitting " + d.name + ": d: " + d + ", assoc: " + d.assoc);
			}
			splitBlocks.add(d);
		}
		
		log.debug("split blocks: " + splitBlocks);
		
		for (QBlock d : splitBlocks) {
			QBlock d_ = d.assoc;
			d.assoc = null;
			
			if (d.elements.size() == 0) {
				d.xblock.qblocks.remove(d);
				qp.blocks.remove(d);
				log.debug("removing " + d.name);
			}
			
			if (d_.xblock.isCompound()) {
				cs.add(d_.xblock);
				log.debug("adding " + d_.xblock + " to compounds");
			}
		}
		
		log.debug("cs: " + cs);
	}
	
	public void test() {
		XPartition xp = new XPartition();
		QPartition qp = new QPartition();
		CompoundSet cs = new CompoundSet();
		
		Element ea = new Element("a");
		Element eb = new Element("b");
		Element ec = new Element("c");
		Element ex = new Element("x");
		Element ee = new Element("e");
		
		List<Element> elements = new ArrayList<Element>();
		elements.add(ea);
		elements.add(ex);
		elements.add(ec);
		elements.add(eb);
		
		List<Edge> edges = new ArrayList<Edge>();
		edges.add(new Edge(ea, ex));
		edges.add(new Edge(ex, eb));
		edges.add(new Edge(ex, ec));
//		edges.add(new Edge(ed, ec));
		
		for (Edge e : edges) {
			e.y.incident.add(e);
			e.x.count++;
		}
		
		for (Edge e : edges) {
			e.count = e.x.count;
		}
		
//		for (Element e : elements) {
//			QBlock qb = new QBlock();
//			qb.elements.add(e);
//			e.qblock = qb;
//			qp.blocks.add(qb);
//		}
		QBlock q1 = new QBlock();
		q1.elements.add(ea);
		q1.elements.add(ex);
		ea.qblock = q1;
		ex.qblock = q1;
		qp.blocks.add(q1);
		
//		QBlock q2 = new QBlock();
//		q2.elements.add(ex);
//		ex.qblock = q2;
//		qp.blocks.add(q2);
		
//		QBlock q3 = new QBlock();
//		q3.elements.add(ec);
//		ec.qblock = q3;
//		qp.blocks.add(q3);
		
		XBlock ublock = new XBlock();
		for (QBlock qblock : qp.blocks) {
			ublock.qblocks.add(qblock);
			qblock.xblock = ublock;
		}
		xp.blocks.add(ublock);
		
		cs.add(ublock);
		
		log.debug(xp);
		log.debug(qp);
		
		while (cs.blocks.size() > 0) {
			// Step 1
			XBlock s = cs.blocks.remove(0);
			
			log.debug("s: " + s);
			
			int smaller;
			
			if (s.qblocks.get(0).elements.size() > s.qblocks.get(1).elements.size())
				smaller = 1;
			else
				smaller = 0;
				
			// Step 2
			QBlock b = s.qblocks.remove(smaller);
			
			log.debug("b: " + b);
			
			XBlock _s = new XBlock();
			_s.qblocks.add(b);
			xp.blocks.add(_s);
			
			if (s.isCompound()) {
				cs.add(s);
				log.debug("adding s " + s + " back to cs");
			}
			
			// Step 3
			LinkedList<Element> _b = new LinkedList<Element>(b.elements);
			
			LinkedList<Element> preimageB = new LinkedList<Element>();
			Set<Element> added = new HashSet<Element>();
			for (Element y : b.elements) {
				for (Edge e : y.incident) {
					if (!added.contains(e.x)) {
						preimageB.add(e.x);
						added.add(e.x);
						e.x.count = 0;
					}
					e.x.count++;
				}
			}
			
			log.debug("preimage of " + b + ": " + preimageB);
			
			// Step 4
			refine(preimageB, qp, cs);
			
			// Step 5
			LinkedList<Element> preimageBSB = new LinkedList<Element>();
			added.clear();
			for (Element y : _b) {
				for (Edge e : y.incident) {
					log.debug(e);
					if (e.x.count == e.count) {
						if (!added.contains(e.x)) {
							preimageBSB.add(e.x);
							added.add(e.x);
						}
					}
				}
			}
			
			log.debug("preimageBSB: " + preimageBSB);
			
			// Step 6
			refine(preimageBSB, qp, cs);
			
			// Step 7
			for (Element y : _b) {
				for (Edge e : y.incident) {
					e.count--;
					if (e.count == 0)
						e.count = e.x.count;
				}
			}
			
			log.debug(xp);
			log.debug(qp);
		}
		
	}
	
	public static void main(String[] args) {
		BiSim bs = new BiSim();
		bs.test();
	}
}

/**
 * 
 */
package edu.unika.aifb.graphindex.algorithm.rcp;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.unika.aifb.graphindex.data.IVertex;

public class Block implements Iterable<IVertex> {
		public IVertex m_head;
		private IVertex m_tail;
//		private Block m_next, m_prev;
		private int m_size;
		private XBlock m_parent;
		private String m_name = "b" + ++RCPFast.block_id;
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
		
		public boolean stable(List<IVertex> vertices) {
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
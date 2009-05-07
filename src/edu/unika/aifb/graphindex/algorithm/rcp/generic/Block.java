/**
 * 
 */
package edu.unika.aifb.graphindex.algorithm.rcp.generic;

import java.util.Iterator;

public class Block<V,E> implements Iterable<GVertex<V,E>> {
		public GVertex<V,E> m_head;
		private GVertex<V,E> m_tail;
		private int m_size;
		private XBlock<V,E> m_parent;
		private static int m_blockId = 0;
		private String m_name = "g" + ++m_blockId;
		private Block<V,E> m_splitBlock;
		
		public Block() {
			m_head = null;
			m_tail = null;
		}
		
		public Block<V,E> getSplitBlock() {
			return m_splitBlock;
		}
		
		public void setSplitBlock(Block<V,E> splitBlock) {
			m_splitBlock = splitBlock;
		}
		
		public XBlock<V,E> getXBlock() {
			return m_parent;
		}
		
		public void setXBlock(XBlock<V,E> xblock) {
			m_parent = xblock;
		}
		
		public void add(GVertex<V,E> v) {
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
		
		public void remove(GVertex<V,E> v) {
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
		
//		public Map<Long,Set<GVertex<V,E>>> image() {
//			Map<Long,Set<GVertex<V,E>>> image = new HashMap<Long,Set<GVertex<V,E>>>();
//			
//			for (GVertex<V,E> v : this) {
//				for (long label : v.getEdgeLabels()) {
//					if (!image.containsKey(label))
//						image.put(label, new HashSet<GVertex<V,E>>());
//					image.get(label).addAll(v.getImage(label));
//				}
//			}
//			
//			return image;
//		}
		
//		public boolean stable(List<GVertex<V,E>> vertices) {
//			Map<Long,Set<GVertex<V,E>>> bimage = new HashMap<Long,Set<GVertex<V,E>>>();
//			
//			for (GVertex<V,E> v : vertices) {
//				for (long label : v.getEdgeLabels()) {
//					if (!bimage.containsKey(label))
//						bimage.put(label, new HashSet<GVertex<V,E>>());
//					bimage.get(label).addAll(v.getImage(label));
//				}
//			}
////			log.debug(this.size() + " ownImage: " + image.size() + ", b: " + b.size());
//			
//			for (long label : bimage.keySet()) {
//				boolean foundOneNotInImage = false;
//				boolean foundOneInImage = false;
//				for (GVertex<V,E> v : this) {
//					if (bimage.get(label).contains(v)) {
//						if (foundOneNotInImage)
//							return false;
//						foundOneInImage = true;
//					}
//					else {
//						if (foundOneInImage)
//							return false;
//						foundOneNotInImage = true;
//					}
//				}
//			}
//			
//			return true;
//		}
		
//		public boolean stable(Block b) {
//			Map<Long,Set<GVertex<V,E>>> bimage = b.image();
//			
//			for (long label : bimage.keySet()) {
//				boolean foundOneNotInImage = false;
//				boolean foundOneInImage = false;
//				for (GVertex<V,E> v : this) {
//					if (bimage.get(label).contains(v)) {
//						if (foundOneNotInImage)
//							return false;
//						foundOneInImage = true;
//					}
//					else {
//						if (foundOneInImage)
//							return false;
//						foundOneNotInImage = true;
//					}
//				}
//			}
//			
//			return true;
// 		}
		
		public String toString() {
			String s = m_name + "[";
			String comma = "";
			if (m_head != null) {
				GVertex<V,E> cur = m_head;
				do {
					s += comma + cur;
					comma = ",";
					cur = cur.getNext();
				}
				while (cur != null);
			}
			return s + "]";
		}
		
		public Iterator<GVertex<V,E>> iterator() {
			return new Iterator<GVertex<V,E>> () {
				private GVertex<V,E> cur = m_head;
				public boolean hasNext() {
					return cur != null;
				}

				public GVertex<V,E> next() {
					GVertex<V,E> ret = cur;
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
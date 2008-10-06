package edu.unika.aifb.graphindex.data;

import edu.unika.aifb.graphindex.algorithm.rcp.Block;

public abstract class AbstractVertex implements IVertex {
	protected long m_id;
	protected IVertex m_next, m_prev;
	protected Block m_block;
	protected int m_movedIn = -1;

	public AbstractVertex(long id) {
		m_id = id;
	}

	public int getMovedIn() {
		return m_movedIn;
	}

	public void setMovedIn(int in) {
		m_movedIn = in;
	}

	public long getId() {
		return m_id;
	}
	
	public IVertex getNext() {
		return m_next;
//		return null;
	}
	
	public IVertex getPrev() {
		return m_prev;
//		return null;
	}
	
	public void setNext(IVertex next) {
		m_next = next;
	}
	
	public void setPrev(IVertex prev) {
		m_prev = prev;
	}
	
	public Block getBlock() {
		return m_block;
	}
	
	public void setBlock(Block b) {
		m_block = b;
	}
	
	@Override
	public String toString() {
		return "" + m_id;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int)(m_id ^ (m_id >>> 32));
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
		IVertex other = (IVertex)obj;
		if (m_id != other.getId())
			return false;
		return true;
	}
}

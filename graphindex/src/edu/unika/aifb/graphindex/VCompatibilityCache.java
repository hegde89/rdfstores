package edu.unika.aifb.graphindex;

import java.util.HashMap;
import java.util.Map;

public class VCompatibilityCache {
	private class Pair {
		public int n1, n2;

		public Pair(int n1, int n2) {
			this.n1 = n1;
			this.n2 = n2;
		}

		@Override
		public int hashCode() {
			return n1 * 31 + n2;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Pair other = (Pair)obj;
			if (n1 != other.n1)
				return false;
			if (n2 != other.n2)
				return false;
			return true;
		}
	}
	private Map<Pair,Boolean> m_cache;
	
	public VCompatibilityCache() {
		m_cache = new HashMap<Pair,Boolean>();
	}
	
	private Pair getPair(int n1, int n2) {
		return new Pair(n1, n2);
	}
	
	public void put(int n1, int n2, boolean value) {
		m_cache.put(getPair(n1, n2), value);
	}
	
	public Boolean get(int n1, int n2) {
		return m_cache.get(getPair(n1, n2));
	}

	public void clear() {
		m_cache = new HashMap<Pair,Boolean>();
	}

	public int size() {
		return m_cache.size();
	}
}

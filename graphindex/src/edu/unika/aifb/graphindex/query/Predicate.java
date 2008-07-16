package edu.unika.aifb.graphindex.query;

public class Predicate {
	private String m_uri;

	public Predicate(String uri) {
		m_uri = uri;
	}
	
	public String getUri() {
		return m_uri;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_uri == null) ? 0 : m_uri.hashCode());
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
		final Predicate other = (Predicate) obj;
		if (m_uri == null) {
			if (other.m_uri != null)
				return false;
		} else if (!m_uri.equals(other.m_uri))
			return false;
		return true;
	}
}

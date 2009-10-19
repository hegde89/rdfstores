package edu.unika.aifb.graphindex.searcher.keyword.model;

import edu.unika.aifb.graphindex.model.impl.Entity;

public class SQueryKeywordElement extends KeywordElement {
	private static final long serialVersionUID = 4476266011103956645L;

	private String m_attachNode;
	
	public SQueryKeywordElement(Entity entity, int type, double score, String attachNode) {
		super(entity, type, score, null, null);
		m_attachNode = attachNode;
	}

	public String getAttachNode() {
		return m_attachNode;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result
				+ ((m_attachNode == null) ? 0 : m_attachNode.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		SQueryKeywordElement other = (SQueryKeywordElement)obj;
		if (m_attachNode == null) {
			if (other.m_attachNode != null)
				return false;
		} else if (!m_attachNode.equals(other.m_attachNode))
			return false;
		return true;
	}

}

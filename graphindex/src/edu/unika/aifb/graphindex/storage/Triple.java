package edu.unika.aifb.graphindex.storage;

public class Triple {
	private String m_subject, m_property, m_object;

	public Triple(String subject, String property, String object) {
		super();
		m_object = object;
		m_property = property;
		m_subject = subject;
	}

	public String getSubject() {
		return m_subject;
	}

	public void setSubject(String m_subject) {
		this.m_subject = m_subject;
	}

	public String getProperty() {
		return m_property;
	}

	public void setProperty(String m_property) {
		this.m_property = m_property;
	}

	public String getObject() {
		return m_object;
	}

	public void setObject(String m_object) {
		this.m_object = m_object;
	}
	
	public String toString() {
		return "(" + m_subject + "," + m_property + "," + m_object +")";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((m_object == null) ? 0 : m_object.hashCode());
		result = prime * result
				+ ((m_property == null) ? 0 : m_property.hashCode());
		result = prime * result
				+ ((m_subject == null) ? 0 : m_subject.hashCode());
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
		Triple other = (Triple)obj;
		if (m_object == null) {
			if (other.m_object != null)
				return false;
		} else if (!m_object.equals(other.m_object))
			return false;
		if (m_property == null) {
			if (other.m_property != null)
				return false;
		} else if (!m_property.equals(other.m_property))
			return false;
		if (m_subject == null) {
			if (other.m_subject != null)
				return false;
		} else if (!m_subject.equals(other.m_subject))
			return false;
		return true;
	}
}

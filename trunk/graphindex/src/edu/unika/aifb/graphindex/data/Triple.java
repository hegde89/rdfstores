package edu.unika.aifb.graphindex.data;

import java.util.ArrayList;
import java.util.List;

/**
 * @author gl
 *
 */
public class Triple {
	private String m_subject, m_property, m_object;
	private String m_subjectExtension;
	
	/**
	 * Triples whose subjects equal the object of this triple.
	 */
	private List<Triple> m_next;
	
	
	/**
	 * Triples whose objects equal the subject of this triple. 
	 */
	private List<Triple> m_prev;

	public Triple(String subject, String property, String object) {
		super();
		m_object = object;
		m_property = property;
		m_subject = subject;
		m_next = new ArrayList<Triple>();
		m_prev = new ArrayList<Triple>();
	}
	
	public Triple(String subject, String property, String object, String subjectExtension) {
		this(subject, property, object);
		m_subjectExtension = subjectExtension;
	}
	
	/**
	 * Returns previously added triples where the subject equals the object of this triple.
	 * 
	 * @return triples whose subjects equal the object of this triple
	 */
	public List<Triple> getNext() {
		return m_next;
	}
	
	/**
	 * Returns previously added triples where the object equals the subject of this triple.
	 * 
	 * @return triples whose objects equal the subject of this triple
	 */
	public List<Triple> getPrev() {
		return m_prev;
	}
	
	public void addNext(Triple t) {
		// TODO check condition
		m_next.add(t);
	}
	
	public void addPrev(Triple t) {
		// TODO check condition
		m_prev.add(t);
	}

	public String getSubject() {
		return m_subject;
	}

	public void setSubject(String m_subject) {
		this.m_subject = m_subject;
	}
	
	public String getSubjectExtension() {
		return m_subjectExtension;
	}
	
	public void setSubjectExtension(String ext) {
		m_subjectExtension = ext;
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

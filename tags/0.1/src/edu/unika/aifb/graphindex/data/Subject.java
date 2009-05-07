package edu.unika.aifb.graphindex.data;

public class Subject {
	private String m_subject;
	private String m_extension;
	
	public Subject(String subject, String ext) {
		m_subject = subject;
		m_extension = ext;
	}
	
	public String getSubject() {
		return m_subject;
	}
	
	public String getExtension() {
		return m_extension;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((m_extension == null) ? 0 : m_extension.hashCode());
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
		Subject other = (Subject)obj;
		if (m_extension == null) {
			if (other.m_extension != null)
				return false;
		} else if (!m_extension.equals(other.m_extension))
			return false;
		if (m_subject == null) {
			if (other.m_subject != null)
				return false;
		} else if (!m_subject.equals(other.m_subject))
			return false;
		return true;
	}

	public String toDataString() {
		return m_subject;// + "\t" + m_extension;
 	}
}

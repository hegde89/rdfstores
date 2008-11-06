package edu.unika.aifb.graphindex.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class AbstractExtensionSegment implements ExtensionSegment {
	protected String m_ext;
	protected String m_object;
	protected String m_property;
	
	public AbstractExtensionSegment(String extUri, String property, String object) {
		m_ext = extUri;
		m_object = object;
		m_property = property;
	}

	public String getExtensionUri() {
		return m_ext;
	}
	
	public String getObject() {
		return m_object;
	}

	public String getProperty() {
		return m_property;
	}
	
	public String toSubjectString() {
		StringBuilder sb = new StringBuilder();
		for (Subject subject : getSubjects())
			sb.append(subject.toDataString()).append("\n");
		return sb.toString();
	}

	public List<Triple> toTriples() {
		List<Triple> triples = new ArrayList<Triple>();
		for (Subject subject : getSubjects()) {
			triples.add(new Triple(subject.getSubject(), getProperty(), getObject(), subject.getExtension()));
		}
		return triples;
	}
}

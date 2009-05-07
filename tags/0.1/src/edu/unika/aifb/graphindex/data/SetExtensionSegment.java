package edu.unika.aifb.graphindex.data;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;


public class SetExtensionSegment extends AbstractExtensionSegment {
	private Set<Subject> m_subjects;
	
	public SetExtensionSegment(String ext, String property, String object) {
		super(ext, property, object);
		m_subjects = new HashSet<Subject>();
	}
	
	public void addSubject(Subject subject) {
		m_subjects.add(subject);
	}

	public Collection<Subject> getSubjects() {
		return m_subjects;
	}

	public int size() {
		return m_subjects.size();
	}

	public void setSubjects(Set<Subject> subjects) {
		m_subjects = subjects;
	}

}

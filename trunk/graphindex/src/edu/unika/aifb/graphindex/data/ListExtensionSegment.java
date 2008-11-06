package edu.unika.aifb.graphindex.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ListExtensionSegment extends AbstractExtensionSegment {
	private List<Subject> m_subjects;
	
	public ListExtensionSegment(String ext, String property, String object) {
		super(ext, property, object);
		m_subjects = new ArrayList<Subject>();
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

	public void setSubjects(List<Subject> subjects) {
		m_subjects = subjects;
	}

}

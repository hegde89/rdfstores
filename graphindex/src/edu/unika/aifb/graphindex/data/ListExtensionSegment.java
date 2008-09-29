package edu.unika.aifb.graphindex.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ListExtensionSegment extends AbstractExtensionSegment {
	private List<String> m_subjects;
	
	public ListExtensionSegment(String ext, String property, String object) {
		super(ext, property, object);
		m_subjects = new ArrayList<String>();
	}
	
	public void addSubject(String subject) {
		m_subjects.add(subject);
	}

	public Collection<String> getSubjects() {
		return m_subjects;
	}

	public int size() {
		return m_subjects.size();
	}

	public void setSubjects(List<String> subjects) {
		m_subjects = subjects;
	}

}

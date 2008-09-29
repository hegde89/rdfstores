package edu.unika.aifb.graphindex.data;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface ExtensionSegment {
	public String getExtensionUri();
	public String getObject();
	public String getProperty();
	public Collection<String> getSubjects();

	public void addSubject(String subject);
	
	public int size();
	
	public List<Triple> toTriples();
	public String toSubjectString();
}
